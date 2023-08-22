/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.streaming;

import java.io.IOError;
import java.io.IOException;
import java.util.Collection;

import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.io.sstable.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamReceiver;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.compress.StreamCompressionInputStream;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.net.MessagingService.current_version;

/**
 * CassandraStreamReader reads from stream and writes to SSTable.
 */
public class CassandraStreamReader implements IStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraStreamReader.class);
    protected final TableId tableId;
    protected final long estimatedKeys;
    protected final Collection<SSTableReader.PartitionPositionBounds> sections;
    protected final StreamSession session;
    protected final Version inputVersion;
    protected final long repairedAt;
    protected final TimeUUID pendingRepair;
    protected final int sstableLevel;
    protected final SerializationHeader.Component header;
    protected final int fileSeqNum;

    public CassandraStreamReader(StreamMessageHeader header, CassandraStreamHeader streamHeader, StreamSession session)
    {
        if (session.getPendingRepair() != null)
        {
            // we should only ever be streaming pending repair
            // sstables if the session has a pending repair id
            assert session.getPendingRepair().equals(header.pendingRepair);
        }
        this.session = session;
        this.tableId = header.tableId;
        this.estimatedKeys = streamHeader.estimatedKeys;
        this.sections = streamHeader.sections;
        this.inputVersion = streamHeader.version;
        this.repairedAt = header.repairedAt;
        this.pendingRepair = header.pendingRepair;
        this.sstableLevel = streamHeader.sstableLevel;
        this.header = streamHeader.serializationHeader;
        this.fileSeqNum = header.sequenceNumber;
    }

    /**
     * @param inputPlus where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    @Override
    public SSTableMultiWriter read(DataInputPlus inputPlus) throws Throwable
    {
        long totalSize = totalSize();

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        if (cfs == null)
            // schema was dropped during streaming
            throw new IllegalStateException("Table " + tableId + " was dropped during streaming");

        logger.debug("[Stream #{}] Start receiving file #{} from {}, repairedAt = {}, size = {}, ks = '{}', table = '{}', pendingRepair = '{}'.",
                     session.planId(), fileSeqNum, session.peer, repairedAt, totalSize, cfs.getKeyspaceName(),
                     cfs.getTableName(), pendingRepair);

        StreamDeserializer deserializer = null;
        SSTableMultiWriter writer = null;
        try (StreamCompressionInputStream streamCompressionInputStream = new StreamCompressionInputStream(inputPlus, current_version))
        {
            TrackedDataInputPlus in = new TrackedDataInputPlus(streamCompressionInputStream);
            deserializer = new StreamDeserializer(cfs.metadata(), in, inputVersion, getHeader(cfs.metadata()));
            writer = createWriter(cfs, totalSize, repairedAt, pendingRepair, inputVersion.format);
            String sequenceName = writer.getFilename() + '-' + fileSeqNum;
            long lastBytesRead = 0;
            while (in.getBytesRead() < totalSize)
            {
                writePartition(deserializer, writer);
                // TODO move this to BytesReadTracker
                long bytesRead = in.getBytesRead();
                long bytesDelta = bytesRead - lastBytesRead;
                lastBytesRead = bytesRead;
                session.progress(sequenceName, ProgressInfo.Direction.IN, bytesRead, bytesDelta, totalSize);
            }
            logger.debug("[Stream #{}] Finished receiving file #{} from {} readBytes = {}, totalSize = {}",
                         session.planId(), fileSeqNum, session.peer, FBUtilities.prettyPrintMemory(in.getBytesRead()), FBUtilities.prettyPrintMemory(totalSize));
            return writer;
        }
        catch (Throwable e)
        {
            Object partitionKey = deserializer != null ? deserializer.partitionKey() : "";
            logger.warn("[Stream {}] Error while reading partition {} from stream on ks='{}' and table='{}'.",
                        session.planId(), partitionKey, cfs.getKeyspaceName(), cfs.getTableName(), e);
            if (writer != null)
                e = writer.abort(e);
            throw e;
        }
    }

    protected SerializationHeader getHeader(TableMetadata metadata) throws UnknownColumnException
    {
        return header != null? header.toHeader(metadata) : null; //pre-3.0 sstable have no SerializationHeader
    }
    protected SSTableMultiWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt, TimeUUID pendingRepair, SSTableFormat<?, ?> format) throws IOException
    {
        Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
        if (localDir == null)
            throw new IOException(String.format("Insufficient disk space to store %s", FBUtilities.prettyPrintMemory(totalSize)));

        StreamReceiver streamReceiver = session.getAggregator(tableId);
        Preconditions.checkState(streamReceiver instanceof CassandraStreamReceiver);
        LifecycleNewTracker lifecycleNewTracker = CassandraStreamReceiver.fromReceiver(session.getAggregator(tableId)).createLifecycleNewTracker();

        RangeAwareSSTableWriter writer = new RangeAwareSSTableWriter(cfs, estimatedKeys, repairedAt, pendingRepair, false, format, sstableLevel, totalSize, lifecycleNewTracker, getHeader(cfs.metadata()));
        return writer;
    }

    protected long totalSize()
    {
        long size = 0;
        for (SSTableReader.PartitionPositionBounds section : sections)
            size += section.upperPosition - section.lowerPosition;
        return size;
    }

    protected void writePartition(StreamDeserializer deserializer, SSTableMultiWriter writer) throws IOException
    {
        writer.append(deserializer.newPartition());
        deserializer.checkForExceptions();
    }

    public static class StreamDeserializer extends UnmodifiableIterator<Unfiltered> implements UnfilteredRowIterator
    {
        private final TableMetadata metadata;
        private final DataInputPlus in;
        private final SerializationHeader header;
        private final DeserializationHelper helper;

        private DecoratedKey key;
        private DeletionTime partitionLevelDeletion;
        private SSTableSimpleIterator iterator;
        private Row staticRow;
        private IOException exception;
        private Version version;

        public StreamDeserializer(TableMetadata metadata, DataInputPlus in, Version version, SerializationHeader header) throws IOException
        {
            this.metadata = metadata;
            this.in = in;
            this.helper = new DeserializationHelper(metadata, version.correspondingMessagingVersion(), DeserializationHelper.Flag.PRESERVE_SIZE);
            this.header = header;
            this.version = version;
        }

        public StreamDeserializer newPartition() throws IOException
        {
            key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
            partitionLevelDeletion = DeletionTime.getSerializer(version).deserialize(in);
            iterator = SSTableSimpleIterator.create(metadata, in, header, helper, partitionLevelDeletion);
            staticRow = iterator.readStaticRow();
            return this;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public RegularAndStaticColumns columns()
        {
            // We don't know which columns we'll get so assume it can be all of them
            return metadata.regularAndStaticColumns();
        }

        public boolean isReverseOrder()
        {
            return false;
        }

        public DecoratedKey partitionKey()
        {
            return key;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public Row staticRow()
        {
            return staticRow;
        }

        public EncodingStats stats()
        {
            return header.stats();
        }

        public boolean hasNext()
        {
            try
            {
                return iterator.hasNext();
            }
            catch (IOError e)
            {
                if (e.getCause() != null && e.getCause() instanceof IOException)
                {
                    exception = (IOException)e.getCause();
                    return false;
                }
                throw e;
            }
        }

        public Unfiltered next()
        {
            // Note that in practice we know that IOException will be thrown by hasNext(), because that's
            // where the actual reading happens, so we don't bother catching RuntimeException here (contrarily
            // to what we do in hasNext)
            Unfiltered unfiltered = iterator.next();
            return metadata.isCounter() && unfiltered.kind() == Unfiltered.Kind.ROW
                   ? maybeMarkLocalToBeCleared((Row) unfiltered)
                   : unfiltered;
        }

        private Row maybeMarkLocalToBeCleared(Row row)
        {
            return metadata.isCounter() ? row.markCounterLocalToBeCleared() : row;
        }

        public void checkForExceptions() throws IOException
        {
            if (exception != null)
                throw exception;
        }

        public void close()
        {
        }
    }
}
