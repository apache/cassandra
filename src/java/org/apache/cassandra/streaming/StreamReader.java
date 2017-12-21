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
package org.apache.cassandra.streaming;

import java.io.*;
import java.util.Collection;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.UnmodifiableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.format.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.streaming.compress.StreamCompressionInputStream;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * StreamReader reads from stream and writes to SSTable.
 */
public class StreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);
    protected final TableId tableId;
    protected final long estimatedKeys;
    protected final Collection<Pair<Long, Long>> sections;
    protected final StreamSession session;
    protected final Version inputVersion;
    protected final long repairedAt;
    protected final UUID pendingRepair;
    protected final SSTableFormat.Type format;
    protected final int sstableLevel;
    protected final SerializationHeader.Component header;
    protected final int fileSeqNum;

    public StreamReader(FileMessageHeader header, StreamSession session)
    {
        if (session.getPendingRepair() != null)
        {
            // we should only ever be streaming pending repair
            // sstables if the session has a pending repair id
            assert session.getPendingRepair().equals(header.pendingRepair);
        }
        this.session = session;
        this.tableId = header.tableId;
        this.estimatedKeys = header.estimatedKeys;
        this.sections = header.sections;
        this.inputVersion = header.version;
        this.repairedAt = header.repairedAt;
        this.pendingRepair = header.pendingRepair;
        this.format = header.format;
        this.sstableLevel = header.sstableLevel;
        this.header = header.header;
        this.fileSeqNum = header.sequenceNumber;
    }

    /**
     * @param inputPlus where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    @SuppressWarnings("resource") // input needs to remain open, streams on top of it can't be closed
    public SSTableMultiWriter read(DataInputPlus inputPlus) throws IOException
    {
        long totalSize = totalSize();

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        if (cfs == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + tableId + " was dropped during streaming");
        }

        logger.debug("[Stream #{}] Start receiving file #{} from {}, repairedAt = {}, size = {}, ks = '{}', table = '{}', pendingRepair = '{}'.",
                     session.planId(), fileSeqNum, session.peer, repairedAt, totalSize, cfs.keyspace.getName(),
                     cfs.getTableName(), pendingRepair);

        StreamDeserializer deserializer = null;
        SSTableMultiWriter writer = null;
        try (StreamCompressionInputStream streamCompressionInputStream = new StreamCompressionInputStream(inputPlus, StreamMessage.CURRENT_VERSION))
        {
            TrackedDataInputPlus in = new TrackedDataInputPlus(streamCompressionInputStream);
            deserializer = new StreamDeserializer(cfs.metadata(), in, inputVersion, getHeader(cfs.metadata()));
            writer = createWriter(cfs, totalSize, repairedAt, pendingRepair, format);
            while (in.getBytesRead() < totalSize)
            {
                writePartition(deserializer, writer);
                // TODO move this to BytesReadTracker
                session.progress(writer.getFilename(), ProgressInfo.Direction.IN, in.getBytesRead(), totalSize);
            }
            logger.debug("[Stream #{}] Finished receiving file #{} from {} readBytes = {}, totalSize = {}",
                         session.planId(), fileSeqNum, session.peer, FBUtilities.prettyPrintMemory(in.getBytesRead()), FBUtilities.prettyPrintMemory(totalSize));
            return writer;
        }
        catch (Throwable e)
        {
            Object partitionKey = deserializer != null ? deserializer.partitionKey() : "";
            logger.warn("[Stream {}] Error while reading partition {} from stream on ks='{}' and table='{}'.",
                        session.planId(), partitionKey, cfs.keyspace.getName(), cfs.getTableName(), e);
            if (writer != null)
            {
                writer.abort(e);
            }
            throw Throwables.propagate(e);
        }
    }

    protected SerializationHeader getHeader(TableMetadata metadata)
    {
        return header != null? header.toHeader(metadata) : null; //pre-3.0 sstable have no SerializationHeader
    }

    protected SSTableMultiWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt, UUID pendingRepair, SSTableFormat.Type format) throws IOException
    {
        Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
        if (localDir == null)
            throw new IOException(String.format("Insufficient disk space to store %s", FBUtilities.prettyPrintMemory(totalSize)));

        RangeAwareSSTableWriter writer = new RangeAwareSSTableWriter(cfs, estimatedKeys, repairedAt, pendingRepair, format, sstableLevel, totalSize, session.getTransaction(tableId), getHeader(cfs.metadata()));
        StreamHook.instance.reportIncomingFile(cfs, writer, session, fileSeqNum);
        return writer;
    }

    protected long totalSize()
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
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
        private final SerializationHelper helper;

        private DecoratedKey key;
        private DeletionTime partitionLevelDeletion;
        private SSTableSimpleIterator iterator;
        private Row staticRow;
        private IOException exception;

        public StreamDeserializer(TableMetadata metadata, DataInputPlus in, Version version, SerializationHeader header) throws IOException
        {
            this.metadata = metadata;
            this.in = in;
            this.helper = new SerializationHelper(metadata, version.correspondingMessagingVersion(), SerializationHelper.Flag.PRESERVE_SIZE);
            this.header = header;
        }

        public StreamDeserializer newPartition() throws IOException
        {
            key = metadata.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
            partitionLevelDeletion = DeletionTime.serializer.deserialize(in);
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
