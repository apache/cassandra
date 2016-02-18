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
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.UUID;

import com.google.common.base.Throwables;
import com.google.common.collect.UnmodifiableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ning.compress.lzf.LZFInputStream;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SSTableSimpleIterator;
import org.apache.cassandra.io.sstable.format.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.RewindableDataInputStreamPlus;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.io.util.TrackedInputStream;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;


/**
 * StreamReader reads from stream and writes to SSTable.
 */
public class StreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReader.class);
    protected final UUID cfId;
    protected final long estimatedKeys;
    protected final Collection<Pair<Long, Long>> sections;
    protected final StreamSession session;
    protected final Version inputVersion;
    protected final long repairedAt;
    protected final SSTableFormat.Type format;
    protected final int sstableLevel;
    protected final SerializationHeader.Component header;
    protected final int fileSeqNum;

    public StreamReader(FileMessageHeader header, StreamSession session)
    {
        this.session = session;
        this.cfId = header.cfId;
        this.estimatedKeys = header.estimatedKeys;
        this.sections = header.sections;
        this.inputVersion = header.version;
        this.repairedAt = header.repairedAt;
        this.format = header.format;
        this.sstableLevel = header.sstableLevel;
        this.header = header.header;
        this.fileSeqNum = header.sequenceNumber;
    }

    /**
     * @param channel where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    @SuppressWarnings("resource") // channel needs to remain open, streams on top of it can't be closed
    public SSTableMultiWriter read(ReadableByteChannel channel) throws IOException
    {
        long totalSize = totalSize();

        Pair<String, String> kscf = Schema.instance.getCF(cfId);
        ColumnFamilyStore cfs = null;
        if (kscf != null)
            cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);

        if (kscf == null || cfs == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + cfId + " was dropped during streaming");
        }

        logger.debug("[Stream #{}] Start receiving file #{} from {}, repairedAt = {}, size = {}, ks = '{}', table = '{}'.",
                     session.planId(), fileSeqNum, session.peer, repairedAt, totalSize, cfs.keyspace.getName(),
                     cfs.getColumnFamilyName());

        TrackedInputStream in = new TrackedInputStream(new LZFInputStream(Channels.newInputStream(channel)));
        StreamDeserializer deserializer = new StreamDeserializer(cfs.metadata, in, inputVersion, getHeader(cfs.metadata),
                                                                 totalSize, session.planId());
        SSTableMultiWriter writer = null;
        try
        {
            writer = createWriter(cfs, totalSize, repairedAt, format);
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
            if (deserializer != null)
                logger.warn("[Stream {}] Error while reading partition {} from stream on ks='{}' and table='{}'.",
                            session.planId(), deserializer.partitionKey(), cfs.keyspace.getName(), cfs.getTableName());
            if (writer != null)
            {
                writer.abort(e);
            }
            drain(in, in.getBytesRead());
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw Throwables.propagate(e);
        }
        finally
        {
            if (deserializer != null)
                deserializer.cleanup();
        }
    }

    protected SerializationHeader getHeader(CFMetaData metadata)
    {
        return header != null? header.toHeader(metadata) : null; //pre-3.0 sstable have no SerializationHeader
    }

    protected SSTableMultiWriter createWriter(ColumnFamilyStore cfs, long totalSize, long repairedAt, SSTableFormat.Type format) throws IOException
    {
        Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
        if (localDir == null)
            throw new IOException(String.format("Insufficient disk space to store %s", FBUtilities.prettyPrintMemory(totalSize)));

        RangeAwareSSTableWriter writer = new RangeAwareSSTableWriter(cfs, estimatedKeys, repairedAt, format, sstableLevel, totalSize, session.getTransaction(cfId), getHeader(cfs.metadata));
        StreamHook.instance.reportIncomingFile(cfs, writer, session, fileSeqNum);
        return writer;
    }

    protected void drain(InputStream dis, long bytesRead) throws IOException
    {
        long toSkip = totalSize() - bytesRead;

        // InputStream.skip can return -1 if dis is inaccessible.
        long skipped = dis.skip(toSkip);
        if (skipped == -1)
            return;

        toSkip = toSkip - skipped;
        while (toSkip > 0)
        {
            skipped = dis.skip(toSkip);
            if (skipped == -1)
                break;
            toSkip = toSkip - skipped;
        }
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
        public static final int INITIAL_MEM_BUFFER_SIZE = Integer.getInteger("cassandra.streamdes.initial_mem_buffer_size", 32768);
        public static final int MAX_MEM_BUFFER_SIZE = Integer.getInteger("cassandra.streamdes.max_mem_buffer_size", 1048576);
        public static final int MAX_SPILL_FILE_SIZE = Integer.getInteger("cassandra.streamdes.max_spill_file_size", Integer.MAX_VALUE);

        public static final String BUFFER_FILE_PREFIX = "buf";
        public static final String BUFFER_FILE_SUFFIX = "dat";

        private final CFMetaData metadata;
        private final DataInputPlus in;
        private final SerializationHeader header;
        private final SerializationHelper helper;

        private DecoratedKey key;
        private DeletionTime partitionLevelDeletion;
        private SSTableSimpleIterator iterator;
        private Row staticRow;
        private IOException exception;

        public StreamDeserializer(CFMetaData metadata, InputStream in, Version version, SerializationHeader header,
                                  long totalSize, UUID sessionId) throws IOException
        {
            this.metadata = metadata;
            // streaming pre-3.0 sstables require mark/reset support from source stream
            if (version.correspondingMessagingVersion() < MessagingService.VERSION_30)
            {
                logger.trace("Initializing rewindable input stream for reading legacy sstable with {} bytes with following " +
                             "parameters: initial_mem_buffer_size={}, max_mem_buffer_size={}, max_spill_file_size={}.",
                             totalSize, INITIAL_MEM_BUFFER_SIZE, MAX_MEM_BUFFER_SIZE, MAX_SPILL_FILE_SIZE);
                File bufferFile = getTempBufferFile(metadata, totalSize, sessionId);
                this.in = new RewindableDataInputStreamPlus(in, INITIAL_MEM_BUFFER_SIZE, MAX_MEM_BUFFER_SIZE, bufferFile, MAX_SPILL_FILE_SIZE);
            } else
                this.in = new DataInputPlus.DataInputStreamPlus(in);
            this.helper = new SerializationHelper(metadata, version.correspondingMessagingVersion(), SerializationHelper.Flag.PRESERVE_SIZE);
            this.header = header;
        }

        public StreamDeserializer newPartition() throws IOException
        {
            key = metadata.decorateKey(ByteBufferUtil.readWithShortLength(in));
            partitionLevelDeletion = DeletionTime.serializer.deserialize(in);
            iterator = SSTableSimpleIterator.create(metadata, in, header, helper, partitionLevelDeletion);
            staticRow = iterator.readStaticRow();
            return this;
        }

        public CFMetaData metadata()
        {
            return metadata;
        }

        public PartitionColumns columns()
        {
            // We don't know which columns we'll get so assume it can be all of them
            return metadata.partitionColumns();
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

        /* We have a separate cleanup method because sometimes close is called before exhausting the
           StreamDeserializer (for instance, when enclosed in an try-with-resources wrapper, such as in
           BigTableWriter.append()).
         */
        public void cleanup()
        {
            if (in instanceof RewindableDataInputStreamPlus)
            {
                try
                {
                    ((RewindableDataInputStreamPlus) in).close(false);
                }
                catch (IOException e)
                {
                    logger.warn("Error while closing RewindableDataInputStreamPlus.", e);
                }
            }
        }

        private static File getTempBufferFile(CFMetaData metadata, long totalSize, UUID sessionId) throws IOException
        {
            ColumnFamilyStore cfs = Keyspace.open(metadata.ksName).getColumnFamilyStore(metadata.cfName);
            if (cfs == null)
            {
                // schema was dropped during streaming
                throw new RuntimeException(String.format("CF %s.%s was dropped during streaming", metadata.ksName, metadata.cfName));
            }

            long maxSize = Math.min(MAX_SPILL_FILE_SIZE, totalSize);
            File tmpDir = cfs.getDirectories().getTemporaryWriteableDirectoryAsFile(maxSize);
            if (tmpDir == null)
                throw new IOException(String.format("No sufficient disk space to stream legacy sstable from {}.{}. " +
                                                         "Required disk space: %s.", FBUtilities.prettyPrintMemory(maxSize)));
            return new File(tmpDir, String.format("%s-%s.%s", BUFFER_FILE_PREFIX, sessionId, BUFFER_FILE_SUFFIX));
        }
    }
}
