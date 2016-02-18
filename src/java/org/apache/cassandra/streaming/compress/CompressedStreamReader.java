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
package org.apache.cassandra.streaming.compress;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import com.google.common.base.Throwables;

import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.RangeAwareSSTableWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.io.util.TrackedInputStream;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * StreamReader that reads from streamed compressed SSTable
 */
public class CompressedStreamReader extends StreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedStreamReader.class);

    protected final CompressionInfo compressionInfo;

    public CompressedStreamReader(FileMessageHeader header, StreamSession session)
    {
        super(header, session);
        this.compressionInfo = header.compressionInfo;
    }

    /**
     * @return SSTable transferred
     * @throws java.io.IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    @Override
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

        CompressedInputStream cis = new CompressedInputStream(Channels.newInputStream(channel), compressionInfo,
                                                              inputVersion.compressedChecksumType(), cfs::getCrcCheckChance);
        TrackedInputStream in = new TrackedInputStream(cis);

        StreamDeserializer deserializer = new StreamDeserializer(cfs.metadata, in, inputVersion, getHeader(cfs.metadata),
                                                                 totalSize, session.planId());
        SSTableMultiWriter writer = null;
        try
        {
            writer = createWriter(cfs, totalSize, repairedAt, format);
            int sectionIdx = 0;
            for (Pair<Long, Long> section : sections)
            {
                assert cis.getTotalCompressedBytesRead() <= totalSize;
                long sectionLength = section.right - section.left;

                logger.trace("[Stream #{}] Reading section {} with length {} from stream.", session.planId(), sectionIdx++, sectionLength);
                // skip to beginning of section inside chunk
                cis.position(section.left);
                in.reset(0);

                while (in.getBytesRead() < sectionLength)
                {
                    writePartition(deserializer, writer);
                    // when compressed, report total bytes of compressed chunks read since remoteFile.size is the sum of chunks transferred
                    session.progress(writer.getFilename(), ProgressInfo.Direction.IN, cis.getTotalCompressedBytesRead(), totalSize);
                }
            }
            logger.debug("[Stream #{}] Finished receiving file #{} from {} readBytes = {}, totalSize = {}", session.planId(), fileSeqNum,
                         session.peer, FBUtilities.prettyPrintMemory(cis.getTotalCompressedBytesRead()), FBUtilities.prettyPrintMemory(totalSize));
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

    @Override
    protected long totalSize()
    {
        long size = 0;
        // calculate total length of transferring chunks
        for (CompressionMetadata.Chunk chunk : compressionInfo.chunks)
            size += chunk.length + 4; // 4 bytes for CRC
        return size;
    }
}
