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

import java.io.IOException;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamReader;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.utils.Throwables.extractIOExceptionCause;

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

        logger.debug("[Stream #{}] Start receiving file #{} from {}, repairedAt = {}, size = {}, ks = '{}', pendingRepair = '{}', table = '{}'.",
                     session.planId(), fileSeqNum, session.peer, repairedAt, totalSize, cfs.keyspace.getName(), pendingRepair,
                     cfs.getTableName());

        StreamDeserializer deserializer = null;
        SSTableMultiWriter writer = null;
        try (CompressedInputStream cis = new CompressedInputStream(inputPlus, compressionInfo, ChecksumType.CRC32, cfs::getCrcCheckChance))
        {
            TrackedDataInputPlus in = new TrackedDataInputPlus(cis);
            deserializer = new StreamDeserializer(cfs.metadata(), in, inputVersion, getHeader(cfs.metadata()));
            writer = createWriter(cfs, totalSize, repairedAt, pendingRepair, format);
            String filename = writer.getFilename();
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
                    session.progress(filename, ProgressInfo.Direction.IN, cis.getTotalCompressedBytesRead(), totalSize);
                }
            }
            logger.debug("[Stream #{}] Finished receiving file #{} from {} readBytes = {}, totalSize = {}", session.planId(), fileSeqNum,
                         session.peer, FBUtilities.prettyPrintMemory(cis.getTotalCompressedBytesRead()), FBUtilities.prettyPrintMemory(totalSize));
            return writer;
        }
        catch (Throwable e)
        {
            Object partitionKey = deserializer != null ? deserializer.partitionKey() : "";
            logger.warn("[Stream {}] Error while reading partition {} from stream on ks='{}' and table='{}'.",
                        session.planId(), partitionKey, cfs.keyspace.getName(), cfs.getTableName());
            if (writer != null)
            {
                writer.abort(e);
            }
            if (extractIOExceptionCause(e).isPresent())
                throw e;
            throw Throwables.propagate(e);
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
