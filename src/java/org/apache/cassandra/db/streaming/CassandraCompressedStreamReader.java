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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CassandraStreamReader that reads from streamed compressed SSTable
 */
public class CassandraCompressedStreamReader extends CassandraStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraCompressedStreamReader.class);

    protected final CompressionInfo compressionInfo;

    public CassandraCompressedStreamReader(StreamMessageHeader header, CassandraStreamHeader streamHeader, StreamSession session)
    {
        super(header, streamHeader, session);
        this.compressionInfo = streamHeader.compressionInfo;
    }

    /**
     * @return SSTable transferred
     * @throws java.io.IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    @Override
    public SSTableMultiWriter read(DataInputPlus inputPlus) throws Throwable
    {
        long totalSize = totalSize();

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);

        if (cfs == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + tableId + " was dropped during streaming");
        }

        logger.debug("[Stream #{}] Start receiving file #{} from {}, repairedAt = {}, size = {}, ks = '{}', pendingRepair = '{}', table = '{}'.",
                     session.planId(), fileSeqNum, session.peer, repairedAt, totalSize, cfs.getKeyspaceName(), pendingRepair,
                     cfs.getTableName());

        StreamDeserializer deserializer = null;
        SSTableMultiWriter writer = null;
        try (CompressedInputStream cis = new CompressedInputStream(inputPlus, compressionInfo, ChecksumType.CRC32, cfs::getCrcCheckChance))
        {
            TrackedDataInputPlus in = new TrackedDataInputPlus(cis);
            deserializer = new StreamDeserializer(cfs.metadata(), in, inputVersion, getHeader(cfs.metadata()));
            writer = createWriter(cfs, totalSize, repairedAt, pendingRepair, inputVersion.format);
            String filename = writer.getFilename();
            String sectionName = filename + '-' + fileSeqNum;
            int sectionIdx = 0;
            for (SSTableReader.PartitionPositionBounds section : sections)
            {
                assert cis.chunkBytesRead() <= totalSize;
                long sectionLength = section.upperPosition - section.lowerPosition;

                logger.trace("[Stream #{}] Reading section {} with length {} from stream.", session.planId(), sectionIdx++, sectionLength);
                // skip to beginning of section inside chunk
                cis.position(section.lowerPosition);
                in.reset(0);

                long lastBytesRead = 0;
                while (in.getBytesRead() < sectionLength)
                {
                    writePartition(deserializer, writer);
                    // when compressed, report total bytes of compressed chunks read since remoteFile.size is the sum of chunks transferred
                    long bytesRead = cis.chunkBytesRead();
                    long bytesDelta = bytesRead - lastBytesRead;
                    lastBytesRead = bytesRead;
                    session.progress(sectionName, ProgressInfo.Direction.IN, bytesRead, bytesDelta, totalSize);
                }
                assert in.getBytesRead() == sectionLength;
            }
            logger.info("[Stream #{}] Finished receiving file #{} from {} readBytes = {}, totalSize = {}", session.planId(), fileSeqNum,
                         session.peer, FBUtilities.prettyPrintMemory(cis.chunkBytesRead()), FBUtilities.prettyPrintMemory(totalSize));
            return writer;
        }
        catch (Throwable e)
        {
            Object partitionKey = deserializer != null ? deserializer.partitionKey() : "";
            logger.warn("[Stream {}] Error while reading partition {} from stream on ks='{}' and table='{}'.",
                        session.planId(), partitionKey, cfs.getKeyspaceName(), cfs.getTableName());
            if (writer != null)
                e = writer.abort(e);
            throw e;
        }
    }

    @Override
    protected long totalSize()
    {
        return compressionInfo.getTotalSize();
    }
}
