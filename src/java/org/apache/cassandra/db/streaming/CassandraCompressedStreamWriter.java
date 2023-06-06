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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingDataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CassandraStreamWriter for compressed SSTable.
 */
public class CassandraCompressedStreamWriter extends CassandraStreamWriter
{
    private static final int CHUNK_SIZE = 1 << 16;
    private static final int CRC_LENGTH = 4;

    private static final Logger logger = LoggerFactory.getLogger(CassandraCompressedStreamWriter.class);

    private final CompressionInfo compressionInfo;
    private final long totalSize;

    public CassandraCompressedStreamWriter(SSTableReader sstable, CassandraStreamHeader header, StreamSession session)
    {
        super(sstable, header, session);
        this.compressionInfo = header.compressionInfo;
        this.totalSize = header.size();
    }

    @Override
    public void write(StreamingDataOutputPlus out) throws IOException
    {
        long totalSize = totalSize();
        logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                     sstable.getFilename(), session.peer, sstable.getSSTableMetadata().repairedAt, totalSize);
        try (ChannelProxy fc = sstable.getDataChannel().newChannel())
        {
            long progress = 0L;

            // we want to send continuous chunks together to minimise reads from disk and network writes
            List<Section> sections = fuseAdjacentChunks(compressionInfo.chunks());

            int sectionIdx = 0;

            // stream each of the required sections of the file
            String filename = sstable.descriptor.fileFor(Components.DATA).toString();
            for (Section section : sections)
            {
                // length of the section to stream
                long length = section.end - section.start;

                logger.debug("[Stream #{}] Writing section {} with length {} to stream.", session.planId(), sectionIdx++, length);

                // tracks write progress
                long bytesTransferred = 0;
                while (bytesTransferred < length)
                {
                    int toTransfer = (int) Math.min(CHUNK_SIZE, length - bytesTransferred);
                    long position = section.start + bytesTransferred;

                    out.writeToChannel(bufferSupplier -> {
                        ByteBuffer outBuffer = bufferSupplier.get(toTransfer);
                        long read = fc.read(outBuffer, position);
                        assert read == toTransfer : String.format("could not read required number of bytes from file to be streamed: read %d bytes, wanted %d bytes", read, toTransfer);
                        outBuffer.flip();
                    }, limiter);

                    bytesTransferred += toTransfer;
                    progress += toTransfer;
                    session.progress(filename, ProgressInfo.Direction.OUT, progress, toTransfer, totalSize);
                }
            }
            logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}",
                         session.planId(), sstable.getFilename(), session.peer, FBUtilities.prettyPrintMemory(progress), FBUtilities.prettyPrintMemory(totalSize));
        }
    }

    @Override
    protected long totalSize()
    {
        return totalSize;
    }

    // chunks are assumed to be sorted by offset
    private List<Section> fuseAdjacentChunks(CompressionMetadata.Chunk[] chunks)
    {
        if (chunks.length == 0)
            return Collections.emptyList();

        long start = chunks[0].offset;
        long end = start + chunks[0].length + CRC_LENGTH;

        List<Section> sections = new ArrayList<>();

        for (int i = 1; i < chunks.length; i++)
        {
            CompressionMetadata.Chunk chunk = chunks[i];

            if (chunk.offset == end)
            {
                end += (chunk.length + CRC_LENGTH);
            }
            else
            {
                sections.add(new Section(start, end));

                start = chunk.offset;
                end = start + chunk.length + CRC_LENGTH;
            }
        }
        sections.add(new Section(start, end));

        return sections;
    }

    // [start, end) positions in the compressed sstable file that we want to stream;
    // each section contains 1..n adjacent compressed chunks in it.
    private static class Section
    {
        private final long start;
        private final long end;

        private Section(long start, long end)
        {
            this.start = start;
            this.end = end;
        }
    }
}
