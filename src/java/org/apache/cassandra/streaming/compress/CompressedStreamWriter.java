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
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamWriter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * StreamWriter for compressed SSTable.
 */
public class CompressedStreamWriter extends StreamWriter
{
    public static final int CHUNK_SIZE = 10 * 1024 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(CompressedStreamWriter.class);

    private final CompressionInfo compressionInfo;

    public CompressedStreamWriter(SSTableReader sstable, Collection<Pair<Long, Long>> sections, CompressionInfo compressionInfo, StreamSession session)
    {
        super(sstable, sections, session);
        this.compressionInfo = compressionInfo;
    }

    @Override
    public void write(DataOutputStreamPlus out) throws IOException
    {
        long totalSize = totalSize();
        logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                     sstable.getFilename(), session.peer, sstable.getSSTableMetadata().repairedAt, totalSize);
        try (ChannelProxy fc = sstable.getDataChannel().sharedCopy())
        {
            long progress = 0L;
            // calculate chunks to transfer. we want to send continuous chunks altogether.
            List<Pair<Long, Long>> sections = getTransferSections(compressionInfo.chunks);

            int sectionIdx = 0;

            // stream each of the required sections of the file
            for (final Pair<Long, Long> section : sections)
            {
                // length of the section to stream
                long length = section.right - section.left;

                logger.trace("[Stream #{}] Writing section {} with length {} to stream.", session.planId(), sectionIdx++, length);

                // tracks write progress
                long bytesTransferred = 0;
                while (bytesTransferred < length)
                {
                    final long bytesTransferredFinal = bytesTransferred;
                    final int toTransfer = (int) Math.min(CHUNK_SIZE, length - bytesTransferred);
                    limiter.acquire(toTransfer);
                    long lastWrite = out.applyToChannel((wbc) -> fc.transferTo(section.left + bytesTransferredFinal, toTransfer, wbc));
                    bytesTransferred += lastWrite;
                    progress += lastWrite;
                    session.progress(sstable.descriptor.filenameFor(Component.DATA), ProgressInfo.Direction.OUT, progress, totalSize);
                }
            }
            logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}",
                         session.planId(), sstable.getFilename(), session.peer, FBUtilities.prettyPrintMemory(progress), FBUtilities.prettyPrintMemory(totalSize));
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

    // chunks are assumed to be sorted by offset
    private List<Pair<Long, Long>> getTransferSections(CompressionMetadata.Chunk[] chunks)
    {
        List<Pair<Long, Long>> transferSections = new ArrayList<>();
        Pair<Long, Long> lastSection = null;
        for (CompressionMetadata.Chunk chunk : chunks)
        {
            if (lastSection != null)
            {
                if (chunk.offset == lastSection.right)
                {
                    // extend previous section to end of this chunk
                    lastSection = Pair.create(lastSection.left, chunk.offset + chunk.length + 4); // 4 bytes for CRC
                }
                else
                {
                    transferSections.add(lastSection);
                    lastSection = Pair.create(chunk.offset, chunk.offset + chunk.length + 4);
                }
            }
            else
            {
                lastSection = Pair.create(chunk.offset, chunk.offset + chunk.length + 4);
            }
        }
        if (lastSection != null)
            transferSections.add(lastSection);
        return transferSections;
    }
}
