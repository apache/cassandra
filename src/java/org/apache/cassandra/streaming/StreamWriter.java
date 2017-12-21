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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.ChecksumValidator;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.streaming.compress.ByteBufCompressionDataOutputStreamPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * StreamWriter writes given section of the SSTable to given channel.
 */
public class StreamWriter
{
    private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    private static final Logger logger = LoggerFactory.getLogger(StreamWriter.class);

    protected final SSTableReader sstable;
    protected final Collection<Pair<Long, Long>> sections;
    protected final StreamRateLimiter limiter;
    protected final StreamSession session;

    public StreamWriter(SSTableReader sstable, Collection<Pair<Long, Long>> sections, StreamSession session)
    {
        this.session = session;
        this.sstable = sstable;
        this.sections = sections;
        this.limiter =  StreamManager.getRateLimiter(session.peer);
    }

    /**
     * Stream file of specified sections to given channel.
     *
     * StreamWriter uses LZF compression on wire to decrease size to transfer.
     *
     * @param output where this writes data to
     * @throws IOException on any I/O error
     */
    public void write(DataOutputStreamPlus output) throws IOException
    {
        long totalSize = totalSize();
        logger.debug("[Stream #{}] Start streaming file {} to {}, repairedAt = {}, totalSize = {}", session.planId(),
                     sstable.getFilename(), session.peer, sstable.getSSTableMetadata().repairedAt, totalSize);

        try(ChannelProxy proxy = sstable.getDataChannel().sharedCopy();
            ChecksumValidator validator = new File(sstable.descriptor.filenameFor(Component.CRC)).exists()
                                          ? DataIntegrityMetadata.checksumValidator(sstable.descriptor)
                                          : null)
        {
            int bufferSize = validator == null ? DEFAULT_CHUNK_SIZE: validator.chunkSize;

            // setting up data compression stream
            long progress = 0L;

            try (DataOutputStreamPlus compressedOutput = new ByteBufCompressionDataOutputStreamPlus(output, limiter))
            {
                // stream each of the required sections of the file
                for (Pair<Long, Long> section : sections)
                {
                    long start = validator == null ? section.left : validator.chunkStart(section.left);
                    // if the transfer does not start on the valididator's chunk boundary, this is the number of bytes to offset by
                    int transferOffset = (int) (section.left - start);
                    if (validator != null)
                        validator.seek(start);

                    // length of the section to read
                    long length = section.right - start;
                    // tracks write progress
                    long bytesRead = 0;
                    while (bytesRead < length)
                    {
                        int toTransfer = (int) Math.min(bufferSize, length - bytesRead);
                        long lastBytesRead = write(proxy, validator, compressedOutput, start, transferOffset, toTransfer, bufferSize);
                        start += lastBytesRead;
                        bytesRead += lastBytesRead;
                        progress += (lastBytesRead - transferOffset);
                        session.progress(sstable.descriptor.filenameFor(Component.DATA), ProgressInfo.Direction.OUT, progress, totalSize);
                        transferOffset = 0;
                    }

                    // make sure that current section is sent
                    output.flush();
                }
                logger.debug("[Stream #{}] Finished streaming file {} to {}, bytesTransferred = {}, totalSize = {}",
                             session.planId(), sstable.getFilename(), session.peer, FBUtilities.prettyPrintMemory(progress), FBUtilities.prettyPrintMemory(totalSize));
            }
        }
    }

    protected long totalSize()
    {
        long size = 0;
        for (Pair<Long, Long> section : sections)
            size += section.right - section.left;
        return size;
    }

    /**
     * Sequentially read bytes from the file and write them to the output stream
     *
     * @param proxy The file reader to read from
     * @param validator validator to verify data integrity
     * @param start The readd offset from the beginning of the {@code proxy} file.
     * @param transferOffset number of bytes to skip transfer, but include for validation.
     * @param toTransfer The number of bytes to be transferred.
     *
     * @return Number of bytes transferred.
     *
     * @throws java.io.IOException on any I/O error
     */
    protected long write(ChannelProxy proxy, ChecksumValidator validator, DataOutputStreamPlus output, long start, int transferOffset, int toTransfer, int bufferSize) throws IOException
    {
        // the count of bytes to read off disk
        int minReadable = (int) Math.min(bufferSize, proxy.size() - start);

        // this buffer will hold the data from disk. as it will be compressed on the fly by
        // ByteBufCompressionDataOutputStreamPlus.write(ByteBuffer), we can release this buffer as soon as we can.
        ByteBuffer buffer = ByteBuffer.allocateDirect(minReadable);
        try
        {
            int readCount = proxy.read(buffer, start);
            assert readCount == minReadable : String.format("could not read required number of bytes from file to be streamed: read %d bytes, wanted %d bytes", readCount, minReadable);
            buffer.flip();

            if (validator != null)
            {
                validator.validate(buffer);
                buffer.flip();
            }

            buffer.position(transferOffset);
            buffer.limit(transferOffset + (toTransfer - transferOffset));
            output.write(buffer);
        }
        finally
        {
            FileUtils.clean(buffer);
        }

        return toTransfer;
    }
}
