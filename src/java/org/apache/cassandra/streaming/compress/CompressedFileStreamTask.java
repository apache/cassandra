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

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.FileStreamTask;
import org.apache.cassandra.streaming.StreamHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

/**
 * FileStreamTask for compressed SSTable.
 *
 * This class sends relevant part of compressed file directly using nio if available.
 */
public class CompressedFileStreamTask extends FileStreamTask
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedFileStreamTask.class);
    // 10MB chunks
    public static final int CHUNK_SIZE = 10*1024*1024;

    public CompressedFileStreamTask(StreamHeader header, InetAddress to)
    {
        super(header, to);
    }

    protected void stream() throws IOException
    {
        assert header.file.compressionInfo != null;

        SocketChannel sc = socket.getChannel();
        byte[] transferBuffer = null;

        // write header
        ByteBuffer headerBuffer = MessagingService.instance().constructStreamHeader(header, false, MessagingService.instance().getVersion(to));
        socket.getOutputStream().write(ByteBufferUtil.getArray(headerBuffer));

        RandomAccessReader file = RandomAccessReader.open(new File(header.file.getFilename()), true);
        FileChannel fc = file.getChannel();

        StreamingMetrics.activeStreamsOutbound.inc();
        // calculate chunks to transfer. we want to send continuous chunks altogether.
        List<Pair<Long, Long>> sections = getTransferSections(header.file.compressionInfo.chunks);
        try
        {
            long totalBytesTransferred = 0;
            // stream each of the required sections of the file
            for (Pair<Long, Long> section : sections)
            {
                // length of the section to stream
                long length = section.right - section.left;
                // tracks write progress
                long bytesTransferred = 0;
                while (bytesTransferred < length)
                {
                    int toTransfer = (int) Math.min(CHUNK_SIZE, length - bytesTransferred);
                    long lastWrite;
                    if (sc != null)
                    {
                        lastWrite = fc.transferTo(section.left + bytesTransferred, toTransfer, sc);
                        throttle.throttleDelta(lastWrite);
                    }
                    else
                    {
                        // NIO is not available. Fall back to normal streaming.
                        // This happens when inter-node encryption is turned on.
                        if (transferBuffer == null)
                            transferBuffer = new byte[CHUNK_SIZE];
                        file.readFully(transferBuffer, 0, toTransfer);
                        socket.getOutputStream().write(transferBuffer, 0, toTransfer);
                        throttle.throttleDelta(toTransfer);
                        lastWrite = toTransfer;
                    }
                    totalBytesTransferred += lastWrite;
                    bytesTransferred += lastWrite;
                    header.file.progress += lastWrite;
                }

                logger.debug("Bytes transferred " + bytesTransferred + "/" + header.file.size);
            }
            StreamingMetrics.totalOutgoingBytes.inc(totalBytesTransferred);
            metrics.outgoingBytes.inc(totalBytesTransferred);
            // receive reply confirmation
            receiveReply();
        }
        finally
        {
            StreamingMetrics.activeStreamsOutbound.dec();

            // no matter what happens close file
            FileUtils.closeQuietly(file);
        }
    }

    // chunks are assumed to be sorted by offset
    private List<Pair<Long, Long>> getTransferSections(CompressionMetadata.Chunk[] chunks)
    {
        List<Pair<Long, Long>> transferSections = new ArrayList<Pair<Long, Long>>();
        Pair<Long, Long> lastSection = null;
        for (CompressionMetadata.Chunk chunk : chunks)
        {
            if (lastSection != null)
            {
                if (chunk.offset == lastSection.right)
                {
                    // extend previous section to end of this chunk
                    lastSection = new Pair<Long, Long>(lastSection.left, chunk.offset + chunk.length + 4); // 4 bytes for CRC
                }
                else
                {
                    transferSections.add(lastSection);
                    lastSection = new Pair<Long, Long>(chunk.offset, chunk.offset + chunk.length + 4);
                }
            }
            else
            {
                lastSection = new Pair<Long, Long>(chunk.offset, chunk.offset + chunk.length + 4);
            }
        }
        if (lastSection != null)
            transferSections.add(lastSection);
        return transferSections;
    }
}
