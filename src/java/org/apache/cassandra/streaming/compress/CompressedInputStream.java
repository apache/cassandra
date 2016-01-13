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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.zip.Checksum;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * InputStream which reads data from underlining source with given {@link CompressionInfo}.
 */
public class CompressedInputStream extends InputStream
{

    private static final Logger logger = LoggerFactory.getLogger(CompressedInputStream.class);

    private final CompressionInfo info;
    // chunk buffer
    private final BlockingQueue<byte[]> dataBuffer;
    private final Supplier<Double> crcCheckChanceSupplier;

    // uncompressed bytes
    private final byte[] buffer;

    // offset from the beginning of the buffer
    protected long bufferOffset = 0;
    // current position in stream
    private long current = 0;
    // number of bytes in the buffer that are actually valid
    protected int validBufferBytes = -1;

    private final Checksum checksum;

    // raw checksum bytes
    private final byte[] checksumBytes = new byte[4];

    private static final byte[] POISON_PILL = new byte[0];

    private long totalCompressedBytesRead;

    /**
     * @param source Input source to read compressed data from
     * @param info Compression info
     */
    public CompressedInputStream(InputStream source, CompressionInfo info, ChecksumType checksumType, Supplier<Double> crcCheckChanceSupplier)
    {
        this.info = info;
        this.checksum =  checksumType.newInstance();
        this.buffer = new byte[info.parameters.chunkLength()];
        // buffer is limited to store up to 1024 chunks
        this.dataBuffer = new ArrayBlockingQueue<>(Math.min(info.chunks.length, 1024));
        this.crcCheckChanceSupplier = crcCheckChanceSupplier;

        new Thread(new Reader(source, info, dataBuffer)).start();
    }

    public int read() throws IOException
    {
        if (current >= bufferOffset + buffer.length || validBufferBytes == -1)
        {
            try
            {
                byte[] compressedWithCRC = dataBuffer.take();
                if (compressedWithCRC == POISON_PILL)
                    throw new EOFException("No chunk available");
                decompress(compressedWithCRC);
            }
            catch (InterruptedException e)
            {
                throw new EOFException("No chunk available");
            }
        }

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        return ((int) buffer[(int) (current++ - bufferOffset)]) & 0xff;
    }

    public void position(long position)
    {
        assert position >= current : "stream can only read forward.";
        current = position;
    }

    private void decompress(byte[] compressed) throws IOException
    {
        // uncompress
        validBufferBytes = info.parameters.getSstableCompressor().uncompress(compressed, 0, compressed.length - checksumBytes.length, buffer, 0);
        totalCompressedBytesRead += compressed.length;

        // validate crc randomly
        if (this.crcCheckChanceSupplier.get() > ThreadLocalRandom.current().nextDouble())
        {
            checksum.update(compressed, 0, compressed.length - checksumBytes.length);

            System.arraycopy(compressed, compressed.length - checksumBytes.length, checksumBytes, 0, checksumBytes.length);
            if (Ints.fromByteArray(checksumBytes) != (int) checksum.getValue())
                throw new IOException("CRC unmatched");

            // reset checksum object back to the original (blank) state
            checksum.reset();
        }

        // buffer offset is always aligned
        bufferOffset = current & ~(buffer.length - 1);
    }

    public long getTotalCompressedBytesRead()
    {
        return totalCompressedBytesRead;
    }

    static class Reader extends WrappedRunnable
    {
        private final InputStream source;
        private final Iterator<CompressionMetadata.Chunk> chunks;
        private final BlockingQueue<byte[]> dataBuffer;

        Reader(InputStream source, CompressionInfo info, BlockingQueue<byte[]> dataBuffer)
        {
            this.source = source;
            this.chunks = Iterators.forArray(info.chunks);
            this.dataBuffer = dataBuffer;
        }

        protected void runMayThrow() throws Exception
        {
            byte[] compressedWithCRC;
            while (chunks.hasNext())
            {
                CompressionMetadata.Chunk chunk = chunks.next();

                int readLength = chunk.length + 4; // read with CRC
                compressedWithCRC = new byte[readLength];

                int bufferRead = 0;
                while (bufferRead < readLength)
                {
                    try
                    {
                        int r = source.read(compressedWithCRC, bufferRead, readLength - bufferRead);
                        if (r < 0)
                        {
                            dataBuffer.put(POISON_PILL);
                            return; // throw exception where we consume dataBuffer
                        }
                        bufferRead += r;
                    }
                    catch (IOException e)
                    {
                        logger.warn("Error while reading compressed input stream.", e);
                        dataBuffer.put(POISON_PILL);
                        return; // throw exception where we consume dataBuffer
                    }
                }
                dataBuffer.put(compressedWithCRC);
            }
        }
    }
}
