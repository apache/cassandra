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
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * InputStream which reads data from underlining source with given {@link CompressionInfo}.
 */
public class CompressedInputStream extends InputStream
{
    private final CompressionInfo info;
    // chunk buffer
    private final BlockingQueue<byte[]> dataBuffer;

    // uncompressed bytes
    private byte[] buffer;

    // offset from the beginning of the buffer
    protected long bufferOffset = 0;
    // current position in stream
    private long current = 0;
    // number of bytes in the buffer that are actually valid
    protected int validBufferBytes = -1;

    private final Checksum checksum = new CRC32();

    // raw checksum bytes
    private final byte[] checksumBytes = new byte[4];

    private long uncompressedBytes;

    /**
     * @param source Input source to read compressed data from
     * @param info Compression info
     */
    public CompressedInputStream(InputStream source, CompressionInfo info)
    {
        this.info = info;
        this.buffer = new byte[info.parameters.chunkLength()];
        // buffer is limited to store up to 1024 chunks
        this.dataBuffer = new ArrayBlockingQueue<byte[]>(Math.min(info.chunks.length, 1024));

        new Thread(new Reader(source, info, dataBuffer)).start();
    }

    public int read() throws IOException
    {
        if (current >= bufferOffset + buffer.length || validBufferBytes == -1)
        {
            try
            {
                decompress(dataBuffer.take());
            }
            catch (InterruptedException e)
            {
                throw new EOFException("No chunk available");
            }
        }

        assert current >= bufferOffset && current < bufferOffset + validBufferBytes;

        return ((int) buffer[(int) (current++ - bufferOffset)]) & 0xff;
    }

    public void position(long position) throws IOException
    {
        assert position >= current : "stream can only read forward.";
        current = position;
    }

    private void decompress(byte[] compressed) throws IOException
    {
        // uncompress
        validBufferBytes = info.parameters.sstableCompressor.uncompress(compressed, 0, compressed.length - checksumBytes.length, buffer, 0);
        uncompressedBytes += validBufferBytes;

        // validate crc randomly
        if (info.parameters.crcChance > FBUtilities.threadLocalRandom().nextDouble())
        {
            checksum.update(buffer, 0, validBufferBytes);

            System.arraycopy(compressed, compressed.length - checksumBytes.length, checksumBytes, 0, checksumBytes.length);
            if (Ints.fromByteArray(checksumBytes) != (int) checksum.getValue())
                throw new IOException("CRC unmatched");

            // reset checksum object back to the original (blank) state
            checksum.reset();
        }

        // buffer offset is always aligned
        bufferOffset = current & ~(buffer.length - 1);
    }

    public long uncompressedBytes()
    {
        return uncompressedBytes;
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
                    bufferRead += source.read(compressedWithCRC, bufferRead, readLength - bufferRead);
                dataBuffer.put(compressedWithCRC);
            }
        }
    }
}
