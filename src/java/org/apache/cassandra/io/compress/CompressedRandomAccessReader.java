/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.compress;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CRAR extends RAR to transparently uncompress blocks from the file into RAR.buffer.  Most of the RAR
 * "read bytes from the buffer, rebuffering when necessary" machinery works unchanged after that.
 */
public class CompressedRandomAccessReader extends RandomAccessReader
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedRandomAccessReader.class);

    public static RandomAccessReader open(String dataFilePath, CompressionMetadata metadata) throws IOException
    {
        return open(dataFilePath, metadata, false);
    }

    public static RandomAccessReader open(String dataFilePath, CompressionMetadata metadata, boolean skipIOCache) throws IOException
    {
        return new CompressedRandomAccessReader(dataFilePath, metadata, skipIOCache);
    }

    private final CompressionMetadata metadata;

    // we read the raw compressed bytes into this buffer, then move the uncompressed ones into super.buffer.
    private ByteBuffer compressed;

    // re-use single crc object
    private final Checksum checksum = new CRC32();

    // raw checksum bytes
    private final ByteBuffer checksumBytes = ByteBuffer.wrap(new byte[4]);

    public CompressedRandomAccessReader(String dataFilePath, CompressionMetadata metadata, boolean skipIOCache) throws IOException
    {
        super(new File(dataFilePath), metadata.chunkLength(), skipIOCache);
        this.metadata = metadata;
        compressed = ByteBuffer.wrap(new byte[metadata.compressor().initialCompressedBufferLength(metadata.chunkLength())]);
    }

    @Override
    protected void reBuffer() throws IOException
    {
        decompressChunk(metadata.chunkFor(current));
    }

    private void decompressChunk(CompressionMetadata.Chunk chunk) throws IOException
    {
        if (channel.position() != chunk.offset)
            channel.position(chunk.offset);

        if (compressed.capacity() < chunk.length)
            compressed = ByteBuffer.wrap(new byte[chunk.length]);
        else
            compressed.clear();
        compressed.limit(chunk.length);

        if (channel.read(compressed) != chunk.length)
            throw new IOException(String.format("(%s) failed to read %d bytes from offset %d.", getPath(), chunk.length, chunk.offset));
        // technically flip() is unnecessary since all the remaining work uses the raw array, but if that changes
        // in the future this will save a lot of hair-pulling
        compressed.flip();
        validBufferBytes = metadata.compressor().uncompress(compressed.array(), 0, chunk.length, buffer, 0);

        if (metadata.parameters.getCrcCheckChance() > FBUtilities.threadLocalRandom().nextDouble())
        {
            checksum.update(buffer, 0, validBufferBytes);

            if (checksum(chunk) != (int) checksum.getValue())
                throw new CorruptedBlockException(getPath(), chunk);

            // reset checksum object back to the original (blank) state
            checksum.reset();
        }

        // buffer offset is always aligned
        bufferOffset = current & ~(buffer.length - 1);
    }

    private int checksum(CompressionMetadata.Chunk chunk) throws IOException
    {
        assert channel.position() == chunk.offset + chunk.length;
        checksumBytes.clear();
        if (channel.read(checksumBytes) != checksumBytes.capacity())
            throw new IOException(String.format("(%s) failed to read checksum of the chunk at %d of length %d.",
                                                getPath(),
                                                chunk.offset,
                                                chunk.length));
        return checksumBytes.getInt(0);
    }

    @Override
    public long length() throws IOException
    {
        return metadata.dataLength;
    }

    @Override
    public String toString()
    {
        return String.format("%s - chunk length %d, data length %d.", getPath(), metadata.chunkLength(), metadata.dataLength);
    }
}
