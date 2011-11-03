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
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO refactor this to separate concept of "buffer to avoid lots of read() syscalls" and "compression buffer"
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
    // used by reBuffer() to escape creating lots of temporary buffers
    private byte[] compressed;

    // re-use single crc object
    private final Checksum checksum = new CRC32();

    // raw checksum bytes
    private final byte[] checksumBytes = new byte[4];

    private final FileInputStream source;
    private final FileChannel channel;

    public CompressedRandomAccessReader(String dataFilePath, CompressionMetadata metadata, boolean skipIOCache) throws IOException
    {
        super(new File(dataFilePath), metadata.chunkLength(), skipIOCache);
        this.metadata = metadata;
        compressed = new byte[metadata.compressor().initialCompressedBufferLength(metadata.chunkLength())];
        // can't use super.read(...) methods
        // that is why we are allocating special InputStream to read data from disk
        // from already open file descriptor
        source = new FileInputStream(getFD());
        channel = source.getChannel(); // for position manipulation
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

        if (compressed.length < chunk.length)
            compressed = new byte[chunk.length];

        if (source.read(compressed, 0, chunk.length) != chunk.length)
            throw new IOException(String.format("(%s) failed to read %d bytes from offset %d.", getPath(), chunk.length, chunk.offset));

        validBufferBytes = metadata.compressor().uncompress(compressed, 0, chunk.length, buffer, 0);

        checksum.update(buffer, 0, validBufferBytes);

        if (checksum(chunk) != (int) checksum.getValue())
            throw new CorruptedBlockException(getPath(), chunk);

        // reset checksum object back to the original (blank) state
        checksum.reset();


        // buffer offset is always aligned
        bufferOffset = current & ~(buffer.length - 1);
    }

    private int checksum(CompressionMetadata.Chunk chunk) throws IOException
    {
        assert channel.position() == chunk.offset + chunk.length;

        if (source.read(checksumBytes, 0, checksumBytes.length) != checksumBytes.length)
            throw new IOException(String.format("(%s) failed to read checksum of the chunk at %d of length %d.",
                                                getPath(),
                                                chunk.offset,
                                                chunk.length));

        return FBUtilities.byteArrayToInt(checksumBytes);
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
