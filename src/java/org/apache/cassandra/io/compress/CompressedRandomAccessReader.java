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
package org.apache.cassandra.io.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;

// TODO refactor this to separate concept of "buffer to avoid lots of read() syscalls" and "compression buffer"
public class CompressedRandomAccessReader extends RandomAccessReader
{
    public static RandomAccessReader open(String dataFilePath, CompressionMetadata metadata)
    {
        return open(dataFilePath, metadata, false);
    }

    public static RandomAccessReader open(String dataFilePath, CompressionMetadata metadata, boolean skipIOCache)
    {
        try
        {
            return new CompressedRandomAccessReader(dataFilePath, metadata, skipIOCache);
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
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

    public CompressedRandomAccessReader(String dataFilePath, CompressionMetadata metadata, boolean skipIOCache) throws FileNotFoundException
    {
        super(new File(dataFilePath), metadata.chunkLength(), skipIOCache);
        this.metadata = metadata;
        compressed = new byte[metadata.compressor().initialCompressedBufferLength(metadata.chunkLength())];
        // can't use super.read(...) methods
        // that is why we are allocating special InputStream to read data from disk
        // from already open file descriptor
        try
        {
            source = new FileInputStream(getFD());
        }
        catch (IOException e)
        {
            // fd == null, Not Supposed To Happen
            throw new RuntimeException(e);
        }

        channel = source.getChannel(); // for position manipulation
    }

    @Override
    protected void reBuffer()
    {
        try
        {
            decompressChunk(metadata.chunkFor(current));
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    private void decompressChunk(CompressionMetadata.Chunk chunk) throws IOException
    {
        if (channel.position() != chunk.offset)
            channel.position(chunk.offset);

        if (compressed.length < chunk.length)
            compressed = new byte[chunk.length];

        if (source.read(compressed, 0, chunk.length) != chunk.length)
            throw new CorruptBlockException(getPath(), chunk);

        try
        {
            validBufferBytes = metadata.compressor().uncompress(compressed, 0, chunk.length, buffer, 0);
        }
        catch (IOException e)
        {
            throw new CorruptBlockException(getPath(), chunk);
        }

        if (metadata.parameters.crcChance > FBUtilities.threadLocalRandom().nextDouble())
        {
            checksum.update(buffer, 0, validBufferBytes);

            if (checksum(chunk) != (int) checksum.getValue())
                throw new CorruptBlockException(getPath(), chunk);

            // reset checksum object back to the original (blank) state
            checksum.reset();
        }

        // buffer offset is always aligned
        bufferOffset = current & ~(buffer.length - 1);
    }

    private int checksum(CompressionMetadata.Chunk chunk) throws IOException
    {
        assert channel.position() == chunk.offset + chunk.length;

        if (source.read(checksumBytes, 0, checksumBytes.length) != checksumBytes.length)
            throw new CorruptBlockException(getPath(), chunk);

        return Ints.fromByteArray(checksumBytes);
    }

    @Override
    public long length()
    {
        return metadata.dataLength;
    }

    @Override
    public String toString()
    {
        return String.format("%s - chunk length %d, data length %d.", getPath(), metadata.chunkLength(), metadata.dataLength);
    }
}
