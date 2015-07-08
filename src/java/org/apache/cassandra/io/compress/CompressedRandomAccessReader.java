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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Adler32;


import com.google.common.primitives.Ints;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * CRAR extends RAR to transparently uncompress blocks from the file into RAR.buffer.  Most of the RAR
 * "read bytes from the buffer, rebuffering when necessary" machinery works unchanged after that.
 */
public class CompressedRandomAccessReader extends RandomAccessReader
{
    public static CompressedRandomAccessReader open(ChannelProxy channel, CompressionMetadata metadata)
    {
        return new CompressedRandomAccessReader(channel, metadata, null);
    }

    public static CompressedRandomAccessReader open(ICompressedFile file)
    {
        return new CompressedRandomAccessReader(file.channel(), file.getMetadata(), file);
    }

    private final TreeMap<Long, MappedByteBuffer> chunkSegments;

    private final CompressionMetadata metadata;

    // we read the raw compressed bytes into this buffer, then move the uncompressed ones into super.buffer.
    private ByteBuffer compressed;

    // re-use single crc object
    private final Adler32 checksum;

    // raw checksum bytes
    private ByteBuffer checksumBytes;

    protected CompressedRandomAccessReader(ChannelProxy channel, CompressionMetadata metadata, ICompressedFile file)
    {
        super(channel, metadata.chunkLength(), metadata.compressedFileLength, metadata.compressor().preferredBufferType());
        this.metadata = metadata;
        checksum = new Adler32();

        chunkSegments = file == null ? null : file.chunkSegments();
        if (chunkSegments == null)
        {
            compressed = allocateBuffer(metadata.compressor().initialCompressedBufferLength(metadata.chunkLength()), metadata.compressor().preferredBufferType());
            checksumBytes = ByteBuffer.wrap(new byte[4]);
        }
    }

    protected int getBufferSize(int size)
    {
        assert Integer.bitCount(size) == 1; //must be a power of two
        return size;
    }

    @Override
    public void close()
    {
        super.close();

        if (compressed != null)
        {
            BufferPool.put(compressed);
            compressed = null;
        }
    }

    private void reBufferStandard()
    {
        try
        {
            long position = current();
            assert position < metadata.dataLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

            if (compressed.capacity() < chunk.length)
                compressed = allocateBuffer(chunk.length, metadata.compressor().preferredBufferType());
            else
                compressed.clear();
            compressed.limit(chunk.length);

            if (channel.read(compressed, chunk.offset) != chunk.length)
                throw new CorruptBlockException(getPath(), chunk);
            compressed.flip();
            buffer.clear();

            try
            {
                metadata.compressor().uncompress(compressed, buffer);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunk);
            }
            finally
            {
                buffer.flip();
            }

            if (metadata.parameters.getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
            {
                compressed.rewind();
                checksum.update(compressed);

                if (checksum(chunk) != (int) checksum.getValue())
                    throw new CorruptBlockException(getPath(), chunk);

                // reset checksum object back to the original (blank) state
                checksum.reset();
            }

            // buffer offset is always aligned
            bufferOffset = position & ~(buffer.capacity() - 1);
            buffer.position((int) (position - bufferOffset));
            // the length() can be provided at construction time, to override the true (uncompressed) length of the file;
            // this is permitted to occur within a compressed segment, so we truncate validBufferBytes if we cross the imposed length
            if (bufferOffset + buffer.limit() > length())
                buffer.limit((int)(length() - bufferOffset));
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

    private void reBufferMmap()
    {
        try
        {
            long position = current();
            assert position < metadata.dataLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

            Map.Entry<Long, MappedByteBuffer> entry = chunkSegments.floorEntry(chunk.offset);
            long segmentOffset = entry.getKey();
            int chunkOffset = Ints.checkedCast(chunk.offset - segmentOffset);
            ByteBuffer compressedChunk = entry.getValue().duplicate(); // TODO: change to slice(chunkOffset) when we upgrade LZ4-java

            compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

            buffer.clear();

            try
            {
                metadata.compressor().uncompress(compressedChunk, buffer);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunk);
            }
            finally
            {
                buffer.flip();
            }

            if (metadata.parameters.getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
            {
                compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

                checksum.update(compressedChunk);

                compressedChunk.limit(compressedChunk.capacity());
                if (compressedChunk.getInt() != (int) checksum.getValue())
                    throw new CorruptBlockException(getPath(), chunk);

                // reset checksum object back to the original (blank) state
                checksum.reset();
            }

            // buffer offset is always aligned
            bufferOffset = position & ~(buffer.capacity() - 1);
            buffer.position((int) (position - bufferOffset));
            // the length() can be provided at construction time, to override the true (uncompressed) length of the file;
            // this is permitted to occur within a compressed segment, so we truncate validBufferBytes if we cross the imposed length
            if (bufferOffset + buffer.limit() > length())
                buffer.limit((int)(length() - bufferOffset));
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }

    }

    @Override
    protected void reBuffer()
    {
        if (chunkSegments != null)
        {
            reBufferMmap();
        }
        else
        {
            reBufferStandard();
        }
    }

    private int checksum(CompressionMetadata.Chunk chunk) throws IOException
    {
        long position = chunk.offset + chunk.length;
        checksumBytes.clear();
        if (channel.read(checksumBytes, position) != checksumBytes.capacity())
            throw new CorruptBlockException(getPath(), chunk);
        return checksumBytes.getInt(0);
    }

    public int getTotalBufferSize()
    {
        return super.getTotalBufferSize() + (chunkSegments != null ? 0 : compressed.capacity());
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
