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
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Checksum;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * CRAR extends RAR to transparently uncompress blocks from the file into RAR.buffer.  Most of the RAR
 * "read bytes from the buffer, rebuffering when necessary" machinery works unchanged after that.
 */
public class CompressedRandomAccessReader extends RandomAccessReader
{
    private final CompressionMetadata metadata;

    // we read the raw compressed bytes into this buffer, then move the uncompressed ones into super.buffer.
    private ByteBuffer compressed;

    // re-use single crc object
    private final Checksum checksum;

    // raw checksum bytes
    private ByteBuffer checksumBytes;

    @VisibleForTesting
    public double getCrcCheckChance()
    {
        return metadata.parameters.getCrcCheckChance();
    }

    protected CompressedRandomAccessReader(Builder builder)
    {
        super(builder);
        this.metadata = builder.metadata;
        this.checksum = metadata.checksumType.newInstance();

        if (regions == null)
        {
            compressed = allocateBuffer(metadata.compressor().initialCompressedBufferLength(metadata.chunkLength()), bufferType);
            checksumBytes = ByteBuffer.wrap(new byte[4]);
        }
    }

    @Override
    protected void releaseBuffer()
    {
        try
        {
            if (buffer != null)
            {
                BufferPool.put(buffer);
                buffer = null;
            }
        }
        finally
        {
            // this will always be null if using mmap access mode (unlike in parent, where buffer is set to a region)
            if (compressed != null)
            {
                BufferPool.put(compressed);
                compressed = null;
            }
        }
    }

    @Override
    protected void reBufferStandard()
    {
        try
        {
            long position = current();
            assert position < metadata.dataLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

            if (compressed.capacity() < chunk.length)
            {
                BufferPool.put(compressed);
                compressed = allocateBuffer(chunk.length, bufferType);
            }
            else
            {
                compressed.clear();
            }

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

            if (getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
            {
                compressed.rewind();
                metadata.checksumType.update( checksum, (compressed));

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

    @Override
    protected void reBufferMmap()
    {
        try
        {
            long position = current();
            assert position < metadata.dataLength;

            CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

            MmappedRegions.Region region = regions.floor(chunk.offset);
            long segmentOffset = region.bottom();
            int chunkOffset = Ints.checkedCast(chunk.offset - segmentOffset);
            ByteBuffer compressedChunk = region.buffer.duplicate(); // TODO: change to slice(chunkOffset) when we upgrade LZ4-java

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

            if (getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
            {
                compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

                metadata.checksumType.update( checksum, compressedChunk);

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

    private int checksum(CompressionMetadata.Chunk chunk) throws IOException
    {
        long position = chunk.offset + chunk.length;
        checksumBytes.clear();
        if (channel.read(checksumBytes, position) != checksumBytes.capacity())
            throw new CorruptBlockException(getPath(), chunk);
        return checksumBytes.getInt(0);
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

    public final static class Builder extends RandomAccessReader.Builder
    {
        private final CompressionMetadata metadata;

        public Builder(ICompressedFile file)
        {
            super(file.channel());
            this.metadata = applyMetadata(file.getMetadata());
            this.regions = file.regions();
        }

        public Builder(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel);
            this.metadata = applyMetadata(metadata);
        }

        private CompressionMetadata applyMetadata(CompressionMetadata metadata)
        {
            this.overrideLength = metadata.compressedFileLength;
            this.bufferSize = metadata.chunkLength();
            this.bufferType = metadata.compressor().preferredBufferType();

            assert Integer.bitCount(this.bufferSize) == 1; //must be a power of two

            return metadata;
        }

        @Override
        protected ByteBuffer createBuffer()
        {
            buffer = allocateBuffer(bufferSize, bufferType);
            buffer.limit(0);
            return buffer;
        }

        @Override
        public RandomAccessReader build()
        {
            return new CompressedRandomAccessReader(this);
        }
    }
}
