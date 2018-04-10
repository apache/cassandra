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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

public abstract class CompressedChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    final CompressionMetadata metadata;

    protected CompressedChunkReader(ChannelProxy channel, CompressionMetadata metadata)
    {
        super(channel, metadata.dataLength);
        this.metadata = metadata;
        assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
    }

    @VisibleForTesting
    public double getCrcCheckChance()
    {
        return metadata.parameters.getCrcCheckChance();
    }

    protected final boolean shouldCheckCrc()
    {
        return getCrcCheckChance() >= 1d || getCrcCheckChance() > ThreadLocalRandom.current().nextDouble();
    }

    @Override
    public String toString()
    {
        return String.format("CompressedChunkReader.%s(%s - %s, chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             metadata.compressor().getClass().getSimpleName(),
                             metadata.chunkLength(),
                             metadata.dataLength);
    }

    @Override
    public int chunkSize()
    {
        return metadata.chunkLength();
    }

    @Override
    public BufferType preferredBufferType()
    {
        return metadata.compressor().preferredBufferType();
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        return new BufferManagingRebufferer.Aligned(this);
    }

    public static class Standard extends CompressedChunkReader
    {
        // we read the raw compressed bytes into this buffer, then uncompressed them into the provided one.
        private final ThreadLocal<ByteBuffer> compressedHolder;

        public Standard(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel, metadata);
            compressedHolder = ThreadLocal.withInitial(this::allocateBuffer);
        }

        public ByteBuffer allocateBuffer()
        {
            return allocateBuffer(metadata.compressor().initialCompressedBufferLength(metadata.chunkLength()));
        }

        public ByteBuffer allocateBuffer(int size)
        {
            return metadata.compressor().preferredBufferType().allocate(size);
        }

        @Override
        public void readChunk(long position, ByteBuffer uncompressed)
        {
            try
            {
                // accesses must always be aligned
                assert (position & -uncompressed.capacity()) == position;
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);
                ByteBuffer compressed = compressedHolder.get();

                boolean shouldCheckCrc = shouldCheckCrc();

                int length = shouldCheckCrc ? chunk.length + Integer.BYTES : chunk.length;

                if (compressed.capacity() < length)
                {
                    compressed = allocateBuffer(length);
                    compressedHolder.set(compressed);
                }
                else
                {
                    compressed.clear();
                }

                compressed.limit(length);
                if (channel.read(compressed, chunk.offset) != length)
                    throw new CorruptBlockException(channel.filePath(), chunk);

                compressed.flip();
                uncompressed.clear();

                compressed.position(0).limit(chunk.length);

                if (shouldCheckCrc)
                {
                    int checksum = (int) metadata.checksumType.of(compressed);

                    compressed.limit(length);
                    if (compressed.getInt() != checksum)
                        throw new CorruptBlockException(channel.filePath(), chunk);

                    compressed.position(0).limit(chunk.length);
                }

                try
                {
                    metadata.compressor().uncompress(compressed, uncompressed);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk, e);
                }
                finally
                {
                    uncompressed.flip();
                }
            }
            catch (CorruptBlockException e)
            {
                throw new CorruptSSTableException(e, channel.filePath());
            }
        }
    }

    public static class Mmap extends CompressedChunkReader
    {
        protected final MmappedRegions regions;

        public Mmap(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
        {
            super(channel, metadata);
            this.regions = regions;
        }

        @Override
        public void readChunk(long position, ByteBuffer uncompressed)
        {
            try
            {
                // accesses must always be aligned
                assert (position & -uncompressed.capacity()) == position;
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

                MmappedRegions.Region region = regions.floor(chunk.offset);
                long segmentOffset = region.offset();
                int chunkOffset = Ints.checkedCast(chunk.offset - segmentOffset);
                ByteBuffer compressedChunk = region.buffer();

                compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

                uncompressed.clear();

                if (shouldCheckCrc())
                {
                    int checksum = (int) metadata.checksumType.of(compressedChunk);

                    compressedChunk.limit(compressedChunk.capacity());
                    if (compressedChunk.getInt() != checksum)
                        throw new CorruptBlockException(channel.filePath(), chunk);

                    compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);
                }

                try
                {
                    metadata.compressor().uncompress(compressedChunk, uncompressed);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk, e);
                }
                finally
                {
                    uncompressed.flip();
                }
            }
            catch (CorruptBlockException e)
            {
                throw new CorruptSSTableException(e, channel.filePath());
            }

        }

        public void close()
        {
            regions.closeQuietly();
            super.close();
        }
    }
}
