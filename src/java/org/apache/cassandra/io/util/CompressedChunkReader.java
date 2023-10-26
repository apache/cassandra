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
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.ChecksumType;

public abstract class CompressedChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    final CompressionMetadata metadata;
    final int maxCompressedLength;
    final Supplier<Double> crcCheckChanceSupplier;

    protected CompressedChunkReader(ChannelProxy channel, CompressionMetadata metadata, Supplier<Double> crcCheckChanceSupplier)
    {
        super(channel, metadata.dataLength);
        this.metadata = metadata;
        this.maxCompressedLength = metadata.maxCompressedLength();
        this.crcCheckChanceSupplier = crcCheckChanceSupplier;
        assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
    }

    @VisibleForTesting
    public double getCrcCheckChance()
    {
        return crcCheckChanceSupplier.get();
    }

    boolean shouldCheckCrc()
    {
        double checkChance = getCrcCheckChance();
        return checkChance >= 1d || (checkChance > 0d && checkChance > ThreadLocalRandom.current().nextDouble());
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
        private final ThreadLocalByteBufferHolder bufferHolder;

        public Standard(ChannelProxy channel, CompressionMetadata metadata, Supplier<Double> crcCheckChanceSupplier)
        {
            super(channel, metadata, crcCheckChanceSupplier);
            bufferHolder = new ThreadLocalByteBufferHolder(metadata.compressor().preferredBufferType());
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
                boolean shouldCheckCrc = shouldCheckCrc();
                int length = shouldCheckCrc ? chunk.length + Integer.BYTES // compressed length + checksum length
                                            : chunk.length;

                if (chunk.length < maxCompressedLength)
                {
                    ByteBuffer compressed = bufferHolder.getBuffer(length);

                    if (channel.read(compressed, chunk.offset) != length)
                        throw new CorruptBlockException(channel.filePath(), chunk);

                    compressed.flip();
                    compressed.limit(chunk.length);
                    uncompressed.clear();

                    if (shouldCheckCrc)
                    {
                        int checksum = (int) ChecksumType.CRC32.of(compressed);

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
                }
                else
                {
                    uncompressed.position(0).limit(chunk.length);
                    if (channel.read(uncompressed, chunk.offset) != chunk.length)
                        throw new CorruptBlockException(channel.filePath(), chunk);

                    if (shouldCheckCrc)
                    {
                        uncompressed.flip();
                        int checksum = (int) ChecksumType.CRC32.of(uncompressed);

                        ByteBuffer scratch = bufferHolder.getBuffer(Integer.BYTES);

                        if (channel.read(scratch, chunk.offset + chunk.length) != Integer.BYTES
                                || scratch.getInt(0) != checksum)
                            throw new CorruptBlockException(channel.filePath(), chunk);
                    }
                }
                uncompressed.flip();
            }
            catch (CorruptBlockException e)
            {
                // Make sure reader does not see stale data.
                uncompressed.position(0).limit(0);
                throw new CorruptSSTableException(e, channel.filePath());
            }
        }
    }

    public static class Mmap extends CompressedChunkReader
    {
        protected final MmappedRegions regions;

        public Mmap(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions, Supplier<Double> crcCheckChanceSupplier)
        {
            super(channel, metadata, crcCheckChanceSupplier);
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

                try
                {
                    if (shouldCheckCrc())
                    {
                        int checksum = (int) ChecksumType.CRC32.of(compressedChunk);

                        compressedChunk.limit(compressedChunk.capacity());
                        if (compressedChunk.getInt() != checksum)
                            throw new CorruptBlockException(channel.filePath(), chunk);

                        compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);
                    }

                    if (chunk.length < maxCompressedLength)
                        metadata.compressor().uncompress(compressedChunk, uncompressed);
                    else
                        uncompressed.put(compressedChunk);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk, e);
                }
                uncompressed.flip();
            }
            catch (CorruptBlockException e)
            {
                // Make sure reader does not see stale data.
                uncompressed.position(0).limit(0);
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
