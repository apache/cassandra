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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Closeable;

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

    protected CompressedChunkReader forScan()
    {
        return this;
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
    public Rebufferer instantiateRebufferer(boolean isScan)
    {
        return new BufferManagingRebufferer.Aligned(isScan ? forScan() : this);
    }

    protected interface CompressedReader extends Closeable
    {
        default void allocateResources()
        {
        }

        default void deallocateResources()
        {
        }

        default boolean allocated()
        {
            return false;
        }

        default void close()
        {

        }


        ByteBuffer read(CompressionMetadata.Chunk chunk, boolean shouldCheckCrc) throws CorruptBlockException;
    }

    private static class RandomAccessCompressedReader implements CompressedReader
    {
        private final ChannelProxy channel;
        private final ThreadLocalByteBufferHolder bufferHolder;

        private RandomAccessCompressedReader(ChannelProxy channel, CompressionMetadata metadata)
        {
            this.channel = channel;
            this.bufferHolder = new ThreadLocalByteBufferHolder(metadata.compressor().preferredBufferType());
        }

        @Override
        public ByteBuffer read(CompressionMetadata.Chunk chunk, boolean shouldCheckCrc) throws CorruptBlockException
        {
            int length = shouldCheckCrc ? chunk.length + Integer.BYTES // compressed length + checksum length
                                        : chunk.length;
            ByteBuffer compressed = bufferHolder.getBuffer(length);
            if (channel.read(compressed, chunk.offset) != length)
                throw new CorruptBlockException(channel.filePath(), chunk);
            compressed.flip();
            compressed.limit(chunk.length);

            if (shouldCheckCrc)
            {
                int checksum = (int) ChecksumType.CRC32.of(compressed);
                compressed.limit(length);
                if (compressed.getInt() != checksum)
                    throw new CorruptBlockException(channel.filePath(), chunk);
                compressed.position(0).limit(chunk.length);
            }
            return compressed;
        }
    }

    private static class ScanCompressedReader implements CompressedReader
    {
        private final ChannelProxy channel;
        private final ThreadLocalByteBufferHolder bufferHolder;
        private final ThreadLocalReadAheadBuffer readAheadBuffer;

        private ScanCompressedReader(ChannelProxy channel, CompressionMetadata metadata, int readAheadBufferSize)
        {
            this.channel = channel;
            this.bufferHolder = new ThreadLocalByteBufferHolder(metadata.compressor().preferredBufferType());
            this.readAheadBuffer = new ThreadLocalReadAheadBuffer(channel, readAheadBufferSize, metadata.compressor().preferredBufferType());
        }

        @Override
        public ByteBuffer read(CompressionMetadata.Chunk chunk, boolean shouldCheckCrc) throws CorruptBlockException
        {
            int length = shouldCheckCrc ? chunk.length + Integer.BYTES // compressed length + checksum length
                                        : chunk.length;
            ByteBuffer compressed = bufferHolder.getBuffer(length);

            int copied = 0;
            while (copied < length)
            {
                readAheadBuffer.fill(chunk.offset + copied);
                int leftToRead = length - copied;
                if (readAheadBuffer.remaining() >= leftToRead)
                    copied += readAheadBuffer.read(compressed, leftToRead);
                else
                    copied += readAheadBuffer.read(compressed, readAheadBuffer.remaining());
            }

            compressed.flip();
            compressed.limit(chunk.length);

            if (shouldCheckCrc)
            {
                int checksum = (int) ChecksumType.CRC32.of(compressed);
                compressed.limit(length);
                if (compressed.getInt() != checksum)
                    throw new CorruptBlockException(channel.filePath(), chunk);
                compressed.position(0).limit(chunk.length);
            }
            return compressed;
        }

        @Override
        public void allocateResources()
        {
            readAheadBuffer.allocateBuffer();
        }

        @Override
        public void deallocateResources()
        {
            readAheadBuffer.clear(true);
        }

        @Override
        public boolean allocated()
        {
            return readAheadBuffer.hasBuffer();
        }

        public void close()
        {
            readAheadBuffer.close();
        }
    }

    public static class Standard extends CompressedChunkReader
    {

        private final CompressedReader reader;
        private final CompressedReader scanReader;

        public Standard(ChannelProxy channel, CompressionMetadata metadata, Supplier<Double> crcCheckChanceSupplier)
        {
            super(channel, metadata, crcCheckChanceSupplier);
            reader = new RandomAccessCompressedReader(channel, metadata);

            int readAheadBufferSize = DatabaseDescriptor.getCompressedReadAheadBufferSize();
            scanReader = (readAheadBufferSize > 0 && readAheadBufferSize > metadata.chunkLength())
                         ? new ScanCompressedReader(channel, metadata, readAheadBufferSize) : null;
        }

        protected CompressedChunkReader forScan()
        {
            if (scanReader != null)
                scanReader.allocateResources();

            return this;
        }

        @Override
        public void releaseUnderlyingResources()
        {
            if (scanReader != null)
                scanReader.deallocateResources();
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

                CompressedReader readFrom = (scanReader != null && scanReader.allocated()) ? scanReader : reader;
                if (chunk.length < maxCompressedLength)
                {
                    ByteBuffer compressed = readFrom.read(chunk, shouldCheckCrc);
                    uncompressed.clear();

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

                        ByteBuffer scratch = ByteBuffer.allocate(Integer.BYTES);
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

        @Override
        public void close()
        {
            reader.close();
            if (scanReader != null)
                scanReader.close();

            super.close();
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
