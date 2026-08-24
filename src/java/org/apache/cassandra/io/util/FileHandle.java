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

import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.EncryptedSequentialWriter;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

import static org.apache.cassandra.utils.Throwables.maybeFail;

/**
 * {@link FileHandle} provides access to a file for reading, including the ones written by various {@link SequentialWriter}
 * instances, and it is typically used by {@link org.apache.cassandra.io.sstable.format.SSTableReader}.
 *
 * Use {@link FileHandle.Builder} to create an instance, and call {@link #createReader()} (and its variants) to
 * access the readers for the underlying file.
 *
 * You can use {@link Builder#complete()} several times during its lifecycle with different {@code overrideLength}(i.e. early opening file).
 * For that reason, the builder keeps a reference to the file channel and makes a copy for each {@link Builder#complete()} call.
 * Therefore, it is important to close the {@link Builder} when it is no longer needed, as well as any {@link FileHandle}
 * instances.
 */
public class FileHandle extends SharedCloseableImpl
{
    private static final Logger logger = LoggerFactory.getLogger(FileHandle.class);

    public final ChannelProxy channel;

    public final long onDiskLength;
    private final ByteOrder order;

    public final SliceDescriptor sliceDescriptor;

    /*
     * Rebufferer factory to use when constructing RandomAccessReaders
     */
    private final RebuffererFactory rebuffererFactory;

    /*
     * Optional CompressionMetadata when dealing with compressed file
     */
    private final Optional<CompressionMetadata> compressionMetadata;

    private FileHandle(Cleanup cleanup,
                       ChannelProxy channel,
                       RebuffererFactory rebuffererFactory,
                       CompressionMetadata compressionMetadata,
                       ByteOrder order,
                       long onDiskLength,
                       SliceDescriptor sliceDescriptor)
    {
        super(cleanup);
        this.rebuffererFactory = rebuffererFactory;
        this.channel = channel;
        this.compressionMetadata = Optional.ofNullable(compressionMetadata);
        this.order = order;
        this.onDiskLength = onDiskLength;
        this.sliceDescriptor = sliceDescriptor;
    }

    private FileHandle(FileHandle copy)
    {
        super(copy);
        channel = copy.channel;
        rebuffererFactory = copy.rebuffererFactory;
        compressionMetadata = copy.compressionMetadata;
        order = copy.order;
        onDiskLength = copy.onDiskLength;
        sliceDescriptor = copy.sliceDescriptor;
    }

    /**
     * @return Path to the file this factory is referencing
     */
    public String path()
    {
        return channel.filePath();
    }

    public long dataLength()
    {
        return rebuffererFactory.fileLength();
    }

    public RebuffererFactory rebuffererFactory()
    {
        return rebuffererFactory;
    }

    public Optional<CompressionMetadata> compressionMetadata()
    {
        return compressionMetadata;
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        compressionMetadata.ifPresent(metadata -> metadata.addTo(identities));
    }

    @Override
    public FileHandle sharedCopy()
    {
        return new FileHandle(this);
    }

    /**
     * Create {@link RandomAccessReader} with configured method of reading content of the file. Positions the reader
     * at the start of the file or the start of the data if the handle is created for a slice (see {@link SliceDescriptor}).
     *
     * @return RandomAccessReader for the file
     */
    public RandomAccessReader createReader()
    {
        return createReader(ReadPattern.RANDOM);
    }

    public RandomAccessReader createReader(ReadPattern accessPattern)
    {
        return createReader(null, accessPattern);
    }

    public RandomAccessReader createReader(RateLimiter limiter, ReadPattern accessPattern)
    {
        return createReader(limiter, sliceDescriptor.dataStart, accessPattern);
    }

    public RandomAccessReader createReader(long position)
    {
        return createReader(null, position, ReadPattern.RANDOM);
    }

    public RandomAccessReader createReader(long position, ReadPattern accessPattern)
    {
        return createReader(null, position, accessPattern);
    }

    /**
     * Create {@link RandomAccessReader} with configured method of reading content of the file.
     * Reading from file will be rate limited by given {@link RateLimiter}.
     *
     * @param limiter RateLimiter to use for rate limiting read
     * @param position Position in the file to start reading from
     * @param accessPattern the access pattern expected for the reads made against the returned reader (this is
     *                      mostly a hint that may optimize the reader for the provided patter, typically enabling
     *                      prefetching for {@link ReadPattern#SEQUENTIAL}).
     * @return RandomAccessReader for the file
     */
    public RandomAccessReader createReader(RateLimiter limiter, long position, ReadPattern accessPattern)
    {
        assert position >= 0 : "Position must be non-negative - file: " + channel.filePath() + ", position: " + position;
        Rebufferer.BufferHolder bufferHolder = position > 0
                                               ? Rebufferer.emptyBufferHolderAt(position)
                                               : Rebufferer.EMPTY;
        return new RandomAccessReader(instantiateRebufferer(limiter, accessPattern), order, bufferHolder);
    }

    /**
     * Drop page cache from start to given {@code before}.
     *
     * @param before uncompressed position from start of the file to be dropped from cache. if 0, to end of file.
     */
    public void dropPageCache(long before)
    {
        long position = compressionMetadata.map(metadata -> {
            if (before >= metadata.dataLength)
                return 0L;
            else
                return metadata.chunkFor(before).offset - metadata.chunkFor(sliceDescriptor.sliceStart).offset;
        }).orElse(before - sliceDescriptor.sliceStart);

        if (position > 0)
            channel.trySkipCache(0, position);
        else
            channel.trySkipCache(0, onDiskLength);
    }

    public Rebufferer instantiateRebufferer()
    {
        return instantiateRebufferer(null, ReadPattern.RANDOM);
    }

    private Rebufferer instantiateRebufferer(RateLimiter limiter, ReadPattern accessPattern)
    {
        Rebufferer rebufferer = accessPattern == ReadPattern.SEQUENTIAL
                                ? PrefetchingRebufferer.withPrefetching(rebuffererFactory)
                                : rebuffererFactory.instantiateRebufferer();

        if (limiter != null && limiter.getRate() < Double.MAX_VALUE)
            rebufferer = new LimitingRebufferer(rebufferer, limiter, DiskOptimizationStrategy.MAX_BUFFER_SIZE);
        return rebufferer;
    }

    public void invalidateIfCached(long position)
    {
        rebuffererFactory.invalidateIfCached(position);
    }

    /**
     * Perform clean up of all resources held by {@link FileHandle}.
     */
    private static class Cleanup implements RefCounted.Tidy
    {
        final ChannelProxy channel;
        final RebuffererFactory rebufferer;
        final CompressionMetadata compressionMetadata;

        private Cleanup(ChannelProxy channel,
                        RebuffererFactory rebufferer,
                        CompressionMetadata compressionMetadata)
        {
            this.channel = channel;
            this.rebufferer = rebufferer;
            this.compressionMetadata = compressionMetadata;
        }

        public String name()
        {
            return channel.filePath();
        }

        public void tidy()
        {
            // Note: we cannot release data held by the chunk cache at this point, because this would release data that
            // is pre-cached by early open. Release is done during SSTableReader cleanup. See EarlyOpenCachingTest.
            try
            {
                if (compressionMetadata != null)
                {
                    compressionMetadata.close();
                }
            }
            finally
            {
                try
                {
                    channel.close();
                }
                finally
                {
                    rebufferer.close();
                }
            }
        }
    }

    /**
     * Configures how the file will be read (compressed, mmapped, use cache etc.)
     */
    public static class Builder implements AutoCloseable
    {
        private final File file;

        private ChannelProxy channel;
        private CompressionMetadata compressionMetadata;
        private MmappedRegions regions;
        private ChunkCache chunkCache;
        private int bufferSize = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        private BufferType bufferType = BufferType.OFF_HEAP;
        private ByteOrder order = ByteOrder.BIG_ENDIAN;

        private SliceDescriptor sliceDescriptor = SliceDescriptor.NONE;

        private boolean mmapped = false;
        private boolean compressed = false;
        private boolean adviseRandom = false;
        private long overrideLength = -1;
        private boolean encryptionOnly = false;

        public Builder(File file)
        {
            this.file = file;
        }

        @VisibleForTesting
        public Builder(ChannelProxy channel)
        {
            this.channel = channel;
            this.file = channel.getFile();
        }

        public Builder compressed(boolean compressed)
        {
            assert !encryptionOnly : "compressed() and maybeEncrypted() are not to be used together";
            this.compressed = compressed;
            return this;
        }

        /**
         * Setting this to true means that the file _may_ be encrypted, if the compressor passed
         * contains an encryption component.
         */
        public Builder maybeEncrypted(boolean dataFileCompressed)
        {
            assert !compressed : "compressed() and maybeEncrypted() are not to be used together";
            this.compressed = dataFileCompressed;
            this.encryptionOnly = dataFileCompressed;
            return this;
        }

        /**
         * Set {@link ChunkCache} to use.
         *
         * @param chunkCache ChunkCache object to use for caching
         * @return this object
         */
        public Builder withChunkCache(ChunkCache chunkCache)
        {
            this.chunkCache = chunkCache;
            return this;
        }

        /**
         * Provide {@link CompressionMetadata} to use when reading compressed file.
         *
         * @param metadata CompressionMetadata to use
         * @return this object
         */
        public Builder withCompressionMetadata(CompressionMetadata metadata)
        {
            this.compressionMetadata = metadata;
            this.compressed = Objects.nonNull(metadata);
            this.encryptionOnly = this.compressed && !metadata.hasOffsets();
            if (compressed && !encryptionOnly)
                this.overrideLength = metadata.compressedFileLength;
            return this;
        }

        /**
         * Set whether to use mmap for reading
         *
         * @param mmapped true if using mmap
         * @return this instance
         */
        public Builder mmapped(boolean mmapped)
        {
            this.mmapped = mmapped;
            return this;
        }

        /**
         * Set the buffer size to use (if appropriate).
         *
         * @param bufferSize Buffer size in bytes
         * @return this instance
         */
        public Builder bufferSize(int bufferSize)
        {
            this.bufferSize = bufferSize;
            return this;
        }

        /**
         * Set the buffer type (on heap or off heap) to use (if appropriate).
         *
         * @param bufferType Buffer type to use
         * @return this instance
         */
        public Builder bufferType(BufferType bufferType)
        {
            this.bufferType = bufferType;
            return this;
        }

        /**
         * Set the byte order to apply to each buffer.
         * @param order
         * @return
         */
        public Builder order(ByteOrder order)
        {
            this.order = order;
            return this;
        }

        public Builder withLength(long length)
        {
            this.overrideLength = length;
            return this;
        }

        public Builder slice(SliceDescriptor sliceDescriptor)
        {
            this.sliceDescriptor = sliceDescriptor;
            return this;
        }

        public Builder adviseRandom()
        {
            adviseRandom = true;
            return this;
        }

        @SuppressWarnings("resource")
        public FileHandle complete()
        {
            boolean channelOpened = false;
            if (channel == null)
            {
                channel = new ChannelProxy(file);
                channelOpened = true;
            }

            ChannelProxy channelCopy = channel.sharedCopy();
            try
            {
                if (compressed && compressionMetadata == null)
                {
                    compressionMetadata = CompressionMetadata.read(channelCopy.getFile(), sliceDescriptor, encryptionOnly);
                    if (!encryptionOnly && overrideLength < 0)
                        overrideLength = compressionMetadata.compressedFileLength;
                    // else the compression metadata is for the corresponding data file rather than the index
                    // for which this is building a handle
                }

                long length = overrideLength > 0
                              ? overrideLength
                              : channelCopy.size();

                ICompressor encryptionOnlyEncryptor = null;
                if (encryptionOnly)
                    encryptionOnlyEncryptor = compressionMetadata.compressor().encryptionOnly();

                RebuffererFactory rebuffererFactory;
                if (length == 0)
                {
                    rebuffererFactory = new EmptyRebufferer(channelCopy);
                }
                else if (mmapped)
                {
                    if (encryptionOnlyEncryptor != null)
                    {
                        // we need to be able to read the whole chunk that contains the last valid position, so map
                        // that too (it is necessarily already written).
                        updateRegions(channel, ((length - 1) | (EncryptedSequentialWriter.CHUNK_SIZE - 1)) + 1, sliceDescriptor.sliceStart);
                        rebuffererFactory = maybeCached(EncryptedChunkReader.createMmap(channelCopy,
                                regions.sharedCopy(),
                                encryptionOnlyEncryptor,
                                compressionMetadata.parameters,
                                length,
                                overrideLength));
                    }
                    else if (compressed && !encryptionOnly)
                    {
                        updateRegions(channelCopy, compressionMetadata, sliceDescriptor.sliceStart, adviseRandom);
                        rebuffererFactory = maybeCached(new CompressedChunkReader.Mmap(channelCopy, compressionMetadata, regions.sharedCopy(), sliceDescriptor.sliceStart));
                    }
                    else
                    {
                        updateRegions(channelCopy, sliceDescriptor.dataEndOr(length), sliceDescriptor.sliceStart);
                        rebuffererFactory = new MmapRebufferer(channelCopy, sliceDescriptor.dataEndOr(length), regions.sharedCopy());
                    }
                }
                else
                {
                    if (adviseRandom)
                        logger.warn("adviseRandom ignored for non-mmapped FileHandle {}", file);

                    regions = null;


                    ChunkReader reader = null;
                    if (encryptionOnlyEncryptor != null)
                        reader = EncryptedChunkReader.createStandard(channelCopy,
                                encryptionOnlyEncryptor,
                                compressionMetadata.parameters,
                                length,
                                overrideLength);
                    else if (compressed && !encryptionOnly)
                        reader = new CompressedChunkReader.Standard(channelCopy, compressionMetadata, sliceDescriptor.sliceStart);


                    if (reader == null)
                    {
                        int chunkSize = DiskOptimizationStrategy.roundForCaching(bufferSize, ChunkCache.roundUp);
                        if (sliceDescriptor.chunkSize > 0 && sliceDescriptor.chunkSize < chunkSize)
                            // if the chunk size in the slice was smaller than the one we used in the rebufferer,
                            // we could end up aligning the file position to the value lower than the slice start
                            chunkSize = sliceDescriptor.chunkSize;
                        reader = new SimpleChunkReader(channelCopy, sliceDescriptor.dataEndOr(length), bufferType, chunkSize, sliceDescriptor.sliceStart);
                    }
                    rebuffererFactory = maybeCached(reader);
                }
                Cleanup cleanup = new Cleanup(channelCopy, rebuffererFactory, compressionMetadata);
                return new FileHandle(cleanup, channelCopy, rebuffererFactory, compressionMetadata, order, length, sliceDescriptor);
            }
            catch (Throwable t)
            {
                channelCopy.close();
                if (channelOpened)
                {
                    ChannelProxy c = channel;
                    channel = null;
                    throw Throwables.cleaned(c.close(t));
                }
                throw t;
            }
        }

        public Throwable close(Throwable accumulate)
        {
            if (regions != null)
                accumulate = regions.close(accumulate);
            if (channel != null)
                return channel.close(accumulate);

            return accumulate;
        }

        public void close()
        {
            maybeFail(close(null));
        }

        private RebuffererFactory maybeCached(ChunkReader reader)
        {
            if (chunkCache != null && chunkCache.capacity() > 0)
                return chunkCache.maybeWrap(reader);
            return reader;
        }

        private void updateRegions(ChannelProxy channel, CompressionMetadata compressionMetadata, long uncompressedSliceOffset, boolean adviseRandom)
        {
            if (regions != null)
                regions.closeQuietly();

            regions = MmappedRegions.map(channel, compressionMetadata, uncompressedSliceOffset, adviseRandom);
        }

        private void updateRegions(ChannelProxy channel, long length, long startOffset)
        {
            if (regions != null && !regions.isValid(channel))
            {
                Throwable err = regions.close(null);
                if (err != null)
                    logger.error("Failed to close mapped regions", err);

                regions = null;
            }

            if (regions == null)
                regions = MmappedRegions.map(channel, length, bufferSize, startOffset, adviseRandom);
            else
                regions.extend(length, bufferSize);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(path='" + path() + '\'' +
               ", length=" + rebuffererFactory.fileLength() +
               ')';
    }
}
