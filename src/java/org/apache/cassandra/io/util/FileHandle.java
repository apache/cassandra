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

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

/**
 * {@link FileHandle} provides access to a file for reading, including the ones written by various {@link SequentialWriter}
 * instances, and it is typically used by {@link org.apache.cassandra.io.sstable.format.SSTableReader}.
 * <p>
 * Use {@link FileHandle.Builder} to create an instance, and call {@link #createReader()} (and its variants) to
 * access the readers for the underlying file.
 * <p>
 * You can use {@link Builder#complete()} several times during its lifecycle with different {@code overrideLength}(i.e. early opening file).
 * For that reason, the builder keeps a reference to the file channel and makes a copy for each {@link Builder#complete()} call.
 * Therefore, it is important to close the {@link Builder} when it is no longer needed, as well as any {@link FileHandle}
 * instances.
 */
public class FileHandle extends SharedCloseableImpl
{
    public final ChannelProxy channel;

    public final long onDiskLength;

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
                       long onDiskLength)
    {
        super(cleanup);
        this.rebuffererFactory = rebuffererFactory;
        this.channel = channel;
        this.compressionMetadata = Optional.ofNullable(compressionMetadata);
        this.onDiskLength = onDiskLength;
    }

    private FileHandle(FileHandle copy)
    {
        super(copy);
        channel = copy.channel;
        rebuffererFactory = copy.rebuffererFactory;
        compressionMetadata = copy.compressionMetadata;
        onDiskLength = copy.onDiskLength;
    }

    /**
     * @return file this factory is referencing
     */
    public File file()
    {
        return new File(channel.filePath());
    }

    public String path()
    {
        return channel.filePath();
    }

    public long dataLength()
    {
        return compressionMetadata.map(c -> c.dataLength).orElseGet(rebuffererFactory::fileLength);
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
    }

    @Override
    public FileHandle sharedCopy()
    {
        return new FileHandle(this);
    }

    /**
     * Create {@link RandomAccessReader} with configured method of reading content of the file.
     *
     * @return RandomAccessReader for the file
     */
    public RandomAccessReader createReader()
    {
        return createReader(null);
    }

    /**
     * Create {@link RandomAccessReader} with configured method of reading content of the file.
     * Reading from file will be rate limited by given {@link RateLimiter}.
     *
     * @param limiter RateLimiter to use for rate limiting read
     * @return RandomAccessReader for the file
     */
    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return new RandomAccessReader(instantiateRebufferer(limiter));
    }

    public FileDataInput createReader(long position)
    {
        RandomAccessReader reader = createReader();
        try
        {
            reader.seek(position);
            return reader;
        }
        catch (Throwable t)
        {
            try
            {
                reader.close();
            }
            catch (Throwable t2)
            {
                t.addSuppressed(t2);
            }
            throw t;
        }
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
                return metadata.chunkFor(before).offset;
        }).orElse(before);
        NativeLibrary.trySkipCache(channel.getFileDescriptor(), 0, position, file().absolutePath());
    }

    public Rebufferer instantiateRebufferer(RateLimiter limiter)
    {
        Rebufferer rebufferer = rebuffererFactory.instantiateRebufferer();

        if (limiter != null)
            rebufferer = new LimitingRebufferer(rebufferer, limiter, DiskOptimizationStrategy.MAX_BUFFER_SIZE);
        return rebufferer;
    }

    /**
     * Perform clean up of all resources held by {@link FileHandle}.
     */
    private static class Cleanup implements RefCounted.Tidy
    {
        final ChannelProxy channel;
        final RebuffererFactory rebufferer;
        final CompressionMetadata compressionMetadata;
        final Optional<ChunkCache> chunkCache;

        private Cleanup(ChannelProxy channel,
                        RebuffererFactory rebufferer,
                        CompressionMetadata compressionMetadata,
                        ChunkCache chunkCache)
        {
            this.channel = channel;
            this.rebufferer = rebufferer;
            this.compressionMetadata = compressionMetadata;
            this.chunkCache = Optional.ofNullable(chunkCache);
        }

        public String name()
        {
            return channel.filePath();
        }

        public void tidy()
        {
            chunkCache.ifPresent(cache -> cache.invalidateFile(name()));
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
    public static class Builder
    {
        public static final long NO_LENGTH_OVERRIDE = -1;

        public final File file;

        private CompressionMetadata compressionMetadata;
        private Supplier<Double> crcCheckChanceSupplier = () -> 1.0;
        private ChunkCache chunkCache;
        private int bufferSize = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        private BufferType bufferType = BufferType.OFF_HEAP;
        private boolean mmapped = false;
        private long lengthOverride = -1;
        private MmappedRegionsCache mmappedRegionsCache;

        public Builder(File file)
        {
            this.file = file;
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
         * Upon completion, builder will create a shared copy of this object and that copy will be used in the created
         * instance of {@link FileHandle}. Therefore, the caller is responsible for closing the instance of
         * {@link CompressionMetadata} passed to this method after builder completion.
         *
         * @param metadata CompressionMetadata to use, can be {@code null} if no compression is used
         * @return this object
         */
        public Builder withCompressionMetadata(CompressionMetadata metadata)
        {
            this.compressionMetadata = metadata;
            return this;
        }

        public Builder withCrcCheckChance(Supplier<Double> crcCheckChanceSupplier)
        {
            this.crcCheckChanceSupplier = crcCheckChanceSupplier;
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

        public Builder mmapped(Config.DiskAccessMode diskAccessMode)
        {
            this.mmapped = diskAccessMode == Config.DiskAccessMode.mmap;
            return this;
        }

        public Builder withMmappedRegionsCache(MmappedRegionsCache mmappedRegionsCache)
        {
            this.mmappedRegionsCache = mmappedRegionsCache;
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
         * Override the file length.
         *
         * @param lengthOverride Override file length (in bytes) so that read cannot go further than this value.
         *                       If the value is less than or equal to 0, then the value is ignored.
         * @return Built file
         */
        public Builder withLengthOverride(long lengthOverride)
        {
            this.lengthOverride = lengthOverride;
            return this;
        }

        /**
         * Complete building {@link FileHandle}.
         */
        public FileHandle complete()
        {
            return complete(ChannelProxy::new);
        }

        @VisibleForTesting
        public FileHandle complete(Function<File, ChannelProxy> channelProxyFactory)
        {
            ChannelProxy channel = null;
            MmappedRegions regions = null;
            CompressionMetadata compressionMetadata = null;
            try
            {
                compressionMetadata = this.compressionMetadata != null ? this.compressionMetadata.sharedCopy() : null;
                channel = channelProxyFactory.apply(file);

                long fileLength = (compressionMetadata != null) ? compressionMetadata.compressedFileLength : channel.size();
                long length = lengthOverride > 0 ? lengthOverride : fileLength;

                RebuffererFactory rebuffererFactory;
                if (length == 0)
                {
                    rebuffererFactory = new EmptyRebufferer(channel);
                }
                else if (mmapped)
                {
                    if (compressionMetadata != null)
                    {
                        regions = mmappedRegionsCache != null ? mmappedRegionsCache.getOrCreate(channel, compressionMetadata)
                                                              : MmappedRegions.map(channel, compressionMetadata);
                        rebuffererFactory = maybeCached(new CompressedChunkReader.Mmap(channel, compressionMetadata, regions, crcCheckChanceSupplier));
                    }
                    else
                    {
                        regions = mmappedRegionsCache != null ? mmappedRegionsCache.getOrCreate(channel, length)
                                                              : MmappedRegions.map(channel, length);
                        rebuffererFactory = new MmapRebufferer(channel, length, regions);
                    }
                }
                else
                {
                    if (compressionMetadata != null)
                    {
                        rebuffererFactory = maybeCached(new CompressedChunkReader.Standard(channel, compressionMetadata, crcCheckChanceSupplier));
                    }
                    else
                    {
                        int chunkSize = DiskOptimizationStrategy.roundForCaching(bufferSize, ChunkCache.roundUp);
                        rebuffererFactory = maybeCached(new SimpleChunkReader(channel, length, bufferType, chunkSize));
                    }
                }
                Cleanup cleanup = new Cleanup(channel, rebuffererFactory, compressionMetadata, chunkCache);

                FileHandle fileHandle = new FileHandle(cleanup, channel, rebuffererFactory, compressionMetadata, length);
                return fileHandle;
            }
            catch (Throwable t)
            {
                Throwables.closeNonNullAndAddSuppressed(t, regions, channel, compressionMetadata);
                throw t;
            }
        }

        private RebuffererFactory maybeCached(ChunkReader reader)
        {
            if (chunkCache != null && chunkCache.capacity() > 0)
                return chunkCache.wrap(reader);
            return reader;
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(path='" + file() + '\'' +
               ", length=" + rebuffererFactory.fileLength() +
               ')';
    }
}
