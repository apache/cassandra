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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

import static org.apache.cassandra.utils.Throwables.maybeFail;

/**
 * Abstracts a read-only file that has been split into segments, each of which can be represented by an independent
 * FileDataInput. Allows for iteration over the FileDataInputs, or random access to the FileDataInput for a given
 * position.
 *
 * The JVM can only map up to 2GB at a time, so each segment is at most that size when using mmap i/o. If a segment
 * would need to be longer than 2GB, that segment will not be mmap'd, and a new RandomAccessFile will be created for
 * each access to that segment.
 */
public abstract class SegmentedFile extends SharedCloseableImpl
{
    public final ChannelProxy channel;
    public final int bufferSize;
    public final long length;

    // This differs from length for compressed files (but we still need length for
    // SegmentIterator because offsets in the file are relative to the uncompressed size)
    public final long onDiskLength;

    /**
     * Use getBuilder to get a Builder to construct a SegmentedFile.
     */
    SegmentedFile(Cleanup cleanup, ChannelProxy channel, int bufferSize, long length)
    {
        this(cleanup, channel, bufferSize, length, length);
    }

    protected SegmentedFile(Cleanup cleanup, ChannelProxy channel, int bufferSize, long length, long onDiskLength)
    {
        super(cleanup);
        this.channel = channel;
        this.bufferSize = bufferSize;
        this.length = length;
        this.onDiskLength = onDiskLength;
    }

    protected SegmentedFile(SegmentedFile copy)
    {
        super(copy);
        channel = copy.channel;
        bufferSize = copy.bufferSize;
        length = copy.length;
        onDiskLength = copy.onDiskLength;
    }

    public String path()
    {
        return channel.filePath();
    }

    protected static class Cleanup implements RefCounted.Tidy
    {
        final ChannelProxy channel;
        protected Cleanup(ChannelProxy channel)
        {
            this.channel = channel;
        }

        public String name()
        {
            return channel.filePath();
        }

        public void tidy()
        {
            channel.close();
        }
    }

    public abstract SegmentedFile sharedCopy();

    public RandomAccessReader createReader()
    {
        return new RandomAccessReader.Builder(channel)
               .overrideLength(length)
               .bufferSize(bufferSize)
               .build();
    }

    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return new RandomAccessReader.Builder(channel)
               .overrideLength(length)
               .bufferSize(bufferSize)
               .limiter(limiter)
               .build();
    }

    public FileDataInput createReader(long position)
    {
        RandomAccessReader reader = createReader();
        reader.seek(position);
        return reader;
    }

    public void dropPageCache(long before)
    {
        CLibrary.trySkipCache(channel.getFileDescriptor(), 0, before, path());
    }

    /**
     * @return A SegmentedFile.Builder.
     */
    public static Builder getBuilder(Config.DiskAccessMode mode, boolean compressed)
    {
        return compressed ? new CompressedSegmentedFile.Builder(null)
                          : mode == Config.DiskAccessMode.mmap ? new MmappedSegmentedFile.Builder()
                                                               : new BufferedSegmentedFile.Builder();
    }

    public static Builder getCompressedBuilder(CompressedSequentialWriter writer)
    {
        return new CompressedSegmentedFile.Builder(writer);
    }

    /**
     * Collects potential segmentation points in an underlying file, and builds a SegmentedFile to represent it.
     */
    public static abstract class Builder implements AutoCloseable
    {
        private ChannelProxy channel;

        /**
         * Called after all potential boundaries have been added to apply this Builder to a concrete file on disk.
         * @param channel The channel to the file on disk.
         */
        protected abstract SegmentedFile complete(ChannelProxy channel, int bufferSize, long overrideLength);

        @SuppressWarnings("resource") // SegmentedFile owns channel
        private SegmentedFile complete(String path, int bufferSize, long overrideLength)
        {
            ChannelProxy channelCopy = getChannel(path);
            try
            {
                return complete(channelCopy, bufferSize, overrideLength);
            }
            catch (Throwable t)
            {
                channelCopy.close();
                throw t;
            }
        }

        public SegmentedFile buildData(Descriptor desc, StatsMetadata stats, IndexSummaryBuilder.ReadableBoundary boundary)
        {
            return complete(desc.filenameFor(Component.DATA), bufferSize(stats), boundary.dataLength);
        }

        public SegmentedFile buildData(Descriptor desc, StatsMetadata stats)
        {
            return complete(desc.filenameFor(Component.DATA), bufferSize(stats), -1L);
        }

        public SegmentedFile buildIndex(Descriptor desc, IndexSummary indexSummary, IndexSummaryBuilder.ReadableBoundary boundary)
        {
            return complete(desc.filenameFor(Component.PRIMARY_INDEX), bufferSize(desc, indexSummary), boundary.indexLength);
        }

        public SegmentedFile buildIndex(Descriptor desc, IndexSummary indexSummary)
        {
            return complete(desc.filenameFor(Component.PRIMARY_INDEX), bufferSize(desc, indexSummary), -1L);
        }

        private static int bufferSize(StatsMetadata stats)
        {
            return bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        }

        private static int bufferSize(Descriptor desc, IndexSummary indexSummary)
        {
            File file = new File(desc.filenameFor(Component.PRIMARY_INDEX));
            return bufferSize(file.length() / indexSummary.size());
        }

        /**
            Return the buffer size for a given record size. For spinning disks always add one page.
            For solid state disks only add one page if the chance of crossing to the next page is more
            than a predifined value, @see Config.disk_optimization_page_cross_chance.
         */
        static int bufferSize(long recordSize)
        {
            Config.DiskOptimizationStrategy strategy = DatabaseDescriptor.getDiskOptimizationStrategy();
            if (strategy == Config.DiskOptimizationStrategy.ssd)
            {
                // The crossing probability is calculated assuming a uniform distribution of record
                // start position in a page, so it's the record size modulo the page size divided by
                // the total page size.
                double pageCrossProbability = (recordSize % 4096) / 4096.;
                // if the page cross probability is equal or bigger than disk_optimization_page_cross_chance we add one page
                if ((pageCrossProbability - DatabaseDescriptor.getDiskOptimizationPageCrossChance()) > -1e-16)
                    recordSize += 4096;

                return roundBufferSize(recordSize);
            }
            else if (strategy == Config.DiskOptimizationStrategy.spinning)
            {
                return roundBufferSize(recordSize + 4096);
            }
            else
            {
                throw new IllegalStateException("Unsupported disk optimization strategy: " + strategy);
            }
        }

        /**
           Round up to the next multiple of 4k but no more than 64k
         */
        static int roundBufferSize(long size)
        {
            if (size <= 0)
                return 4096;

            size = (size + 4095) & ~4095;
            return (int)Math.min(size, 1 << 16);
        }

        public void serializeBounds(DataOutput out, Version version) throws IOException
        {
            if (!version.hasBoundaries())
                return;

            out.writeUTF(DatabaseDescriptor.getDiskAccessMode().name());
        }

        public void deserializeBounds(DataInput in, Version version) throws IOException
        {
            if (!version.hasBoundaries())
                return;

            if (!in.readUTF().equals(DatabaseDescriptor.getDiskAccessMode().name()))
                throw new IOException("Cannot deserialize SSTable Summary component because the DiskAccessMode was changed!");
        }

        public Throwable close(Throwable accumulate)
        {
            if (channel != null)
                return channel.close(accumulate);

            return accumulate;
        }

        public void close()
        {
            maybeFail(close(null));
        }

        private ChannelProxy getChannel(String path)
        {
            if (channel != null)
            {
                // This is really fragile, both path and channel.filePath()
                // must agree, i.e. they both must be absolute or both relative
                // eventually we should really pass the filePath to the builder
                // constructor and remove this
                if (channel.filePath().equals(path))
                    return channel.sharedCopy();
                else
                    channel.close();
            }

            channel = new ChannelProxy(path);
            return channel.sharedCopy();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(path='" + path() + '\'' +
               ", length=" + length +
               ')';
}
}
