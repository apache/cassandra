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
import java.nio.MappedByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.Pair;
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
    public final long length;

    // This differs from length for compressed files (but we still need length for
    // SegmentIterator because offsets in the file are relative to the uncompressed size)
    public final long onDiskLength;

    /**
     * Use getBuilder to get a Builder to construct a SegmentedFile.
     */
    SegmentedFile(Cleanup cleanup, ChannelProxy channel, long length)
    {
        this(cleanup, channel, length, length);
    }

    protected SegmentedFile(Cleanup cleanup, ChannelProxy channel, long length, long onDiskLength)
    {
        super(cleanup);
        this.channel = channel;
        this.length = length;
        this.onDiskLength = onDiskLength;
    }

    public SegmentedFile(SegmentedFile copy)
    {
        super(copy);
        channel = copy.channel;
        length = copy.length;
        onDiskLength = copy.onDiskLength;
    }

    public String path()
    {
        return channel.filePath();
    }

    protected static abstract class Cleanup implements RefCounted.Tidy
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
        return RandomAccessReader.open(channel, length);
    }

    public RandomAccessReader createThrottledReader(RateLimiter limiter)
    {
        assert limiter != null;
        return ThrottledReader.open(channel, length, limiter);
    }

    public FileDataInput getSegment(long position)
    {
        RandomAccessReader reader = createReader();
        reader.seek(position);
        return reader;
    }

    public void dropPageCache(long before)
    {
        CLibrary.trySkipCache(channel.getFileDescriptor(), 0, before);
    }

    /**
     * @return A SegmentedFile.Builder.
     */
    public static Builder getBuilder(Config.DiskAccessMode mode, boolean compressed)
    {
        return compressed ? new CompressedPoolingSegmentedFile.Builder(null)
                          : mode == Config.DiskAccessMode.mmap ? new MmappedSegmentedFile.Builder()
                                                               : new BufferedPoolingSegmentedFile.Builder();
    }

    public static Builder getCompressedBuilder(CompressedSequentialWriter writer)
    {
        return new CompressedPoolingSegmentedFile.Builder(writer);
    }

    /**
     * @return An Iterator over segments, beginning with the segment containing the given position: each segment must be closed after use.
     */
    public Iterator<FileDataInput> iterator(long position)
    {
        return new SegmentIterator(position);
    }

    /**
     * Collects potential segmentation points in an underlying file, and builds a SegmentedFile to represent it.
     */
    public static abstract class Builder implements AutoCloseable
    {
        private ChannelProxy channel;

        /**
         * Adds a position that would be a safe place for a segment boundary in the file. For a block/row based file
         * format, safe boundaries are block/row edges.
         * @param boundary The absolute position of the potential boundary in the file.
         */
        public abstract void addPotentialBoundary(long boundary);

        /**
         * Called after all potential boundaries have been added to apply this Builder to a concrete file on disk.
         * @param channel The channel to the file on disk.
         */
        protected abstract SegmentedFile complete(ChannelProxy channel, long overrideLength);

        public SegmentedFile complete(String path)
        {
            return complete(getChannel(path), -1);
        }

        public SegmentedFile complete(String path, long overrideLength)
        {
            return complete(getChannel(path), overrideLength);
        }

        public void serializeBounds(DataOutput out) throws IOException
        {
            out.writeUTF(DatabaseDescriptor.getDiskAccessMode().name());
        }

        public void deserializeBounds(DataInput in) throws IOException
        {
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
                if (channel.filePath().equals(path))
                    return channel.sharedCopy();
                else
                    channel.close();
            }

            channel = new ChannelProxy(path);
            return channel.sharedCopy();
        }
    }

    static final class Segment extends Pair<Long, MappedByteBuffer> implements Comparable<Segment>
    {
        public Segment(long offset, MappedByteBuffer segment)
        {
            super(offset, segment);
        }

        public final int compareTo(Segment that)
        {
            return (int)Math.signum(this.left - that.left);
        }
    }

    /**
     * A lazy Iterator over segments in forward order from the given position.  It is caller's responsibility
     * to close the FileDataIntputs when finished.
     */
    final class SegmentIterator implements Iterator<FileDataInput>
    {
        private long nextpos;
        public SegmentIterator(long position)
        {
            this.nextpos = position;
        }

        public boolean hasNext()
        {
            return nextpos < length;
        }

        public FileDataInput next()
        {
            long position = nextpos;
            if (position >= length)
                throw new NoSuchElementException();

            FileDataInput segment = getSegment(nextpos);
            try
            {
                nextpos = nextpos + segment.bytesRemaining();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, path());
            }
            return segment;
        }

        public void remove() { throw new UnsupportedOperationException(); }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(path='" + path() + "'" +
               ", length=" + length +
               ")";
}
}
