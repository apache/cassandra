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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

import static java.util.stream.Stream.of;
import static org.apache.cassandra.utils.Throwables.perform;

public class MmappedRegions extends SharedCloseableImpl
{
    /** In a perfect world, MAX_SEGMENT_SIZE would be final, but we need to test with a smaller size */
    public static int MAX_SEGMENT_SIZE = Integer.MAX_VALUE;

    /** When we need to grow the arrays, we add this number of region slots */
    static final int REGION_ALLOC_SIZE = 15;

    /** The original state, which is shared with the tidier and
     * contains all the regions mapped so far. It also
     * does the actual mapping. */
    private final State state;

    /** A copy of the latest state. We update this each time the original state is
     * updated and we share this with copies. If we are a copy, then this
     * is null. Copies can only access existing regions, they cannot create
     * new ones. This is for thread safety and because MmappedRegions is
     * reference counted, only the original state will be cleaned-up,
     * therefore only the original state should create new mapped regions.
     */
    private volatile State copy;

    private MmappedRegions(ChannelProxy channel, CompressionMetadata metadata, long length)
    {
        this(new State(channel), metadata, length);
    }

    private MmappedRegions(State state, CompressionMetadata metadata, long length)
    {
        super(new Tidier(state));

        this.state = state;

        if (metadata != null)
        {
            assert length == 0 : "expected no length with metadata";
            updateState(metadata);
        }
        else if (length > 0)
        {
            updateState(length);
        }

        this.copy = new State(state);
    }

    private MmappedRegions(MmappedRegions original)
    {
        super(original);
        this.state = original.copy;
    }

    public static MmappedRegions empty(ChannelProxy channel)
    {
        return new MmappedRegions(channel, null, 0);
    }

    /**
     * @param channel file to map. the MmappedRegions instance will hold shared copy of given channel.
     * @param metadata
     * @return new instance
     */
    public static MmappedRegions map(ChannelProxy channel, CompressionMetadata metadata)
    {
        if (metadata == null)
            throw new IllegalArgumentException("metadata cannot be null");

        return new MmappedRegions(channel, metadata, 0);
    }

    public static MmappedRegions map(ChannelProxy channel, long length)
    {
        if (length <= 0)
            throw new IllegalArgumentException("Length must be positive");

        return new MmappedRegions(channel, null, length);
    }

    /**
     * @return a snapshot of the memory mapped regions. The snapshot can
     * only use existing regions, it cannot create new ones.
     */
    public MmappedRegions sharedCopy()
    {
        return new MmappedRegions(this);
    }

    private boolean isCopy()
    {
        return copy == null;
    }

    public void extend(long length)
    {
        if (length < 0)
            throw new IllegalArgumentException("Length must not be negative");

        assert !isCopy() : "Copies cannot be extended";

        if (length <= state.length)
            return;

        updateState(length);
        copy = new State(state);
    }

    private void updateState(long length)
    {
        state.length = length;
        long pos = state.getPosition();
        while (pos < length)
        {
            long size = Math.min(MAX_SEGMENT_SIZE, length - pos);
            state.add(pos, size);
            pos += size;
        }
    }

    private void updateState(CompressionMetadata metadata)
    {
        long offset = 0;
        long lastSegmentOffset = 0;
        long segmentSize = 0;

        while (offset < metadata.dataLength)
        {
            CompressionMetadata.Chunk chunk = metadata.chunkFor(offset);

            //Reached a new mmap boundary
            if (segmentSize + chunk.length + 4 > MAX_SEGMENT_SIZE)
            {
                if (segmentSize > 0)
                {
                    state.add(lastSegmentOffset, segmentSize);
                    lastSegmentOffset += segmentSize;
                    segmentSize = 0;
                }
            }

            segmentSize += chunk.length + 4; //checksum
            offset += metadata.chunkLength();
        }

        if (segmentSize > 0)
            state.add(lastSegmentOffset, segmentSize);

        state.length = lastSegmentOffset + segmentSize;
    }

    public boolean isValid(ChannelProxy channel)
    {
        return state.isValid(channel);
    }

    public boolean isEmpty()
    {
        return state.isEmpty();
    }

    public Region floor(long position)
    {
        assert !isCleanedUp() : "Attempted to use closed region";
        return state.floor(position);
    }
    
    public void closeQuietly()
    {
        Throwable err = close(null);
        if (err != null)
        {
            JVMStabilityInspector.inspectThrowable(err);

            // This is not supposed to happen
            LoggerFactory.getLogger(getClass()).error("Error while closing mmapped regions", err);
        }
    }

    public static final class Region implements Rebufferer.BufferHolder
    {
        public final long offset;
        public final ByteBuffer buffer;

        public Region(long offset, ByteBuffer buffer)
        {
            this.offset = offset;
            this.buffer = buffer;
        }

        public ByteBuffer buffer()
        {
            return buffer.duplicate();
        }

        public long offset()
        {
            return offset;
        }

        public long end()
        {
            return offset + buffer.capacity();
        }

        public void release()
        {
            // only released after no readers are present
        }
    }

    private static final class State
    {
        /** The file channel */
        private final ChannelProxy channel;

        /** An array of region buffers, synchronized with offsets */
        private ByteBuffer[] buffers;

        /** An array of region offsets, synchronized with buffers */
        private long[] offsets;

        /** The maximum file length we have mapped */
        private long length;

        /** The index to the last region added */
        private int last;

        private State(ChannelProxy channel)
        {
            this.channel = channel.sharedCopy();
            this.buffers = new ByteBuffer[REGION_ALLOC_SIZE];
            this.offsets = new long[REGION_ALLOC_SIZE];
            this.length = 0;
            this.last = -1;
        }

        private State(State original)
        {
            this.channel = original.channel;
            this.buffers = original.buffers;
            this.offsets = original.offsets;
            this.length = original.length;
            this.last = original.last;
        }

        private boolean isEmpty()
        {
            return last < 0;
        }

        private boolean isValid(ChannelProxy channel)
        {
            return this.channel.filePath().equals(channel.filePath());
        }

        private Region floor(long position)
        {
            assert 0 <= position && position <= length : String.format("%d > %d", position, length);

            int idx = Arrays.binarySearch(offsets, 0, last +1, position);
            assert idx != -1 : String.format("Bad position %d for regions %s, last %d in %s", position, Arrays.toString(offsets), last, channel);
            if (idx < 0)
                idx = -(idx + 2); // round down to entry at insertion point

            return new Region(offsets[idx], buffers[idx]);
        }

        private long getPosition()
        {
            return last < 0 ? 0 : offsets[last] + buffers[last].capacity();
        }

        private void add(long pos, long size)
        {
            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);

            ++last;

            if (last == offsets.length)
            {
                offsets = Arrays.copyOf(offsets, offsets.length + REGION_ALLOC_SIZE);
                buffers = Arrays.copyOf(buffers, buffers.length + REGION_ALLOC_SIZE);
            }

            offsets[last] = pos;
            buffers[last] = buffer;
        }

        private Throwable close(Throwable accumulate)
        {
            accumulate = channel.close(accumulate);

            /*
             * Try forcing the unmapping of segments using undocumented unsafe sun APIs.
             * If this fails (non Sun JVM), we'll have to wait for the GC to finalize the mapping.
             * If this works and a thread tries to access any segment, hell will unleash on earth.
             */
            if (!FileUtils.isCleanerAvailable)
                return accumulate;

            return perform(accumulate, channel.filePath(), Throwables.FileOpType.READ,
                           of(buffers)
                           .map((buffer) ->
                                () ->
                                {
                                    if (buffer != null)
                                        FileUtils.clean(buffer);
                                }));
        }
    }

    public static final class Tidier implements RefCounted.Tidy
    {
        final State state;

        Tidier(State state)
        {
            this.state = state;
        }

        public String name()
        {
            return state.channel.filePath();
        }

        public void tidy()
        {
            try
            {
                Throwables.maybeFail(state.close(null));
            }
            catch (Exception e)
            {
                throw new FSReadError(e, state.channel.filePath());
            }
        }
    }

}
