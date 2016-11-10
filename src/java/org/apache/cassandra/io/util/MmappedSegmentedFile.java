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

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class MmappedSegmentedFile extends SegmentedFile
{
    private static final Logger logger = LoggerFactory.getLogger(MmappedSegmentedFile.class);

    // in a perfect world, MAX_SEGMENT_SIZE would be final, but we need to test with a smaller size to stay sane.
    public static long MAX_SEGMENT_SIZE = Integer.MAX_VALUE;

    /**
     * Sorted array of segment offsets and MappedByteBuffers for segments. If mmap is completely disabled, or if the
     * segment would be too long to mmap, the value for an offset will be null, indicating that we need to fall back
     * to a RandomAccessFile.
     */
    private final Segment[] segments;

    public MmappedSegmentedFile(ChannelProxy channel, long length, Segment[] segments)
    {
        super(new Cleanup(channel, segments), channel, length);
        this.segments = segments;
    }

    private MmappedSegmentedFile(MmappedSegmentedFile copy)
    {
        super(copy);
        this.segments = copy.segments;
    }

    public MmappedSegmentedFile sharedCopy()
    {
        return new MmappedSegmentedFile(this);
    }

    /**
     * @return The segment entry for the given position.
     */
    private Segment floor(long position)
    {
        assert 0 <= position && position < length: String.format("%d >= %d in %s", position, length, path());
        Segment seg = new Segment(position, null);
        int idx = Arrays.binarySearch(segments, seg);
        assert idx != -1 : String.format("Bad position %d for segments %s in %s", position, Arrays.toString(segments), path());
        if (idx < 0)
            // round down to entry at insertion point
            idx = -(idx + 2);
        return segments[idx];
    }

    /**
     * @return The segment containing the given position: must be closed after use.
     */
    public FileDataInput getSegment(long position)
    {
        Segment segment = floor(position);
        if (segment.right != null)
        {
            // segment is mmap'd
            return new ByteBufferDataInput(segment.right, path(), segment.left, (int) (position - segment.left));
        }

        // we can have single cells or partitions larger than 2Gb, which is our maximum addressable range in a single segment;
        // in this case we open as a normal random access reader
        // FIXME: brafs are unbounded, so this segment will cover the rest of the file, rather than just the row
        RandomAccessReader file = RandomAccessReader.open(channel);
        file.seek(position);
        return file;
    }

    @Override
    public long[] copyReadableBounds()
    {
        long[] bounds  = new long[segments.length + 1];
        for (int i = 0; i < segments.length; i++)
            bounds[i] = segments[i].left;
        bounds[segments.length] = length;
        return bounds;
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
    {
        final Segment[] segments;
        protected Cleanup(ChannelProxy channel, Segment[] segments)
        {
            super(channel);
            this.segments = segments;
        }

        public void tidy()
        {
            super.tidy();

            if (!FileUtils.isCleanerAvailable())
                return;

        /*
         * Try forcing the unmapping of segments using undocumented unsafe sun APIs.
         * If this fails (non Sun JVM), we'll have to wait for the GC to finalize the mapping.
         * If this works and a thread tries to access any segment, hell will unleash on earth.
         */
            try
            {
                for (Segment segment : segments)
                {
                    if (segment.right == null)
                        continue;
                    FileUtils.clean(segment.right);
                }
                logger.trace("All segments have been unmapped successfully");
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                // This is not supposed to happen
                logger.error("Error while unmapping segments", e);
            }
        }
    }

    // see CASSANDRA-10357
    public static boolean maybeRepair(CFMetaData metadata, Descriptor descriptor, IndexSummary indexSummary, SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder)
    {
        boolean mayNeedRepair = false;
        if (ibuilder instanceof Builder)
            mayNeedRepair = ((Builder) ibuilder).mayNeedRepair(descriptor.filenameFor(Component.PRIMARY_INDEX));
        if (dbuilder instanceof Builder)
            mayNeedRepair |= ((Builder) dbuilder).mayNeedRepair(descriptor.filenameFor(Component.DATA));

        if (mayNeedRepair)
            forceRepair(metadata, descriptor, indexSummary, ibuilder, dbuilder);
        return mayNeedRepair;
    }

    // if one of the index/data files have boundaries larger than we can mmap, and they were written by a version that did not guarantee correct boundaries were saved,
    // rebuild the boundaries and save them again
    private static void forceRepair(CFMetaData metadata, Descriptor descriptor, IndexSummary indexSummary, SegmentedFile.Builder ibuilder, SegmentedFile.Builder dbuilder)
    {
        if (ibuilder instanceof Builder)
            ((Builder) ibuilder).boundaries.clear();
        if (dbuilder instanceof Builder)
            ((Builder) dbuilder).boundaries.clear();

        RowIndexEntry.IndexSerializer rowIndexEntrySerializer = descriptor.version.getSSTableFormat().getIndexSerializer(metadata);
        try (RandomAccessFile raf = new RandomAccessFile(descriptor.filenameFor(Component.PRIMARY_INDEX), "r");)
        {
            long iprev = 0, dprev = 0;
            for (int i = 0; i < indexSummary.size(); i++)
            {
                // first read the position in the summary, and read the corresponding position in the data file
                long icur = indexSummary.getPosition(i);
                raf.seek(icur);
                ByteBufferUtil.readWithShortLength(raf);
                RowIndexEntry rie = rowIndexEntrySerializer.deserialize(raf, descriptor.version);
                long dcur = rie.position;

                // if these positions are small enough to map out a segment from the prior version (i.e. less than 2Gb),
                // just add these as a boundary and proceed to the next index summary record; most scenarios will be
                // served by this, keeping the cost of rebuild to a minimum.

                if (Math.max(icur - iprev , dcur - dprev) > MAX_SEGMENT_SIZE)
                {
                    // otherwise, loop over its index block, providing each RIE as a potential boundary for both files
                    raf.seek(iprev);
                    while (raf.getFilePointer() < icur)
                    {
                        // add the position of this record in the index file as an index file boundary
                        ibuilder.addPotentialBoundary(raf.getFilePointer());
                        // then read the RIE, and add its data file position as a boundary for the data file
                        ByteBufferUtil.readWithShortLength(raf);
                        rie = rowIndexEntrySerializer.deserialize(raf, descriptor.version);
                        dbuilder.addPotentialBoundary(rie.position);
                    }
                }

                ibuilder.addPotentialBoundary(icur);
                dbuilder.addPotentialBoundary(dcur);

                iprev = icur;
                dprev = dcur;
            }
        }
        catch (IOException e)
        {
            logger.error("Failed to recalculate boundaries for {}; mmap access may degrade to buffered for this file", descriptor);
        }
    }

    /**
     * Overrides the default behaviour to create segments of a maximum size.
     */
    public static class Builder extends SegmentedFile.Builder
    {
        @VisibleForTesting
        public static class Boundaries
        {
            private long[] boundaries;

            // number of boundaries we have "fixed" (i.e. have determined the final value of)
            private int fixedCount;

            public Boundaries()
            {
                // we always have a boundary of zero, so we start with a fixedCount of 1
                this(new long[8], 1);
            }

            public Boundaries(long[] boundaries, int fixedCount)
            {
                init(boundaries, fixedCount);
            }

            void init(long[] boundaries, int fixedCount)
            {
                this.boundaries = boundaries;
                this.fixedCount = fixedCount;
            }

            public void addCandidate(long candidate)
            {
                // we make sure we have room before adding another element, so that we can share the addCandidate logic statically
                boundaries = ensureCapacity(boundaries, fixedCount);
                fixedCount = addCandidate(boundaries, fixedCount, candidate);
            }

            private static int addCandidate(long[] boundaries, int fixedCount, long candidate)
            {
                // check how far we are from the last fixed boundary
                long delta = candidate - boundaries[fixedCount - 1];
                assert delta >= 0;
                if (delta != 0)
                {
                    if (delta <= MAX_SEGMENT_SIZE)
                        // overwrite the unfixed (potential) boundary if the resultant segment would still be mmappable
                        boundaries[fixedCount] = candidate;
                    else if (boundaries[fixedCount] == 0)
                        // or, if it is not initialised, we cannot make an mmapped segment here, so this is the fixed boundary
                        boundaries[fixedCount++] = candidate;
                    else
                        // otherwise, fix the prior boundary and initialise our unfixed boundary
                        boundaries[++fixedCount] = candidate;
                }
                return fixedCount;
            }

            // ensures there is room for another fixed boundary AND an unfixed candidate boundary, i.e. fixedCount + 2 items
            private static long[] ensureCapacity(long[] boundaries, int fixedCount)
            {
                if (fixedCount + 1 >= boundaries.length)
                    return Arrays.copyOf(boundaries, Math.max(fixedCount + 2, boundaries.length * 2));
                return boundaries;
            }

            void clear()
            {
                fixedCount = 1;
                Arrays.fill(boundaries, 0);
            }

            // returns the fixed boundaries, truncated to a correctly sized long[]
            public long[] truncate()
            {
                return Arrays.copyOf(boundaries, fixedCount);
            }

            // returns the finished boundaries for the provided length, truncated to a correctly sized long[]
            public long[] finish(long length, boolean isFinal)
            {
                assert length > 0;
                // ensure there's room for the length to be added
                boundaries = ensureCapacity(boundaries, fixedCount);

                // clone our current contents, so we don't corrupt them
                int fixedCount = this.fixedCount;
                long[] boundaries = this.boundaries.clone();

                // if we're finishing early, our length may be before some of our boundaries,
                // so walk backwards until our boundaries are <= length
                while (boundaries[fixedCount - 1] >= length)
                    boundaries[fixedCount--] = 0;
                if (boundaries[fixedCount] >= length)
                    boundaries[fixedCount] = 0;

                // add our length as a boundary
                fixedCount = addCandidate(boundaries, fixedCount, length);

                // if we have any unfixed boundary at the end, it's now fixed, since we're done
                if (boundaries[fixedCount] != 0)
                    fixedCount++;

                boundaries = Arrays.copyOf(boundaries, fixedCount);
                if (isFinal)
                {
                    // if this is the final one, save it
                    this.boundaries = boundaries;
                    this.fixedCount = fixedCount;
                }
                return boundaries;
            }
        }

        private final Boundaries boundaries = new Boundaries();

        public Builder()
        {
            super();
        }

        public long[] boundaries()
        {
            return boundaries.truncate();
        }

        // indicates if we may need to repair the mmapped file boundaries. this is a cheap check to see if there
        // are any spans larger than an mmap segment size, which should be rare to occur in practice.
        boolean mayNeedRepair(String path)
        {
            // old boundaries were created without the length, so add it as a candidate
            long length = new File(path).length();
            boundaries.addCandidate(length);
            long[] boundaries = this.boundaries.truncate();

            long prev = 0;
            for (long boundary : boundaries)
            {
                if (boundary - prev > MAX_SEGMENT_SIZE)
                    return true;
                prev = boundary;
            }
            return false;
        }

        public void addPotentialBoundary(long boundary)
        {
            boundaries.addCandidate(boundary);
        }

        public SegmentedFile complete(ChannelProxy channel, long overrideLength)
        {
            long length = overrideLength > 0 ? overrideLength : channel.size();
            // create the segments

            long[] boundaries = this.boundaries.finish(length, overrideLength <= 0);

            int segcount = boundaries.length - 1;
            Segment[] segments = new Segment[segcount];

            for (int i = 0; i < segcount; i++)
            {
                long start = boundaries[i];
                long size = boundaries[i + 1] - start;
                MappedByteBuffer segment = size <= MAX_SEGMENT_SIZE
                                           ? channel.map(FileChannel.MapMode.READ_ONLY, start, size)
                                           : null;
                segments[i] = new Segment(start, segment);
            }

            return new MmappedSegmentedFile(channel, length, segments);
        }

        @Override
        public void serializeBounds(DataOutput out) throws IOException
        {
            super.serializeBounds(out);
            long[] boundaries = this.boundaries.truncate();
            out.writeInt(boundaries.length);
            for (long boundary : boundaries)
                out.writeLong(boundary);
        }

        @Override
        public void deserializeBounds(DataInput in) throws IOException
        {
            super.deserializeBounds(in);

            int size = in.readInt();
            long[] boundaries = new long[size];
            for (int i = 0; i < size; i++)
                boundaries[i] = in.readLong();

            this.boundaries.init(boundaries, size);
        }
    }
}
