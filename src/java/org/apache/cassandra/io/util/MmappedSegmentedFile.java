package org.apache.cassandra.io.util;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MmappedSegmentedFile extends SegmentedFile
{
    // in a perfect world, MAX_SEGMENT_SIZE would be final, but we need to test with a smaller size to stay sane.
    public static long MAX_SEGMENT_SIZE = Integer.MAX_VALUE;

    /**
     * Sorted array of segment offsets and MappedByteBuffers for segments. If mmap is completely disabled, or if the
     * segment would be too long to mmap, the value for an offset will be null, indicating that we need to fall back
     * to a RandomAccessFile.
     */
    private final Segment[] segments;

    public MmappedSegmentedFile(String path, long length, Segment[] segments)
    {
        super(path, length);
        this.segments = segments;
    }

    /**
     * @return The segment entry for the given position.
     */
    private Segment floor(long position)
    {
        assert 0 <= position && position < length: position + " vs " + length;
        Segment seg = new Segment(position, null);
        int idx = Arrays.binarySearch(segments, seg);
        assert idx != -1 : "Bad position " + position + " in segments " + Arrays.toString(segments);
        if (idx < 0)
            // round down to entry at insertion point
            idx = -(idx + 2);
        return segments[idx];
    }

    /**
     * @return The segment containing the given position: must be closed after use.
     */
    public FileDataInput getSegment(long position, int bufferSize)
    {
        Segment segment = floor(position);
        if (segment.right != null)
        {
            // segment is mmap'd
            return new MappedFileDataInput(segment.right, path, (int) (position - segment.left));
        }

        // not mmap'd: open a braf covering the segment
        try
        {
            // FIXME: brafs are unbounded, so this segment will cover the rest of the file, rather than just the row
            BufferedRandomAccessFile file = new BufferedRandomAccessFile(path, "r", bufferSize);
            file.seek(position);
            return file;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Overrides the default behaviour to create segments of a maximum size.
     */
    static class Builder extends SegmentedFile.Builder
    {
        // planned segment boundaries
        private final List<Long> boundaries;

        // offset of the open segment (first segment begins at 0).
        private long currentStart = 0;

        // current length of the open segment.
        // used to allow merging multiple too-large-to-mmap segments, into a single buffered segment.
        private long currentSize = 0;

        public Builder()
        {
            super();
            boundaries = new ArrayList<Long>();
            boundaries.add(0L);
        }

        public void addPotentialBoundary(long boundary)
        {
            if (boundary - currentStart <= MAX_SEGMENT_SIZE)
            {
                // boundary fits into current segment: expand it
                currentSize = boundary - currentStart;
                return;
            }

            // close the current segment to try and make room for the boundary
            if (currentSize > 0)
            {
                currentStart += currentSize;
                boundaries.add(currentStart);
            }
            currentSize = boundary - currentStart;

            // if we couldn't make room, the boundary needs its own segment
            if (currentSize > MAX_SEGMENT_SIZE)
            {
                currentStart = boundary;
                boundaries.add(currentStart);
                currentSize = 0;
            }
        }

        public SegmentedFile complete(String path)
        {
            long length = new File(path).length();
            // add a sentinel value == length
            boundaries.add(Long.valueOf(length));
            // create the segments
            return new MmappedSegmentedFile(path, length, createSegments(path));
        }

        private Segment[] createSegments(String path)
        {
            int segcount = boundaries.size() - 1;
            Segment[] segments = new Segment[segcount];
            RandomAccessFile raf = null;
            try
            {
                raf = new RandomAccessFile(path, "r");
                for (int i = 0; i < segcount; i++)
                {
                    long start = boundaries.get(i);
                    long size = boundaries.get(i + 1) - start;
                    MappedByteBuffer segment = size <= MAX_SEGMENT_SIZE
                                               ? raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size)
                                               : null;
                    segments[i] = new Segment(start, segment);
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                FileUtils.closeQuietly(raf);
            }
            return segments;
        }
    }
}
