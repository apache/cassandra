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
package org.apache.cassandra.service.tracking;

import com.google.common.base.Preconditions;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.MutationId;

import java.util.Arrays;

import static org.apache.cassandra.db.MutationId.*;

public class SequenceIds
{
    private static final int INITIAL_CAPACITY = 16;

    // even index is range start, odd index is range end (inclusive)
    private long[] bounds;
    private int size;

    public SequenceIds()
    {
        size = 0;
        bounds = new long[INITIAL_CAPACITY];
    }

    private SequenceIds(long[] bounds)
    {
        this.bounds = bounds;
        this.size = bounds.length;
    }

    private SequenceIds(long[] bounds, int size)
    {
        this.bounds = bounds;
        this.size = size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        SequenceIds that = (SequenceIds) o;
        return size == that.size && Arrays.equals(bounds, 0, size, that.bounds, 0, size);
    }

    @Override
    public int hashCode()
    {
        int result = Integer.hashCode(size);
        for (int i = 0; i < size; i++)
            result = 31 * result + Long.hashCode(bounds[i]);
        return result;
    }

    public SequenceIds copy()
    {
        return new SequenceIds(Arrays.copyOf(bounds, size));
    }

    public int rangeCount()
    {
        return size / 2;
    }

    public int idCount()
    {
        int count = 0, i = 0;
        while (i < size)
        {
            long start = bounds[i++];
            long   end = bounds[i++];
            count += offset(end) - offset(start) + 1;
        }
        return count;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("{");
        int i = 0;
        while (i < size)
        {
            long start = bounds[i++];
            long   end = bounds[i++];
            builder.append('[')
                   .append('<').append(offset(start)).append(',').append(timestamp(start)).append('>')
                   .append(',')
                   .append('<').append(offset(end)).append(',').append(timestamp(end)).append('>')
                   .append(']');
            if (i < size) builder.append(',');
        }
        builder.append('}');
        return builder.toString();
    }

    public boolean contains(long id)
    {
        if (size == 0)
            return false;

        int pos = Arrays.binarySearch(bounds, 0, size, id);
        if (pos >= 0) return true; // matches one of the bounds

        pos = -pos - 1;
        return ((pos - 1) % 2 == 0); // id falls within bounds of an existing range if the bound to the left is an open one
    }

    public void digest(Digest digest)
    {
        digest.updateWithInt(size);
        for (int i = 0; i < size; i++)
            digest.updateWithLong(bounds[i]);
    }

    public void addAll(SequenceIds other, RangeConsumer onAdded)
    {
        for (int i = 0; i < other.size; i += 2)
            add(other.bounds[i], other.bounds[i + 1], onAdded);
    }

    public void addAll(SequenceIds other)
    {
        addAll(other, RangeConsumer.NONE);
    }

    public boolean add(long id)
    {
        if (size == 0)
        {
            append(id, id);
            return true;
        }

        int pos = Arrays.binarySearch(bounds, 0, size, id);
        if (pos >= 0) return false; // matches one of the bounds

        pos = -pos - 1;
        if (pos == size) // after all existing ranges
        {
            if (offset(bounds[size - 1]) == offset(id) - 1)
                bounds[size - 1] = id; // extend the last range
            else
                append(id, id); // append a new single-id range

            return true;
        }
        else if (pos == 0) // before all existing ranges
        {
            if (offset(bounds[0]) == offset(id) + 1)
                bounds[0] = id; // extend the first range
            else
                insert(0, id, id); // prepend a new single-id range

            return true;
        }
        else if ((pos - 1) % 2 == 0) // id falls within bounds of an existing range (bound to the left is an open bound)
        {
            return false;
        }

        // between two existing ranges
        boolean extendsPrev = offset(bounds[pos - 1]) == offset(id) - 1;
        boolean extendsNext = offset(bounds[pos]) == offset(id) + 1;

        if (extendsPrev && extendsNext) // closes the gap between two adjacent ranges
        {
            bounds[pos - 1] = bounds[pos + 1];
            System.arraycopy(bounds, pos + 2, bounds, pos, size - pos - 2);
            bounds[--size] = 0;
            bounds[--size] = 0;
        }
        else if (extendsPrev)
        {
            bounds[pos - 1] = id;
        }
        else if (extendsNext)
        {
            bounds[pos] = id;
        }
        else
        {
            insert(pos, id, id);
        }

        return true;
    }

    private static int rangeStart(int range)
    {
        return range * 2;
    }

    private static int rangeEnd(int range)
    {
        return rangeStart(range) + 1;
    }

    private static long sequenceId(int sequence)
    {
        return MutationId.sequenceId(sequence, 0);
    }

    private enum AddAction
    {
        INSERT, MOVE, INCLUDE;

        boolean move()
        {
            return this == MOVE;
        }

        boolean include()
        {
            return this == INCLUDE;
        }

        boolean insert()
        {
            return this == INSERT;
        }

        boolean isMoveOrInclude()
        {
            return this == MOVE || this == INCLUDE;
        }
    }

    public boolean add(long start, long end, RangeConsumer onAdded)
    {
        if (size == 0)
        {
            append(start, end);
            return true;
        }

        if (start == end)
        {
            boolean added = add(start);
            if (added)
                onAdded.consume(start, end);
            return added;
        }

        Preconditions.checkArgument(start < end);
        int spos = Arrays.binarySearch(bounds, 0, size, start);
        int epos = Arrays.binarySearch(bounds, 0, size, end);

        if (spos >= 0 && spos % 2 == 0 && epos == spos + 1) return false; // matches an existing bound

        if (spos < 0) spos = -spos - 1;
        if (epos < 0) epos = -epos - 1;

        int numRanges = rangeCount();
        int sRange = Math.min(spos/2, numRanges - 1);
        int eRange = Math.min(epos/2, numRanges - 1);

        AddAction sMerge;
        {
            int sOffset = offset(start);
            int rStart = offset(bounds[rangeStart(sRange)]);
            int rEnd = offset(bounds[rangeEnd(sRange)]);
            if (sOffset >= rStart)
            {
                // already included in the range or adjacent to range end
                sMerge = sOffset <= rEnd + 1
                        ? AddAction.INCLUDE  // included in the range
                        : AddAction.INSERT; // past the end of the range
            }
            else if (sRange > 0 && sOffset == offset(bounds[rangeEnd(sRange-1)]) + 1)
            {
                // adjacent to the previous range, so say we're included in it to merge
                sRange--;
                sMerge = AddAction.INCLUDE;
            }
            else
            {
                sMerge = AddAction.MOVE;
            }
        }

        AddAction eMerge;
        {
            int eOffset = offset(end);
            int rStart = offset(bounds[rangeStart(eRange)]);
            int rEnd = offset(bounds[rangeEnd(eRange)]);

            if (eOffset <= rEnd)
            {
                if (eOffset >= rStart - 1)
                {
                    // included in the range or adjacent to range start
                    eMerge = AddAction.INCLUDE;
                }
                else if (sRange == eRange - 1)
                {
                    // if we're before the start of this range, and the start is assigned to
                    // the previous range, then we should just extend the previous range
                    eRange--;
                    eMerge = AddAction.MOVE;
                }
                else
                {
                    // before the start of the range
                    eMerge = AddAction.INSERT;
                }
            }
            else if (eRange < numRanges - 1 && eOffset == offset(bounds[rangeStart(eRange+1)]) - 1)
            {
                // adjacent to the next range, so say we're included in it to merge
                eRange++;
                eMerge = AddAction.INCLUDE;
            }

            else
            {
                eMerge = AddAction.MOVE;
            }
        }

        // this range isn't adjacent and doesn't intersect any existing, so create a new range
        if (sMerge.move() && eMerge.insert())
        {
            Preconditions.checkState(sRange == eRange);
            onAdded.consume(start, end);
            insert(rangeStart(sRange), start, end);
            return true;
        }

        // this should only happen if we're adding a range to the very end of the set
        if (sMerge.insert() && eMerge.move())
        {
            Preconditions.checkState(sRange == eRange);
            Preconditions.checkState(sRange == numRanges - 1);
            onAdded.consume(start, end);
            append(start, end);
            return true;
        }

        boolean adjusted = false;
        if (sMerge.move())
        {
            onAdded.consume(start, sequenceId(offset(bounds[rangeStart(sRange)]) - 1));
            bounds[rangeStart(sRange)] = start;
            adjusted = true;
        }

        // combine existing ranges
        if (sRange != eRange)
        {
            Preconditions.checkState(sMerge.isMoveOrInclude());
            Preconditions.checkState(eMerge.isMoveOrInclude());

            adjusted = true;
            // report merged ranges
            for (int i = sRange; i < eRange; i++)
            {
                int sEnd = offset(bounds[rangeEnd(i)]);
                int eStart = offset(bounds[rangeStart(i + 1)]);
                onAdded.consume(sequenceId(sEnd + 1), sequenceId(eStart - 1));
            }

            // move array back -
            int dstIdx = rangeEnd(sRange);
            int srcIdx = rangeEnd(eRange);
            System.arraycopy(bounds, srcIdx, bounds, dstIdx, size - srcIdx);
            while (eRange > sRange)
            {
                eRange--;
                bounds[--size] = 0;
                bounds[--size] = 0;
            }
        }

        if (eMerge.move())
        {
            onAdded.consume(sequenceId(offset(bounds[rangeEnd(eRange)]) + 1), end);
            bounds[rangeEnd(eRange)] = end;
            adjusted = true;
        }

        return adjusted;
    }

    public boolean add(long start, long end)
    {
        return add(start, end, RangeConsumer.NONE);
    }

    private void insert(int pos, long start, long end)
    {
        if (bounds.length == size)
        {
            long[] newBounds = new long[bounds.length * 2];
            System.arraycopy(bounds, 0, newBounds, 0, pos);
            System.arraycopy(bounds, pos, newBounds, pos + 2, size - pos);
            bounds = newBounds;
        }
        else
        {
            System.arraycopy(bounds, pos, bounds, pos + 2, size - pos);
        }
        bounds[pos] = start;
        bounds[pos + 1] = end;
        size += 2;
    }

    private void append(long start, long end)
    {
        if (bounds.length == size)
        {
            long[] newBounds = new long[bounds.length * 2];
            System.arraycopy(bounds, 0, newBounds, 0, bounds.length);
            bounds = newBounds;
        }
        bounds[size++] = start;
        bounds[size++] = end;
    }

    public void append(long id)
    {
        if (size == 0)
        {
            append(id, id);
            return;
        }

        long tail = bounds[size - 1];
        if (offset(id) <= offset(tail))
            throw new IllegalArgumentException("Can't append " + MutationId.sequenceString(id) + " to " + MutationId.sequenceString(tail));

        if (offset(id) == offset(tail) + 1)
            bounds[size-1] = id;
        else
            append(id, id);
    }

    private static class SetSupport
    {
        private static final long NO_SPLIT_SENTINEL = Long.MIN_VALUE;

        private enum RangeOverlap
        {
            BEFORE, BEFORE_ADJACENT, AFTER, AFTER_ADJACENT, INTERSECTING
        }

        private static RangeOverlap calculateRangeOverlap(long aSplit, long[] a, int aRange, long[] b, int bRange)
        {
            long aStart = aSplit != NO_SPLIT_SENTINEL ? aSplit : a[rangeStart(aRange)];
            long aEnd = a[rangeEnd(aRange)];
            Preconditions.checkState(aStart <= aEnd);

            long bStart = b[rangeStart(bRange)];
            long bEnd = b[rangeEnd(bRange)];
            Preconditions.checkState(bStart <= bEnd);

            if (aEnd < bStart)
                return offset(aEnd) == offset(bStart) - 1 ? RangeOverlap.BEFORE_ADJACENT : RangeOverlap.BEFORE;

            if (bEnd < aStart)
                return offset(bEnd) == offset(aStart) - 1 ? RangeOverlap.AFTER_ADJACENT : RangeOverlap.AFTER;

            return RangeOverlap.INTERSECTING;
        }

        private static RangeOverlap calculateRangeOverlap(long[] a, int aRange, long[] b, int bRange)
        {
            return calculateRangeOverlap(NO_SPLIT_SENTINEL, a, aRange, b, bRange);

        }

        private static long[] ensureCapacity(long[] ids, int capacity, int expectedMaxCapacity)
        {
            Preconditions.checkArgument(capacity > 0);
            if (capacity <= ids.length)
                return ids;

            int newCapacity = capacity * 2;

            // if we overflowed, set to max
            if (newCapacity < 0)
                newCapacity = Integer.MAX_VALUE;

            if (newCapacity > expectedMaxCapacity && ids.length < expectedMaxCapacity)
                newCapacity = expectedMaxCapacity;

            long[] newIds = new long[newCapacity];
            System.arraycopy(ids, 0, newIds, 0, ids.length);

            return newIds;
        }

        private static SequenceIds addRemainder(long dstSplit, long[] dst, int dstRange, long[] src, int srcRange, int srcNumRanges)
        {
            int capacity = (dstRange + srcNumRanges - srcRange) * 2;
            dst = ensureCapacity(dst, capacity, capacity);
            while (srcRange < srcNumRanges)
            {
                long addStart = dstSplit != NO_SPLIT_SENTINEL ? dstSplit : src[rangeStart(srcRange)];
                long addEnd = src[rangeEnd(srcRange)];
                // extend last range if possible
                if (dstRange > 0 && offset(addStart) <= offset(dst[rangeEnd(dstRange - 1)]) + 1)
                {
                    dst[rangeEnd(dstRange - 1)] = addEnd;
                }
                else
                {
                    dst[rangeStart(dstRange)] = addStart;
                    dst[rangeEnd(dstRange)] = addEnd;
                    dstRange++;
                }
                dstSplit = NO_SPLIT_SENTINEL;
                srcRange++;
            }
            return new SequenceIds(dst, dstRange * 2);
        }

        private static SequenceIds addRemainder(long[] dst, int dstRange, long[] src, int srcRange, int srcNumRanges)
        {
            return addRemainder(NO_SPLIT_SENTINEL, dst, dstRange, src, srcRange, srcNumRanges);
        }

        private static long[] ensureCapacity(long[] ids, int capacity)
        {
            return ensureCapacity(ids, capacity, Integer.MAX_VALUE);
        }
    }

    public static SequenceIds union(SequenceIds a, SequenceIds b)
    {
        int aNumRanges = a.rangeCount();
        int bNumRanges = b.rangeCount();

        if (aNumRanges == 0)
            return b.copy();

        if (bNumRanges == 0)
            return a.copy();


        int aRange = 0;
        int bRange = 0;

        int cRange = 0;
        long[] c = new long[Math.max(aNumRanges, bNumRanges) * 2];

        while (aRange < aNumRanges && bRange < bNumRanges)
        {
            long addStart;
            long addEnd;
            SetSupport.RangeOverlap rangeOverlap = SetSupport.calculateRangeOverlap(a.bounds, aRange, b.bounds, bRange);
            switch (rangeOverlap)
            {
                case BEFORE:
                    addStart = a.bounds[rangeStart(aRange)];
                    addEnd = a.bounds[rangeEnd(aRange)];
                    aRange++;
                    break;
                case AFTER:
                    addStart = b.bounds[rangeStart(bRange)];
                    addEnd = b.bounds[rangeEnd(bRange)];
                    bRange++;
                    break;
                case BEFORE_ADJACENT:
                case AFTER_ADJACENT:
                case INTERSECTING:
                    addStart = Math.min(a.bounds[rangeStart(aRange)], b.bounds[rangeStart(bRange)]);
                    addEnd = Math.max(a.bounds[rangeEnd(aRange)], b.bounds[rangeEnd(bRange)]);
                    aRange++;
                    bRange++;
                    break;
                default:
                    throw new IllegalStateException("Unhandled union op: " + rangeOverlap);
            }

            // extend the tail if we can
            if (cRange > 0 && offset(addStart) <= offset(c[rangeEnd(cRange-1)]) + 1)
            {
                c[rangeEnd(cRange - 1)] = addEnd;
            }
            else
            {
                c = SetSupport.ensureCapacity(c, (cRange + 1) * 2);
                c[rangeStart(cRange)] = addStart;
                c[rangeEnd(cRange)] = addEnd;
                cRange++;
            }
        }

        if (aRange < aNumRanges)
        {
            Preconditions.checkState(bRange == bNumRanges);
            return SetSupport.addRemainder(c, cRange, a.bounds, aRange, aNumRanges);
        }

        if (bRange < bNumRanges)
        {
            Preconditions.checkState(aRange == aNumRanges);
            return SetSupport.addRemainder(c, cRange, b.bounds, bRange, bNumRanges);
        }

        return new SequenceIds(c, cRange * 2);
    }

    /**
     * Subtract b from a
     */
    public static SequenceIds difference(SequenceIds a, SequenceIds b)
    {
        int aNumRanges = a.rangeCount();
        int bNumRanges = b.rangeCount();

        if (aNumRanges == 0)
            return new SequenceIds();

        if (bNumRanges == 0)
            return a.copy();

        int aRange = 0;
        int bRange = 0;

        int cRange = 0;
        long[] c = new long[Math.max(aNumRanges, bNumRanges) * 2];

        long aSplit = SetSupport.NO_SPLIT_SENTINEL;
        while (aRange < aNumRanges && bRange < bNumRanges)
        {
            long addStart = Long.MIN_VALUE;
            long addEnd = Long.MIN_VALUE;

            SetSupport.RangeOverlap rangeOverlap = SetSupport.calculateRangeOverlap(aSplit, a.bounds, aRange, b.bounds, bRange);
            switch (rangeOverlap)
            {
                case BEFORE:
                case BEFORE_ADJACENT:
                    addStart = aSplit != SetSupport.NO_SPLIT_SENTINEL ? aSplit : a.bounds[rangeStart(aRange)];
                    addEnd = a.bounds[rangeEnd(aRange)];
                    aSplit = SetSupport.NO_SPLIT_SENTINEL;
                    aRange++;
                    break;
                case AFTER:
                case AFTER_ADJACENT:
                    aSplit = SetSupport.NO_SPLIT_SENTINEL;
                    bRange++;
                    continue;
                case INTERSECTING:
                    long aStart = aSplit != SetSupport.NO_SPLIT_SENTINEL ? aSplit : a.bounds[rangeStart(aRange)];
                    long aEnd = a.bounds[rangeEnd(aRange)];
                    long bStart = b.bounds[rangeStart(bRange)];
                    long bEnd = b.bounds[rangeEnd(bRange)];

                    if (bStart <= aStart)
                    {
                        if (bEnd >= aEnd)
                        {
                            // b consumes the entire a range
                            aSplit = SetSupport.NO_SPLIT_SENTINEL;
                            aRange++;
                        }
                        else
                        {
                            // set the split and start over, the next b range may intersect with the current range
                            Preconditions.checkState(bEnd < aEnd);
                            aSplit = sequenceId(offset(bEnd) + 1);
                            bRange++;
                        }
                        continue;
                    }

                    // does not consume the start of the range
                    Preconditions.checkState(bStart > aStart);
                    addStart = aStart;
                    addEnd = sequenceId(offset(bStart) - 1);

                    if (bEnd < aEnd)
                    {
                        // b splits the range
                        aSplit = sequenceId(offset(bEnd) + 1);
                        bRange++;
                    }
                    else
                    {
                        // b consumes the rest of the a range
                        aSplit = SetSupport.NO_SPLIT_SENTINEL;
                        aRange++;
                    }
            }

            // confirm we didn't forget to assign start and end values
            Preconditions.checkState(addStart != Long.MIN_VALUE);
            Preconditions.checkState(addEnd != Long.MIN_VALUE);

            // extend the tail if we can
            if (cRange > 0 && offset(addStart) <= offset(c[rangeEnd(cRange-1)]) + 1)
            {
                c[rangeEnd(cRange - 1)] = addEnd;
            }
            else
            {
                c = SetSupport.ensureCapacity(c, (cRange + 1) * 2);
                c[rangeStart(cRange)] = addStart;
                c[rangeEnd(cRange)] = addEnd;
                cRange++;
            }
        }

        if (aRange < aNumRanges)
        {
            Preconditions.checkState(bRange == bNumRanges);
            return SetSupport.addRemainder(aSplit, c, cRange, a.bounds, aRange, aNumRanges);
        }

        return new SequenceIds(c, cRange * 2);
    }

    public interface RangeConsumer
    {
        RangeConsumer NONE = (s, e) -> {};

        void consume(long start, long end);
    }
}
