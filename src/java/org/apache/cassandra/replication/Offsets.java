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
package org.apache.cassandra.replication;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class Offsets
{
    private static final int INITIAL_CAPACITY = 16;

    private final CoordinatorLogId logId;
    // even index is range start, odd index is range end (inclusive)
    private int[] bounds;
    private int size;

    public Offsets(RangeIterator rangeIterator)
    {
        this(rangeIterator.logId());
        while (rangeIterator.tryAdvance())
            append(rangeIterator.start(), rangeIterator.end());
    }

    public Offsets(CoordinatorLogId logId)
    {
        this(logId, INITIAL_CAPACITY);
    }

    public Offsets(CoordinatorLogId logId, int capacity)
    {
        this.logId = logId;
        this.size = 0;
        this.bounds = new int[capacity];
    }

    private Offsets(CoordinatorLogId logId, int[] bounds)
    {
        this(logId, bounds, bounds.length);
    }

    private Offsets(CoordinatorLogId logId, int[] bounds, int size)
    {
        this.logId = logId;
        this.bounds = bounds;
        this.size = size;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        Offsets that = (Offsets) o;
        return size == that.size && Objects.equal(logId, that.logId) && Arrays.equals(bounds, 0, size, that.bounds, 0, size);
    }

    @Override
    public int hashCode()
    {
        int result = logId.hashCode();
        result = 31 * result + Integer.hashCode(size);
        for (int i = 0; i < size; i++)
            result = 31 * result + Integer.hashCode(bounds[i]);
        return result;
    }

    public Offsets copy()
    {
        return new Offsets(logId, Arrays.copyOf(bounds, size));
    }

    public CoordinatorLogId logId()
    {
        return logId;
    }

    public int rangeCount()
    {
        return size / 2;
    }

    public int offsetCount()
    {
        int count = 0, i = 0;
        while (i < size)
        {
            int start = bounds[i++];
            int   end = bounds[i++];
            count += end - start + 1;
        }
        return count;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    private static void forEachOffsetInRange(CoordinatorLogId logId, int start, int end, OffsetConsumer consumer)
    {
        for (int offset = start; offset <= end; offset++)
            consumer.accept(logId, offset);
    }

    public void forEachOffset(OffsetConsumer consumer)
    {
        for (int i = 0; i < size; i += 2)
        {
            int start = bounds[i];
            int   end = bounds[i + 1];
            forEachOffsetInRange(logId, start, end, consumer);
        }
    }

    public static void forEachOffset(RangeIterator iter, OffsetConsumer consumer)
    {
        while (iter.tryAdvance())
            forEachOffsetInRange(iter.logId(), iter.start(), iter.end(), consumer);
    }

    public void collectIds(Collection<ShortMutationId> into)
    {
        for (int i = 0; i < size; i += 2)
        {
            int start = bounds[i];
            int   end = bounds[i + 1];
            for (int offset = start; offset <= end; offset++)
                into.add(new ShortMutationId(logId, offset));
        }
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("{");
        int i = 0;
        while (i < size)
        {
            int start = bounds[i++];
            int   end = bounds[i++];
            builder.append('[').append(start).append(',').append(end).append(']');
            if (i < size) builder.append(',');
        }
        return builder.append('}').toString();
    }

    public boolean contains(int offset)
    {
        if (size == 0)
            return false;

        int pos = Arrays.binarySearch(bounds, 0, size, offset);
        if (pos >= 0) return true; // matches one of the bounds

        pos = -pos - 1;
        return (pos - 1) % 2 == 0; // offset falls within bounds of an existing range if the bound to the left is an open one
    }

    public void digest(Digest digest)
    {
        digest.updateWithLong(logId.asLong());
        digest.updateWithInt(size);
        for (int i = 0; i < size; i++)
            digest.updateWithInt(bounds[i]);
    }

    public void addAll(Offsets other, RangeConsumer onAdded)
    {
        for (int i = 0; i < other.size; i += 2)
            add(other.bounds[i], other.bounds[i + 1], onAdded);
    }

    public void addAll(Offsets other)
    {
        addAll(other, RangeConsumer.NONE);
    }

    public boolean add(int offset, RangeConsumer onAdded)
    {
        boolean added = add(offset);
        if (added) onAdded.consume(logId, offset, offset);
        return added;
    }

    public boolean add(int offset)
    {
        if (size == 0)
        {
            append(offset, offset);
            return true;
        }

        int pos = Arrays.binarySearch(bounds, 0, size, offset);
        if (pos >= 0) return false; // matches one of the bounds

        pos = -pos - 1;
        if (pos == size) // after all existing ranges
        {
            if (bounds[size - 1] == offset - 1)
                bounds[size - 1] = offset; // extend the last range
            else
                append(offset, offset); // append a new single-offset range

            return true;
        }
        else if (pos == 0) // before all existing ranges
        {
            if (bounds[0] == offset + 1)
                bounds[0] = offset; // extend the first range
            else
                insert(0, offset, offset); // prepend a new single-offset range

            return true;
        }
        else if ((pos - 1) % 2 == 0) // offset falls within bounds of an existing range (bound to the left is an open bound)
        {
            return false;
        }

        // between two existing ranges
        boolean extendsPrev = bounds[pos - 1] == offset - 1;
        boolean extendsNext = bounds[pos] == offset + 1;

        if (extendsPrev && extendsNext) // closes the gap between two adjacent ranges
        {
            bounds[pos - 1] = bounds[pos + 1];
            System.arraycopy(bounds, pos + 2, bounds, pos, size - pos - 2);
            bounds[--size] = 0;
            bounds[--size] = 0;
        }
        else if (extendsPrev)
        {
            bounds[pos - 1] = offset;
        }
        else if (extendsNext)
        {
            bounds[pos] = offset;
        }
        else
        {
            insert(pos, offset, offset);
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

    private enum AddAction
    {
        INSERT, MOVE, INCLUDE;

        boolean isMove()
        {
            return this == MOVE;
        }

        boolean isInclude()
        {
            return this == INCLUDE;
        }

        boolean isInsert()
        {
            return this == INSERT;
        }

        boolean isMoveOrInclude()
        {
            return this == MOVE || this == INCLUDE;
        }
    }

    public boolean add(final int start, final int end, RangeConsumer onAdded)
    {
        Preconditions.checkArgument(start <= end);

        if (size == 0)
        {
            append(start, end);
            return true;
        }

        if (start == end)
            return add(start, onAdded);

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
            int rStart = bounds[rangeStart(sRange)];
            int rEnd = bounds[rangeEnd(sRange)];
            if (start >= rStart)
            {
                // already included in the range or adjacent to range end
                sMerge = start <= rEnd + 1
                       ? AddAction.INCLUDE // included in the range
                       : AddAction.INSERT; // past the end of the range
            }
            else if (sRange > 0 && start == bounds[rangeEnd(sRange - 1)] + 1)
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
            int rStart = bounds[rangeStart(eRange)];
            int rEnd = bounds[rangeEnd(eRange)];

            if (end <= rEnd)
            {
                if (end >= rStart - 1)
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
            else if (eRange < numRanges - 1 && end == bounds[rangeStart(eRange + 1)] - 1)
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
        if (sMerge.isMove() && eMerge.isInsert())
        {
            Preconditions.checkState(sRange == eRange);
            onAdded.consume(logId, start, end);
            insert(rangeStart(sRange), start, end);
            return true;
        }

        // this should only happen if we're adding a range to the very end of the set
        if (sMerge.isInsert() && eMerge.isMove())
        {
            Preconditions.checkState(sRange == eRange);
            Preconditions.checkState(sRange == numRanges - 1);
            onAdded.consume(logId, start, end);
            append(start, end);
            return true;
        }

        boolean adjusted = false;
        if (sMerge.isMove())
        {
            onAdded.consume(logId, start, bounds[rangeStart(sRange)] - 1);
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
                int sEnd = bounds[rangeEnd(i)];
                int eStart = bounds[rangeStart(i + 1)];
                onAdded.consume(logId, sEnd + 1, eStart - 1);
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

        if (eMerge.isMove())
        {
            onAdded.consume(logId, bounds[rangeEnd(eRange)] + 1, end);
            bounds[rangeEnd(eRange)] = end;
            adjusted = true;
        }

        return adjusted;
    }

    public boolean add(int start, int end)
    {
        return add(start, end, RangeConsumer.NONE);
    }

    private void insert(int pos, int start, int end)
    {
        if (bounds.length == size)
        {
            int[] newBounds = new int[bounds.length * 2];
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

    private void append(int start, int end)
    {
        if (bounds.length == size)
        {
            int[] newBounds = new int[Math.max(bounds.length * 2, INITIAL_CAPACITY)];
            System.arraycopy(bounds, 0, newBounds, 0, bounds.length);
            bounds = newBounds;
        }
        Preconditions.checkState(size == 0 || start > bounds[size - 1]);
        bounds[size++] = start;
        bounds[size++] = end;
    }

    public void append(int offset)
    {
        if (size == 0)
        {
            append(offset, offset);
            return;
        }

        int tail = bounds[size - 1];
        if (offset <= tail)
            throw new IllegalArgumentException("Can't append " + offset + " to " + tail);

        if (offset == tail + 1)
            bounds[size-1] = offset;
        else
            append(offset, offset);
    }

    public RangeIterator rangeIterator()
    {
        int numRanges = rangeCount();

        return new RangeIterator()
        {
            int range = -1;

            @Override
            public int start()
            {
                Preconditions.checkState(range >= 0 && range <= numRanges);
                return bounds[rangeStart(range)];
            }

            @Override
            public int end()
            {
                Preconditions.checkState(range >= 0 && range <= numRanges);
                return bounds[rangeEnd(range)];
            }

            @Override
            public boolean tryAdvance()
            {
                if (isFinished())
                    return false;
                range++;
                return !isFinished();
            }

            @Override
            public CoordinatorLogId logId()
            {
                return logId;
            }

            @Override
            public boolean isFinished()
            {
                return range >= numRanges;
            }
        };
    }

    private static class SetSupport
    {
        private static final int NO_SPLIT_SENTINEL = Integer.MIN_VALUE;

        private enum RangeOverlap
        {
            BEFORE, BEFORE_ADJACENT, AFTER, AFTER_ADJACENT, INTERSECTING
        }

        private static RangeOverlap calculateRangeOverlap(int aStart, int aEnd, int bStart, int bEnd)
        {
            Preconditions.checkState(aStart <= aEnd);
            Preconditions.checkState(bStart <= bEnd);

            if (aEnd < bStart)
                return aEnd == bStart - 1 ? RangeOverlap.BEFORE_ADJACENT : RangeOverlap.BEFORE;

            if (bEnd < aStart)
                return bEnd == aStart - 1 ? RangeOverlap.AFTER_ADJACENT : RangeOverlap.AFTER;

            return RangeOverlap.INTERSECTING;
        }

        private static RangeOverlap calculateRangeOverlap(int aSplit, int aStart, int aEnd, int bSplit, int bStart, int bEnd)
        {
            aStart = aSplit != NO_SPLIT_SENTINEL ? aSplit : aStart;
            bStart = bSplit != NO_SPLIT_SENTINEL ? bSplit : bStart;
            return calculateRangeOverlap(aStart, aEnd, bStart, bEnd);
        }

        private static RangeOverlap calculateRangeOverlap(int aSplit, int[] a, int aRange, int bSplit, int[] b, int bRange)
        {
            return calculateRangeOverlap(aSplit, a[rangeStart(aRange)], a[rangeEnd(aRange)], bSplit, b[rangeStart(bRange)], b[rangeEnd(bRange)]);
        }

        private static RangeOverlap calculateRangeOverlap(int[] a, int aRange, int[] b, int bRange)
        {
            return calculateRangeOverlap(NO_SPLIT_SENTINEL, a, aRange, NO_SPLIT_SENTINEL, b, bRange);
        }

        private static int[] ensureCapacity(int[] offsets, int capacity, int expectedMaxCapacity)
        {
            Preconditions.checkArgument(capacity > 0);
            if (capacity <= offsets.length)
                return offsets;

            int newCapacity = capacity * 2;

            // if we overflowed, set to max
            if (newCapacity < 0)
                newCapacity = Integer.MAX_VALUE;

            if (newCapacity > expectedMaxCapacity && offsets.length < expectedMaxCapacity)
                newCapacity = expectedMaxCapacity;

            int[] newOffsets = new int[newCapacity];
            System.arraycopy(offsets, 0, newOffsets, 0, offsets.length);

            return newOffsets;
        }

        private static Offsets addRemainder(CoordinatorLogId logId, int dstSplit, int[] dst, int dstRange, int[] src, int srcRange, int srcNumRanges)
        {
            int capacity = (dstRange + srcNumRanges - srcRange) * 2;
            dst = ensureCapacity(dst, capacity, capacity);
            while (srcRange < srcNumRanges)
            {
                int addStart = dstSplit != NO_SPLIT_SENTINEL ? dstSplit : src[rangeStart(srcRange)];
                int addEnd = src[rangeEnd(srcRange)];
                // extend last range if possible
                if (dstRange > 0 && addStart <= dst[rangeEnd(dstRange - 1)] + 1)
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
            return new Offsets(logId, dst, dstRange * 2);
        }

        private static Offsets addRemainder(CoordinatorLogId logId, int[] dst, int dstRange, int[] src, int srcRange, int srcNumRanges)
        {
            return addRemainder(logId, NO_SPLIT_SENTINEL, dst, dstRange, src, srcRange, srcNumRanges);
        }

        private static int[] ensureCapacity(int[] offsets, int capacity)
        {
            return ensureCapacity(offsets, capacity, Integer.MAX_VALUE);
        }
    }

    public static Offsets union(Offsets a, Offsets b)
    {
        if (a == null)
            return b;

        if (b == null)
            return a;

        Preconditions.checkArgument(a.logId.equals(b.logId));
        CoordinatorLogId logId = a.logId;

        int aNumRanges = a.rangeCount();
        int bNumRanges = b.rangeCount();

        if (aNumRanges == 0)
            return b.copy();

        if (bNumRanges == 0)
            return a.copy();

        int aRange = 0;
        int bRange = 0;

        int cRange = 0;
        int[] c = new int[Math.max(aNumRanges, bNumRanges) * 2];

        while (aRange < aNumRanges && bRange < bNumRanges)
        {
            int addStart;
            int addEnd;
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
            if (cRange > 0 && addStart <= c[rangeEnd(cRange-1)] + 1)
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
            return SetSupport.addRemainder(logId, c, cRange, a.bounds, aRange, aNumRanges);
        }

        if (bRange < bNumRanges)
        {
            Preconditions.checkState(aRange == aNumRanges);
            return SetSupport.addRemainder(logId, c, cRange, b.bounds, bRange, bNumRanges);
        }

        return new Offsets(logId, c, cRange * 2);
    }

    private static abstract class AbstractSetIterator implements RangeIterator
    {
        enum State
        {
            INITIALIZED, VALID, FINISHED;

            boolean isValid()
            {
                return this == VALID;
            }

            boolean isFinished()
            {
                return this == FINISHED;
            }
        }
        final RangeIterator a;
        final RangeIterator b;
        int start;
        int end;
        private State state = State.INITIALIZED;

        public AbstractSetIterator(RangeIterator a, RangeIterator b)
        {
            Preconditions.checkNotNull(a);
            Preconditions.checkNotNull(b);
            Preconditions.checkArgument(a.logId().equals(b.logId()));
            this.a = a;
            this.b = b;
        }

        @Override
        public int start()
        {
            Preconditions.checkState(state.isValid());
            return start;
        }

        @Override
        public int end()
        {
            Preconditions.checkState(state.isValid());
            return end;
        }

        protected abstract State computeNext();

        @Override
        public boolean tryAdvance()
        {
            switch (state)
            {
                case INITIALIZED:
                    a.tryAdvance();
                    b.tryAdvance();
                    state = State.VALID;
                case VALID:
                    state = computeNext();
                    if (!state.isFinished())
                        break;;
                case FINISHED:
                    return false;
                default:
                    throw new IllegalStateException("Unhandled state: " + state);
            }
            return true;
        }

        @Override
        public boolean isFinished()
        {
            return state.isFinished();
        }

        @Override
        public CoordinatorLogId logId()
        {
            return a.logId();
        }
    }

    private static class UnionIterator extends AbstractSetIterator
    {
        public UnionIterator(RangeIterator a, RangeIterator b)
        {
            super(a, b);
        }

        void extendEnd(RangeIterator iter)
        {
            while (iter.tryAdvance())
            {
                SetSupport.RangeOverlap rangeOverlap = SetSupport.calculateRangeOverlap(iter.start(), iter.end(), start, end);
                switch (rangeOverlap)
                {
                    case BEFORE:
                    case BEFORE_ADJACENT:
                        throw new IllegalStateException();
                    case AFTER:
                        return;
                    case INTERSECTING:
                    case AFTER_ADJACENT:
                        end = Math.max(end, iter.end());
                        continue;
                    default:
                        throw new IllegalStateException("Unhandled union op: " + rangeOverlap);
                }
            }
        }

        @Override
        protected State computeNext()
        {

            if (a.isFinished())
            {
                if (b.isFinished())
                    return State.FINISHED;

                start = b.start();
                end = b.end();
                b.tryAdvance();
                return State.VALID;
            }

            if (b.isFinished())
            {
                start = a.start();
                end = a.end();
                a.tryAdvance();
                return State.VALID;
            }

            SetSupport.RangeOverlap rangeOverlap = SetSupport.calculateRangeOverlap(a.start(), a.end(), b.start(), b.end());
            switch (rangeOverlap)
            {
                case BEFORE:
                    start = a.start();
                    end = a.end();
                    a.tryAdvance();
                    break;
                case AFTER:
                    start = b.start();
                    end = b.end();
                    b.tryAdvance();
                    break;
                case BEFORE_ADJACENT:
                case AFTER_ADJACENT:
                case INTERSECTING:
                    start = Math.min(a.start(), b.start());
                    end = Math.max(a.end(), b.end());
                    extendEnd(a);
                    extendEnd(b);
                    break;
                default:
                    throw new IllegalStateException("Unhandled union op: " + rangeOverlap);
            }

            return State.VALID;
        }
    }

    public static RangeIterator union(RangeIterator a, RangeIterator b)
    {
        return new UnionIterator(a, b);
    }

    /**
     * Subtract b from a
     */
    public static Offsets difference(Offsets a, Offsets b)
    {
        Preconditions.checkArgument(a != null);
        if (b == null)
            return a.copy();

        Preconditions.checkArgument(b == null || a.logId.equals(b.logId));
        CoordinatorLogId logId = a.logId;
        int aNumRanges = a.rangeCount();
        int bNumRanges = b.rangeCount();

        if (aNumRanges == 0)
            return new Offsets(logId);

        if (bNumRanges == 0)
            return a.copy();

        int aRange = 0;
        int bRange = 0;

        int cRange = 0;
        int[] c = new int[Math.max(aNumRanges, bNumRanges) * 2];

        int aSplit = SetSupport.NO_SPLIT_SENTINEL;
        while (aRange < aNumRanges && bRange < bNumRanges)
        {
            int addStart;
            int addEnd;

            SetSupport.RangeOverlap rangeOverlap = SetSupport.calculateRangeOverlap(aSplit, a.bounds, aRange, SetSupport.NO_SPLIT_SENTINEL, b.bounds, bRange);
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
                    int aStart = aSplit != SetSupport.NO_SPLIT_SENTINEL ? aSplit : a.bounds[rangeStart(aRange)];
                    int aEnd = a.bounds[rangeEnd(aRange)];
                    int bStart = b.bounds[rangeStart(bRange)];
                    int bEnd = b.bounds[rangeEnd(bRange)];

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
                            aSplit = bEnd + 1;
                            bRange++;
                        }
                        continue;
                    }

                    // does not consume the start of the range
                    Preconditions.checkState(bStart > aStart);
                    addStart = aStart;
                    addEnd = bStart - 1;

                    if (bEnd < aEnd)
                    {
                        // b splits the range
                        aSplit = bEnd + 1;
                        bRange++;
                    }
                    else
                    {
                        // b consumes the rest of the a range
                        aSplit = SetSupport.NO_SPLIT_SENTINEL;
                        aRange++;
                    }
                    break;
                default:
                    throw new IllegalStateException("Unhandled union op: " + rangeOverlap);
            }

            // extend the tail if we can
            if (cRange > 0 && addStart <= c[rangeEnd(cRange-1)] + 1)
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
            return SetSupport.addRemainder(logId, aSplit, c, cRange, a.bounds, aRange, aNumRanges);
        }

        return new Offsets(logId, c, cRange * 2);
    }

    private static class DifferenceIterator extends AbstractSetIterator
    {
        private int aSplit = SetSupport.NO_SPLIT_SENTINEL;

        public DifferenceIterator(RangeIterator a, RangeIterator b)
        {
            super(a, b);
        }

        @Override
        protected State computeNext()
        {
            while (!a.isFinished() && !b.isFinished())
            {
                SetSupport.RangeOverlap rangeOverlap = SetSupport.calculateRangeOverlap(aSplit, a.start(), a.end(),
                                                                                        SetSupport.NO_SPLIT_SENTINEL, b.start(), b.end());
                switch (rangeOverlap)
                {
                    case BEFORE:
                    case BEFORE_ADJACENT:
                        start = aSplit != SetSupport.NO_SPLIT_SENTINEL ? aSplit : a.start();
                        end = a.end();
                        aSplit = SetSupport.NO_SPLIT_SENTINEL;
                        a.tryAdvance();
                        return State.VALID;
                    case AFTER:
                    case AFTER_ADJACENT:
                        aSplit = SetSupport.NO_SPLIT_SENTINEL;
                        b.tryAdvance();
                        continue;
                    case INTERSECTING:
                        int aStart = aSplit != SetSupport.NO_SPLIT_SENTINEL ? aSplit : a.start();
                        int aEnd = a.end();
                        int bStart = b.start();
                        int bEnd = b.end();

                        if (bStart <= aStart)
                        {
                            if (bEnd >= aEnd)
                            {
                                // b consumes the entire a range
                                aSplit = SetSupport.NO_SPLIT_SENTINEL;
                                a.tryAdvance();
                            }
                            else
                            {
                                // set the split and start over, the next b range may intersect with the current range
                                aSplit = bEnd + 1;
                                b.tryAdvance();
                            }
                            continue;
                        }

                        // does not consume the start of the range
                        Preconditions.checkState(bStart > aStart);
                        start = aStart;
                        end = bStart - 1;

                        if (bEnd < aEnd)
                        {
                            // b splits the range
                            aSplit = bEnd + 1;
                            b.tryAdvance();
                        }
                        else
                        {
                            // b consumes the rest of the a range
                            aSplit = SetSupport.NO_SPLIT_SENTINEL;
                            a.tryAdvance();
                        }
                        return State.VALID;
                    default:
                        throw new IllegalStateException("Unhandled union op: " + rangeOverlap);
                }
            }

            if (a.isFinished())
                return State.FINISHED;

            if (b.isFinished())
            {
                start = aSplit != SetSupport.NO_SPLIT_SENTINEL ? aSplit : a.start();
                end = a.end();
                aSplit = SetSupport.NO_SPLIT_SENTINEL;
                a.tryAdvance();
                return State.VALID;
            }

            // shouldn't be possible to get here
            throw new IllegalStateException();
        }
    }

    public static RangeIterator difference(RangeIterator a, RangeIterator b)
    {
        return new DifferenceIterator(a, b);
    }

    public static Offsets intersection(Offsets a, Offsets b)
    {
        Preconditions.checkArgument(a.logId.equals(b.logId));
        CoordinatorLogId logId = a.logId;

        int aNumRanges = a.rangeCount();
        int bNumRanges = b.rangeCount();

        if (aNumRanges == 0 || bNumRanges == 0)
            return new Offsets(logId);

        int aRange = 0;
        int bRange = 0;

        int cRange = 0;
        int[] c = new int[Math.max(aNumRanges, bNumRanges) * 2];

        int aSplit = SetSupport.NO_SPLIT_SENTINEL;
        int bSplit = SetSupport.NO_SPLIT_SENTINEL;
        while (aRange < aNumRanges && bRange < bNumRanges)
        {
            int addStart;
            int addEnd;

            SetSupport.RangeOverlap rangeOverlap = SetSupport.calculateRangeOverlap(aSplit, a.bounds, aRange, bSplit, b.bounds, bRange);
            switch (rangeOverlap)
            {
                case BEFORE:
                case BEFORE_ADJACENT:
                    aSplit = SetSupport.NO_SPLIT_SENTINEL;
                    bSplit = SetSupport.NO_SPLIT_SENTINEL;
                    aRange++;
                    continue;
                case AFTER:
                case AFTER_ADJACENT:
                    aSplit = SetSupport.NO_SPLIT_SENTINEL;
                    bSplit = SetSupport.NO_SPLIT_SENTINEL;
                    bRange++;
                    continue;
                case INTERSECTING:
                    int aStart = aSplit != SetSupport.NO_SPLIT_SENTINEL ? aSplit : a.bounds[rangeStart(aRange)];
                    int aEnd = a.bounds[rangeEnd(aRange)];
                    int bStart = bSplit != SetSupport.NO_SPLIT_SENTINEL ? bSplit : b.bounds[rangeStart(bRange)];
                    int bEnd = b.bounds[rangeEnd(bRange)];

                    addStart = Math.max(aStart, bStart);
                    addEnd = Math.min(aEnd, bEnd);

                    if (aEnd < bEnd)
                    {
                        aSplit = SetSupport.NO_SPLIT_SENTINEL;
                        bSplit = aEnd;
                        aRange++;
                    }
                    else if (bEnd < aEnd)
                    {
                        aSplit = bEnd;
                        bSplit = SetSupport.NO_SPLIT_SENTINEL;
                        bRange++;
                    }
                    else
                    {
                        aSplit = SetSupport.NO_SPLIT_SENTINEL;
                        bSplit = SetSupport.NO_SPLIT_SENTINEL;
                        aRange++;
                        bRange++;
                    }
                    break;

                default:
                    throw new IllegalStateException("Unhandled union op: " + rangeOverlap);
            }

            // extend the tail if we can, though it shouldn't be possible unless one of the input lists were malformed
            if (cRange > 0 && addStart <= c[rangeEnd(cRange-1)] + 1)
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

        return new Offsets(logId, c, cRange * 2);
    }

    public interface OffsetConsumer
    {
        void accept(CoordinatorLogId logId, int offset);
    }

    public interface RangeConsumer
    {
        RangeConsumer NONE = (log, start, end) -> {};

        void consume(CoordinatorLogId logId, int startOffset, int endOffset);
    }

    /**
     * Intrusive iterator. Adjacent ranges may or may not be merged
     *
     * iterator is created in an invalid state, and tryAdvance needs to be called before calling start or
     * stop. This is to support empty iterators, and makes iterating through the contents less verbose than
     * have to check isFinished before reading the values, and makes iterator initialization less error-prone;
     */
    public interface RangeIterator
    {
        int start();
        int end();
        boolean tryAdvance();
        boolean isFinished();
        CoordinatorLogId logId();
    }

    // TODO (consider): delta-encoding + vints
    public static final IVersionedSerializer<Offsets> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Offsets offsets, DataOutputPlus out, int version) throws IOException
        {
            CoordinatorLogId.serializer.serialize(offsets.logId, out, version);
            out.writeInt(offsets.size);
            for (int i = 0; i < offsets.size; i++)
                out.writeInt(offsets.bounds[i]);
        }

        @Override
        public Offsets deserialize(DataInputPlus in, int version) throws IOException
        {
            CoordinatorLogId logId = CoordinatorLogId.serializer.deserialize(in, version);
            int size = in.readInt();
            Preconditions.checkArgument(size >= 0 && size % 2 == 0);
            int[] bounds = new int[size];
            for (int i = 0; i < size; i++)
                bounds[i] = in.readInt();
            return new Offsets(logId, bounds);
        }

        @Override
        public long serializedSize(Offsets offsets, int version)
        {
            long size = CoordinatorLogId.serializer.serializedSize(offsets.logId, version);
            size += TypeSizes.INT_SIZE;
            size += (long) TypeSizes.INT_SIZE * offsets.size;
            return size;
        }
    };
}
