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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.utils.AbstractIterator;
import com.google.common.collect.Iterators;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Data structure holding the range tombstones of a ColumnFamily.
 * <p>
 * This is essentially a sorted list of non-overlapping (tombstone) ranges.
 * <p>
 * A range tombstone has 4 elements: the start and end of the range covered,
 * and the deletion infos (markedAt timestamp and local deletion time). The
 * markedAt timestamp is what define the priority of 2 overlapping tombstones.
 * That is, given 2 tombstones {@code [0, 10]@t1 and [5, 15]@t2, then if t2 > t1} (and
 * are the tombstones markedAt values), the 2nd tombstone take precedence over
 * the first one on [5, 10]. If such tombstones are added to a RangeTombstoneList,
 * the range tombstone list will store them as [[0, 5]@t1, [5, 15]@t2].
 * <p>
 * The only use of the local deletion time is to know when a given tombstone can
 * be purged, which will be done by the purge() method.
 */
public class RangeTombstoneList implements Iterable<RangeTombstone>, IMeasurableMemory
{
    private static long EMPTY_SIZE = ObjectSizes.measure(new RangeTombstoneList(null, 0));

    private final ClusteringComparator comparator;

    // Note: we don't want to use a List for the markedAts and delTimes to avoid boxing. We could
    // use a List for starts and ends, but having arrays everywhere is almost simpler.
    private ClusteringBound[] starts;
    private ClusteringBound[] ends;
    private long[] markedAts;
    private int[] delTimes;

    private long boundaryHeapSize;
    private int size;

    private RangeTombstoneList(ClusteringComparator comparator, ClusteringBound[] starts, ClusteringBound[] ends, long[] markedAts, int[] delTimes, long boundaryHeapSize, int size)
    {
        assert starts.length == ends.length && starts.length == markedAts.length && starts.length == delTimes.length;
        this.comparator = comparator;
        this.starts = starts;
        this.ends = ends;
        this.markedAts = markedAts;
        this.delTimes = delTimes;
        this.size = size;
        this.boundaryHeapSize = boundaryHeapSize;
    }

    public RangeTombstoneList(ClusteringComparator comparator, int capacity)
    {
        this(comparator, new ClusteringBound[capacity], new ClusteringBound[capacity], new long[capacity], new int[capacity], 0, 0);
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int size()
    {
        return size;
    }

    public ClusteringComparator comparator()
    {
        return comparator;
    }

    public RangeTombstoneList copy()
    {
        return new RangeTombstoneList(comparator,
                                      Arrays.copyOf(starts, size),
                                      Arrays.copyOf(ends, size),
                                      Arrays.copyOf(markedAts, size),
                                      Arrays.copyOf(delTimes, size),
                                      boundaryHeapSize, size);
    }

    public RangeTombstoneList copy(AbstractAllocator allocator)
    {
        RangeTombstoneList copy =  new RangeTombstoneList(comparator,
                                                          new ClusteringBound[size],
                                                          new ClusteringBound[size],
                                                          Arrays.copyOf(markedAts, size),
                                                          Arrays.copyOf(delTimes, size),
                                                          boundaryHeapSize, size);


        for (int i = 0; i < size; i++)
        {
            copy.starts[i] = clone(starts[i], allocator);
            copy.ends[i] = clone(ends[i], allocator);
        }

        return copy;
    }

    private static ClusteringBound clone(ClusteringBound bound, AbstractAllocator allocator)
    {
        ByteBuffer[] values = new ByteBuffer[bound.size()];
        for (int i = 0; i < values.length; i++)
            values[i] = allocator.clone(bound.get(i));
        return new ClusteringBound(bound.kind(), values);
    }

    public void add(RangeTombstone tombstone)
    {
        add(tombstone.deletedSlice().start(),
            tombstone.deletedSlice().end(),
            tombstone.deletionTime().markedForDeleteAt(),
            tombstone.deletionTime().localDeletionTime());
    }

    /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case),
     * but it doesn't assume it.
     */
    public void add(ClusteringBound start, ClusteringBound end, long markedAt, int delTime)
    {
        if (isEmpty())
        {
            addInternal(0, start, end, markedAt, delTime);
            return;
        }

        int c = comparator.compare(ends[size-1], start);

        // Fast path if we add in sorted order
        if (c <= 0)
        {
            addInternal(size, start, end, markedAt, delTime);
        }
        else
        {
            // Note: insertFrom expect i to be the insertion point in term of interval ends
            int pos = Arrays.binarySearch(ends, 0, size, start, comparator);
            insertFrom((pos >= 0 ? pos+1 : -pos-1), start, end, markedAt, delTime);
        }
        boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
    }

    /**
     * Adds all the range tombstones of {@code tombstones} to this RangeTombstoneList.
     */
    public void addAll(RangeTombstoneList tombstones)
    {
        if (tombstones.isEmpty())
            return;

        if (isEmpty())
        {
            copyArrays(tombstones, this);
            return;
        }

        /*
         * We basically have 2 techniques we can use here: either we repeatedly call add() on tombstones values,
         * or we do a merge of both (sorted) lists. If this lists is bigger enough than the one we add, then
         * calling add() will be faster, otherwise it's merging that will be faster.
         *
         * Let's note that during memtables updates, it might not be uncommon that a new update has only a few range
         * tombstones, while the CF we're adding it to (the one in the memtable) has many. In that case, using add() is
         * likely going to be faster.
         *
         * In other cases however, like when diffing responses from multiple nodes, the tombstone lists we "merge" will
         * be likely sized, so using add() might be a bit inefficient.
         *
         * Roughly speaking (this ignore the fact that updating an element is not exactly constant but that's not a big
         * deal), if n is the size of this list and m is tombstones size, merging is O(n+m) while using add() is O(m*log(n)).
         *
         * But let's not crank up a logarithm computation for that. Long story short, merging will be a bad choice only
         * if this list size is lot bigger that the other one, so let's keep it simple.
         */
        if (size > 10 * tombstones.size)
        {
            for (int i = 0; i < tombstones.size; i++)
                add(tombstones.starts[i], tombstones.ends[i], tombstones.markedAts[i], tombstones.delTimes[i]);
        }
        else
        {
            int i = 0;
            int j = 0;
            while (i < size && j < tombstones.size)
            {
                if (comparator.compare(tombstones.starts[j], ends[i]) < 0)
                {
                    insertFrom(i, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
                    j++;
                }
                else
                {
                    i++;
                }
            }
            // Addds the remaining ones from tombstones if any (note that addInternal will increment size if relevant).
            for (; j < tombstones.size; j++)
                addInternal(size, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(Clustering clustering, Cell cell)
    {
        int idx = searchInternal(clustering, 0, size);
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        return idx >= 0 && (cell.isCounterCell() || markedAts[idx] >= cell.timestamp());
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime searchDeletionTime(Clustering name)
    {
        int idx = searchInternal(name, 0, size);
        return idx < 0 ? null : new DeletionTime(markedAts[idx], delTimes[idx]);
    }

    public RangeTombstone search(Clustering name)
    {
        int idx = searchInternal(name, 0, size);
        return idx < 0 ? null : rangeTombstone(idx);
    }

    /*
     * Return is the index of the range covering name if name is covered. If the return idx is negative,
     * no range cover name and -idx-1 is the index of the first range whose start is greater than name.
     *
     * Note that bounds are not in the range if they fall on its boundary.
     */
    private int searchInternal(ClusteringPrefix name, int startIdx, int endIdx)
    {
        if (isEmpty())
            return -1;

        int pos = Arrays.binarySearch(starts, startIdx, endIdx, name, comparator);
        if (pos >= 0)
        {
            // Equality only happens for bounds (as used by forward/reverseIterator), and bounds are equal only if they
            // are the same or complementary, in either case the bound itself is not part of the range.
            return -pos - 1;
        }
        else
        {
            // We potentially intersect the range before our "insertion point"
            int idx = -pos-2;
            if (idx < 0)
                return -1;

            return comparator.compare(name, ends[idx]) < 0 ? idx : -idx-2;
        }
    }

    public int dataSize()
    {
        int dataSize = TypeSizes.sizeof(size);
        for (int i = 0; i < size; i++)
        {
            dataSize += starts[i].dataSize() + ends[i].dataSize();
            dataSize += TypeSizes.sizeof(markedAts[i]);
            dataSize += TypeSizes.sizeof(delTimes[i]);
        }
        return dataSize;
    }

    public long maxMarkedAt()
    {
        long max = Long.MIN_VALUE;
        for (int i = 0; i < size; i++)
            max = Math.max(max, markedAts[i]);
        return max;
    }

    public void collectStats(EncodingStats.Collector collector)
    {
        for (int i = 0; i < size; i++)
        {
            collector.updateTimestamp(markedAts[i]);
            collector.updateLocalDeletionTime(delTimes[i]);
        }
    }

    public void updateAllTimestamp(long timestamp)
    {
        for (int i = 0; i < size; i++)
            markedAts[i] = timestamp;
    }

    private RangeTombstone rangeTombstone(int idx)
    {
        return new RangeTombstone(Slice.make(starts[idx], ends[idx]), new DeletionTime(markedAts[idx], delTimes[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewStart(int idx, ClusteringBound newStart)
    {
        return new RangeTombstone(Slice.make(newStart, ends[idx]), new DeletionTime(markedAts[idx], delTimes[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewEnd(int idx, ClusteringBound newEnd)
    {
        return new RangeTombstone(Slice.make(starts[idx], newEnd), new DeletionTime(markedAts[idx], delTimes[idx]));
    }

    private RangeTombstone rangeTombstoneWithNewBounds(int idx, ClusteringBound newStart, ClusteringBound newEnd)
    {
        return new RangeTombstone(Slice.make(newStart, newEnd), new DeletionTime(markedAts[idx], delTimes[idx]));
    }

    public Iterator<RangeTombstone> iterator()
    {
        return iterator(false);
    }

    public Iterator<RangeTombstone> iterator(boolean reversed)
    {
        return reversed
             ? new AbstractIterator<RangeTombstone>()
             {
                 private int idx = size - 1;

                 protected RangeTombstone computeNext()
                 {
                     if (idx < 0)
                         return endOfData();

                     return rangeTombstone(idx--);
                 }
             }
             : new AbstractIterator<RangeTombstone>()
             {
                 private int idx;

                 protected RangeTombstone computeNext()
                 {
                     if (idx >= size)
                         return endOfData();

                     return rangeTombstone(idx++);
                 }
             };
    }

    public Iterator<RangeTombstone> iterator(final Slice slice, boolean reversed)
    {
        return reversed ? reverseIterator(slice) : forwardIterator(slice);
    }

    private Iterator<RangeTombstone> forwardIterator(final Slice slice)
    {
        int startIdx = slice.start() == ClusteringBound.BOTTOM ? 0 : searchInternal(slice.start(), 0, size);
        final int start = startIdx < 0 ? -startIdx-1 : startIdx;

        if (start >= size)
            return Collections.emptyIterator();

        int finishIdx = slice.end() == ClusteringBound.TOP ? size - 1 : searchInternal(slice.end(), start, size);
        // if stopIdx is the first range after 'slice.end()' we care only until the previous range
        final int finish = finishIdx < 0 ? -finishIdx-2 : finishIdx;

        if (start > finish)
            return Collections.emptyIterator();

        if (start == finish)
        {
            // We want to make sure the range are stricly included within the queried slice as this
            // make it easier to combine things when iterating over successive slices.
            ClusteringBound s = comparator.compare(starts[start], slice.start()) < 0 ? slice.start() : starts[start];
            ClusteringBound e = comparator.compare(slice.end(), ends[start]) < 0 ? slice.end() : ends[start];
            return Iterators.<RangeTombstone>singletonIterator(rangeTombstoneWithNewBounds(start, s, e));
        }

        return new AbstractIterator<RangeTombstone>()
        {
            private int idx = start;

            protected RangeTombstone computeNext()
            {
                if (idx >= size || idx > finish)
                    return endOfData();

                // We want to make sure the range are stricly included within the queried slice as this
                // make it easier to combine things when iterating over successive slices. This means that
                // for the first and last range we might have to "cut" the range returned.
                if (idx == start && comparator.compare(starts[idx], slice.start()) < 0)
                    return rangeTombstoneWithNewStart(idx++, slice.start());
                if (idx == finish && comparator.compare(slice.end(), ends[idx]) < 0)
                    return rangeTombstoneWithNewEnd(idx++, slice.end());
                return rangeTombstone(idx++);
            }
        };
    }

    private Iterator<RangeTombstone> reverseIterator(final Slice slice)
    {
        int startIdx = slice.end() == ClusteringBound.TOP ? size - 1 : searchInternal(slice.end(), 0, size);
        // if startIdx is the first range after 'slice.end()' we care only until the previous range
        final int start = startIdx < 0 ? -startIdx-2 : startIdx;

        if (start < 0)
            return Collections.emptyIterator();

        int finishIdx = slice.start() == ClusteringBound.BOTTOM ? 0 : searchInternal(slice.start(), 0, start + 1);  // include same as finish
        // if stopIdx is the first range after 'slice.end()' we care only until the previous range
        final int finish = finishIdx < 0 ? -finishIdx-1 : finishIdx;

        if (start < finish)
            return Collections.emptyIterator();

        if (start == finish)
        {
            // We want to make sure the range are stricly included within the queried slice as this
            // make it easier to combine things when iterator over successive slices.
            ClusteringBound s = comparator.compare(starts[start], slice.start()) < 0 ? slice.start() : starts[start];
            ClusteringBound e = comparator.compare(slice.end(), ends[start]) < 0 ? slice.end() : ends[start];
            return Iterators.<RangeTombstone>singletonIterator(rangeTombstoneWithNewBounds(start, s, e));
        }

        return new AbstractIterator<RangeTombstone>()
        {
            private int idx = start;

            protected RangeTombstone computeNext()
            {
                if (idx < 0 || idx < finish)
                    return endOfData();
                // We want to make sure the range are stricly included within the queried slice as this
                // make it easier to combine things when iterator over successive slices. This means that
                // for the first and last range we might have to "cut" the range returned.
                if (idx == start && comparator.compare(slice.end(), ends[idx]) < 0)
                    return rangeTombstoneWithNewEnd(idx--, slice.end());
                if (idx == finish && comparator.compare(starts[idx], slice.start()) < 0)
                    return rangeTombstoneWithNewStart(idx--, slice.start());
                return rangeTombstone(idx--);
            }
        };
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof RangeTombstoneList))
            return false;
        RangeTombstoneList that = (RangeTombstoneList)o;
        if (size != that.size)
            return false;

        for (int i = 0; i < size; i++)
        {
            if (!starts[i].equals(that.starts[i]))
                return false;
            if (!ends[i].equals(that.ends[i]))
                return false;
            if (markedAts[i] != that.markedAts[i])
                return false;
            if (delTimes[i] != that.delTimes[i])
                return false;
        }
        return true;
    }

    @Override
    public final int hashCode()
    {
        int result = size;
        for (int i = 0; i < size; i++)
        {
            result += starts[i].hashCode() + ends[i].hashCode();
            result += (int)(markedAts[i] ^ (markedAts[i] >>> 32));
            result += delTimes[i];
        }
        return result;
    }

    private static void copyArrays(RangeTombstoneList src, RangeTombstoneList dst)
    {
        dst.grow(src.size);
        System.arraycopy(src.starts, 0, dst.starts, 0, src.size);
        System.arraycopy(src.ends, 0, dst.ends, 0, src.size);
        System.arraycopy(src.markedAts, 0, dst.markedAts, 0, src.size);
        System.arraycopy(src.delTimes, 0, dst.delTimes, 0, src.size);
        dst.size = src.size;
        dst.boundaryHeapSize = src.boundaryHeapSize;
    }

    /*
     * Inserts a new element starting at index i. This method assumes that:
     *    ends[i-1] <= start < ends[i]
     * (note that start can be equal to ends[i-1] in the case where we have a boundary, i.e. for instance
     * ends[i-1] is the exclusive end of X and start is the inclusive start of X).
     *
     * A RangeTombstoneList is a list of range [s_0, e_0]...[s_n, e_n] such that:
     *   - s_i is a start bound and e_i is a end bound
     *   - s_i < e_i
     *   - e_i <= s_i+1
     * Basically, range are non overlapping and in order.
     */
    private void insertFrom(int i, ClusteringBound start, ClusteringBound end, long markedAt, int delTime)
    {
        while (i < size)
        {
            assert start.isStart() && end.isEnd();
            assert i == 0 || comparator.compare(ends[i-1], start) <= 0;
            assert comparator.compare(start, ends[i]) < 0;

            if (Slice.isEmpty(comparator, start, end))
                return;

            // Do we overwrite the current element?
            if (markedAt > markedAts[i])
            {
                // We do overwrite.

                // First deal with what might come before the newly added one.
                if (comparator.compare(starts[i], start) < 0)
                {
                    ClusteringBound newEnd = start.invert();
                    if (!Slice.isEmpty(comparator, starts[i], newEnd))
                    {
                        addInternal(i, starts[i], newEnd, markedAts[i], delTimes[i]);
                        i++;
                        setInternal(i, start, ends[i], markedAts[i], delTimes[i]);
                    }
                }

                // now, start <= starts[i]

                // Does the new element stops before the current one,
                int endCmp = comparator.compare(end, starts[i]);
                if (endCmp < 0)
                {
                    // Here start <= starts[i] and end < starts[i]
                    // This means the current element is before the current one.
                    addInternal(i, start, end, markedAt, delTime);
                    return;
                }

                // Do we overwrite the current element fully?
                int cmp = comparator.compare(ends[i], end);
                if (cmp <= 0)
                {
                    // We do overwrite fully:
                    // update the current element until it's end and continue on with the next element (with the new inserted start == current end).

                    // If we're on the last element, or if we stop before the next start, we set the current element and are done
                    // Note that the comparison below is inclusive: if a end equals a start, this means they form a boundary, or
                    // in other words that they are for the same element but one is inclusive while the other exclusive. In which case we know
                    // we're good with the next element
                    if (i == size-1 || comparator.compare(end, starts[i+1]) <= 0)
                    {
                        setInternal(i, start, end, markedAt, delTime);
                        return;
                    }

                    setInternal(i, start, starts[i+1].invert(), markedAt, delTime);
                    start = starts[i+1];
                    i++;
                }
                else
                {
                    // We don't overwrite fully. Insert the new interval, and then update the now next
                    // one to reflect the not overwritten parts. We're then done.
                    addInternal(i, start, end, markedAt, delTime);
                    i++;
                    ClusteringBound newStart = end.invert();
                    if (!Slice.isEmpty(comparator, newStart, ends[i]))
                    {
                        setInternal(i, newStart, ends[i], markedAts[i], delTimes[i]);
                    }
                    return;
                }
            }
            else
            {
                // we don't overwrite the current element

                // If the new interval starts before the current one, insert that new interval
                if (comparator.compare(start, starts[i]) < 0)
                {
                    // If we stop before the start of the current element, just insert the new interval and we're done;
                    // otherwise insert until the beginning of the current element
                    if (comparator.compare(end, starts[i]) <= 0)
                    {
                        addInternal(i, start, end, markedAt, delTime);
                        return;
                    }
                    ClusteringBound newEnd = starts[i].invert();
                    if (!Slice.isEmpty(comparator, start, newEnd))
                    {
                        addInternal(i, start, newEnd, markedAt, delTime);
                        i++;
                    }
                }

                // After that, we're overwritten on the current element but might have
                // some residual parts after ...

                // ... unless we don't extend beyond it.
                if (comparator.compare(end, ends[i]) <= 0)
                    return;

                start = ends[i].invert();
                i++;
            }
        }

        // If we got there, then just insert the remainder at the end
        addInternal(i, start, end, markedAt, delTime);
    }

    private int capacity()
    {
        return starts.length;
    }

    /*
     * Adds the new tombstone at index i, growing and/or moving elements to make room for it.
     */
    private void addInternal(int i, ClusteringBound start, ClusteringBound end, long markedAt, int delTime)
    {
        assert i >= 0;

        if (size == capacity())
            growToFree(i);
        else if (i < size)
            moveElements(i);

        setInternal(i, start, end, markedAt, delTime);
        size++;
    }

    /*
     * Grow the arrays, leaving index i "free" in the process.
     */
    private void growToFree(int i)
    {
        int newLength = (capacity() * 3) / 2 + 1;
        grow(i, newLength);
    }

    /*
     * Grow the arrays to match newLength capacity.
     */
    private void grow(int newLength)
    {
        if (capacity() < newLength)
            grow(-1, newLength);
    }

    private void grow(int i, int newLength)
    {
        starts = grow(starts, size, newLength, i);
        ends = grow(ends, size, newLength, i);
        markedAts = grow(markedAts, size, newLength, i);
        delTimes = grow(delTimes, size, newLength, i);
    }

    private static ClusteringBound[] grow(ClusteringBound[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        ClusteringBound[] newA = new ClusteringBound[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static long[] grow(long[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        long[] newA = new long[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    private static int[] grow(int[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        int[] newA = new int[newLength];
        System.arraycopy(a, 0, newA, 0, i);
        System.arraycopy(a, i, newA, i+1, size - i);
        return newA;
    }

    /*
     * Move elements so that index i is "free", assuming the arrays have at least one free slot at the end.
     */
    private void moveElements(int i)
    {
        if (i >= size)
            return;

        System.arraycopy(starts, i, starts, i+1, size - i);
        System.arraycopy(ends, i, ends, i+1, size - i);
        System.arraycopy(markedAts, i, markedAts, i+1, size - i);
        System.arraycopy(delTimes, i, delTimes, i+1, size - i);
        // we set starts[i] to null to indicate the position is now empty, so that we update boundaryHeapSize
        // when we set it
        starts[i] = null;
    }

    private void setInternal(int i, ClusteringBound start, ClusteringBound end, long markedAt, int delTime)
    {
        if (starts[i] != null)
            boundaryHeapSize -= starts[i].unsharedHeapSize() + ends[i].unsharedHeapSize();
        starts[i] = start;
        ends[i] = end;
        markedAts[i] = markedAt;
        delTimes[i] = delTime;
        boundaryHeapSize += start.unsharedHeapSize() + end.unsharedHeapSize();
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
                + boundaryHeapSize
                + ObjectSizes.sizeOfArray(starts)
                + ObjectSizes.sizeOfArray(ends)
                + ObjectSizes.sizeOfArray(markedAts)
                + ObjectSizes.sizeOfArray(delTimes);
    }
}
