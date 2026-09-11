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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterators;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

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
 * Snapshots share immutable BTree nodes and interval entries. Inserting one disjoint range
 * allocates O(log n) nodes; an overlapping update visits the affected intervals, copying
 * O(log n) nodes per changed interval. The list itself, like its array-backed predecessor,
 * requires a single writer; immutable snapshots may be read concurrently.
 */
public class RangeTombstoneList implements Iterable<RangeTombstone>, IMeasurableMemory, UpdateFunction<RangeTombstoneList.Range, RangeTombstoneList.Range>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RangeTombstoneList((ClusteringComparator) null));
    private static final long RANGE_SIZE = ObjectSizes.measure(new Range(null, null, 0, 0));

    private final ClusteringComparator comparator;
    private final BoundComparator byStart;
    private final BoundComparator byEnd;

    // Roots and entries are immutable. A writer replaces only its own root, never a published
    // partition's nodes. In particular, copy followed by an append copies a tree path, not all ranges.
    private Object[] tree = BTree.empty();
    private long boundaryHeapSize;
    private long treeHeapSize;
    private int size;

    public RangeTombstoneList(ClusteringComparator comparator)
    {
        this.comparator = comparator;
        this.byStart = new BoundComparator(comparator, true);
        this.byEnd = new BoundComparator(comparator, false);
    }

    private RangeTombstoneList(RangeTombstoneList source)
    {
        comparator = source.comparator;
        byStart = source.byStart;
        byEnd = source.byEnd;
        tree = source.tree;
        boundaryHeapSize = source.boundaryHeapSize;
        treeHeapSize = source.treeHeapSize;
        size = source.size;
    }

    // UpdateFunction<Range, Range>, used only by addInternal to insert a single new entry by end bound.
    public Range insert(Range range)
    {
        return range;
    }

    public Range merge(Range existing, Range update)
    {
        throw new IllegalStateException("Duplicate range end");
    }

    public void onAllocatedOnHeap(long delta)
    {
        treeHeapSize += delta;
    }

    static final class Range
    {
        final ClusteringBound<?> start;
        final ClusteringBound<?> end;
        final long markedAt;
        final int delTime;

        Range(ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTime)
        {
            this.start = start;
            this.end = end;
            this.markedAt = markedAt;
            this.delTime = delTime;
        }
    }

    private static final class BoundComparator implements Comparator<Object>
    {
        private final ClusteringComparator comparator;
        private final boolean start;

        BoundComparator(ClusteringComparator comparator, boolean start)
        {
            this.comparator = comparator;
            this.start = start;
        }

        private ClusteringPrefix<?> bound(Object value)
        {
            if (!(value instanceof Range))
                return (ClusteringPrefix<?>) value;
            Range range = (Range) value;
            return start ? range.start : range.end;
        }

        public int compare(Object left, Object right)
        {
            return comparator.compare(bound(left), bound(right));
        }
    }

    private Range get(int index)
    {
        return BTree.findByIndex(tree, index);
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
        return new RangeTombstoneList(this);
    }

    public RangeTombstoneList clone(ByteBufferCloner cloner)
    {
        RangeTombstoneList copy = copy();
        copy.tree = BTree.<Range, Range>transform(tree, range -> new Range(clone(range.start, cloner),
                                                                        clone(range.end, cloner),
                                                                        range.markedAt, range.delTime));
        return copy;
    }

    private static <T> ClusteringBound<ByteBuffer> clone(ClusteringBound<T> bound, ByteBufferCloner cloner)
    {
        ByteBuffer[] values = new ByteBuffer[bound.size()];
        for (int i = 0; i < values.length; i++)
            values[i] = cloner.clone(bound.get(i), bound.accessor());
        return new BufferClusteringBound(bound.kind(), values);
    }

    public void add(RangeTombstone tombstone)
    {
        add(tombstone.deletedSlice().start(),
            tombstone.deletedSlice().end(),
            tombstone.deletionTime().markedForDeleteAt(),
            tombstone.deletionTime().localDeletionTimeUnsignedInteger());
    }

    /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case),
     * but it doesn't assume it.
     */
    private void add(ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInteger)
    {
        if (isEmpty())
        {
            addInternal(0, start, end, markedAt, delTimeUnsignedInteger);
            return;
        }

        if (Slice.isEmpty(comparator, start, end))
            return;

        int c = comparator.compare(get(size-1).end, start);

        // Fast path if we add in sorted order
        if (c <= 0)
        {
            addInternal(size, start, end, markedAt, delTimeUnsignedInteger);
        }
        else
        {
            // Note: insertFrom expect i to be the insertion point in term of interval ends
            int pos = BTree.findIndex(tree, byEnd, start);
            insertFrom((pos >= 0 ? pos+1 : -pos-1), start, end, markedAt, delTimeUnsignedInteger);
        }
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
            tree = tombstones.tree;
            size = tombstones.size;
            boundaryHeapSize = tombstones.boundaryHeapSize;
            treeHeapSize = tombstones.treeHeapSize;
            return;
        }

        Iterator<Range> ranges = BTree.iterator(tombstones.tree);
        while (ranges.hasNext())
        {
            Range range = ranges.next();
            add(range.start, range.end, range.markedAt, range.delTime);
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(Clustering<?> clustering, Cell<?> cell)
    {
        int idx = searchInternal(clustering);
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        return idx >= 0 && (cell.isCounterCell() || get(idx).markedAt >= cell.timestamp());
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime searchDeletionTime(Clustering<?> name)
    {
        int idx = searchInternal(name);
        if (idx < 0)
            return null;
        Range range = get(idx);
        return DeletionTime.buildUnsafeWithUnsignedInteger(range.markedAt, range.delTime);
    }

    public RangeTombstone search(Clustering<?> name)
    {
        int idx = searchInternal(name);
        return idx < 0 ? null : rangeTombstone(get(idx));
    }

    /*
     * Return is the index of the range covering name if name is covered. If the return idx is negative,
     * no range cover name and -idx-1 is the index of the first range whose start is greater than name.
     *
     * Note that bounds are not in the range if they fall on its boundary.
     */
    private int searchInternal(ClusteringPrefix<?> name)
    {
        if (isEmpty())
            return -1;

        int pos = BTree.findIndex(tree, byStart, name);
        if (pos >= 0)
        {
            // Equality only happens for bounds (as used by slice iteration), and bounds are equal only if they
            // are the same or complementary, in either case the bound itself is not part of the range.
            return -pos - 1;
        }
        else
        {
            // We potentially intersect the range before our "insertion point"
            int idx = -pos-2;
            if (idx < 0)
                return -1;

            return comparator.compare(name, get(idx).end) < 0 ? idx : -idx-2;
        }
    }

    public int dataSize()
    {
        int dataSize = TypeSizes.sizeof(size);
        Iterator<Range> ranges = BTree.iterator(tree);
        while (ranges.hasNext())
        {
            Range range = ranges.next();
            dataSize += range.start.dataSize() + range.end.dataSize();
            dataSize += TypeSizes.sizeof(range.markedAt);
            dataSize += TypeSizes.sizeof(range.delTime);
        }
        return dataSize;
    }

    public long maxMarkedAt()
    {
        long max = Long.MIN_VALUE;
        Iterator<Range> ranges = BTree.iterator(tree);
        while (ranges.hasNext())
            max = Math.max(max, ranges.next().markedAt);
        return max;
    }

    public void collectStats(EncodingStats.Collector collector)
    {
        Iterator<Range> ranges = BTree.iterator(tree);
        while (ranges.hasNext())
        {
            Range range = ranges.next();
            collector.updateTimestamp(range.markedAt);
            collector.updateLocalDeletionTime(CassandraUInt.toLong(range.delTime));
        }
    }

    public void updateAllTimestamp(long timestamp)
    {
        tree = BTree.<Range, Range>transform(tree, range -> range.markedAt == timestamp
                                                        ? range
                                                        : new Range(range.start, range.end, timestamp, range.delTime));
    }

    public void updateAllTimestampAndLocalDeletionTime(long timestamp, long localDeletionTime)
    {
        int delTime = Cell.deletionTimeLongToUnsignedInteger(localDeletionTime);
        tree = BTree.<Range, Range>transform(tree, range -> range.markedAt == timestamp && range.delTime == delTime
                                                        ? range
                                                        : new Range(range.start, range.end, timestamp, delTime));
    }

    private static RangeTombstone rangeTombstone(Range range)
    {
        return new RangeTombstone(Slice.make(range.start, range.end),
                                  DeletionTime.buildUnsafeWithUnsignedInteger(range.markedAt, range.delTime));
    }

    public Iterator<RangeTombstone> iterator()
    {
        return iterator(false);
    }

    public Iterator<RangeTombstone> iterator(boolean reversed)
    {
        return Iterators.transform(BTree.<Range>iterator(tree, reversed ? BTree.Dir.DESC : BTree.Dir.ASC),
                                   RangeTombstoneList::rangeTombstone);
    }

    public Iterator<RangeTombstone> iterator(final Slice slice, boolean reversed)
    {
        if (isEmpty() || Slice.isEmpty(comparator, slice.start(), slice.end()))
            return Collections.emptyIterator();

        int startIdx = slice.start().isBottom() ? 0 : searchInternal(slice.start());
        final int start = startIdx < 0 ? -startIdx-1 : startIdx;
        int finishIdx = slice.end().isTop() ? size - 1 : searchInternal(slice.end());
        final int finish = finishIdx < 0 ? -finishIdx-2 : finishIdx;
        if (start > finish)
            return Collections.emptyIterator();

        Iterator<Range> ranges = BTree.iterator(tree, start, finish, reversed ? BTree.Dir.DESC : BTree.Dir.ASC);
        return new AbstractIterator<RangeTombstone>()
        {
            private int idx = reversed ? finish : start;

            protected RangeTombstone computeNext()
            {
                if (!ranges.hasNext())
                    return endOfData();

                Range range = ranges.next();
                ClusteringBound<?> s = idx == start && comparator.compare(range.start, slice.start()) < 0
                                       ? slice.start() : range.start;
                ClusteringBound<?> e = idx == finish && comparator.compare(slice.end(), range.end) < 0
                                       ? slice.end() : range.end;
                idx += reversed ? -1 : 1;
                return new RangeTombstone(Slice.make(s, e),
                                          DeletionTime.buildUnsafeWithUnsignedInteger(range.markedAt, range.delTime));
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

        Iterator<Range> left = BTree.iterator(tree);
        Iterator<Range> right = BTree.iterator(that.tree);
        while (left.hasNext())
        {
            Range a = left.next();
            Range b = right.next();
            if (!a.start.equals(b.start) || !a.end.equals(b.end)
                || a.markedAt != b.markedAt || a.delTime != b.delTime)
                return false;
        }
        return true;
    }

    @Override
    public final int hashCode()
    {
        int result = size;
        Iterator<Range> ranges = BTree.iterator(tree);
        while (ranges.hasNext())
        {
            Range range = ranges.next();
            result += range.start.hashCode() + range.end.hashCode();
            result += (int)(range.markedAt ^ (range.markedAt >>> 32));
            result += range.delTime;
        }
        return result;
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
    private void insertFrom(int i, ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInternal)
    {
        // A tombstone that supersedes many intervals would path-copy the tree once per interval; past a small
        // fraction of the list it is cheaper to merge the affected run in an array and rebuild the tree once.
        int pos = BTree.findIndex(tree, byStart, end);
        int to = pos >= 0 ? pos : -pos - 1;
        if (to - i >= 32 && to - i >= size / 32)
        {
            Window window = new Window(i, to);
            insertFrom(window, 0, start, end, markedAt, delTimeUnsignedInternal);
            window.commit(i, to);
        }
        else
        {
            insertFrom(new TreeRanges(), i, start, end, markedAt, delTimeUnsignedInternal);
        }
    }

    private void insertFrom(Ranges ranges, int i, ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTimeUnsignedInternal)
    {
        while (i < ranges.size())
        {
            Range current = ranges.get(i);
            assert start.isStart() && end.isEnd();
            assert i == 0 || comparator.compare(ranges.get(i - 1).end, start) <= 0;
            assert comparator.compare(start, current.end) < 0;

            if (Slice.isEmpty(comparator, start, end))
                return;

            // Do we overwrite the current element?
            if (markedAt > current.markedAt)
            {
                // We do overwrite.

                // First deal with what might come before the newly added one.
                if (comparator.compare(current.start, start) < 0)
                {
                    ClusteringBound<?> newEnd = start.invert();
                    if (!Slice.isEmpty(comparator, current.start, newEnd))
                    {
                        ranges.add(i, new Range(current.start, newEnd, current.markedAt, current.delTime));
                        i++;
                        ranges.set(i, new Range(start, current.end, current.markedAt, current.delTime));
                        current = ranges.get(i);
                    }
                }

                // now, start <= starts[i]

                // Does the new element stops before the current one,
                int endCmp = comparator.compare(end, current.start);
                if (endCmp < 0)
                {
                    // Here start <= starts[i] and end < starts[i]
                    // This means the current element is before the current one.
                    ranges.add(i, new Range(start, end, markedAt, delTimeUnsignedInternal));
                    return;
                }

                // Do we overwrite the current element fully?
                int cmp = comparator.compare(current.end, end);
                if (cmp <= 0)
                {
                    // We do overwrite fully:
                    // update the current element until it's end and continue on with the next element (with the new inserted start == current end).

                    // If we're on the last element, or if we stop before the next start, we set the current element and are done
                    // Note that the comparison below is inclusive: if a end equals a start, this means they form a boundary, or
                    // in other words that they are for the same element but one is inclusive while the other exclusive. In which case we know
                    // we're good with the next element
                    Range next = i == ranges.size() - 1 ? null : ranges.get(i + 1);
                    if (next == null || comparator.compare(end, next.start) <= 0)
                    {
                        ranges.set(i, new Range(start, end, markedAt, delTimeUnsignedInternal));
                        return;
                    }

                    ranges.set(i, new Range(start, next.start.invert(), markedAt, delTimeUnsignedInternal));
                    start = next.start;
                    i++;
                }
                else
                {
                    // We don't overwrite fully. Insert the new interval, and then update the now next
                    // one to reflect the not overwritten parts. We're then done.
                    ranges.add(i, new Range(start, end, markedAt, delTimeUnsignedInternal));
                    i++;
                    ClusteringBound<?> newStart = end.invert();
                    current = ranges.get(i);
                    if (!Slice.isEmpty(comparator, newStart, current.end))
                    {
                        ranges.set(i, new Range(newStart, current.end, current.markedAt, current.delTime));
                    }
                    return;
                }
            }
            else
            {
                // we don't overwrite the current element

                // If the new interval starts before the current one, insert that new interval
                if (comparator.compare(start, current.start) < 0)
                {
                    // If we stop before the start of the current element, just insert the new interval and we're done;
                    // otherwise insert until the beginning of the current element
                    if (comparator.compare(end, current.start) <= 0)
                    {
                        ranges.add(i, new Range(start, end, markedAt, delTimeUnsignedInternal));
                        return;
                    }
                    ClusteringBound<?> newEnd = current.start.invert();
                    if (!Slice.isEmpty(comparator, start, newEnd))
                    {
                        ranges.add(i, new Range(start, newEnd, markedAt, delTimeUnsignedInternal));
                        i++;
                    }
                }

                // After that, we're overwritten on the current element but might have
                // some residual parts after ...

                // ... unless we don't extend beyond it.
                if (comparator.compare(end, current.end) <= 0)
                    return;

                start = current.end.invert();
                i++;
            }
        }

        // If we got there, then just insert the remainder at the end
        ranges.add(i, new Range(start, end, markedAt, delTimeUnsignedInternal));
    }

    /*
     * Entries are ordered by end bound. During a split the new left fragment can have the
     * same start as its successor, but its end is strictly smaller.
     */
    private void addInternal(int i, ClusteringBound<?> start, ClusteringBound<?> end, long markedAt, int delTime)
    {
        addInternal(i, new Range(start, end, markedAt, delTime));
    }

    private void addInternal(int i, Range range)
    {
        assert i >= 0 && i <= size;
        tree = BTree.update(tree, BTree.singleton(range), byEnd, this);
        assert get(i) == range;
        boundaryHeapSize += range.start.unsharedHeapSize() + range.end.unsharedHeapSize();
        size++;
    }

    private void setInternal(int i, Range range)
    {
        Range previous = get(i);
        if (previous.start == range.start && previous.end == range.end && previous.markedAt == range.markedAt && previous.delTime == range.delTime)
            return;
        tree = BTree.replace(tree, i, range);
        boundaryHeapSize += range.start.unsharedHeapSize() + range.end.unsharedHeapSize()
                            - previous.start.unsharedHeapSize() - previous.end.unsharedHeapSize();
    }

    /** The intervals insertFrom merges into: either this list's tree or an array copy of the affected run. */
    private interface Ranges
    {
        Range get(int i);
        void add(int i, Range range);
        void set(int i, Range range);
        int size();
    }

    private final class TreeRanges implements Ranges
    {
        public Range get(int i)
        {
            return RangeTombstoneList.this.get(i);
        }

        public void add(int i, Range range)
        {
            addInternal(i, range);
        }

        public void set(int i, Range range)
        {
            setInternal(i, range);
        }

        public int size()
        {
            return size;
        }
    }

    private final class Window implements Ranges
    {
        private final List<Range> ranges;
        private long boundaryDelta;

        Window(int from, int to)
        {
            ranges = new ArrayList<>(to - from + 1);
            Iterators.addAll(ranges, BTree.iterator(tree, from, to - 1, BTree.Dir.ASC));
        }

        public Range get(int i)
        {
            return ranges.get(i);
        }

        public void add(int i, Range range)
        {
            ranges.add(i, range);
            boundaryDelta += range.start.unsharedHeapSize() + range.end.unsharedHeapSize();
        }

        public void set(int i, Range range)
        {
            Range previous = ranges.set(i, range);
            boundaryDelta += range.start.unsharedHeapSize() + range.end.unsharedHeapSize()
                             - previous.start.unsharedHeapSize() - previous.end.unsharedHeapSize();
        }

        public int size()
        {
            return ranges.size();
        }

        /** Replaces the run [from, to) of the tree with this window's intervals, rebuilding the tree once. */
        void commit(int from, int to)
        {
            int newSize = size - (to - from) + ranges.size();
            Iterator<Range> merged = Iterators.concat(BTree.iterator(tree, 0, from - 1, BTree.Dir.ASC),
                                                      ranges.iterator(),
                                                      BTree.iterator(tree, to, size - 1, BTree.Dir.ASC));
            treeHeapSize = 0;
            tree = BTree.build(BulkIterator.of(merged), newSize, RangeTombstoneList.this);
            size = newSize;
            boundaryHeapSize += boundaryDelta;
        }
    }

    @Override
    public long unsharedHeapSize()
    {
        // byStart/byEnd are shared with every copy() derived from this list, so they are not counted here.
        return EMPTY_SIZE + boundaryHeapSize + treeHeapSize + size * RANGE_SIZE;
    }
}
