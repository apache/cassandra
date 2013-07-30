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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data structure holding the range tombstones of a ColumnFamily.
 * <p>
 * This is essentially a sorted list of non-overlapping (tombstone) ranges.
 * <p>
 * A range tombstone has 4 elements: the start and end of the range covered,
 * and the deletion infos (markedAt timestamp and local deletion time). The
 * markedAt timestamp is what define the priority of 2 overlapping tombstones.
 * That is, given 2 tombstones [0, 10]@t1 and [5, 15]@t2, then if t2 > t1 (and
 * are the tombstones markedAt values), the 2nd tombstone take precedence over
 * the first one on [5, 10]. If such tombstones are added to a RangeTombstoneList,
 * the range tombstone list will store them as [[0, 5]@t1, [5, 15]@t2].
 * <p>
 * The only use of the local deletion time is to know when a given tombstone can
 * be purged, which will be done by the purge() method.
 */
public class RangeTombstoneList implements Iterable<RangeTombstone>
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTombstoneList.class);

    public static final Serializer serializer = new Serializer();

    private final Comparator<ByteBuffer> comparator;

    // Note: we don't want to use a List for the markedAts and delTimes to avoid boxing. We could
    // use a List for starts and ends, but having arrays everywhere is almost simpler.
    private ByteBuffer[] starts;
    private ByteBuffer[] ends;
    private long[] markedAts;
    private int[] delTimes;

    private int size;

    private RangeTombstoneList(Comparator<ByteBuffer> comparator, ByteBuffer[] starts, ByteBuffer[] ends, long[] markedAts, int[] delTimes, int size)
    {
        assert starts.length == ends.length && starts.length == markedAts.length && starts.length == delTimes.length;
        this.comparator = comparator;
        this.starts = starts;
        this.ends = ends;
        this.markedAts = markedAts;
        this.delTimes = delTimes;
        this.size = size;
    }

    public RangeTombstoneList(Comparator<ByteBuffer> comparator, int capacity)
    {
        this(comparator, new ByteBuffer[capacity], new ByteBuffer[capacity], new long[capacity], new int[capacity], 0);
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    public int size()
    {
        return size;
    }

    public Comparator<ByteBuffer> comparator()
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
                                      size);
    }

    public void add(RangeTombstone tombstone)
    {
        add(tombstone.min, tombstone.max, tombstone.data.markedForDeleteAt, tombstone.data.localDeletionTime);
    }

    /**
     * Adds a new range tombstone.
     *
     * This method will be faster if the new tombstone sort after all the currently existing ones (this is a common use case), 
     * but it doesn't assume it.
     */
    public void add(ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        if (isEmpty())
        {
            addInternal(0, start, end, markedAt, delTime);
            return;
        }

        int c = comparator.compare(starts[size-1], start);

        // Fast path if we add in sorted order
        if (c <= 0)
        {
            // Note that we may still overlap the last range
            insertFrom(size-1, start, end, markedAt, delTime);
        }
        else
        {
            int pos = Arrays.binarySearch(starts, 0, size, start, comparator);
            if (pos >= 0)
                insertFrom(pos, start, end, markedAt, delTime);
            else
                // Insertion point (-pos-1) is such start < start[-pos-1], so we should insert from the previous
                insertFrom(-pos-2, start, end, markedAt, delTime);
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
            copyArrays(tombstones, this);
            return;
        }

        /*
         * We basically have 2 techniques we can use here: either we repeatedly call add() on tombstones values,
         * or we do a merge of both (sorted) lists. If this lists is bigger enough than the one we add, the
         * calling add() will be faster, otherwise it's merging that will be faster.
         *
         * Let's note that during memtables updates, it might not be uncommon that a new update has only a few range
         * tombstones, while the CF we're adding it to (the on in the memtable) has many. In that case, using add() is
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
                if (comparator.compare(tombstones.starts[j], starts[i]) < 0)
                {
                    insertFrom(i-1, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
                    j++;
                }
                else
                {
                    i++;
                }
            }
            // Addds the remaining ones from tombstones if any (not that insertFrom will increment size if relevant).
            for (; j < tombstones.size; j++)
                insertFrom(size - 1, tombstones.starts[j], tombstones.ends[j], tombstones.markedAts[j], tombstones.delTimes[j]);
        }
    }

    /**
     * Returns whether the given name/timestamp pair is deleted by one of the tombstone
     * of this RangeTombstoneList.
     */
    public boolean isDeleted(ByteBuffer name, long timestamp)
    {
        int idx = searchInternal(name);
        return idx >= 0 && markedAts[idx] >= timestamp;
    }

    /**
     * Returns a new {@link InOrderTester}.
     */
    InOrderTester inOrderTester()
    {
        return new InOrderTester();
    }

    /**
     * Returns the DeletionTime for the tombstone overlapping {@code name} (there can't be more than one),
     * or null if {@code name} is not covered by any tombstone.
     */
    public DeletionTime search(ByteBuffer name) {
        int idx = searchInternal(name);
        return idx < 0 ? null : new DeletionTime(markedAts[idx], delTimes[idx]);
    }

    private int searchInternal(ByteBuffer name)
    {
        if (isEmpty())
            return -1;

        int pos = Arrays.binarySearch(starts, 0, size, name, comparator);
        if (pos >= 0)
        {
            // We're exactly on an interval start. The one subtility is that we need to check if
            // the previous is not equal to us and doesn't have a higher marked at
            if (pos > 0 && comparator.compare(name, ends[pos-1]) == 0 && markedAts[pos-1] > markedAts[pos])
                return pos-1;
            else
                return pos;
        }
        else
        {
            // We potentially intersect the range before our "insertion point"
            int idx = -pos-2;
            if (idx < 0)
                return -1;

            return comparator.compare(name, ends[idx]) <= 0 ? idx : -1;
        }
    }

    public int dataSize()
    {
        int dataSize = TypeSizes.NATIVE.sizeof(size);
        for (int i = 0; i < size; i++)
        {
            dataSize += starts[i].remaining() + ends[i].remaining();
            dataSize += TypeSizes.NATIVE.sizeof(markedAts[i]);
            dataSize += TypeSizes.NATIVE.sizeof(delTimes[i]);
        }
        return dataSize;
    }

    public long minMarkedAt()
    {
        long min = Long.MAX_VALUE;
        for (int i = 0; i < size; i++)
            min = Math.min(min, markedAts[i]);
        return min;
    }

    public long maxMarkedAt()
    {
        long max = Long.MIN_VALUE;
        for (int i = 0; i < size; i++)
            max = Math.max(max, markedAts[i]);
        return max;
    }

    public void updateAllTimestamp(long timestamp)
    {
        for (int i = 0; i < size; i++)
            markedAts[i] = timestamp;
    }

    /**
     * Removes all range tombstones whose local deletion time is older than gcBefore.
     */
    public void purge(int gcBefore)
    {
        int j = 0;
        for (int i = 0; i < size; i++)
        {
            if (delTimes[i] >= gcBefore)
                setInternal(j++, starts[i], ends[i], markedAts[i], delTimes[i]);
        }
        size = j;
    }

    /**
     * Returns whether {@code purge(gcBefore)} would remove something or not.
     */
    public boolean hasIrrelevantData(int gcBefore)
    {
        for (int i = 0; i < size; i++)
        {
            if (delTimes[i] < gcBefore)
                return true;
        }
        return false;
    }

    public Iterator<RangeTombstone> iterator()
    {
        return new AbstractIterator<RangeTombstone>()
        {
            private int idx;

            protected RangeTombstone computeNext()
            {
                if (idx >= size)
                    return endOfData();

                RangeTombstone t = new RangeTombstone(starts[idx], ends[idx], markedAts[idx], delTimes[idx]);
                idx++;
                return t;
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
    }

    /*
     * Inserts a new element whose start should be inserted at index i. This method
     * assumes that:
     *   - starts[i] <= start
     *   - start < starts[i+1] or there is no i+1 element.
     */
    private void insertFrom(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        if (i < 0)
        {
            insertAfter(i, start, end, markedAt, delTime);
            return;
        }

        /*
         * We have elt(i) = [s_i, e_i]@t_i and want to insert X = [s, e]@t, knowing that s_i < s < s_i+1.
         * We can have 3 cases:
         *  - s < e_i && e <= e_i: we're fully contained in i.
         *  - s < e_i && e > e_i: we rewrite X to X1=[s, e_i]@t + X2=[e_i, e]@t. X1 is fully contained
         *             in i and X2 is the insertAfter() case for i.
         *  - s >= e_i: we're in the insertAfter() case for i.
         */
        if (comparator.compare(start, ends[i]) < 0)
        {
            if (comparator.compare(end, ends[i]) <= 0)
            {
                update(i, start, end, markedAt, delTime);
            }
            else
            {
                insertAfter(i, ends[i], end, markedAt, delTime);
                update(i, start, ends[i], markedAt, delTime);
            }
        }
        else
        {
            insertAfter(i, start, end, markedAt, delTime);
        }
    }

    /*
     * Inserts a new element knowing that the new element start strictly after
     * the one at index i, i.e that:
     *   - ends[i] <= start (or i == -1)
     */
    private void insertAfter(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        if (i == size - 1)
        {
            addInternal(i+1, start, end, markedAt, delTime);
            return;
        }

        /*
         * We have the following intervals:
         *           i            i+1
         *   ..., [s1, e1]@t1, [s2, e2]@t2, ...
         *
         * And we want to insert X = [s, e]@t, knowing that e1 <= s.
         * We can have 2 cases:
         *  - s < s2: we rewrite X to X1=[s, s2]@t + X2=[s2, e]@t. X2 meet the weakInsertFrom() condition
         *            for i+1, and X1 is a new element between i and i+1.
         *  - s2 <= s: we're in the weakInsertFrom() case for i+1.
         */
        if (comparator.compare(start, starts[i+1]) < 0)
        {
            /*
             * If it happens the new element is fully before the current one, we insert it and
             * we're done
             */
            if (comparator.compare(end, starts[i+1]) <= 0)
            {
                addInternal(i+1, start, end, markedAt, delTime);
                return;
            }

            weakInsertFrom(i+1, starts[i+1], end, markedAt, delTime);
            addInternal(i+1, start, starts[i+1], markedAt, delTime);
        }
        else
        {
            weakInsertFrom(i+1, start, end, markedAt, delTime);
        }
    }

    /*
     * Weak version of insertFrom that only assumes the new element starts after index i,
     * but without knowing about the 2nd condition, i.e. this only assume that:
     *   - starts[i] <= start
     */
    private void weakInsertFrom(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        /*
         * Either start is before the next element start, and we're in fact in the insertFrom()
         * case, or it's not and it's an weakInsertFrom for the next index.
         */
        if (i == size - 1 || comparator.compare(start, starts[i+1]) < 0)
            insertFrom(i, start, end, markedAt, delTime);
        else
            weakInsertFrom(i+1, start, end, markedAt, delTime);
    }

    /*
     * Update index i with new element, assuming that new element is contained in the element i,
     * i.e that:
     *   - starts[i] <= s
     *   - e <= end[i]
     */
    private void update(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        /*
         * If the new markedAt is lower than the one of i, we can ignore the
         * new element, otherwise we split the current element.
         */
        if (markedAts[i] < markedAt)
        {
            if (comparator.compare(ends[i], end) != 0)
                addInternal(i+1, end, ends[i], markedAts[i], delTimes[i]);

            if (comparator.compare(starts[i], start) == 0)
            {
                markedAts[i] = markedAt;
                delTimes[i] = delTime;
                ends[i] = end;
            }
            else
            {
                addInternal(i+1, start, end, markedAt, delTime);
                ends[i] = start;
            }
        }
    }

    private int capacity()
    {
        return starts.length;
    }

    /*
     * Adds the new tombstone at index i, growing and/or moving elements to make room for it.
     */
    private void addInternal(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
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

    private static ByteBuffer[] grow(ByteBuffer[] a, int size, int newLength, int i)
    {
        if (i < 0 || i >= size)
            return Arrays.copyOf(a, newLength);

        ByteBuffer[] newA = new ByteBuffer[newLength];
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
    }

    private void setInternal(int i, ByteBuffer start, ByteBuffer end, long markedAt, int delTime)
    {
        starts[i] = start;
        ends[i] = end;
        markedAts[i] = markedAt;
        delTimes[i] = delTime;
    }

    public static class Serializer implements IVersionedSerializer<RangeTombstoneList>
    {
        private Serializer() {}

        public void serialize(RangeTombstoneList tombstones, DataOutput out, int version) throws IOException
        {
            if (tombstones == null)
            {
                out.writeInt(0);
                return;
            }

            out.writeInt(tombstones.size);
            for (int i = 0; i < tombstones.size; i++)
            {
                ByteBufferUtil.writeWithShortLength(tombstones.starts[i], out);
                ByteBufferUtil.writeWithShortLength(tombstones.ends[i], out);
                out.writeInt(tombstones.delTimes[i]);
                out.writeLong(tombstones.markedAts[i]);
            }
        }

        /*
         * RangeTombstoneList depends on the column family comparator, but it is not serialized.
         * Thus deserialize(DataInput, int, Comparator<ByteBuffer>) should be used instead of this method.
         */
        public RangeTombstoneList deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public RangeTombstoneList deserialize(DataInput in, int version, Comparator<ByteBuffer> comparator) throws IOException
        {
            int size = in.readInt();
            if (size == 0)
                return null;

            RangeTombstoneList tombstones = new RangeTombstoneList(comparator, size);

            for (int i = 0; i < size; i++)
            {
                ByteBuffer start = ByteBufferUtil.readWithShortLength(in);
                ByteBuffer end = ByteBufferUtil.readWithShortLength(in);
                int delTime =  in.readInt();
                long markedAt = in.readLong();

                if (version >= MessagingService.VERSION_20)
                {
                    tombstones.setInternal(i, start, end, markedAt, delTime);
                }
                else
                {
                    /*
                     * The old implementation used to have range sorted by left value, but with potentially
                     * overlapping range. So we need to use the "slow" path.
                     */
                    tombstones.add(start, end, markedAt, delTime);
                }
            }

            // The "slow" path take care of updating the size, but not the fast one
            if (version >= MessagingService.VERSION_20)
                tombstones.size = size;
            return tombstones;
        }

        public long serializedSize(RangeTombstoneList tombstones, TypeSizes typeSizes, int version)
        {
            if (tombstones == null)
                return typeSizes.sizeof(0);

            long size = typeSizes.sizeof(tombstones.size);
            for (int i = 0; i < tombstones.size; i++)
            {
                int startSize = tombstones.starts[i].remaining();
                size += typeSizes.sizeof((short)startSize) + startSize;
                int endSize = tombstones.ends[i].remaining();
                size += typeSizes.sizeof((short)endSize) + endSize;
                size += typeSizes.sizeof(tombstones.delTimes[i]);
                size += typeSizes.sizeof(tombstones.markedAts[i]);
            }
            return size;
        }

        public long serializedSize(RangeTombstoneList tombstones, int version)
        {
            return serializedSize(tombstones, TypeSizes.NATIVE, version);
        }
    }

    /**
     * This object allow testing whether a given column (name/timestamp) is deleted
     * or not by this RangeTombstoneList, assuming that the column given to this
     * object are passed in (comparator) sorted order.
     *
     * This is more efficient that calling RangeTombstoneList.isDeleted() repeatedly
     * in that case since we're able to take the sorted nature of the RangeTombstoneList
     * into account.
     */
    public class InOrderTester
    {
        private int idx;

        public boolean isDeleted(ByteBuffer name, long timestamp)
        {
            while (idx < size)
            {
                int cmp = comparator.compare(name, starts[idx]);
                if (cmp == 0)
                {
                    // As for searchInternal, we need to check the previous end
                    if (idx > 0 && comparator.compare(name, ends[idx-1]) == 0 && markedAts[idx-1] > markedAts[idx])
                        return markedAts[idx-1] >= timestamp;
                    else
                        return markedAts[idx] >= timestamp;
                }
                else if (cmp < 0)
                {
                    return false;
                }
                else
                {
                    if (comparator.compare(name, ends[idx]) <= 0)
                        return markedAts[idx] >= timestamp;
                    else
                        idx++;
                }
            }
            return false;
        }
    }
}
