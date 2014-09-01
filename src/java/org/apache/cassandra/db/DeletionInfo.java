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

import java.util.Iterator;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * A combination of a top-level (partition) tombstone and range tombstones describing the deletions
 * within a partition.
 */
public class DeletionInfo implements IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new DeletionInfo(0, 0));

    /**
     * This represents a deletion of the entire partition. We can't represent this within the RangeTombstoneList, so it's
     * kept separately. This also slightly optimizes the common case of a full partition deletion.
     */
    private DeletionTime partitionDeletion;

    /**
     * A list of range tombstones within the partition.  This is left as null if there are no range tombstones
     * (to save an allocation (since it's a common case).
     */
    private RangeTombstoneList ranges;

    /**
     * Creates a DeletionInfo with only a top-level (row) tombstone.
     * @param markedForDeleteAt the time after which the entire row should be considered deleted
     * @param localDeletionTime what time the deletion write was applied locally (for purposes of
     *                          purging the tombstone after gc_grace_seconds).
     */
    public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        this(new SimpleDeletionTime(markedForDeleteAt, localDeletionTime == Integer.MIN_VALUE ? Integer.MAX_VALUE : localDeletionTime));
    }

    public DeletionInfo(DeletionTime partitionDeletion)
    {
        this(partitionDeletion, null);
    }

    public DeletionInfo(ClusteringComparator comparator, Slice slice, long markedForDeleteAt, int localDeletionTime)
    {
        this(DeletionTime.LIVE, new RangeTombstoneList(comparator, 1));
        ranges.add(slice.start(), slice.end(), markedForDeleteAt, localDeletionTime);
    }

    public DeletionInfo(DeletionTime partitionDeletion, RangeTombstoneList ranges)
    {
        this.partitionDeletion = partitionDeletion.takeAlias();
        this.ranges = ranges;
    }

    /**
     * Returns a new DeletionInfo that has no top-level tombstone or any range tombstones.
     */
    public static DeletionInfo live()
    {
        return new DeletionInfo(DeletionTime.LIVE);
    }

    public DeletionInfo copy()
    {
        return new DeletionInfo(partitionDeletion, ranges == null ? null : ranges.copy());
    }

    public DeletionInfo copy(AbstractAllocator allocator)
    {
        RangeTombstoneList rangesCopy = null;
        if (ranges != null)
             rangesCopy = ranges.copy(allocator);

        return new DeletionInfo(partitionDeletion, rangesCopy);
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return partitionDeletion.isLive() && (ranges == null || ranges.isEmpty());
    }

    /**
     * Return whether a given cell is deleted by this deletion info.
     *
     * @param clustering the clustering for the cell to check.
     * @param cell the cell to check.
     * @return true if the cell is deleted, false otherwise
     */
    private boolean isDeleted(Clustering clustering, Cell cell)
    {
        // If we're live, don't consider anything deleted, even if the cell ends up having as timestamp Long.MIN_VALUE
        // (which shouldn't happen in practice, but it would invalid to consider it deleted if it does).
        if (isLive())
            return false;

        if (cell.livenessInfo().timestamp() <= partitionDeletion.markedForDeleteAt())
            return true;

        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        if (!partitionDeletion.isLive() && cell.isCounterCell())
            return true;

        return ranges != null && ranges.isDeleted(clustering, cell);
    }

    /**
     * Potentially replaces the top-level tombstone with another, keeping whichever has the higher markedForDeleteAt
     * timestamp.
     * @param newInfo
     */
    public void add(DeletionTime newInfo)
    {
        if (newInfo.supersedes(partitionDeletion))
            partitionDeletion = newInfo;
    }

    public void add(RangeTombstone tombstone, ClusteringComparator comparator)
    {
        if (ranges == null)
            ranges = new RangeTombstoneList(comparator, 1);

        ranges.add(tombstone);
    }

    /**
     * Combines another DeletionInfo with this one and returns the result.  Whichever top-level tombstone
     * has the higher markedForDeleteAt timestamp will be kept, along with its localDeletionTime.  The
     * range tombstones will be combined.
     *
     * @return this object.
     */
    public DeletionInfo add(DeletionInfo newInfo)
    {
        add(newInfo.partitionDeletion);

        if (ranges == null)
            ranges = newInfo.ranges == null ? null : newInfo.ranges.copy();
        else if (newInfo.ranges != null)
            ranges.addAll(newInfo.ranges);

        return this;
    }

    public DeletionTime getPartitionDeletion()
    {
        return partitionDeletion;
    }

    // Use sparingly, not the most efficient thing
    public Iterator<RangeTombstone> rangeIterator(boolean reversed)
    {
        return ranges == null ? Iterators.<RangeTombstone>emptyIterator() : ranges.iterator(reversed);
    }

    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
    {
        return ranges == null ? Iterators.<RangeTombstone>emptyIterator() : ranges.iterator(slice, reversed);
    }

    public RangeTombstone rangeCovering(Clustering name)
    {
        return ranges == null ? null : ranges.search(name);
    }

    public int dataSize()
    {
        int size = TypeSizes.NATIVE.sizeof(partitionDeletion.markedForDeleteAt());
        return size + (ranges == null ? 0 : ranges.dataSize());
    }

    public boolean hasRanges()
    {
        return ranges != null && !ranges.isEmpty();
    }

    public int rangeCount()
    {
        return hasRanges() ? ranges.size() : 0;
    }

    /**
     * Whether this deletion info may modify the provided one if added to it.
     */
    public boolean mayModify(DeletionInfo delInfo)
    {
        return partitionDeletion.compareTo(delInfo.partitionDeletion) > 0 || hasRanges();
    }

    @Override
    public String toString()
    {
        if (ranges == null || ranges.isEmpty())
            return String.format("{%s}", partitionDeletion);
        else
            return String.format("{%s, ranges=%s}", partitionDeletion, rangesAsString());
    }

    private String rangesAsString()
    {
        assert !ranges.isEmpty();
        StringBuilder sb = new StringBuilder();
        ClusteringComparator cc = ranges.comparator();
        Iterator<RangeTombstone> iter = rangeIterator(false);
        while (iter.hasNext())
        {
            RangeTombstone i = iter.next();
            sb.append(i.deletedSlice().toString(cc));
            sb.append("@");
            sb.append(i.deletionTime());
        }
        return sb.toString();
    }

    // Updates all the timestamp of the deletion contained in this DeletionInfo to be {@code timestamp}.
    public DeletionInfo updateAllTimestamp(long timestamp)
    {
        if (partitionDeletion.markedForDeleteAt() != Long.MIN_VALUE)
            partitionDeletion = new SimpleDeletionTime(timestamp, partitionDeletion.localDeletionTime());

        if (ranges != null)
            ranges.updateAllTimestamp(timestamp);
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionInfo))
            return false;
        DeletionInfo that = (DeletionInfo)o;
        return partitionDeletion.equals(that.partitionDeletion) && Objects.equal(ranges, that.ranges);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(partitionDeletion, ranges);
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + partitionDeletion.unsharedHeapSize() + (ranges == null ? 0 : ranges.unsharedHeapSize());
    }
}
