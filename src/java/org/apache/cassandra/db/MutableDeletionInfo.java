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

import java.util.Collections;
import java.util.Iterator;

import com.google.common.base.Objects;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * A mutable implementation of {@code DeletionInfo}.
 */
public class MutableDeletionInfo implements DeletionInfo
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new MutableDeletionInfo(0, 0));

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
    public MutableDeletionInfo(long markedForDeleteAt, long localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        this(DeletionTime.build(markedForDeleteAt, localDeletionTime == Integer.MIN_VALUE ? Long.MAX_VALUE : localDeletionTime));
    }

    public MutableDeletionInfo(DeletionTime partitionDeletion)
    {
        this(partitionDeletion, null);
    }

    public MutableDeletionInfo(DeletionTime partitionDeletion, RangeTombstoneList ranges)
    {
        this.partitionDeletion = partitionDeletion;
        this.ranges = ranges;
    }

    /**
     * Returns a new DeletionInfo that has no top-level tombstone or any range tombstones.
     */
    public static MutableDeletionInfo live()
    {
        return new MutableDeletionInfo(DeletionTime.LIVE);
    }

    public MutableDeletionInfo mutableCopy()
    {
        return new MutableDeletionInfo(partitionDeletion, ranges == null ? null : ranges.copy());
    }

    @Override
    public MutableDeletionInfo clone(ByteBufferCloner cloner)
    {
        RangeTombstoneList rangesCopy = null;
        if (ranges != null)
             rangesCopy = ranges.clone(cloner);

        return new MutableDeletionInfo(partitionDeletion, rangesCopy);
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return partitionDeletion.isLive() && (ranges == null || ranges.isEmpty());
    }

    /**
     * Potentially replaces the top-level tombstone with another, keeping whichever has the higher markedForDeleteAt
     * timestamp.
     * @param newInfo the deletion time to add to this deletion info.
     */
    public void add(DeletionTime newInfo)
    {
        if (newInfo.supersedes(partitionDeletion))
            partitionDeletion = newInfo;
    }

    public void add(RangeTombstone tombstone, ClusteringComparator comparator)
    {
        if (ranges == null) // Introduce getInitialRangeTombstoneAllocationSize
            ranges = new RangeTombstoneList(comparator, DatabaseDescriptor.getInitialRangeTombstoneListAllocationSize());

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
        add(newInfo.getPartitionDeletion());

        // We know MutableDeletionInfo is the only impelementation and we're not mutating it, it's just to get access to the
        // RangeTombstoneList directly.
        assert newInfo instanceof MutableDeletionInfo;
        RangeTombstoneList newRanges = ((MutableDeletionInfo)newInfo).ranges;

        if (ranges == null)
            ranges = newRanges == null ? null : newRanges.copy();
        else if (newRanges != null)
            ranges.addAll(newRanges);

        return this;
    }

    public DeletionTime getPartitionDeletion()
    {
        return partitionDeletion;
    }

    // Use sparingly, not the most efficient thing
    public Iterator<RangeTombstone> rangeIterator(boolean reversed)
    {
        return ranges == null ? Collections.emptyIterator() : ranges.iterator(reversed);
    }

    public Iterator<RangeTombstone> rangeIterator(Slice slice, boolean reversed)
    {
        return ranges == null ? Collections.emptyIterator() : ranges.iterator(slice, reversed);
    }

    public RangeTombstone rangeCovering(Clustering<?> name)
    {
        return ranges == null ? null : ranges.search(name);
    }

    public int dataSize()
    {
        int size = TypeSizes.sizeof(partitionDeletion.markedForDeleteAt());
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

    public long maxTimestamp()
    {
        return ranges == null ? partitionDeletion.markedForDeleteAt() : Math.max(partitionDeletion.markedForDeleteAt(), ranges.maxMarkedAt());
    }

    /**
     * Whether this deletion info may modify the provided one if added to it.
     */
    public boolean mayModify(DeletionInfo delInfo)
    {
        return partitionDeletion.compareTo(delInfo.getPartitionDeletion()) > 0 || hasRanges();
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
            sb.append('@');
            sb.append(i.deletionTime());
        }
        return sb.toString();
    }

    // Updates all the timestamp of the deletion contained in this DeletionInfo to be {@code timestamp}.
    public DeletionInfo updateAllTimestamp(long timestamp)
    {
        if (partitionDeletion.markedForDeleteAt() != Long.MIN_VALUE)
            partitionDeletion = DeletionTime.build(timestamp, partitionDeletion.localDeletionTime());

        if (ranges != null)
            ranges.updateAllTimestamp(timestamp);
        return this;
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof MutableDeletionInfo))
            return false;
        MutableDeletionInfo that = (MutableDeletionInfo)o;
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
        if (this == LIVE)
            return 0;

        return EMPTY_SIZE + partitionDeletion.unsharedHeapSize() + (ranges == null ? 0 : ranges.unsharedHeapSize());
    }

    public void collectStats(EncodingStats.Collector collector)
    {
        collector.update(partitionDeletion);
        if (ranges != null)
            ranges.collectStats(collector);
    }

    public static Builder builder(DeletionTime partitionLevelDeletion, ClusteringComparator comparator, boolean reversed)
    {
        return new Builder(partitionLevelDeletion, comparator, reversed);
    }

    /**
     * Builds DeletionInfo object from (in order) range tombstone markers.
     */
    public static class Builder
    {
        private final MutableDeletionInfo deletion;
        private final ClusteringComparator comparator;

        private final boolean reversed;

        private RangeTombstoneMarker openMarker;

        private Builder(DeletionTime partitionLevelDeletion, ClusteringComparator comparator, boolean reversed)
        {
            this.deletion = new MutableDeletionInfo(partitionLevelDeletion);
            this.comparator = comparator;
            this.reversed = reversed;
        }

        public void add(RangeTombstoneMarker marker)
        {
            // We need to start by the close case in case that's a boundary

            if (marker.isClose(reversed))
            {
                DeletionTime openDeletion = openMarker.openDeletionTime(reversed);
                assert marker.closeDeletionTime(reversed).equals(openDeletion);

                ClusteringBound<?> open = openMarker.openBound(reversed);
                ClusteringBound<?> close = marker.closeBound(reversed);

                Slice slice = reversed ? Slice.make(close, open) : Slice.make(open, close);
                deletion.add(new RangeTombstone(slice, openDeletion), comparator);
            }

            if (marker.isOpen(reversed))
            {
                openMarker = marker;
            }
        }

        public MutableDeletionInfo build()
        {
            return deletion;
        }
    }
}
