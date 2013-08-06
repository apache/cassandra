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
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.IVersionedSerializer;

public class DeletionInfo
{
    private static final Serializer serializer = new Serializer();

    // We don't have way to represent the full interval of keys (Interval don't support the minimum token as the right bound),
    // so we keep the topLevel deletion info separatly. This also slightly optimize the case of full row deletion which is rather common.
    private DeletionTime topLevel;
    private RangeTombstoneList ranges; // null if no range tombstones (to save an allocation since it's a common case).

    public DeletionInfo(long markedForDeleteAt, int localDeletionTime)
    {
        // Pre-1.1 node may return MIN_VALUE for non-deleted container, but the new default is MAX_VALUE
        // (see CASSANDRA-3872)
        this(new DeletionTime(markedForDeleteAt, localDeletionTime == Integer.MIN_VALUE ? Integer.MAX_VALUE : localDeletionTime));
    }

    public DeletionInfo(DeletionTime topLevel)
    {
        this(topLevel, null);
    }

    public DeletionInfo(ByteBuffer start, ByteBuffer end, Comparator<ByteBuffer> comparator, long markedForDeleteAt, int localDeletionTime)
    {
        this(DeletionTime.LIVE, new RangeTombstoneList(comparator, 1));
        ranges.add(start, end, markedForDeleteAt, localDeletionTime);
    }

    public DeletionInfo(RangeTombstone rangeTombstone, Comparator<ByteBuffer> comparator)
    {
        this(rangeTombstone.min, rangeTombstone.max, comparator, rangeTombstone.data.markedForDeleteAt, rangeTombstone.data.localDeletionTime);
    }

    public static DeletionInfo live()
    {
        return new DeletionInfo(DeletionTime.LIVE);
    }

    private DeletionInfo(DeletionTime topLevel, RangeTombstoneList ranges)
    {
        this.topLevel = topLevel;
        this.ranges = ranges;
    }

    public static Serializer serializer()
    {
        return serializer;
    }

    public DeletionInfo copy()
    {
        return new DeletionInfo(topLevel, ranges == null ? null : ranges.copy());
    }

    /**
     * Returns whether this DeletionInfo is live, that is deletes no columns.
     */
    public boolean isLive()
    {
        return topLevel.markedForDeleteAt == Long.MIN_VALUE
            && topLevel.localDeletionTime == Integer.MAX_VALUE
            && (ranges == null || ranges.isEmpty());
    }

    /**
     * Return whether a given column is deleted by the container having this
     * deletion info.
     *
     * @param column the column to check.
     * @return true if the column is deleted, false otherwise
     */
    public boolean isDeleted(Column column)
    {
        return isDeleted(column.name(), column.timestamp());
    }

    public boolean isDeleted(ByteBuffer name, long timestamp)
    {
        // We do rely on this test: if topLevel.markedForDeleteAt is MIN_VALUE, we should not
        // consider the column deleted even if timestamp=MIN_VALUE, otherwise this break QueryFilter.isRelevant
        if (isLive())
            return false;

        if (timestamp <= topLevel.markedForDeleteAt)
            return true;

        return ranges != null && ranges.isDeleted(name, timestamp);
    }

    /**
     * Returns a new {@link InOrderTester} in forward order.
     */
    InOrderTester inOrderTester()
    {
        return inOrderTester(false);
    }

    /**
     * Returns a new {@link InOrderTester} given the order in which
     * columns will be passed to it.
     */
    public InOrderTester inOrderTester(boolean reversed)
    {
        return new InOrderTester(reversed);
    }

    /**
     * Purge every tombstones that are older than {@code gcbefore}.
     *
     * @param gcBefore timestamp (in seconds) before which tombstones should
     * be purged
     */
    public void purge(int gcBefore)
    {
        topLevel = topLevel.localDeletionTime < gcBefore ? DeletionTime.LIVE : topLevel;

        if (ranges != null)
        {
            ranges.purge(gcBefore);
            if (ranges.isEmpty())
                ranges = null;
        }
    }

    public boolean hasIrrelevantData(int gcBefore)
    {
        if (topLevel.localDeletionTime < gcBefore)
            return true;

        return ranges != null && ranges.hasIrrelevantData(gcBefore);
    }

    public void add(DeletionTime newInfo)
    {
        if (topLevel.markedForDeleteAt < newInfo.markedForDeleteAt)
            topLevel = newInfo;
    }

    public void add(RangeTombstone tombstone, Comparator<ByteBuffer> comparator)
    {
        if (ranges == null)
            ranges = new RangeTombstoneList(comparator, 1);

        ranges.add(tombstone);
    }

    /**
     * Adds the provided deletion infos to the current ones.
     *
     * @return this object.
     */
    public DeletionInfo add(DeletionInfo newInfo)
    {
        add(newInfo.topLevel);

        if (ranges == null)
            ranges = newInfo.ranges == null ? null : newInfo.ranges.copy();
        else if (newInfo.ranges != null)
            ranges.addAll(newInfo.ranges);

        return this;
    }

    public long minTimestamp()
    {
        return ranges == null
             ? topLevel.markedForDeleteAt
             : Math.min(topLevel.markedForDeleteAt, ranges.minMarkedAt());
    }

    /**
     * The maximum timestamp mentioned by this DeletionInfo.
     */
    public long maxTimestamp()
    {
        return ranges == null
             ? topLevel.markedForDeleteAt
             : Math.max(topLevel.markedForDeleteAt, ranges.maxMarkedAt());
    }

    public DeletionTime getTopLevelDeletion()
    {
        return topLevel;
    }

    // Use sparingly, not the most efficient thing
    public Iterator<RangeTombstone> rangeIterator()
    {
        return ranges == null ? Iterators.<RangeTombstone>emptyIterator() : ranges.iterator();
    }

    public DeletionTime rangeCovering(ByteBuffer name)
    {
        return ranges == null ? null : ranges.search(name);
    }

    public int dataSize()
    {
        int size = TypeSizes.NATIVE.sizeof(topLevel.markedForDeleteAt);
        return size + (ranges == null ? 0 : ranges.dataSize());
    }

    public boolean hasRanges()
    {
        return ranges != null && !ranges.isEmpty();
    }

    @Override
    public String toString()
    {
        if (ranges == null || ranges.isEmpty())
            return String.format("{%s}", topLevel);
        else
            return String.format("{%s, ranges=%s}", topLevel, rangesAsString());
    }

    private String rangesAsString()
    {
        assert !ranges.isEmpty();
        StringBuilder sb = new StringBuilder();
        AbstractType at = (AbstractType)ranges.comparator();
        assert at != null;
        Iterator<RangeTombstone> iter = rangeIterator();
        while (iter.hasNext())
        {
            RangeTombstone i = iter.next();
            sb.append("[");
            sb.append(at.getString(i.min)).append("-");
            sb.append(at.getString(i.max)).append(", ");
            sb.append(i.data);
            sb.append("]");
        }
        return sb.toString();
    }

    // Updates all the timestamp of the deletion contained in this DeletionInfo to be {@code timestamp}.
    public void updateAllTimestamp(long timestamp)
    {
        if (topLevel.markedForDeleteAt != Long.MIN_VALUE)
            topLevel = new DeletionTime(timestamp, topLevel.localDeletionTime);

        if (ranges != null)
            ranges.updateAllTimestamp(timestamp);
    }

    @Override
    public boolean equals(Object o)
    {
        if(!(o instanceof DeletionInfo))
            return false;
        DeletionInfo that = (DeletionInfo)o;
        return topLevel.equals(that.topLevel) && Objects.equal(ranges, that.ranges);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(topLevel, ranges);
    }

    public static class Serializer implements IVersionedSerializer<DeletionInfo>
    {
        public void serialize(DeletionInfo info, DataOutput out, int version) throws IOException
        {
            DeletionTime.serializer.serialize(info.topLevel, out);
            RangeTombstoneList.serializer.serialize(info.ranges, out, version);
        }

        /*
         * Range tombstones internally depend on the column family serializer, but it is not serialized.
         * Thus deserialize(DataInput, int, Comparator<ByteBuffer>) should be used instead of this method.
         */
        public DeletionInfo deserialize(DataInput in, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        public DeletionInfo deserialize(DataInput in, int version, Comparator<ByteBuffer> comparator) throws IOException
        {
            DeletionTime topLevel = DeletionTime.serializer.deserialize(in);
            RangeTombstoneList ranges = RangeTombstoneList.serializer.deserialize(in, version, comparator);
            return new DeletionInfo(topLevel, ranges);
        }

        public long serializedSize(DeletionInfo info, TypeSizes typeSizes, int version)
        {
            long size = DeletionTime.serializer.serializedSize(info.topLevel, typeSizes);
            return size + RangeTombstoneList.serializer.serializedSize(info.ranges, typeSizes, version);
        }

        public long serializedSize(DeletionInfo info, int version)
        {
            return serializedSize(info, TypeSizes.NATIVE, version);
        }
    }

    /**
     * This object allow testing whether a given column (name/timestamp) is deleted
     * or not by this DeletionInfo, assuming that the column given to this
     * object are passed in forward or reversed comparator sorted order.
     *
     * This is more efficient that calling DeletionInfo.isDeleted() repeatedly
     * in that case.
     */
    public class InOrderTester
    {
        /*
         * Note that because because range tombstone are added to this DeletionInfo while we iterate,
         * ranges may be null initially and we need to wait the first range to create the tester (once
         * created the test will pick up new tombstones however). We do are guaranteed that a range tombstone
         * will be added *before* we test any column that it may delete so this is ok.
         */
        private RangeTombstoneList.InOrderTester tester;
        private final boolean reversed;

        private InOrderTester(boolean reversed)
        {
            this.reversed = reversed;
        }

        public boolean isDeleted(Column column)
        {
            return isDeleted(column.name(), column.timestamp());
        }

        public boolean isDeleted(ByteBuffer name, long timestamp)
        {
            if (timestamp <= topLevel.markedForDeleteAt)
                return true;

            /*
             * We don't optimize the reversed case for now because RangeTombstoneList
             * is always in forward sorted order.
             */
            if (reversed)
                 return DeletionInfo.this.isDeleted(name, timestamp);

            // Maybe create the tester if we hadn't yet and we now have some ranges (see above).
            if (tester == null && ranges != null)
                tester = ranges.inOrderTester();

            return tester != null && tester.isDeleted(name, timestamp);
        }
    }
}
