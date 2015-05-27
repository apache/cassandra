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
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Objects;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.dht.*;

/**
 * Groups key range and column filter for range queries.
 *
 * The main "trick" of this class is that the column filter can only
 * be obtained by providing the row key on which the column filter will
 * be applied (which we always know before actually querying the columns).
 *
 * This allows the paging DataRange to return a filter for most rows but a
 * potentially different ones for the starting and stopping key. Could
 * allow more fancy stuff in the future too, like column filters that
 * depend on the actual key value :)
 */
public class DataRange
{
    protected final AbstractBounds<RowPosition> keyRange;
    protected IDiskAtomFilter columnFilter;
    protected final boolean selectFullRow;

    public DataRange(AbstractBounds<RowPosition> range, IDiskAtomFilter columnFilter)
    {
        this.keyRange = range;
        this.columnFilter = columnFilter;
        this.selectFullRow = columnFilter instanceof SliceQueryFilter
                           ? isFullRowSlice((SliceQueryFilter)columnFilter)
                           : false;
    }

    public static boolean isFullRowSlice(SliceQueryFilter filter)
    {
        return filter.slices.length == 1
            && filter.start().isEmpty()
            && filter.finish().isEmpty()
            && filter.count == Integer.MAX_VALUE;
    }

    public static DataRange allData(IPartitioner partitioner)
    {
        return forKeyRange(new Range<Token>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
    }

    public static DataRange forKeyRange(Range<Token> keyRange)
    {
        return new DataRange(keyRange.toRowBounds(), new IdentityQueryFilter());
    }

    public AbstractBounds<RowPosition> keyRange()
    {
        return keyRange;
    }

    public RowPosition startKey()
    {
        return keyRange.left;
    }

    public RowPosition stopKey()
    {
        return keyRange.right;
    }

    /**
     * Returns true if tombstoned partitions should not be included in results or count towards the limit.
     * See CASSANDRA-8490 for more details on why this is needed (and done this way).
     * */
    public boolean ignoredTombstonedPartitions()
    {
        if (!(columnFilter instanceof SliceQueryFilter))
            return false;

        return ((SliceQueryFilter) columnFilter).compositesToGroup == SliceQueryFilter.IGNORE_TOMBSTONED_PARTITIONS;
    }

    // Whether the bounds of this DataRange actually wraps around.
    public boolean isWrapAround()
    {
        // On range can ever wrap
        return keyRange instanceof Range && ((Range)keyRange).isWrapAround();
    }

    public boolean contains(RowPosition pos)
    {
        return keyRange.contains(pos);
    }

    public int getLiveCount(ColumnFamily data, long now)
    {
        return columnFilter instanceof SliceQueryFilter
             ? ((SliceQueryFilter)columnFilter).lastCounted()
             : columnFilter.getLiveCount(data, now);
    }

    public boolean selectsFullRowFor(ByteBuffer rowKey)
    {
        return selectFullRow;
    }

    /**
     * Returns a column filter that should be used for a particular row key.  Note that in the case of paging,
     * slice starts and ends may change depending on the row key.
     */
    public IDiskAtomFilter columnFilter(ByteBuffer rowKey)
    {
        return columnFilter;
    }

    /**
     * Sets a new limit on the number of (grouped) cells to fetch. This is currently only used when the query limit applies
     * to CQL3 rows.
     */
    public void updateColumnsLimit(int count)
    {
        columnFilter.updateColumnsLimit(count);
    }

    public static class Paging extends DataRange
    {
        // The slice of columns that we want to fetch for each row, ignoring page start/end issues.
        private final SliceQueryFilter sliceFilter;

        private final CFMetaData cfm;

        private final Comparator<Composite> comparator;

        // used to restrict the start of the slice for the first partition in the range
        private final Composite firstPartitionColumnStart;

        // used to restrict the end of the slice for the last partition in the range
        private final Composite lastPartitionColumnFinish;

        // tracks the last key that we updated the filter for to avoid duplicating work
        private ByteBuffer lastKeyFilterWasUpdatedFor;

        private Paging(AbstractBounds<RowPosition> range, SliceQueryFilter filter, Composite firstPartitionColumnStart,
                       Composite lastPartitionColumnFinish, CFMetaData cfm, Comparator<Composite> comparator)
        {
            super(range, filter);

            // When using a paging range, we don't allow wrapped ranges, as it's unclear how to handle them properly.
            // This is ok for now since we only need this in range slice queries, and the range are "unwrapped" in that case.
            assert !(range instanceof Range) || !((Range)range).isWrapAround() || range.right.isMinimum() : range;

            this.sliceFilter = filter;
            this.cfm = cfm;
            this.comparator = comparator;
            this.firstPartitionColumnStart = firstPartitionColumnStart;
            this.lastPartitionColumnFinish = lastPartitionColumnFinish;
            this.lastKeyFilterWasUpdatedFor = null;
        }

        public Paging(AbstractBounds<RowPosition> range, SliceQueryFilter filter, Composite columnStart, Composite columnFinish, CFMetaData cfm)
        {
            this(range, filter, columnStart, columnFinish, cfm, filter.isReversed() ? cfm.comparator.reverseComparator() : cfm.comparator);
        }

        @Override
        public boolean selectsFullRowFor(ByteBuffer rowKey)
        {
            // If we initial filter is not the full filter, don't bother
            if (!selectFullRow)
                return false;

            if (!equals(startKey(), rowKey) && !equals(stopKey(), rowKey))
                return true;

            return isFullRowSlice((SliceQueryFilter)columnFilter(rowKey));
        }

        private boolean equals(RowPosition pos, ByteBuffer rowKey)
        {
            return pos instanceof DecoratedKey && ((DecoratedKey)pos).getKey().equals(rowKey);
        }

        @Override
        public IDiskAtomFilter columnFilter(ByteBuffer rowKey)
        {
            /*
             * We have that ugly hack that for slice queries, when we ask for
             * the live count, we reach into the query filter to get the last
             * counter number of columns to avoid recounting.
             * Maybe we should just remove that hack, but in the meantime, we
             * need to keep a reference the last returned filter.
             */
            if (equals(startKey(), rowKey) || equals(stopKey(), rowKey))
            {
                if (!rowKey.equals(lastKeyFilterWasUpdatedFor))
                {
                    this.lastKeyFilterWasUpdatedFor = rowKey;
                    columnFilter = sliceFilter.withUpdatedSlices(slicesForKey(rowKey));
                }
            }
            else
            {
                columnFilter = sliceFilter;
            }

            return columnFilter;
        }

        /** Returns true if the slice includes static columns, false otherwise. */
        private boolean sliceIncludesStatics(ColumnSlice slice, boolean reversed, CFMetaData cfm)
        {
            return cfm.hasStaticColumns() &&
                   slice.includes(reversed ? cfm.comparator.reverseComparator() : cfm.comparator, cfm.comparator.staticPrefix().end());
        }

        private ColumnSlice[] slicesForKey(ByteBuffer key)
        {
            // Also note that firstPartitionColumnStart and lastPartitionColumnFinish, when used, only "restrict" the filter slices,
            // it doesn't expand on them. As such, we can ignore the case where they are empty and we do
            // as it screw up with the logic below (see #6592)
            Composite newStart = equals(startKey(), key) && !firstPartitionColumnStart.isEmpty() ? firstPartitionColumnStart : null;
            Composite newFinish = equals(stopKey(), key) && !lastPartitionColumnFinish.isEmpty() ? lastPartitionColumnFinish : null;

            // in the common case, we'll have the same number of slices
            List<ColumnSlice> newSlices = new ArrayList<>(sliceFilter.slices.length);

            // Check our slices to see if any fall before the page start (in which case they can be removed) or
            // if they contain the page start (in which case they should start from the page start).  However, if the
            // slices would include static columns, we need to ensure they are also fetched, and so a separate
            // slice for the static columns may be required.
            // Note that if the query is reversed, we can't handle statics by simply adding a separate slice here, so
            // the reversed case is handled by SliceFromReadCommand instead. See CASSANDRA-8502 for more details.
            for (ColumnSlice slice : sliceFilter.slices)
            {
                if (newStart != null)
                {
                    if (slice.isBefore(comparator, newStart))
                    {
                        if (!sliceFilter.reversed && sliceIncludesStatics(slice, false, cfm))
                            newSlices.add(new ColumnSlice(Composites.EMPTY, cfm.comparator.staticPrefix().end()));

                        continue;
                    }

                    if (slice.includes(comparator, newStart))
                    {
                        if (!sliceFilter.reversed && sliceIncludesStatics(slice, false, cfm) && !newStart.equals(Composites.EMPTY))
                            newSlices.add(new ColumnSlice(Composites.EMPTY, cfm.comparator.staticPrefix().end()));

                        slice = new ColumnSlice(newStart, slice.finish);
                    }

                    // once we see a slice that either includes the page start or is after it, we can stop checking
                    // against the page start (because the slices are ordered)
                    newStart = null;
                }

                assert newStart == null;
                if (newFinish != null && !slice.isBefore(comparator, newFinish))
                {
                    if (slice.includes(comparator, newFinish))
                        newSlices.add(new ColumnSlice(slice.start, newFinish));
                    // In any case, we're done
                    break;
                }
                newSlices.add(slice);
            }

            return newSlices.toArray(new ColumnSlice[newSlices.size()]);
        }

        @Override
        public void updateColumnsLimit(int count)
        {
            columnFilter.updateColumnsLimit(count);
            sliceFilter.updateColumnsLimit(count);
        }

        @Override
        public String toString()
        {
            return Objects.toStringHelper(this)
                          .add("keyRange", keyRange)
                          .add("sliceFilter", sliceFilter)
                          .add("columnFilter", columnFilter)
                          .add("firstPartitionColumnStart", firstPartitionColumnStart == null ? "null" : cfm.comparator.getString(firstPartitionColumnStart))
                          .add("lastPartitionColumnFinish", lastPartitionColumnFinish == null ? "null" : cfm.comparator.getString(lastPartitionColumnFinish))
                          .toString();
        }
    }
}
