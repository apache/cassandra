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

import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
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
    private final AbstractBounds<RowPosition> keyRange;
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
            && filter.start().remaining() == 0
            && filter.finish().remaining() == 0
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

    public IDiskAtomFilter columnFilter(ByteBuffer rowKey)
    {
        return columnFilter;
    }

    public void updateColumnsLimit(int count)
    {
        columnFilter.updateColumnsLimit(count);
    }

    public static class Paging extends DataRange
    {
        private final SliceQueryFilter sliceFilter;
        private final Comparator<ByteBuffer> comparator;
        private final ByteBuffer columnStart;
        private final ByteBuffer columnFinish;

        private Paging(AbstractBounds<RowPosition> range, SliceQueryFilter filter, ByteBuffer columnStart, ByteBuffer columnFinish, Comparator<ByteBuffer> comparator)
        {
            super(range, filter);

            // When using a paging range, we don't allow wrapped ranges, as it's unclear how to handle them properly.
            // This is ok for now since we only need this in range slice queries, and the range are "unwrapped" in that case.
            assert !(range instanceof Range) || !((Range)range).isWrapAround() || range.right.isMinimum() : range;

            this.sliceFilter = filter;
            this.comparator = comparator;
            this.columnStart = columnStart;
            this.columnFinish = columnFinish;
        }

        public Paging(AbstractBounds<RowPosition> range, SliceQueryFilter filter, ByteBuffer columnStart, ByteBuffer columnFinish, AbstractType<?> comparator)
        {
            this(range, filter, columnStart, columnFinish, filter.isReversed() ? comparator.reverseComparator : comparator);
        }

        @Override
        public boolean selectsFullRowFor(ByteBuffer rowKey)
        {
            // If we initial filter is not the full filter, don't bother
            if (!selectFullRow)
                return false;

            if (!equals(startKey(), rowKey) && !equals(stopKey(), rowKey))
                return selectFullRow;

            return isFullRowSlice((SliceQueryFilter)columnFilter(rowKey));
        }

        private boolean equals(RowPosition pos, ByteBuffer rowKey)
        {
            return pos instanceof DecoratedKey && ((DecoratedKey)pos).key.equals(rowKey);
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
            columnFilter = equals(startKey(), rowKey) || equals(stopKey(), rowKey)
                         ? sliceFilter.withUpdatedSlices(slicesForKey(rowKey))
                         : sliceFilter;
            return columnFilter;
        }

        private ColumnSlice[] slicesForKey(ByteBuffer key)
        {
            // We don't call that until it's necessary, so assume we have to do some hard work
            ByteBuffer newStart = equals(startKey(), key) ? columnStart : null;
            ByteBuffer newFinish = equals(stopKey(), key) ? columnFinish : null;

            List<ColumnSlice> newSlices = new ArrayList<ColumnSlice>(sliceFilter.slices.length); // in the common case, we'll have the same number of slices

            for (ColumnSlice slice : sliceFilter.slices)
            {
                if (newStart != null)
                {
                    if (slice.isBefore(comparator, newStart))
                        continue; // we skip that slice

                    if (slice.includes(comparator, newStart))
                        slice = new ColumnSlice(newStart, slice.finish);

                    // Whether we've updated the slice or not, we don't have to bother about newStart anymore
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
    }
}
