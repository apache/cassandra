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
package org.apache.cassandra.db.partitions;

import java.util.*;

import com.google.common.collect.Lists;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.SearchIterator;

/**
 * Abstract common class for all non-thread safe Partition implementations.
 */
public abstract class AbstractThreadUnsafePartition implements Partition, Iterable<Row>
{
    protected final CFMetaData metadata;
    protected final DecoratedKey key;

    protected final PartitionColumns columns;

    protected final List<Row> rows;

    protected AbstractThreadUnsafePartition(CFMetaData metadata,
                                            DecoratedKey key,
                                            PartitionColumns columns,
                                            List<Row> rows)
    {
        this.metadata = metadata;
        this.key = key;
        this.columns = columns;
        this.rows = rows;
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return deletionInfo().getPartitionDeletion();
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public abstract Row staticRow();

    protected abstract boolean canHaveShadowedData();

    /**
     * The deletion info for the partition update.
     *
     * Note: do not cast the result to a {@code MutableDeletionInfo} to modify it!
     *
     * @return the deletion info for the partition update for use as read-only.
     */
    public abstract DeletionInfo deletionInfo();

    public int rowCount()
    {
        return rows.size();
    }

    public boolean isEmpty()
    {
        return deletionInfo().isLive() && rows.isEmpty() && staticRow().isEmpty();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        CFMetaData metadata = metadata();
        sb.append(String.format("Partition[%s.%s] key=%s columns=%s%s",
                                 metadata().ksName,
                                 metadata().cfName,
                                 metadata().getKeyValidator().getString(partitionKey().getKey()),
                                 columns(),
                                 deletionInfo().isLive() ? "" : " " + deletionInfo()));

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(staticRow().toString(metadata, true));

        // We use createRowIterator() directly instead of iterator() because that avoids
        // sorting for PartitionUpdate (which inherit this method) and that is useful because
        //  1) it can help with debugging and 2) we can't write after sorting but we want to
        // be able to print an update while we build it (again for debugging)
        for (Row row : this)
            sb.append("\n    ").append(row.toString(metadata, true));

        return sb.toString();
    }

    public Row getRow(Clustering clustering)
    {
        Row row = searchIterator(ColumnFilter.selection(columns()), false).next(clustering);
        // Note that for statics, this will never return null, this will return an empty row. However,
        // it's more consistent for this method to return null if we don't really have a static row.
        return row == null || (clustering == Clustering.STATIC_CLUSTERING && row.isEmpty()) ? null : row;
    }

    /**
     * Returns an iterator that iterators over the rows of this update in clustering order.
     *
     * @return an iterator over the rows of this partition.
     */
    public Iterator<Row> iterator()
    {
        return rows.iterator();
    }

    public SearchIterator<Clustering, Row> searchIterator(final ColumnFilter columns, boolean reversed)
    {
        final RowSearcher searcher = reversed ? new ReverseRowSearcher() : new ForwardRowSearcher();
        return new SearchIterator<Clustering, Row>()
        {
            public boolean hasNext()
            {
                return !searcher.isDone();
            }

            public Row next(Clustering clustering)
            {
                if (clustering == Clustering.STATIC_CLUSTERING)
                {
                    Row staticRow = staticRow();
                    return staticRow.isEmpty() || columns.fetchedColumns().statics.isEmpty()
                         ? Rows.EMPTY_STATIC_ROW
                         : staticRow.filter(columns, partitionLevelDeletion(), true, metadata);
                }

                Row row = searcher.search(clustering);
                RangeTombstone rt = deletionInfo().rangeCovering(clustering);

                // A search iterator only return a row, so it doesn't allow to directly account for deletion that should apply to to row
                // (the partition deletion or the deletion of a range tombstone that covers it). So if needs be, reuse the row deletion
                // to carry the proper deletion on the row.
                DeletionTime activeDeletion = partitionLevelDeletion();
                if (rt != null && rt.deletionTime().supersedes(activeDeletion))
                    activeDeletion = rt.deletionTime();

                if (row == null)
                    return activeDeletion.isLive() ? null : BTreeBackedRow.emptyDeletedRow(clustering, activeDeletion);

                return row.filter(columns, activeDeletion, true, metadata);
            }
        };
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.all(metadata()), Slices.ALL, false);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
    {
        return slices.makeSliceIterator(sliceableUnfilteredIterator(columns, reversed));
    }

    protected SliceableUnfilteredRowIterator sliceableUnfilteredIterator()
    {
        return sliceableUnfilteredIterator(ColumnFilter.all(metadata()), false);
    }

    protected SliceableUnfilteredRowIterator sliceableUnfilteredIterator(ColumnFilter selection, boolean reversed)
    {
        return new SliceableIterator(this, selection, reversed);
    }

    /**
     * Simple binary search for a given row (in the rows list).
     *
     * The return value has the exact same meaning that the one of Collections.binarySearch() but
     * we don't use the later because we're searching for a 'Clustering' in an array of 'Row' (and while
     * both are Clusterable, it's slightly faster to use the 'Clustering' comparison (see comment on
     * ClusteringComparator.rowComparator())).
     */
    private int binarySearch(Clustering clustering, int fromIndex, int toIndex)
    {
        ClusteringComparator comparator = metadata().comparator;
        int low = fromIndex;
        int mid = toIndex;
        int high = mid - 1;
        int result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            if ((result = comparator.compare(clustering, rows.get(mid).clustering())) > 0)
                low = mid + 1;
            else if (result == 0)
                return mid;
            else
                high = mid - 1;
        }
        return -mid - (result < 0 ? 1 : 2);
    }

    private class SliceableIterator extends AbstractUnfilteredRowIterator implements SliceableUnfilteredRowIterator
    {
        private final ColumnFilter columns;
        private RowSearcher searcher;

        private Iterator<Unfiltered> iterator;

        private SliceableIterator(AbstractThreadUnsafePartition partition, ColumnFilter columns, boolean isReverseOrder)
        {
            super(partition.metadata(),
                  partition.partitionKey(),
                  partition.partitionLevelDeletion(),
                  columns.fetchedColumns(),
                  partition.staticRow().isEmpty() ? Rows.EMPTY_STATIC_ROW : partition.staticRow().filter(columns, partition.partitionLevelDeletion(), false, partition.metadata()),
                  isReverseOrder,
                  partition.stats());
            this.columns = columns;
        }

        protected Unfiltered computeNext()
        {
            if (iterator == null)
                iterator = merge(isReverseOrder ? Lists.reverse(rows).iterator(): iterator(), deletionInfo().rangeIterator(isReverseOrder()));

            return iterator.hasNext() ? iterator.next() : endOfData();
        }

        public Iterator<Unfiltered> slice(Slice slice)
        {
            if (searcher == null)
                searcher = isReverseOrder() ? new ReverseRowSearcher() : new ForwardRowSearcher();
            return merge(searcher.slice(slice), deletionInfo().rangeIterator(slice, isReverseOrder()));
        }

        private Iterator<Unfiltered> merge(Iterator<Row> rows, Iterator<RangeTombstone> ranges)
        {
            return new RowAndDeletionMergeIterator(metadata,
                                                   partitionKey,
                                                   partitionLevelDeletion,
                                                   columns,
                                                   staticRow(),
                                                   isReverseOrder(),
                                                   stats(),
                                                   rows,
                                                   ranges,
                                                   canHaveShadowedData());
        }
    }

    /**
     * Utility class to search for rows or slice of rows in order.
     */
    private abstract class RowSearcher
    {
        public abstract boolean isDone();

        public abstract Row search(Clustering name);

        public abstract Iterator<Row> slice(Slice slice);

        protected int search(Clustering clustering, int from, int to)
        {
            return binarySearch(clustering, from, to);
        }

        protected int search(Slice.Bound bound, int from, int to)
        {
            return Collections.binarySearch(rows.subList(from, to), bound, metadata.comparator);
        }
    }

    private class ForwardRowSearcher extends RowSearcher
    {
        private int nextIdx = 0;

        public boolean isDone()
        {
            return nextIdx >= rows.size();
        }

        public Row search(Clustering name)
        {
            if (isDone())
                return null;

            int idx = search(name, nextIdx, rows.size());
            if (idx < 0)
            {
                nextIdx = -idx - 1;
                return null;
            }
            else
            {
                nextIdx = idx + 1;
                return rows.get(idx);
            }
        }

        public Iterator<Row> slice(Slice slice)
        {
            // Note that because a Slice.Bound can never sort equally to a Clustering, we know none of the search will
            // be a match, so we save from testing for it.

            final int start = -search(slice.start(), nextIdx, rows.size()) - 1; // First index to include
            if (start >= rows.size())
                return Collections.emptyIterator();

            final int end = -search(slice.end(), start, rows.size()) - 1; // First index to exclude

            // Remember the end to speed up potential further slice search
            nextIdx = end;

            if (start >= end)
                return Collections.emptyIterator();

            return rows.subList(start, end).iterator();
        }
    }

    private class ReverseRowSearcher extends RowSearcher
    {
        private int nextIdx = rows.size() - 1;

        public boolean isDone()
        {
            return nextIdx < 0;
        }

        public Row search(Clustering name)
        {
            if (isDone())
                return null;

            int idx = search(name, 0, nextIdx);
            if (idx < 0)
            {
                // The insertion point is the first element greater than name, so we want start from the previous one next time
                nextIdx = -idx - 2;
                return null;
            }
            else
            {
                nextIdx = idx - 1;
                return rows.get(idx);
            }
        }

        public Iterator<Row> slice(Slice slice)
        {
            // Note that because a Slice.Bound can never sort equally to a Clustering, we know none of the search will
            // be a match, so we save from testing for it.

            // The insertion point is the first element greater than slice.end(), so we want the previous index
            final int start = -search(slice.end(), 0, nextIdx + 1) - 2;  // First index to include
            if (start < 0)
                return Collections.emptyIterator();

            final int end = -search(slice.start(), 0, start + 1) - 2; // First index to exclude

            // Remember the end to speed up potential further slice search
            nextIdx = end;

            if (start < end)
                return Collections.emptyIterator();

            return Lists.reverse(rows.subList(end+1, start+1)).iterator();
        }
    }
}
