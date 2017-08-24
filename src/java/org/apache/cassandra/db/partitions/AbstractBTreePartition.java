/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.partitions;

import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;

import static org.apache.cassandra.utils.btree.BTree.Dir.desc;

public abstract class AbstractBTreePartition implements Partition, Iterable<Row>
{
    protected static final Holder EMPTY = new Holder(PartitionColumns.NONE, BTree.empty(), DeletionInfo.LIVE, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);

    protected final CFMetaData metadata;
    protected final DecoratedKey partitionKey;

    protected abstract Holder holder();
    protected abstract boolean canHaveShadowedData();

    protected AbstractBTreePartition(CFMetaData metadata, DecoratedKey partitionKey)
    {
        this.metadata = metadata;
        this.partitionKey = partitionKey;
    }

    protected static final class Holder
    {
        final PartitionColumns columns;
        final DeletionInfo deletionInfo;
        // the btree of rows
        final Object[] tree;
        final Row staticRow;
        final EncodingStats stats;

        Holder(PartitionColumns columns, Object[] tree, DeletionInfo deletionInfo, Row staticRow, EncodingStats stats)
        {
            this.columns = columns;
            this.tree = tree;
            this.deletionInfo = deletionInfo;
            this.staticRow = staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
            this.stats = stats;
        }
    }

    public DeletionInfo deletionInfo()
    {
        return holder().deletionInfo;
    }

    public Row staticRow()
    {
        return holder().staticRow;
    }

    public boolean isEmpty()
    {
        Holder holder = holder();
        return holder.deletionInfo.isLive() && BTree.isEmpty(holder.tree) && holder.staticRow.isEmpty();
    }

    public boolean hasRows()
    {
        Holder holder = holder();
        return !BTree.isEmpty(holder.tree);
    }

    public CFMetaData metadata()
    {
        return metadata;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return deletionInfo().getPartitionDeletion();
    }

    public PartitionColumns columns()
    {
        return holder().columns;
    }

    public EncodingStats stats()
    {
        return holder().stats;
    }

    public Row getRow(Clustering clustering)
    {
        Row row = searchIterator(ColumnFilter.selection(columns()), false).next(clustering);
        // Note that for statics, this will never return null, this will return an empty row. However,
        // it's more consistent for this method to return null if we don't really have a static row.
        return row == null || (clustering == Clustering.STATIC_CLUSTERING && row.isEmpty()) ? null : row;
    }

    private Row staticRow(Holder current, ColumnFilter columns, boolean setActiveDeletionToRow)
    {
        DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
        if (columns.fetchedColumns().statics.isEmpty() || (current.staticRow.isEmpty() && partitionDeletion.isLive()))
            return Rows.EMPTY_STATIC_ROW;

        Row row = current.staticRow.filter(columns, partitionDeletion, setActiveDeletionToRow, metadata);
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    public SearchIterator<Clustering, Row> searchIterator(final ColumnFilter columns, final boolean reversed)
    {
        // TODO: we could optimize comparison for "NativeRow" à la #6755
        final Holder current = holder();
        return new SearchIterator<Clustering, Row>()
        {
            private final SearchIterator<Clustering, Row> rawIter = new BTreeSearchIterator<>(current.tree, metadata.comparator, desc(reversed));
            private final DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();

            public Row next(Clustering clustering)
            {
                if (clustering == Clustering.STATIC_CLUSTERING)
                    return staticRow(current, columns, true);

                Row row = rawIter.next(clustering);
                RangeTombstone rt = current.deletionInfo.rangeCovering(clustering);

                // A search iterator only return a row, so it doesn't allow to directly account for deletion that should apply to to row
                // (the partition deletion or the deletion of a range tombstone that covers it). So if needs be, reuse the row deletion
                // to carry the proper deletion on the row.
                DeletionTime activeDeletion = partitionDeletion;
                if (rt != null && rt.deletionTime().supersedes(activeDeletion))
                    activeDeletion = rt.deletionTime();

                if (row == null)
                    return activeDeletion.isLive() ? null : BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(activeDeletion));

                return row.filter(columns, activeDeletion, true, metadata);
            }
        };
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.selection(columns()), Slices.ALL, false);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return unfilteredIterator(holder(), selection, slices, reversed);
    }

    public UnfilteredRowIterator unfilteredIterator(Holder current, ColumnFilter selection, Slices slices, boolean reversed)
    {
        Row staticRow = staticRow(current, selection, false);
        if (slices.size() == 0)
        {
            DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
            return UnfilteredRowIterators.noRowsIterator(metadata, partitionKey(), staticRow, partitionDeletion, reversed);
        }

        return slices.size() == 1
               ? sliceIterator(selection, slices.get(0), reversed, current, staticRow)
               : new SlicesIterator(selection, slices, reversed, current, staticRow);
    }

    private UnfilteredRowIterator sliceIterator(ColumnFilter selection, Slice slice, boolean reversed, Holder current, Row staticRow)
    {
        ClusteringBound start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
        ClusteringBound end = slice.end() == ClusteringBound.TOP ? null : slice.end();
        Iterator<Row> rowIter = BTree.slice(current.tree, metadata.comparator, start, true, end, true, desc(reversed));
        Iterator<RangeTombstone> deleteIter = current.deletionInfo.rangeIterator(slice, reversed);
        return merge(rowIter, deleteIter, selection, reversed, current, staticRow);
    }

    private RowAndDeletionMergeIterator merge(Iterator<Row> rowIter, Iterator<RangeTombstone> deleteIter,
                                              ColumnFilter selection, boolean reversed, Holder current, Row staticRow)
    {
        return new RowAndDeletionMergeIterator(metadata, partitionKey(), current.deletionInfo.getPartitionDeletion(),
                                               selection, staticRow, reversed, current.stats,
                                               rowIter, deleteIter,
                                               canHaveShadowedData());
    }

    private abstract class AbstractIterator extends AbstractUnfilteredRowIterator
    {
        final Holder current;
        final ColumnFilter selection;

        private AbstractIterator(Holder current, Row staticRow, ColumnFilter selection, boolean isReversed)
        {
            super(AbstractBTreePartition.this.metadata,
                  AbstractBTreePartition.this.partitionKey(),
                  current.deletionInfo.getPartitionDeletion(),
                  selection.fetchedColumns(), // non-selected columns will be filtered in subclasses by RowAndDeletionMergeIterator
                                              // it would also be more precise to return the intersection of the selection and current.columns,
                                              // but its probably not worth spending time on computing that.
                  staticRow,
                  isReversed,
                  current.stats);
            this.current = current;
            this.selection = selection;
        }
    }

    public class SlicesIterator extends AbstractIterator
    {
        private final Slices slices;

        private int idx;
        private Iterator<Unfiltered> currentSlice;

        private SlicesIterator(ColumnFilter selection,
                               Slices slices,
                               boolean isReversed,
                               Holder current,
                               Row staticRow)
        {
            super(current, staticRow, selection, isReversed);
            this.slices = slices;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentSlice == null)
                {
                    if (idx >= slices.size())
                        return endOfData();

                    int sliceIdx = isReverseOrder ? slices.size() - idx - 1 : idx;
                    currentSlice = sliceIterator(selection, slices.get(sliceIdx), isReverseOrder, current, Rows.EMPTY_STATIC_ROW);
                    idx++;
                }

                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }
    }

    protected static Holder build(UnfilteredRowIterator iterator, int initialRowCapacity)
    {
        return build(iterator, initialRowCapacity, true);
    }

    protected static Holder build(UnfilteredRowIterator iterator, int initialRowCapacity, boolean ordered)
    {
        CFMetaData metadata = iterator.metadata();
        PartitionColumns columns = iterator.columns();
        boolean reversed = iterator.isReverseOrder();

        BTree.Builder<Row> builder = BTree.builder(metadata.comparator, initialRowCapacity);
        builder.auto(!ordered);
        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(iterator.partitionLevelDeletion(), metadata.comparator, reversed);

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                builder.add((Row)unfiltered);
            else
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
        }

        if (reversed)
            builder.reverse();

        return new Holder(columns, builder.build(), deletionBuilder.build(), iterator.staticRow(), iterator.stats());
    }

    // Note that when building with a RowIterator, deletion will generally be LIVE, but we allow to pass it nonetheless because PartitionUpdate
    // passes a MutableDeletionInfo that it mutates later.
    protected static Holder build(RowIterator rows, DeletionInfo deletion, boolean buildEncodingStats, int initialRowCapacity)
    {
        CFMetaData metadata = rows.metadata();
        PartitionColumns columns = rows.columns();
        boolean reversed = rows.isReverseOrder();

        BTree.Builder<Row> builder = BTree.builder(metadata.comparator, initialRowCapacity);
        builder.auto(false);
        while (rows.hasNext())
            builder.add(rows.next());

        if (reversed)
            builder.reverse();

        Row staticRow = rows.staticRow();
        Object[] tree = builder.build();
        EncodingStats stats = buildEncodingStats ? EncodingStats.Collector.collect(staticRow, BTree.iterator(tree), deletion)
                                                 : EncodingStats.NO_STATS;
        return new Holder(columns, tree, deletion, staticRow, stats);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("[%s.%s] key=%s partition_deletion=%s columns=%s",
                                metadata.ksName,
                                metadata.cfName,
                                metadata.getKeyValidator().getString(partitionKey().getKey()),
                                partitionLevelDeletion(),
                                columns()));

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(staticRow().toString(metadata, true));

        try (UnfilteredRowIterator iter = unfilteredIterator())
        {
            while (iter.hasNext())
                sb.append("\n    ").append(iter.next().toString(metadata, true));
        }

        return sb.toString();
    }

    public int rowCount()
    {
        return BTree.size(holder().tree);
    }

    public Iterator<Row> iterator()
    {
        return BTree.<Row>iterator(holder().tree);
    }

    public Row lastRow()
    {
        Object[] tree = holder().tree;
        if (BTree.isEmpty(tree))
            return null;

        return BTree.findByIndex(tree, BTree.size(tree) - 1);
    }
}
