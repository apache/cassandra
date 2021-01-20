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

import java.util.Collections;
import java.util.Iterator;
import java.util.NavigableSet;

import com.google.common.collect.Iterators;

import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;

import static org.apache.cassandra.utils.btree.BTree.Dir.desc;

public abstract class AbstractBTreePartition implements Partition, Iterable<Row>
{
    protected final DecoratedKey partitionKey;

    protected abstract BTreePartitionData holder();
    protected abstract boolean canHaveShadowedData();

    protected AbstractBTreePartition(DecoratedKey partitionKey)
    {
        this.partitionKey = partitionKey;
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
        BTreePartitionData holder = holder();
        return holder.deletionInfo.isLive() && BTree.isEmpty(holder.tree) && holder.staticRow.isEmpty();
    }

    public boolean hasRows()
    {
        BTreePartitionData holder = holder();
        return !BTree.isEmpty(holder.tree);
    }

    public abstract TableMetadata metadata();

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return deletionInfo().getPartitionDeletion();
    }

    public RegularAndStaticColumns columns()
    {
        return holder().columns;
    }

    public EncodingStats stats()
    {
        return holder().stats;
    }

    public Row getRow(Clustering<?> clustering)
    {
        ColumnFilter columns = ColumnFilter.selection(columns());
        BTreePartitionData holder = holder();

        if (clustering == Clustering.STATIC_CLUSTERING)
        {
            // Note that for statics, this will never return null, this will return an empty row. However,
            // it's more consistent for this method to return null if we don't really have a static row.
            Row staticRow = staticRow(holder, columns, true);
            return staticRow.isEmpty() ? null : staticRow;
        }

        final Row row = (Row) BTree.find(holder.tree, metadata().comparator, clustering);
        DeletionTime activeDeletion = holder.deletionInfo.getPartitionDeletion();
        RangeTombstone rt = holder.deletionInfo.rangeCovering(clustering);

        if (rt != null && rt.deletionTime().supersedes(activeDeletion))
            activeDeletion = rt.deletionTime();


        if (row == null)
        {
            // this means our partition level deletion supersedes all other deletions and we don't have to keep the row deletions
            if (activeDeletion == holder.deletionInfo.getPartitionDeletion())
                return null;
            // no need to check activeDeletion.isLive here - if anything superseedes the partitionDeletion
            // it must be non-live
            return BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(activeDeletion));
        }
        return row.filter(columns, activeDeletion, true, metadata());
    }

    private Row staticRow(BTreePartitionData current, ColumnFilter columns, boolean setActiveDeletionToRow)
    {
        DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
        if (columns.fetchedColumns().statics.isEmpty() || (current.staticRow.isEmpty() && partitionDeletion.isLive()))
            return Rows.EMPTY_STATIC_ROW;

        Row row = current.staticRow.filter(columns, partitionDeletion, setActiveDeletionToRow, metadata());
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    @Override
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed)
    {
        Row staticRow = staticRow(holder(), selection, false);
        if (clusteringsInQueryOrder.isEmpty())
        {
            DeletionTime partitionDeletion = holder().deletionInfo.getPartitionDeletion();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }

        return new ClusteringsIterator(selection, clusteringsInQueryOrder, reversed, holder(), staticRow);
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.selection(columns()), Slices.ALL, false);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        return unfilteredIterator(holder(), selection, slices, reversed);
    }

    public UnfilteredRowIterator unfilteredIterator(BTreePartitionData current, ColumnFilter selection, Slices slices, boolean reversed)
    {
        Row staticRow = staticRow(current, selection, false);
        if (slices.size() == 0)
        {
            DeletionTime partitionDeletion = current.deletionInfo.getPartitionDeletion();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }

        return slices.size() == 1
               ? sliceIterator(selection, slices.get(0), reversed, current, staticRow)
               : new SlicesIterator(selection, slices, reversed, current, staticRow);
    }

    private UnfilteredRowIterator sliceIterator(ColumnFilter selection, Slice slice, boolean reversed, BTreePartitionData current, Row staticRow)
    {
        ClusteringBound<?> start = slice.start().isBottom() ? null : slice.start();
        ClusteringBound<?> end = slice.end().isTop() ? null : slice.end();
        Iterator<Row> rowIter = BTree.slice(current.tree, metadata().comparator, start, true, end, true, desc(reversed));
        Iterator<RangeTombstone> deleteIter = current.deletionInfo.rangeIterator(slice, reversed);
        return merge(rowIter, deleteIter, selection, reversed, current, staticRow);
    }

    private RowAndDeletionMergeIterator merge(Iterator<Row> rowIter, Iterator<RangeTombstone> deleteIter,
                                              ColumnFilter selection, boolean reversed, BTreePartitionData current, Row staticRow)
    {
        return new RowAndDeletionMergeIterator(metadata(), partitionKey(), current.deletionInfo.getPartitionDeletion(),
                                               selection, staticRow, reversed, current.stats,
                                               rowIter, deleteIter,
                                               canHaveShadowedData());
    }

    private abstract class AbstractIterator extends AbstractUnfilteredRowIterator
    {
        final BTreePartitionData current;
        final ColumnFilter selection;

        private AbstractIterator(BTreePartitionData current, Row staticRow, ColumnFilter selection, boolean isReversed)
        {
            super(AbstractBTreePartition.this.metadata(),
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

    private class SlicesIterator extends AbstractIterator
    {
        private final Slices slices;

        private int idx;
        private Iterator<Unfiltered> currentSlice;

        private SlicesIterator(ColumnFilter selection, Slices slices, boolean isReversed, BTreePartitionData current, Row staticRow)
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

    private class ClusteringsIterator extends AbstractIterator
    {
        private final Iterator<Clustering<?>> clusteringsInQueryOrder;
        private final SearchIterator<Clustering<?>, Row> rowSearcher;

        private Iterator<Unfiltered> currentIterator;

        private ClusteringsIterator(ColumnFilter selection,
                                    NavigableSet<Clustering<?>> clusteringsInQueryOrder,
                                    boolean isReversed,
                                    BTreePartitionData current,
                                    Row staticRow)
        {
            super(current, staticRow, selection, isReversed);

            this.clusteringsInQueryOrder = clusteringsInQueryOrder.iterator();
            this.rowSearcher = BTree.slice(current.tree, metadata().comparator, desc(isReversed));
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentIterator == null)
                {
                    if (!clusteringsInQueryOrder.hasNext())
                        return endOfData();

                    currentIterator = nextIterator(clusteringsInQueryOrder.next());
                }

                if (currentIterator != null && currentIterator.hasNext())
                    return currentIterator.next();

                currentIterator = null;
            }
        }

        private Iterator<Unfiltered> nextIterator(Clustering<?> next)
        {
            Row nextRow = rowSearcher.next(next);
            // rangeCovering() will return original RT covering clustering key, but we want to generate fake RT with
            // given clustering bound to be consistent with fake RT generated from sstable read.
            Iterator<RangeTombstone> deleteIter = current.deletionInfo.rangeIterator(Slice.make(next), isReverseOrder());

            if (nextRow == null && !deleteIter.hasNext())
                return null;

            Iterator<Row> rowIterator = nextRow == null ? Collections.emptyIterator() : Iterators.singletonIterator(nextRow);
            return merge(rowIterator, deleteIter, selection, isReverseOrder, current, staticRow);
        }
    }

    protected static BTreePartitionData build(UnfilteredRowIterator iterator, int initialRowCapacity)
    {
        return build(iterator, initialRowCapacity, true);
    }

    protected static BTreePartitionData build(UnfilteredRowIterator iterator, int initialRowCapacity, boolean ordered)
    {
        TableMetadata metadata = iterator.metadata();
        RegularAndStaticColumns columns = iterator.columns();
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

        return new BTreePartitionData(columns, builder.build(), deletionBuilder.build(), iterator.staticRow(), iterator.stats());
    }

    // Note that when building with a RowIterator, deletion will generally be LIVE, but we allow to pass it nonetheless because PartitionUpdate
    // passes a MutableDeletionInfo that it mutates later.
    protected static BTreePartitionData build(RowIterator rows, DeletionInfo deletion, boolean buildEncodingStats)
    {
        RegularAndStaticColumns columns = rows.columns();
        boolean reversed = rows.isReverseOrder();

        try (BTree.FastBuilder<Row> builder = BTree.fastBuilder())
        {
            while (rows.hasNext())
                builder.add(rows.next());


            Object[] tree = reversed ? builder.buildReverse()
                                     : builder.build();

            Row staticRow = rows.staticRow();
            EncodingStats stats = buildEncodingStats ? EncodingStats.Collector.collect(staticRow, BTree.iterator(tree), deletion)
                                                     : EncodingStats.NO_STATS;
            return new BTreePartitionData(columns, tree, deletion, staticRow, stats);
        }
    }

    @Override
    public String toString()
    {
        return toString(true);
    }

    public String toString(boolean includeFullDetails)
    {
        StringBuilder sb = new StringBuilder();
        if (includeFullDetails)
        {
            sb.append(String.format("[%s.%s] key=%s partition_deletion=%s columns=%s",
                                    metadata().keyspace,
                                    metadata().name,
                                    metadata().partitionKeyType.getString(partitionKey().getKey()),
                                    partitionLevelDeletion(),
                                    columns()));
        }
        else
        {
            sb.append("key=").append(metadata().partitionKeyType.getString(partitionKey().getKey()));
        }

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(staticRow().toString(metadata(), includeFullDetails));

        try (UnfilteredRowIterator iter = unfilteredIterator())
        {
            while (iter.hasNext())
                sb.append("\n    ").append(iter.next().toString(metadata(), includeFullDetails));
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof PartitionUpdate))
            return false;

        PartitionUpdate that = (PartitionUpdate) obj;
        BTreePartitionData a = this.holder(), b = that.holder();
        return partitionKey.equals(that.partitionKey)
               && metadata().id.equals(that.metadata().id)
               && a.deletionInfo.equals(b.deletionInfo)
               && a.staticRow.equals(b.staticRow)
               && Iterators.elementsEqual(iterator(), that.iterator());
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
