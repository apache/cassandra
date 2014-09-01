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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.UnmodifiableIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.SearchIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract common class for all non-thread safe Partition implementations.
 */
public abstract class AbstractPartitionData implements Partition, Iterable<Row>
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPartitionData.class);

    protected final CFMetaData metadata;
    protected final DecoratedKey key;

    protected final DeletionInfo deletionInfo;
    protected final PartitionColumns columns;

    protected Row staticRow;

    protected int rows;

    // The values for the clustering columns of the rows contained in this partition object. If
    // clusteringSize is the size of the clustering comparator for this table, clusterings has size
    // clusteringSize * rows where rows is the number of rows stored, and row i has it's clustering
    // column values at indexes [clusteringSize * i, clusteringSize * (i + 1)).
    protected ByteBuffer[] clusterings;

    // The partition key column liveness infos for the rows of this partition (row i has its liveness info at index i).
    protected final LivenessInfoArray livenessInfos;
    // The row deletion for the rows of this partition (row i has its row deletion at index i).
    protected final DeletionTimeArray deletions;

    // The row data (cells data + complex deletions for complex columns) for the rows contained in this partition.
    protected final RowDataBlock data;

    // Stats over the rows stored in this partition.
    private final RowStats.Collector statsCollector = new RowStats.Collector();

    // The maximum timestamp for any data contained in this partition.
    protected long maxTimestamp = Long.MIN_VALUE;

    private AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionInfo deletionInfo,
                                    ByteBuffer[] clusterings,
                                    LivenessInfoArray livenessInfos,
                                    DeletionTimeArray deletions,
                                    PartitionColumns columns,
                                    RowDataBlock data)
    {
        this.metadata = metadata;
        this.key = key;
        this.deletionInfo = deletionInfo;
        this.clusterings = clusterings;
        this.livenessInfos = livenessInfos;
        this.deletions = deletions;
        this.columns = columns;
        this.data = data;

        collectStats(deletionInfo.getPartitionDeletion());
        Iterator<RangeTombstone> iter = deletionInfo.rangeIterator(false);
        while (iter.hasNext())
            collectStats(iter.next().deletionTime());
    }

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionInfo deletionInfo,
                                    PartitionColumns columns,
                                    RowDataBlock data,
                                    int initialRowCapacity)
    {
        this(metadata,
             key,
             deletionInfo,
             new ByteBuffer[initialRowCapacity * metadata.clusteringColumns().size()],
             new LivenessInfoArray(initialRowCapacity),
             new DeletionTimeArray(initialRowCapacity),
             columns,
             data);
    }

    protected AbstractPartitionData(CFMetaData metadata,
                                    DecoratedKey key,
                                    DeletionTime partitionDeletion,
                                    PartitionColumns columns,
                                    int initialRowCapacity,
                                    boolean sortable)
    {
        this(metadata,
             key,
             new DeletionInfo(partitionDeletion.takeAlias()),
             columns,
             new RowDataBlock(columns.regulars, initialRowCapacity, sortable, metadata.isCounter()),
             initialRowCapacity);
    }

    private void collectStats(DeletionTime dt)
    {
        statsCollector.updateDeletionTime(dt);
        maxTimestamp = Math.max(maxTimestamp, dt.markedForDeleteAt());
    }

    private void collectStats(LivenessInfo info)
    {
        statsCollector.updateTimestamp(info.timestamp());
        statsCollector.updateTTL(info.ttl());
        statsCollector.updateLocalDeletionTime(info.localDeletionTime());
        maxTimestamp = Math.max(maxTimestamp, info.timestamp());
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
        return deletionInfo.getPartitionDeletion();
    }

    public PartitionColumns columns()
    {
        return columns;
    }

    public Row staticRow()
    {
        return staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow;
    }

    public RowStats stats()
    {
        return statsCollector.get();
    }

    /**
     * The deletion info for the partition update.
     *
     * <b>warning:</b> the returned object should be used in a read-only fashion. In particular,
     * it should not be used to add new range tombstones to this deletion. For that,
     * {@link addRangeTombstone} should be used instead. The reason being that adding directly to
     * the returned object would bypass some stats collection that {@code addRangeTombstone} does.
     *
     * @return the deletion info for the partition update for use as read-only.
     */
    public DeletionInfo deletionInfo()
    {
        // TODO: it is a tad fragile that deletionInfo can be but shouldn't be modified. We
        // could add the option of providing a read-only view of a DeletionInfo instead.
        return deletionInfo;
    }

    public void addPartitionDeletion(DeletionTime deletionTime)
    {
        collectStats(deletionTime);
        deletionInfo.add(deletionTime);
    }

    public void addRangeTombstone(Slice deletedSlice, DeletionTime deletion)
    {
        addRangeTombstone(new RangeTombstone(deletedSlice, deletion.takeAlias()));
    }

    public void addRangeTombstone(RangeTombstone range)
    {
        collectStats(range.deletionTime());
        deletionInfo.add(range, metadata.comparator);
    }

    /**
     * Swap row i and j.
     *
     * This is only used when we need to reorder rows because those were not added in clustering order,
     * which happens in {@link PartitionUpdate#sort} and {@link ArrayBackedPartition#create}. This method
     * is public only because {@code PartitionUpdate} needs to implement {@link Sorting.Sortable}, but
     * it should really only be used by subclasses (and with care) in practice.
     */
    public void swap(int i, int j)
    {
        int cs = metadata.clusteringColumns().size();
        for (int k = 0; k < cs; k++)
        {
            ByteBuffer tmp = clusterings[j * cs + k];
            clusterings[j * cs + k] = clusterings[i * cs + k];
            clusterings[i * cs + k] = tmp;
        }

        livenessInfos.swap(i, j);
        deletions.swap(i, j);
        data.swap(i, j);
    }

    public int rowCount()
    {
        return rows;
    }

    public boolean isEmpty()
    {
        return deletionInfo.isLive() && rows == 0 && staticRow().isEmpty();
    }

    protected void clear()
    {
        rows = 0;
        Arrays.fill(clusterings, null);
        livenessInfos.clear();
        deletions.clear();
        data.clear();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        CFMetaData metadata = metadata();
        sb.append(String.format("Partition[%s.%s] key=%s columns=%s deletion=%s",
                    metadata.ksName,
                    metadata.cfName,
                    metadata.getKeyValidator().getString(partitionKey().getKey()),
                    columns(),
                    deletionInfo));

        if (staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(staticRow().toString(metadata, true));

        // We use createRowIterator() directly instead of iterator() because that avoids
        // sorting for PartitionUpdate (which inherit this method) and that is useful because
        //  1) it can help with debugging and 2) we can't write after sorting but we want to
        // be able to print an update while we build it (again for debugging)
        Iterator<Row> iterator = createRowIterator(null, false);
        while (iterator.hasNext())
            sb.append("\n    ").append(iterator.next().toString(metadata, true));

        return sb.toString();
    }

    protected void reverse()
    {
        for (int i = 0; i < rows / 2; i++)
            swap(i, rows - 1 - i);
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
     * @return an iterator over the rows of this update.
     */
    public Iterator<Row> iterator()
    {
        return createRowIterator(null, false);
    }

    public SearchIterator<Clustering, Row> searchIterator(final ColumnFilter columns, boolean reversed)
    {
        final RowIterator iter = createRowIterator(columns, reversed);
        return new SearchIterator<Clustering, Row>()
        {
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public Row next(Clustering key)
            {
                if (key == Clustering.STATIC_CLUSTERING)
                {
                    if (columns.fetchedColumns().statics.isEmpty() || staticRow().isEmpty())
                        return Rows.EMPTY_STATIC_ROW;

                    return FilteringRow.columnsFilteringRow(columns).setTo(staticRow());
                }

                return iter.seekTo(key) ? iter.next() : null;
            }
        };
    }

    public UnfilteredRowIterator unfilteredIterator()
    {
        return unfilteredIterator(ColumnFilter.selection(columns()), Slices.ALL, false);
    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed)
    {
        return slices.makeSliceIterator(sliceableUnfilteredIterator(columns, reversed));
    }

    protected SliceableUnfilteredRowIterator sliceableUnfilteredIterator()
    {
        return sliceableUnfilteredIterator(ColumnFilter.selection(columns()), false);
    }

    protected SliceableUnfilteredRowIterator sliceableUnfilteredIterator(final ColumnFilter selection, final boolean reversed)
    {
        return new AbstractSliceableIterator(this, selection.fetchedColumns(), reversed)
        {
            private final RowIterator rowIterator = createRowIterator(selection, reversed);
            private RowAndTombstoneMergeIterator mergeIterator = new RowAndTombstoneMergeIterator(metadata.comparator, reversed);

            protected Unfiltered computeNext()
            {
                if (!mergeIterator.isSet())
                    mergeIterator.setTo(rowIterator, deletionInfo.rangeIterator(reversed));

                return mergeIterator.hasNext() ? mergeIterator.next() : endOfData();
            }

            public Iterator<Unfiltered> slice(Slice slice)
            {
                return mergeIterator.setTo(rowIterator.slice(slice), deletionInfo.rangeIterator(slice, reversed));
            }
        };
    }

    private RowIterator createRowIterator(ColumnFilter columns, boolean reversed)
    {
        return reversed ? new ReverseRowIterator(columns) : new ForwardRowIterator(columns);
    }

    /**
     * An iterator over the rows of this partition that reuse the same row object.
     */
    private abstract class RowIterator extends UnmodifiableIterator<Row>
    {
        protected final InternalReusableClustering clustering = new InternalReusableClustering();
        protected final InternalReusableRow reusableRow;
        protected final FilteringRow filter;

        protected int next;

        protected RowIterator(final ColumnFilter columns)
        {
            this.reusableRow = new InternalReusableRow(clustering);
            this.filter = columns == null ? null : FilteringRow.columnsFilteringRow(columns);
        }

        /*
         * Move the iterator so that row {@code name} is returned next by {@code next} if that
         * row exists. Otherwise the first row sorting after {@code name} will be returned.
         * Returns whether {@code name} was found or not.
         */
        public abstract boolean seekTo(Clustering name);

        public abstract Iterator<Row> slice(Slice slice);

        protected Row setRowTo(int row)
        {
            reusableRow.setTo(row);
            return filter == null ? reusableRow : filter.setTo(reusableRow);
        }

        /**
         * Simple binary search.
         */
        protected int binarySearch(ClusteringPrefix name, int fromIndex, int toIndex)
        {
            int low = fromIndex;
            int mid = toIndex;
            int high = mid - 1;
            int result = -1;
            while (low <= high)
            {
                mid = (low + high) >> 1;
                if ((result = metadata.comparator.compare(name, clustering.setTo(mid))) > 0)
                    low = mid + 1;
                else if (result == 0)
                    return mid;
                else
                    high = mid - 1;
            }
            return -mid - (result < 0 ? 1 : 2);
        }
    }

    private class ForwardRowIterator extends RowIterator
    {
        private ForwardRowIterator(ColumnFilter columns)
        {
            super(columns);
            this.next = 0;
        }

        public boolean hasNext()
        {
            return next < rows;
        }

        public Row next()
        {
            return setRowTo(next++);
        }

        public boolean seekTo(Clustering name)
        {
            if (next >= rows)
                return false;

            int idx = binarySearch(name, next, rows);
            next = idx >= 0 ? idx : -idx - 1;
            return idx >= 0;
        }

        public Iterator<Row> slice(Slice slice)
        {
            int sidx = binarySearch(slice.start(), next, rows);
            final int start = sidx >= 0 ? sidx : -sidx - 1;
            if (start >= rows)
                return Collections.emptyIterator();

            int eidx = binarySearch(slice.end(), start, rows);
            // The insertion point is the first element greater than slice.end(), so we want the previous index
            final int end = eidx >= 0 ? eidx : -eidx - 2;

            // Remember the end to speed up potential further slice search
            next = end;

            if (start > end)
                return Collections.emptyIterator();

            return new AbstractIterator<Row>()
            {
                private int i = start;

                protected Row computeNext()
                {
                    if (i >= rows || i > end)
                        return endOfData();

                    return setRowTo(i++);
                }
            };
        }
    }

    private class ReverseRowIterator extends RowIterator
    {
        private ReverseRowIterator(ColumnFilter columns)
        {
            super(columns);
            this.next = rows - 1;
        }

        public boolean hasNext()
        {
            return next >= 0;
        }

        public Row next()
        {
            return setRowTo(next--);
        }

        public boolean seekTo(Clustering name)
        {
            // We only use that method with forward iterators.
            throw new UnsupportedOperationException();
        }

        public Iterator<Row> slice(Slice slice)
        {
            int sidx = binarySearch(slice.end(), 0, next + 1);
            // The insertion point is the first element greater than slice.end(), so we want the previous index
            final int start = sidx >= 0 ? sidx : -sidx - 2;
            if (start < 0)
                return Collections.emptyIterator();

            int eidx = binarySearch(slice.start(), 0, start + 1);
            final int end = eidx >= 0 ? eidx : -eidx - 1;

            // Remember the end to speed up potential further slice search
            next = end;

            if (start < end)
                return Collections.emptyIterator();

            return new AbstractIterator<Row>()
            {
                private int i = start;

                protected Row computeNext()
                {
                    if (i < 0 || i < end)
                        return endOfData();

                    return setRowTo(i--);
                }
            };
        }
    }

    /**
     * A reusable view over the clustering of this partition.
     */
    protected class InternalReusableClustering extends Clustering
    {
        final int size = metadata.clusteringColumns().size();
        private int base;

        public int size()
        {
            return size;
        }

        public Clustering setTo(int row)
        {
            base = row * size;
            return this;
        }

        public ByteBuffer get(int i)
        {
            return clusterings[base + i];
        }

        public ByteBuffer[] getRawValues()
        {
            ByteBuffer[] values = new ByteBuffer[size];
            for (int i = 0; i < size; i++)
                values[i] = get(i);
            return values;
        }
    };

    /**
     * A reusable view over the rows of this partition.
     */
    protected class InternalReusableRow extends AbstractReusableRow
    {
        private final LivenessInfoArray.Cursor liveness = new LivenessInfoArray.Cursor();
        private final DeletionTimeArray.Cursor deletion = new DeletionTimeArray.Cursor();
        private final InternalReusableClustering clustering;

        private int row;

        public InternalReusableRow()
        {
            this(new InternalReusableClustering());
        }

        public InternalReusableRow(InternalReusableClustering clustering)
        {
            this.clustering = clustering;
        }

        protected RowDataBlock data()
        {
            return data;
        }

        public Row setTo(int row)
        {
            this.clustering.setTo(row);
            this.liveness.setTo(livenessInfos, row);
            this.deletion.setTo(deletions, row);
            this.row = row;
            return this;
        }

        protected int row()
        {
            return row;
        }

        public Clustering clustering()
        {
            return clustering;
        }

        public LivenessInfo primaryKeyLivenessInfo()
        {
            return liveness;
        }

        public DeletionTime deletion()
        {
            return deletion;
        }
    };

    private static abstract class AbstractSliceableIterator extends AbstractUnfilteredRowIterator implements SliceableUnfilteredRowIterator
    {
        private AbstractSliceableIterator(AbstractPartitionData data, PartitionColumns columns, boolean isReverseOrder)
        {
            super(data.metadata, data.key, data.partitionLevelDeletion(), columns, data.staticRow(), isReverseOrder, data.stats());
        }
    }

    /**
     * A row writer to add rows to this partition.
     */
    protected class Writer extends RowDataBlock.Writer
    {
        private int clusteringBase;

        private int simpleColumnsSetInRow;
        private final Set<ColumnDefinition> complexColumnsSetInRow = new HashSet<>();

        public Writer(boolean inOrderCells)
        {
            super(data, inOrderCells);
        }

        public void writeClusteringValue(ByteBuffer value)
        {
            ensureCapacity(row);
            clusterings[clusteringBase++] = value;
        }

        public void writePartitionKeyLivenessInfo(LivenessInfo info)
        {
            ensureCapacity(row);
            livenessInfos.set(row, info);
            collectStats(info);
        }

        public void writeRowDeletion(DeletionTime deletion)
        {
            ensureCapacity(row);
            if (!deletion.isLive())
                deletions.set(row, deletion);

            collectStats(deletion);
        }

        @Override
        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
        {
            ensureCapacity(row);
            collectStats(info);

            if (column.isComplex())
                complexColumnsSetInRow.add(column);
            else
                ++simpleColumnsSetInRow;

            super.writeCell(column, isCounter, value, info, path);
        }

        @Override
        public void writeComplexDeletion(ColumnDefinition c, DeletionTime complexDeletion)
        {
            ensureCapacity(row);
            collectStats(complexDeletion);

            super.writeComplexDeletion(c, complexDeletion);
        }

        @Override
        public void endOfRow()
        {
            super.endOfRow();
            ++rows;

            statsCollector.updateColumnSetPerRow(simpleColumnsSetInRow + complexColumnsSetInRow.size());

            simpleColumnsSetInRow = 0;
            complexColumnsSetInRow.clear();
        }

        public int currentRow()
        {
            return row;
        }

        private void ensureCapacity(int rowToSet)
        {
            int originalCapacity = livenessInfos.size();
            if (rowToSet < originalCapacity)
                return;

            int newCapacity = RowDataBlock.computeNewCapacity(originalCapacity, rowToSet);

            int clusteringSize = metadata.clusteringColumns().size();

            clusterings = Arrays.copyOf(clusterings, newCapacity * clusteringSize);

            livenessInfos.resize(newCapacity);
            deletions.resize(newCapacity);
        }

        @Override
        public Writer reset()
        {
            super.reset();
            clusteringBase = 0;
            simpleColumnsSetInRow = 0;
            complexColumnsSetInRow.clear();
            return this;
        }
    }

    /**
     * A range tombstone marker writer to add range tombstone markers to this partition.
     */
    protected class RangeTombstoneCollector implements RangeTombstoneMarker.Writer
    {
        private final boolean reversed;

        private final ByteBuffer[] nextValues = new ByteBuffer[metadata().comparator.size()];
        private int size;
        private RangeTombstone.Bound.Kind nextKind;

        private Slice.Bound openBound;
        private DeletionTime openDeletion;

        public RangeTombstoneCollector(boolean reversed)
        {
            this.reversed = reversed;
        }

        public void writeClusteringValue(ByteBuffer value)
        {
            nextValues[size++] = value;
        }

        public void writeBoundKind(RangeTombstone.Bound.Kind kind)
        {
            nextKind = kind;
        }

        private ByteBuffer[] getValues()
        {
            return Arrays.copyOfRange(nextValues, 0, size);
        }

        private void open(RangeTombstone.Bound.Kind kind, DeletionTime deletion)
        {
            openBound = Slice.Bound.create(kind, getValues());
            openDeletion = deletion.takeAlias();
        }

        private void close(RangeTombstone.Bound.Kind kind, DeletionTime deletion)
        {
            assert deletion.equals(openDeletion) : "Expected " + openDeletion + " but was "  + deletion;
            Slice.Bound closeBound = Slice.Bound.create(kind, getValues());
            Slice slice = reversed
                        ? Slice.make(closeBound, openBound)
                        : Slice.make(openBound, closeBound);
            addRangeTombstone(slice, openDeletion);
        }

        public void writeBoundDeletion(DeletionTime deletion)
        {
            assert !nextKind.isBoundary();
            if (nextKind.isOpen(reversed))
                open(nextKind, deletion);
            else
                close(nextKind, deletion);
        }

        public void writeBoundaryDeletion(DeletionTime endDeletion, DeletionTime startDeletion)
        {
            assert nextKind.isBoundary();
            DeletionTime closeTime = reversed ? startDeletion : endDeletion;
            DeletionTime openTime = reversed ? endDeletion : startDeletion;

            close(nextKind.closeBoundOfBoundary(reversed), closeTime);
            open(nextKind.openBoundOfBoundary(reversed), openTime);
        }

        public void endOfMarker()
        {
            clear();
        }

        private void addRangeTombstone(Slice deletionSlice, DeletionTime dt)
        {
            AbstractPartitionData.this.addRangeTombstone(deletionSlice, dt);
        }

        private void clear()
        {
            size = 0;
            Arrays.fill(nextValues, null);
            nextKind = null;
        }

        public void reset()
        {
            openBound = null;
            openDeletion = null;
            clear();
        }
    }
}
