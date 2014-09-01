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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Sorting;

/**
 * Stores updates made on a partition.
 * <p>
 * A PartitionUpdate object requires that all writes are performed before we
 * try to read the updates (attempts to write to the PartitionUpdate after a
 * read method has been called will result in an exception being thrown).
 * In other words, a Partition is mutable while we do a write and become
 * immutable as soon as it is read.
 * <p>
 * Row updates are added to this update through the {@link #writer} method which
 * returns a {@link Row.Writer}. Multiple rows can be added to this writer as required and
 * those row do not have to added in (clustering) order, and the same row can be added
 * multiple times. Further, for a given row, the writer actually supports intermingling
 * the writing of cells for different complex cells (note that this is usually not supported
 * by {@code Row.Writer} implementations, but is supported here because
 * {@code ModificationStatement} requires that (because we could have multiple {@link Operation}
 * on the same column in a given statement)).
 */
public class PartitionUpdate extends AbstractPartitionData implements Sorting.Sortable
{
    protected static final Logger logger = LoggerFactory.getLogger(PartitionUpdate.class);

    // Records whether the partition update has been sorted (it is the rows contained in the partition
    // that are sorted since we don't require rows to be added in order). Sorting happens when the
    // update is read, and writting is rejected as soon as the update is sorted (it's actually possible
    // to manually allow new update by using allowNewUpdates(), and we could make that more implicit, but
    // as only triggers really requires it, we keep it simple for now).
    private boolean isSorted;

    public static final PartitionUpdateSerializer serializer = new PartitionUpdateSerializer();

    private final Writer writer;

    // Used by compare for the sake of implementing the Sorting.Sortable interface (which is in turn used
    // to sort the rows of this update).
    private final InternalReusableClustering p1 = new InternalReusableClustering();
    private final InternalReusableClustering p2 = new InternalReusableClustering();

    private PartitionUpdate(CFMetaData metadata,
                            DecoratedKey key,
                            DeletionInfo delInfo,
                            RowDataBlock data,
                            PartitionColumns columns,
                            int initialRowCapacity)
    {
        super(metadata, key, delInfo, columns, data, initialRowCapacity);
        this.writer = createWriter();
    }

    public PartitionUpdate(CFMetaData metadata,
                           DecoratedKey key,
                           DeletionInfo delInfo,
                           PartitionColumns columns,
                           int initialRowCapacity)
    {
        this(metadata,
             key,
             delInfo,
             new RowDataBlock(columns.regulars, initialRowCapacity, true, metadata.isCounter()),
             columns,
             initialRowCapacity);
    }

    public PartitionUpdate(CFMetaData metadata,
                           DecoratedKey key,
                           PartitionColumns columns,
                           int initialRowCapacity)
    {
        this(metadata,
             key,
             DeletionInfo.live(),
             columns,
             initialRowCapacity);
    }

    protected Writer createWriter()
    {
        return new RegularWriter();
    }

    protected StaticWriter createStaticWriter()
    {
        return new StaticWriter();
    }

    /**
     * Deserialize a partition update from a provided byte buffer.
     *
     * @param bytes the byte buffer that contains the serialized update.
     * @param version the version with which the update is serialized.
     * @param key the partition key for the update. This is only used if {@code version &lt 3.0}
     * and can be {@code null} otherwise.
     *
     * @return the deserialized update or {@code null} if {@code bytes == null}.
     */
    public static PartitionUpdate fromBytes(ByteBuffer bytes, int version, DecoratedKey key)
    {
        if (bytes == null)
            return null;

        try
        {
            return serializer.deserialize(new DataInputStream(ByteBufferUtil.inputStream(bytes)),
                                          version,
                                          SerializationHelper.Flag.LOCAL,
                                          version < MessagingService.VERSION_30 ? key : null);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Serialize a partition update as a byte buffer.
     *
     * @param update the partition update to serialize.
     * @param version the version to serialize the update into.
     *
     * @return a newly allocated byte buffer containing the serialized update.
     */
    public static ByteBuffer toBytes(PartitionUpdate update, int version)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(update, out, MessagingService.current_version);
            return ByteBuffer.wrap(out.getData(), 0, out.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a empty immutable partition update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the created update.
     *
     * @return the newly created empty (and immutable) update.
     */
    public static PartitionUpdate emptyUpdate(CFMetaData metadata, DecoratedKey key)
    {
        return new PartitionUpdate(metadata, key, PartitionColumns.NONE, 0)
        {
            public Row.Writer staticWriter()
            {
                throw new UnsupportedOperationException();
            }

            public Row.Writer writer()
            {
                throw new UnsupportedOperationException();
            }

            public void addPartitionDeletion(DeletionTime deletionTime)
            {
                throw new UnsupportedOperationException();
            }

            public void addRangeTombstone(RangeTombstone range)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Creates a partition update that entirely deletes a given partition.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition that the created update should delete.
     * @param timestamp the timestamp for the deletion.
     * @param nowInSec the current time in seconds to use as local deletion time for the partition deletion.
     *
     * @return the newly created partition deletion update.
     */
    public static PartitionUpdate fullPartitionDelete(CFMetaData metadata, DecoratedKey key, long timestamp, int nowInSec)
    {
        return new PartitionUpdate(metadata,
                                   key,
                                   new DeletionInfo(timestamp, nowInSec),
                                   new RowDataBlock(Columns.NONE, 0, true, metadata.isCounter()),
                                   PartitionColumns.NONE,
                                   0);
    }

    /**
     * Merges the provided updates, yielding a new update that incorporates all those updates.
     *
     * @param updates the collection of updates to merge. This shouldn't be empty.
     *
     * @return a partition update that include (merge) all the updates from {@code updates}.
     */
    public static PartitionUpdate merge(Collection<PartitionUpdate> updates)
    {
        assert !updates.isEmpty();
        if (updates.size() == 1)
            return Iterables.getOnlyElement(updates);

        int totalSize = 0;
        PartitionColumns.Builder builder = PartitionColumns.builder();
        DecoratedKey key = null;
        CFMetaData metadata = null;
        for (PartitionUpdate update : updates)
        {
            totalSize += update.rows;
            builder.addAll(update.columns());

            if (key == null)
                key = update.partitionKey();
            else
                assert key.equals(update.partitionKey());

            if (metadata == null)
                metadata = update.metadata();
            else
                assert metadata.cfId.equals(update.metadata().cfId);
        }

        // Used when merging row to decide of liveness
        int nowInSec = FBUtilities.nowInSeconds();
        PartitionUpdate newUpdate = new PartitionUpdate(metadata, key, builder.build(), totalSize);
        for (PartitionUpdate update : updates)
        {
            newUpdate.deletionInfo.add(update.deletionInfo);
            if (!update.staticRow().isEmpty())
            {
                if (newUpdate.staticRow().isEmpty())
                    newUpdate.staticRow = update.staticRow().takeAlias();
                else
                    Rows.merge(newUpdate.staticRow(), update.staticRow(), newUpdate.columns().statics, newUpdate.staticWriter(), nowInSec, SecondaryIndexManager.nullUpdater);
            }
            for (Row row : update)
                row.copyTo(newUpdate.writer);
        }
        return newUpdate;
    }

    /**
     * The number of "operations" contained in the update.
     * <p>
     * This is used by {@code Memtable} to approximate how much work this update does. In practice, this
     * count how many rows are updated and how many ranges are deleted by the partition update.
     *
     * @return the number of "operations" performed by the update.
     */
    public int operationCount()
    {
        return rowCount()
             + deletionInfo.rangeCount()
             + (deletionInfo.getPartitionDeletion().isLive() ? 0 : 1);
    }

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    public int dataSize()
    {
        int clusteringSize = metadata().comparator.size();
        int size = 0;
        for (Row row : this)
        {
            size += row.clustering().dataSize();
            for (Cell cell : row)
                size += cell.dataSize();
        }
        return size;
    }

    /**
     * If a partition update has been read (and is thus unmodifiable), a call to this method
     * makes the update modifiable again.
     * <p>
     * Please note that calling this method won't result in optimal behavior in the sense that
     * even if very little is added to the update after this call, the whole update will be sorted
     * again on read. This should thus be used sparingly (and if it turns that we end up using
     * this often, we should consider optimizing the behavior).
     */
    public synchronized void allowNewUpdates()
    {
        // This is synchronized to make extra sure things work properly even if this is
        // called concurrently with sort() (which should be avoided in the first place, but
        // better safe than sorry).
        isSorted = false;
    }

    /**
     * Returns an iterator that iterators over the rows of this update in clustering order.
     * <p>
     * Note that this might trigger a sorting of the update, and as such the update will not
     * be modifiable anymore after this call.
     *
     * @return an iterator over the rows of this update.
     */
    @Override
    public Iterator<Row> iterator()
    {
        maybeSort();
        return super.iterator();
    }

    @Override
    protected SliceableUnfilteredRowIterator sliceableUnfilteredIterator(ColumnFilter columns, boolean reversed)
    {
        maybeSort();
        return super.sliceableUnfilteredIterator(columns, reversed);
    }

    /**
     * Validates the data contained in this update.
     *
     * @throws MarshalException if some of the data contained in this update is corrupted.
     */
    public void validate()
    {
        for (Row row : this)
        {
            metadata().comparator.validate(row.clustering());
            for (Cell cell : row)
                cell.validate();
        }
    }

    /**
     * The maximum timestamp used in this update.
     *
     * @return the maximum timestamp used in this update.
     */
    public long maxTimestamp()
    {
        return maxTimestamp;
    }

    /**
     * For an update on a counter table, returns a list containing a {@code CounterMark} for
     * every counter contained in the update.
     *
     * @return a list with counter marks for every counter in this update.
     */
    public List<CounterMark> collectCounterMarks()
    {
        assert metadata().isCounter();

        InternalReusableClustering clustering = new InternalReusableClustering();
        List<CounterMark> l = new ArrayList<>();
        int i = 0;
        for (Row row : this)
        {
            for (Cell cell : row)
                if (cell.isCounterCell())
                    l.add(new CounterMark(clustering, i, cell.column(), cell.path()));
            i++;
        }
        return l;
    }

    /**
     * Returns a row writer for the static row of this partition update.
     *
     * @return a row writer for the static row of this partition update. A partition
     * update contains only one static row so only one row should be written through
     * this writer (but if multiple rows are added, the latest written one wins).
     */
    public Row.Writer staticWriter()
    {
        return createStaticWriter();
    }

    /**
     * Returns a row writer to add (non-static) rows to this partition update.
     *
     * @return a row writer to add (non-static) rows to this partition update.
     * Multiple rows can be successively added this way and the rows added do not have
     * to be in clustering order. Further, the same row can be added multiple time.
     *
     */
    public Row.Writer writer()
    {
        if (isSorted)
            throw new IllegalStateException("An update should not written again once it has been read");

        return writer;
    }

    /**
     * Returns a range tombstone marker writer to add range tombstones to this
     * partition update.
     * <p>
     * Note that if more convenient, range tombstones can also be added using
     * {@link addRangeTombstone}.
     *
     * @param isReverseOrder whether the range tombstone marker will be provided to the returned writer
     * in clustering order or in reverse clustering order.
     * @return a range tombstone marker writer to add range tombstones to this update.
     */
    public RangeTombstoneMarker.Writer markerWriter(boolean isReverseOrder)
    {
        return new RangeTombstoneCollector(isReverseOrder);
    }

    /**
     * The number of rows contained in this update.
     *
     * @return the number of rows contained in this update.
     */
    public int size()
    {
        return rows;
    }

    private void maybeSort()
    {
        if (isSorted)
            return;

        sort();
    }

    private synchronized void sort()
    {
        if (isSorted)
            return;

        if (rows <= 1)
        {
            isSorted = true;
            return;
        }

        // Sort the rows - will still potentially contain duplicate (non-reconciled) rows
        Sorting.sort(this);

        // Now find duplicates and merge them together
        int previous = 0; // The last element that was set
        int nowInSec = FBUtilities.nowInSeconds();
        for (int current = 1; current < rows; current++)
        {
            // There is really only 2 possible comparison: < 0 or == 0 since we've sorted already
            int cmp = compare(previous, current);
            if (cmp == 0)
            {
                // current and previous are the same row. Merge current into previous
                // (and so previous + 1 will be "free").
                data.merge(current, previous, nowInSec);
            }
            else
            {
                // data[current] != [previous], so move current just after previous if needs be
                ++previous;
                if (previous != current)
                    data.move(current, previous);
            }
        }

        // previous is on the last value to keep
        rows = previous + 1;

        isSorted = true;
    }

    /**
     * This method is note meant to be used externally: it is only public so this
     * update conform to the {@link Sorting.Sortable} interface.
     */
    public int compare(int i, int j)
    {
        return metadata.comparator.compare(p1.setTo(i), p2.setTo(j));
    }

    protected class StaticWriter extends StaticRow.Builder
    {
        protected StaticWriter()
        {
            super(columns.statics, false, metadata().isCounter());
        }

        @Override
        public void endOfRow()
        {
            super.endOfRow();
            if (staticRow == null)
            {
                staticRow = build();
            }
            else
            {
                StaticRow.Builder builder = StaticRow.builder(columns.statics, true, metadata().isCounter());
                Rows.merge(staticRow, build(), columns.statics, builder, FBUtilities.nowInSeconds());
                staticRow = builder.build();
            }
        }
    }

    protected class RegularWriter extends Writer
    {
        // For complex column, the writer assumptions is that for a given row, cells of different
        // complex columns are not intermingled (they also should be in cellPath order). We however
        // don't yet guarantee that this will be the case for updates (both UpdateStatement and
        // RowUpdateBuilder can potentially break that assumption; we could change those classes but
        // that's non trivial, at least for UpdateStatement).
        // To deal with that problem, we record which complex columns have been updated (for the current
        // row) and if we detect a violation of our assumption, we switch the row we're writing
        // into (which is ok because everything will be sorted and merged in maybeSort()).
        private final Set<ColumnDefinition> updatedComplex = new HashSet();
        private ColumnDefinition lastUpdatedComplex;
        private CellPath lastUpdatedComplexPath;

        public RegularWriter()
        {
            super(false);
        }

        @Override
        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
        {
            if (column.isComplex())
            {
                if (updatedComplex.contains(column)
                    && (!column.equals(lastUpdatedComplex) || (column.cellPathComparator().compare(path, lastUpdatedComplexPath)) <= 0))
                {
                    // We've updated that complex already, but we've either updated another complex or it's not in order: as this
                    // break the writer assumption, switch rows.
                    endOfRow();

                    // Copy the clustering values from the previous row
                    int clusteringSize = metadata.clusteringColumns().size();
                    int base = (row - 1) * clusteringSize;
                    for (int i = 0; i < clusteringSize; i++)
                        writer.writeClusteringValue(clusterings[base + i]);

                    updatedComplex.clear();
                }

                lastUpdatedComplex = column;
                lastUpdatedComplexPath = path;
                updatedComplex.add(column);
            }
            super.writeCell(column, isCounter, value, info, path);
        }

        @Override
        public void endOfRow()
        {
            super.endOfRow();
            clear();
        }

        @Override
        public Writer reset()
        {
            super.reset();
            clear();
            return this;
        }

        private void clear()
        {
            updatedComplex.clear();
            lastUpdatedComplex = null;
            lastUpdatedComplexPath = null;
        }
    }

    public static class PartitionUpdateSerializer
    {
        public void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException();

                // if (cf == null)
                // {
                //     out.writeBoolean(false);
                //     return;
                // }

                // out.writeBoolean(true);
                // serializeCfId(cf.id(), out, version);
                // cf.getComparator().deletionInfoSerializer().serialize(cf.deletionInfo(), out, version);
                // ColumnSerializer columnSerializer = cf.getComparator().columnSerializer();
                // int count = cf.getColumnCount();
                // out.writeInt(count);
                // int written = 0;
                // for (Cell cell : cf)
                // {
                //     columnSerializer.serialize(cell, out);
                //     written++;
                // }
                // assert count == written: "Table had " + count + " columns, but " + written + " written";
            }

            try (UnfilteredRowIterator iter = update.sliceableUnfilteredIterator())
            {
                assert !iter.isReverseOrder();
                UnfilteredRowIteratorSerializer.serializer.serialize(iter, out, version, update.rows);
            }
        }

        public PartitionUpdate deserialize(DataInput in, int version, SerializationHelper.Flag flag, DecoratedKey key) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                assert key != null;

                // This is only used in mutation, and mutation have never allowed "null" column families
                boolean present = in.readBoolean();
                assert present;

                CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
                LegacyLayout.LegacyDeletionInfo info = LegacyLayout.LegacyDeletionInfo.serializer.deserialize(metadata, in, version);
                int size = in.readInt();
                Iterator<LegacyLayout.LegacyCell> cells = LegacyLayout.deserializeCells(metadata, in, flag, size);
                SerializationHelper helper = new SerializationHelper(version, flag);
                try (UnfilteredRowIterator iterator = LegacyLayout.onWireCellstoUnfilteredRowIterator(metadata, key, info, cells, false, helper))
                {
                    return UnfilteredRowIterators.toUpdate(iterator);
                }
            }

            assert key == null; // key is only there for the old format

            UnfilteredRowIteratorSerializer.Header h = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(in, version, flag);
            if (h.isEmpty)
                return emptyUpdate(h.metadata, h.key);

            assert !h.isReversed;
            assert h.rowEstimate >= 0;
            PartitionUpdate upd = new PartitionUpdate(h.metadata,
                                                      h.key,
                                                      new DeletionInfo(h.partitionDeletion),
                                                      new RowDataBlock(h.sHeader.columns().regulars, h.rowEstimate, false, h.metadata.isCounter()),
                                                      h.sHeader.columns(),
                                                      h.rowEstimate);

            upd.staticRow = h.staticRow;

            RangeTombstoneMarker.Writer markerWriter = upd.markerWriter(false);
            UnfilteredRowIteratorSerializer.serializer.deserialize(in, new SerializationHelper(version, flag), h.sHeader, upd.writer(), markerWriter);

            // Mark sorted after we're read it all since that's what we use in the writer() method to detect bad uses
            upd.isSorted = true;

            return upd;
        }

        public long serializedSize(PartitionUpdate update, int version, TypeSizes sizes)
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException("Version is " + version);
                //if (cf == null)
                //{
                //    return typeSizes.sizeof(false);
                //}
                //else
                //{
                //    return typeSizes.sizeof(true)  /* nullness bool */
                //        + cfIdSerializedSize(cf.id(), typeSizes, version)  /* id */
                //        + contentSerializedSize(cf, typeSizes, version);
                //}
            }

            try (UnfilteredRowIterator iter = update.sliceableUnfilteredIterator())
            {
                return UnfilteredRowIteratorSerializer.serializer.serializedSize(iter, version, update.rows, sizes);
            }
        }
    }

    /**
     * A counter mark is basically a pointer to a counter update inside this partition update. That pointer allows
     * us to update the counter value based on the pre-existing value read during the read-before-write that counters
     * do. See {@link CounterMutation} to understand how this is used.
     */
    public class CounterMark
    {
        private final InternalReusableClustering clustering;
        private final int row;
        private final ColumnDefinition column;
        private final CellPath path;

        private CounterMark(InternalReusableClustering clustering, int row, ColumnDefinition column, CellPath path)
        {
            this.clustering = clustering;
            this.row = row;
            this.column = column;
            this.path = path;
        }

        public Clustering clustering()
        {
            return clustering.setTo(row);
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public CellPath path()
        {
            return path;
        }

        public ByteBuffer value()
        {
            return data.getValue(row, column, path);
        }

        public void setValue(ByteBuffer value)
        {
            data.setValue(row, column, path, value);
        }
    }
}
