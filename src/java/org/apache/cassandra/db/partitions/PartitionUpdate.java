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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.NIODataInputStream;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;

/**
 * Stores updates made on a partition.
 * <p>
 * A PartitionUpdate object requires that all writes/additions are performed before we
 * try to read the updates (attempts to write to the PartitionUpdate after a read method
 * has been called will result in an exception being thrown). In other words, a Partition
 * is mutable while it's written but becomes immutable as soon as it is read.
 * <p>
 * A typical usage is to create a new update ({@code new PartitionUpdate(metadata, key, columns, capacity)})
 * and then add rows and range tombstones through the {@code add()} methods (the partition
 * level deletion time can also be set with {@code addPartitionDeletion()}). However, there
 * is also a few static helper constructor methods for special cases ({@code emptyUpdate()},
 * {@code fullPartitionDelete} and {@code singleRowUpdate}).
 */
public class PartitionUpdate extends AbstractThreadUnsafePartition
{
    protected static final Logger logger = LoggerFactory.getLogger(PartitionUpdate.class);

    public static final PartitionUpdateSerializer serializer = new PartitionUpdateSerializer();

    private final int createdAtInSec = FBUtilities.nowInSeconds();

    // Records whether this update is "built", i.e. if the build() method has been called, which
    // happens when the update is read. Further writing is then rejected though a manual call
    // to allowNewUpdates() allow new writes. We could make that more implicit but only triggers
    // really requires that so we keep it simple for now).
    private boolean isBuilt;
    private boolean canReOpen = true;

    private final MutableDeletionInfo deletionInfo;
    private RowStats stats; // will be null if isn't built

    private Row staticRow = Rows.EMPTY_STATIC_ROW;

    private final boolean canHaveShadowedData;

    private PartitionUpdate(CFMetaData metadata,
                            DecoratedKey key,
                            PartitionColumns columns,
                            Row staticRow,
                            List<Row> rows,
                            MutableDeletionInfo deletionInfo,
                            RowStats stats,
                            boolean isBuilt,
                            boolean canHaveShadowedData)
    {
        super(metadata, key, columns, rows);
        this.staticRow = staticRow;
        this.deletionInfo = deletionInfo;
        this.stats = stats;
        this.isBuilt = isBuilt;
        this.canHaveShadowedData = canHaveShadowedData;
    }

    public PartitionUpdate(CFMetaData metadata,
                           DecoratedKey key,
                           PartitionColumns columns,
                           int initialRowCapacity)
    {
        this(metadata, key, columns, Rows.EMPTY_STATIC_ROW, new ArrayList<>(initialRowCapacity), MutableDeletionInfo.live(), null, false, true);
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
        return new PartitionUpdate(metadata, key, PartitionColumns.NONE, Rows.EMPTY_STATIC_ROW, Collections.<Row>emptyList(), MutableDeletionInfo.live(), RowStats.NO_STATS, true, false);
    }

    /**
     * Creates an immutable partition update that entirely deletes a given partition.
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
        return new PartitionUpdate(metadata, key, PartitionColumns.NONE, Rows.EMPTY_STATIC_ROW, Collections.<Row>emptyList(), new MutableDeletionInfo(timestamp, nowInSec), RowStats.NO_STATS, true, false);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition that the created update should delete.
     * @param row the row for the update.
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static PartitionUpdate singleRowUpdate(CFMetaData metadata, DecoratedKey key, Row row)
    {
        return row.isStatic()
             ? new PartitionUpdate(metadata, key, new PartitionColumns(row.columns(), Columns.NONE), row, Collections.<Row>emptyList(), MutableDeletionInfo.live(), RowStats.NO_STATS, true, false)
             : new PartitionUpdate(metadata, key, new PartitionColumns(Columns.NONE, row.columns()), Rows.EMPTY_STATIC_ROW, Collections.singletonList(row), MutableDeletionInfo.live(), RowStats.NO_STATS, true, false);
    }

    /**
     * Turns the given iterator into an update.
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    public static PartitionUpdate fromIterator(UnfilteredRowIterator iterator)
    {
        CFMetaData metadata = iterator.metadata();
        boolean reversed = iterator.isReverseOrder();

        List<Row> rows = new ArrayList<>();
        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(iterator.partitionLevelDeletion(), metadata.comparator, reversed);

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                rows.add((Row)unfiltered);
            else
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
        }

        if (reversed)
            Collections.reverse(rows);

        return new PartitionUpdate(metadata, iterator.partitionKey(), iterator.columns(), iterator.staticRow(), rows, deletionBuilder.build(), iterator.stats(), true, false);
    }

    public static PartitionUpdate fromIterator(RowIterator iterator)
    {
        CFMetaData metadata = iterator.metadata();
        boolean reversed = iterator.isReverseOrder();

        List<Row> rows = new ArrayList<>();

        RowStats.Collector collector = new RowStats.Collector();

        while (iterator.hasNext())
        {
            Row row = iterator.next();
            rows.add(row);
            Rows.collectStats(row, collector);
        }

        if (reversed)
            Collections.reverse(rows);

        return new PartitionUpdate(metadata, iterator.partitionKey(), iterator.columns(), iterator.staticRow(), rows, MutableDeletionInfo.live(), collector.get(), true, false);
    }

    protected boolean canHaveShadowedData()
    {
        return canHaveShadowedData;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
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
            return serializer.deserialize(new NIODataInputStream(bytes, true),
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
            serializer.serialize(update, out, version);
            return ByteBuffer.wrap(out.getData(), 0, out.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
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
        final int size = updates.size();

        if (size == 1)
            return Iterables.getOnlyElement(updates);

        // Used when merging row to decide of liveness
        int nowInSec = FBUtilities.nowInSeconds();

        PartitionColumns.Builder builder = PartitionColumns.builder();
        DecoratedKey key = null;
        CFMetaData metadata = null;
        MutableDeletionInfo deletion = MutableDeletionInfo.live();
        Row staticRow = Rows.EMPTY_STATIC_ROW;
        List<Iterator<Row>> updateRowIterators = new ArrayList<>(size);
        RowStats stats = RowStats.NO_STATS;

        for (PartitionUpdate update : updates)
        {
            builder.addAll(update.columns());
            deletion.add(update.deletionInfo());
            if (!update.staticRow().isEmpty())
                staticRow = staticRow == Rows.EMPTY_STATIC_ROW ? update.staticRow() : Rows.merge(staticRow, update.staticRow(), nowInSec);
            updateRowIterators.add(update.iterator());
            stats = stats.mergeWith(update.stats());

            if (key == null)
                key = update.partitionKey();
            else
                assert key.equals(update.partitionKey());

            if (metadata == null)
                metadata = update.metadata();
            else
                assert metadata.cfId.equals(update.metadata().cfId);
        }

        PartitionColumns columns = builder.build();

        final Row.Merger merger = new Row.Merger(size, nowInSec, columns.regulars);

        Iterator<Row> merged = MergeIterator.get(updateRowIterators, metadata.comparator, new MergeIterator.Reducer<Row, Row>()
        {
            @Override
            public boolean trivialReduceIsTrivial()
            {
                return true;
            }

            public void reduce(int idx, Row current)
            {
                merger.add(idx, current);
            }

            protected Row getReduced()
            {
                // Note that while merger.getRow() can theoretically return null, it won't in this case because
                // we don't pass an "activeDeletion".
                return merger.merge(DeletionTime.LIVE);
            }

            @Override
            protected void onKeyChange()
            {
                merger.clear();
            }
        });

        List<Row> rows = new ArrayList<>();
        Iterators.addAll(rows, merged);

        return new PartitionUpdate(metadata, key, columns, staticRow, rows, deletion, stats, true, true);
    }

    /**
     * Modify this update to set every timestamp for live data to {@code newTimestamp} and
     * every deletion timestamp to {@code newTimestamp - 1}.
     *
     * There is no reason to use that expect on the Paxos code path, where we need ensure that
     * anything inserted use the ballot timestamp (to respect the order of update decided by
     * the Paxos algorithm). We use {@code newTimestamp - 1} for deletions because tombstones
     * always win on timestamp equality and we don't want to delete our own insertions
     * (typically, when we overwrite a collection, we first set a complex deletion to delete the
     * previous collection before adding new elements. If we were to set that complex deletion
     * to the same timestamp that the new elements, it would delete those elements). And since
     * tombstones always wins on timestamp equality, using -1 guarantees our deletion will still
     * delete anything from a previous update.
     */
    public void updateAllTimestamp(long newTimestamp)
    {
        // We know we won't be updating that update again after this call, and doing is post built is potentially
        // slightly more efficient (things are more "compact"). So force a build if it hasn't happened yet.
        maybeBuild();

        deletionInfo.updateAllTimestamp(newTimestamp - 1);

        if (!staticRow.isEmpty())
            staticRow = staticRow.updateAllTimestamp(newTimestamp);

        for (int i = 0; i < rows.size(); i++)
            rows.set(i, rows.get(i).updateAllTimestamp(newTimestamp));
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
        return rows.size()
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
        int size = 0;
        for (Row row : this)
        {
            size += row.clustering().dataSize();
            for (ColumnData cd : row)
                size += cd.dataSize();
        }
        return size;
    }

    @Override
    public int rowCount()
    {
        maybeBuild();
        return super.rowCount();
    }

    public RowStats stats()
    {
        maybeBuild();
        return stats;
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
        if (!canReOpen)
            throw new IllegalStateException("You cannot do more updates on collectCounterMarks has been called");

        // This is synchronized to make extra sure things work properly even if this is
        // called concurrently with sort() (which should be avoided in the first place, but
        // better safe than sorry).
        isBuilt = false;
    }

    /**
     * Returns an iterator that iterates over the rows of this update in clustering order.
     * <p>
     * Note that this might trigger a sorting of the update, and as such the update will not
     * be modifiable anymore after this call.
     *
     * @return an iterator over the rows of this update.
     */
    @Override
    public Iterator<Row> iterator()
    {
        maybeBuild();
        return super.iterator();
    }

    @Override
    protected SliceableUnfilteredRowIterator sliceableUnfilteredIterator(ColumnFilter columns, boolean reversed)
    {
        maybeBuild();
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
            for (ColumnData cd : row)
                cd.validate();
        }
    }

    /**
     * The maximum timestamp used in this update.
     *
     * @return the maximum timestamp used in this update.
     */
    public long maxTimestamp()
    {
        maybeBuild();

        long maxTimestamp = deletionInfo.maxTimestamp();
        for (Row row : this)
        {
            maxTimestamp = Math.max(maxTimestamp, row.primaryKeyLivenessInfo().timestamp());
            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    maxTimestamp = Math.max(maxTimestamp, ((Cell)cd).timestamp());
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    maxTimestamp = Math.max(maxTimestamp, complexData.complexDeletion().markedForDeleteAt());
                    for (Cell cell : complexData)
                        maxTimestamp = Math.max(maxTimestamp, cell.timestamp());
                }
            }
        }
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
        maybeBuild();
        // We will take aliases on the rows of this update, and update them in-place. So we should be sure the
        // update is no immutable for all intent and purposes.
        canReOpen = false;

        List<CounterMark> l = new ArrayList<>();
        for (Row row : rows)
        {
            for (Cell cell : row.cells())
            {
                if (cell.isCounterCell())
                    l.add(new CounterMark(row, cell.column(), cell.path()));
            }
        }
        return l;
    }

    private void assertNotBuilt()
    {
        if (isBuilt)
            throw new IllegalStateException("An update should not be written again once it has been read");
    }

    public void addPartitionDeletion(DeletionTime deletionTime)
    {
        assertNotBuilt();
        deletionInfo.add(deletionTime);
    }

    public void add(RangeTombstone range)
    {
        assertNotBuilt();
        deletionInfo.add(range, metadata.comparator);
    }

    /**
     * Adds a row to this update.
     *
     * There is no particular assumption made on the order of row added to a partition update. It is further
     * allowed to add the same row (more precisely, multiple row objects for the same clustering).
     *
     * Note however that the columns contained in the added row must be a subset of the columns used when
     * creating this update.
     *
     * @param row the row to add.
     */
    public void add(Row row)
    {
        if (row.isEmpty())
            return;

        assertNotBuilt();

        if (row.isStatic())
        {
            // We test for == first because in most case it'll be true and that is faster
            assert columns().statics == row.columns() || columns().statics.contains(row.columns());
            staticRow = staticRow.isEmpty()
                      ? row
                      : Rows.merge(staticRow, row, createdAtInSec);
        }
        else
        {
            // We test for == first because in most case it'll be true and that is faster
            assert columns().regulars == row.columns() || columns().regulars.contains(row.columns());
            rows.add(row);
        }
    }

    /**
     * The number of rows contained in this update.
     *
     * @return the number of rows contained in this update.
     */
    public int size()
    {
        return rows.size();
    }

    private void maybeBuild()
    {
        if (isBuilt)
            return;

        build();
    }

    private synchronized void build()
    {
        if (isBuilt)
            return;

        if (rows.size() <= 1)
        {
            finishBuild();
            return;
        }

        Comparator<Row> comparator = metadata.comparator.rowComparator();
        // Sort the rows. Because the same row can have been added multiple times, we can still have duplicates after that
        Collections.sort(rows, comparator);

        // Now find the duplicates and merge them together
        int previous = 0; // The last element that was set
        for (int current = 1; current < rows.size(); current++)
        {
            // There is really only 2 possible comparison: < 0 or == 0 since we've sorted already
            Row previousRow = rows.get(previous);
            Row currentRow = rows.get(current);
            int cmp = comparator.compare(previousRow, currentRow);
            if (cmp == 0)
            {
                // current and previous are the same row. Merge current into previous
                // (and so previous + 1 will be "free").
                rows.set(previous, Rows.merge(previousRow, currentRow, createdAtInSec));
            }
            else
            {
                // current != previous, so move current just after previous if needs be
                ++previous;
                if (previous != current)
                    rows.set(previous, currentRow);
            }
        }

        // previous is on the last value to keep
        for (int j = rows.size() - 1; j > previous; j--)
            rows.remove(j);

        finishBuild();
    }

    private void finishBuild()
    {
        RowStats.Collector collector = new RowStats.Collector();
        deletionInfo.collectStats(collector);
        for (Row row : rows)
            Rows.collectStats(row, collector);
        stats = collector.get();
        isBuilt = true;
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
                UnfilteredRowIteratorSerializer.serializer.serialize(iter, out, version, update.rows.size());
            }
        }

        public PartitionUpdate deserialize(DataInputPlus in, int version, SerializationHelper.Flag flag, DecoratedKey key) throws IOException
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
                SerializationHelper helper = new SerializationHelper(metadata, version, flag);
                try (UnfilteredRowIterator iterator = LegacyLayout.onWireCellstoUnfilteredRowIterator(metadata, key, info, cells, false, helper))
                {
                    return PartitionUpdate.fromIterator(iterator);
                }
            }

            assert key == null; // key is only there for the old format

            UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(in, version, flag);
            if (header.isEmpty)
                return emptyUpdate(header.metadata, header.key);

            assert !header.isReversed;
            assert header.rowEstimate >= 0;

            MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(header.partitionDeletion, header.metadata.comparator, false);
            List<Row> rows = new ArrayList<>(header.rowEstimate);

            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, flag, header))
            {
                while (partition.hasNext())
                {
                    Unfiltered unfiltered = partition.next();
                    if (unfiltered.kind() == Unfiltered.Kind.ROW)
                        rows.add((Row)unfiltered);
                    else
                        deletionBuilder.add((RangeTombstoneMarker)unfiltered);
                }
            }

            return new PartitionUpdate(header.metadata,
                                       header.key,
                                       header.sHeader.columns(),
                                       header.staticRow,
                                       rows,
                                       deletionBuilder.build(),
                                       header.sHeader.stats(),
                                       true,
                                       false);
        }

        public long serializedSize(PartitionUpdate update, int version)
        {
            if (version < MessagingService.VERSION_30)
            {
                // TODO
                throw new UnsupportedOperationException("Version is " + version);
                //if (cf == null)
                //{
                //    return TypeSizes.sizeof(false);
                //}
                //else
                //{
                //    return TypeSizes.sizeof(true)  /* nullness bool */
                //        + cfIdSerializedSize(cf.id(), typeSizes, version)  /* id */
                //        + contentSerializedSize(cf, typeSizes, version);
                //}
            }

            try (UnfilteredRowIterator iter = update.sliceableUnfilteredIterator())
            {
                return UnfilteredRowIteratorSerializer.serializer.serializedSize(iter, version, update.rows.size());
            }
        }
    }

    /**
     * A counter mark is basically a pointer to a counter update inside this partition update. That pointer allows
     * us to update the counter value based on the pre-existing value read during the read-before-write that counters
     * do. See {@link CounterMutation} to understand how this is used.
     */
    public static class CounterMark
    {
        private final Row row;
        private final ColumnDefinition column;
        private final CellPath path;

        private CounterMark(Row row, ColumnDefinition column, CellPath path)
        {
            this.row = row;
            this.column = column;
            this.path = path;
        }

        public Clustering clustering()
        {
            return row.clustering();
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
            return path == null
                 ? row.getCell(column).value()
                 : row.getCell(column, path).value();
        }

        public void setValue(ByteBuffer value)
        {
            // This is a bit of a giant hack as this is the only place where we mutate a Row object. This makes it more efficient
            // for counters however and this won't be needed post-#6506 so that's probably fine.
            assert row instanceof ArrayBackedRow;
            ((ArrayBackedRow)row).setValue(column, path, value);
        }
    }
}
