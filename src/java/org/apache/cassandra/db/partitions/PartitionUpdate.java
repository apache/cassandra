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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

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
public class PartitionUpdate extends AbstractBTreePartition
{
    protected static final Logger logger = LoggerFactory.getLogger(PartitionUpdate.class);

    public static final PartitionUpdateSerializer serializer = new PartitionUpdateSerializer();

    private final int createdAtInSec = FBUtilities.nowInSeconds();

    // Records whether this update is "built", i.e. if the build() method has been called, which
    // happens when the update is read. Further writing is then rejected though a manual call
    // to allowNewUpdates() allow new writes. We could make that more implicit but only triggers
    // really requires that so we keep it simple for now).
    private volatile boolean isBuilt;
    private boolean canReOpen = true;

    private Holder holder;
    private BTree.Builder<Row> rowBuilder;
    private MutableDeletionInfo deletionInfo;

    private final boolean canHaveShadowedData;

    private PartitionUpdate(CFMetaData metadata,
                            DecoratedKey key,
                            PartitionColumns columns,
                            MutableDeletionInfo deletionInfo,
                            int initialRowCapacity,
                            boolean canHaveShadowedData)
    {
        super(metadata, key);
        this.deletionInfo = deletionInfo;
        this.holder = new Holder(columns, BTree.empty(), deletionInfo, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
        this.canHaveShadowedData = canHaveShadowedData;
        rowBuilder = builder(initialRowCapacity);
    }

    private PartitionUpdate(CFMetaData metadata,
                            DecoratedKey key,
                            Holder holder,
                            MutableDeletionInfo deletionInfo,
                            boolean canHaveShadowedData)
    {
        super(metadata, key);
        this.holder = holder;
        this.deletionInfo = deletionInfo;
        this.isBuilt = true;
        this.canHaveShadowedData = canHaveShadowedData;
    }

    public PartitionUpdate(CFMetaData metadata,
                           DecoratedKey key,
                           PartitionColumns columns,
                           int initialRowCapacity)
    {
        this(metadata, key, columns, MutableDeletionInfo.live(), initialRowCapacity, true);
    }

    public PartitionUpdate(CFMetaData metadata,
                           ByteBuffer key,
                           PartitionColumns columns,
                           int initialRowCapacity)
    {
        this(metadata,
             metadata.decorateKey(key),
             columns,
             initialRowCapacity);
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
        MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
        Holder holder = new Holder(PartitionColumns.NONE, BTree.empty(), deletionInfo, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
        return new PartitionUpdate(metadata, key, holder, deletionInfo, false);
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
        MutableDeletionInfo deletionInfo = new MutableDeletionInfo(timestamp, nowInSec);
        Holder holder = new Holder(PartitionColumns.NONE, BTree.empty(), deletionInfo, Rows.EMPTY_STATIC_ROW, EncodingStats.NO_STATS);
        return new PartitionUpdate(metadata, key, holder, deletionInfo, false);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update (may be null).
     * @param row the static row for the update (may be null).
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static PartitionUpdate singleRowUpdate(CFMetaData metadata, DecoratedKey key, Row row, Row staticRow)
    {
        MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
        Holder holder = new Holder(
            new PartitionColumns(
                staticRow == null ? Columns.NONE : Columns.from(staticRow.columns()),
                row == null ? Columns.NONE : Columns.from(row.columns())
            ),
            row == null ? BTree.empty() : BTree.singleton(row),
            deletionInfo,
            staticRow == null ? Rows.EMPTY_STATIC_ROW : staticRow,
            EncodingStats.NO_STATS
        );
        return new PartitionUpdate(metadata, key, holder, deletionInfo, false);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update (may be static).
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static PartitionUpdate singleRowUpdate(CFMetaData metadata, DecoratedKey key, Row row)
    {
        return singleRowUpdate(metadata, key, row.isStatic() ? null : row, row.isStatic() ? row : null);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update.
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static PartitionUpdate singleRowUpdate(CFMetaData metadata, ByteBuffer key, Row row)
    {
        return singleRowUpdate(metadata, metadata.decorateKey(key), row);
    }

    /**
     * Turns the given iterator into an update.
     *
     * @param iterator the iterator to turn into updates.
     * @param filter the column filter used when querying {@code iterator}. This is used to make
     * sure we don't include data for which the value has been skipped while reading (as we would
     * then be writing something incorrect).
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    public static PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter)
    {
        iterator = UnfilteredRowIterators.withOnlyQueriedData(iterator, filter);
        Holder holder = build(iterator, 16);
        MutableDeletionInfo deletionInfo = (MutableDeletionInfo) holder.deletionInfo;
        return new PartitionUpdate(iterator.metadata(), iterator.partitionKey(), holder, deletionInfo, false);
    }

    /**
     * Turns the given iterator into an update.
     *
     * @param iterator the iterator to turn into updates.
     * @param filter the column filter used when querying {@code iterator}. This is used to make
     * sure we don't include data for which the value has been skipped while reading (as we would
     * then be writing something incorrect).
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    public static PartitionUpdate fromIterator(RowIterator iterator, ColumnFilter filter)
    {
        iterator = RowIterators.withOnlyQueriedData(iterator, filter);
        MutableDeletionInfo deletionInfo = MutableDeletionInfo.live();
        Holder holder = build(iterator, deletionInfo, true, 16);
        return new PartitionUpdate(iterator.metadata(), iterator.partitionKey(), holder, deletionInfo, false);
    }

    protected boolean canHaveShadowedData()
    {
        return canHaveShadowedData;
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
            return serializer.deserialize(new DataInputBuffer(bytes, true),
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
            return out.buffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
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
    public static PartitionUpdate fullPartitionDelete(CFMetaData metadata, ByteBuffer key, long timestamp, int nowInSec)
    {
        return fullPartitionDelete(metadata, metadata.decorateKey(key), timestamp, nowInSec);
    }

    /**
     * Merges the provided updates, yielding a new update that incorporates all those updates.
     *
     * @param updates the collection of updates to merge. This shouldn't be empty.
     *
     * @return a partition update that include (merge) all the updates from {@code updates}.
     */
    public static PartitionUpdate merge(List<PartitionUpdate> updates)
    {
        assert !updates.isEmpty();
        final int size = updates.size();

        if (size == 1)
            return Iterables.getOnlyElement(updates);

        int nowInSecs = FBUtilities.nowInSeconds();
        List<UnfilteredRowIterator> asIterators = Lists.transform(updates, AbstractBTreePartition::unfilteredIterator);
        return fromIterator(UnfilteredRowIterators.merge(asIterators, nowInSecs), ColumnFilter.all(updates.get(0).metadata()));
    }

    // We override this, because the version in the super-class calls holder(), which build the update preventing
    // further updates, but that's not necessary here and being able to check at least the partition deletion without
    // "locking" the update is nice (and used in DataResolver.RepairMergeListener.MergeListener).
    @Override
    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
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
        Holder holder = holder();
        deletionInfo.updateAllTimestamp(newTimestamp - 1);
        Object[] tree = BTree.<Row>transformAndFilter(holder.tree, (x) -> x.updateAllTimestamp(newTimestamp));
        Row staticRow = holder.staticRow.updateAllTimestamp(newTimestamp);
        EncodingStats newStats = EncodingStats.Collector.collect(staticRow, BTree.<Row>iterator(tree), deletionInfo);
        this.holder = new Holder(holder.columns, tree, deletionInfo, staticRow, newStats);
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
             + (staticRow().isEmpty() ? 0 : 1)
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
    public PartitionColumns columns()
    {
        // The superclass implementation calls holder(), but that triggers a build of the PartitionUpdate. But since
        // the columns are passed to the ctor, we know the holder always has the proper columns even if it doesn't have
        // the built rows yet, so just bypass the holder() method.
        return holder.columns;
    }

    protected Holder holder()
    {
        maybeBuild();
        return holder;
    }

    public EncodingStats stats()
    {
        return holder().stats;
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
        if (rowBuilder == null)
            rowBuilder = builder(16);
    }

    private BTree.Builder<Row> builder(int initialCapacity)
    {
        return BTree.<Row>builder(metadata.comparator, initialCapacity)
                    .setQuickResolver((a, b) ->
                                      Rows.merge(a, b, createdAtInSec));
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

    /**
     * Validates the data contained in this update.
     *
     * @throws org.apache.cassandra.serializers.MarshalException if some of the data contained in this update is corrupted.
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
        // update is now immutable for all intent and purposes.
        canReOpen = false;

        List<CounterMark> marks = new ArrayList<>();
        addMarksForRow(staticRow(), marks);
        for (Row row : this)
            addMarksForRow(row, marks);
        return marks;
    }

    private void addMarksForRow(Row row, List<CounterMark> marks)
    {
        for (Cell cell : row.cells())
        {
            if (cell.isCounterCell())
                marks.add(new CounterMark(row, cell.column(), cell.path()));
        }
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
            // this assert is expensive, and possibly of limited value; we should consider removing it
            // or introducing a new class of assertions for test purposes
            assert columns().statics.containsAll(row.columns()) : columns().statics + " is not superset of " + row.columns();
            Row staticRow = holder.staticRow.isEmpty()
                      ? row
                      : Rows.merge(holder.staticRow, row, createdAtInSec);
            holder = new Holder(holder.columns, holder.tree, holder.deletionInfo, staticRow, holder.stats);
        }
        else
        {
            // this assert is expensive, and possibly of limited value; we should consider removing it
            // or introducing a new class of assertions for test purposes
            assert columns().regulars.containsAll(row.columns()) : columns().regulars + " is not superset of " + row.columns();
            rowBuilder.add(row);
        }
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

        Holder holder = this.holder;
        Object[] cur = holder.tree;
        Object[] add = rowBuilder.build();
        Object[] merged = BTree.<Row>merge(cur, add, metadata.comparator,
                                           UpdateFunction.Simple.of((a, b) -> Rows.merge(a, b, createdAtInSec)));

        assert deletionInfo == holder.deletionInfo;
        EncodingStats newStats = EncodingStats.Collector.collect(holder.staticRow, BTree.<Row>iterator(merged), deletionInfo);

        this.holder = new Holder(holder.columns, merged, holder.deletionInfo, holder.staticRow, newStats);
        rowBuilder = null;
        isBuilt = true;
    }

    @Override
    public String toString()
    {
        if (isBuilt)
            return super.toString();

        // We intentionally override AbstractBTreePartition#toString() to avoid iterating over the rows in the
        // partition, which can result in build() being triggered and lead to errors if the PartitionUpdate is later
        // modified.

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("[%s.%s] key=%s columns=%s",
                                metadata.ksName,
                                metadata.cfName,
                                metadata.getKeyValidator().getString(partitionKey().getKey()),
                                columns()));

        sb.append("\n    deletionInfo=").append(deletionInfo);
        sb.append(" (not built)");
        return sb.toString();
    }

    /**
     * Creates a new simple partition update builder.
     *
     * @param metadata the metadata for the table this is a partition of.
     * @param partitionKeyValues the values for partition key columns identifying this partition. The values for each
     * partition key column can be passed either directly as {@code ByteBuffer} or using a "native" value (int for
     * Int32Type, string for UTF8Type, ...). It is also allowed to pass a single {@code DecoratedKey} value directly.
     * @return a newly created builder.
     */
    public static SimpleBuilder simpleBuilder(CFMetaData metadata, Object... partitionKeyValues)
    {
        return new SimpleBuilders.PartitionUpdateBuilder(metadata, partitionKeyValues);
    }

    /**
     * Interface for building partition updates geared towards human.
     * <p>
     * This should generally not be used when performance matters too much, but provides a more convenient interface to
     * build an update than using the class constructor when performance is not of the utmost importance.
     */
    public interface SimpleBuilder
    {
        /**
         * The metadata of the table this is a builder on.
         */
        public CFMetaData metadata();

        /**
         * Sets the timestamp to use for the following additions to this builder or any derived (row) builder.
         *
         * @param timestamp the timestamp to use for following additions. If that timestamp hasn't been set, the current
         * time in microseconds will be used.
         * @return this builder.
         */
        public SimpleBuilder timestamp(long timestamp);

        /**
         * Sets the ttl to use for the following additions to this builder or any derived (row) builder.
         *
         * @param ttl the ttl to use for following additions. If that ttl hasn't been set, no ttl will be used.
         * @return this builder.
         */
        public SimpleBuilder ttl(int ttl);

        /**
         * Sets the current time to use for the following additions to this builder or any derived (row) builder.
         *
         * @param nowInSec the current time to use for following additions. If the current time hasn't been set, the current
         * time in seconds will be used.
         * @return this builder.
         */
        public SimpleBuilder nowInSec(int nowInSec);

        /**
         * Adds the row identifier by the provided clustering and return a builder for that row.
         *
         * @param clusteringValues the value for the clustering columns of the row to add to this build. There may be no
         * values if either the table has no clustering column, or if you want to edit the static row. Note that as a
         * shortcut it is also allowed to pass a {@code Clustering} object directly, in which case that should be the
         * only argument.
         * @return a builder for the row identified by {@code clusteringValues}.
         */
        public Row.SimpleBuilder row(Object... clusteringValues);

        /**
         * Deletes the partition identified by this builder (using a partition level deletion).
         *
         * @return this builder.
         */
        public SimpleBuilder delete();

        /**
         * Adds a new range tombstone to this update, returning a builder for that range.
         *
         * @return the range tombstone builder for the newly added range.
         */
        public RangeTombstoneBuilder addRangeTombstone();

        /**
         * Build the update represented by this builder.
         *
         * @return the built update.
         */
        public PartitionUpdate build();

        /**
         * As shortcut for {@code new Mutation(build())}.
         *
         * @return the built update, wrapped in a {@code Mutation}.
         */
        public Mutation buildAsMutation();

        /**
         * Interface to build range tombstone.
         *
         * By default, if no other methods are called, the represented range is inclusive of both start and end and
         * includes everything (its start is {@code BOTTOM} and it's end is {@code TOP}).
         */
        public interface RangeTombstoneBuilder
        {
            /**
             * Sets the start for the built range using the provided values.
             *
             * @param values the value for the start of the range. They act like the {@code clusteringValues} argument
             * of the {@link SimpleBuilder#row(Object...)} method, except that it doesn't have to be a full
             * clustering, it can only be a prefix.
             * @return this builder.
             */
            public RangeTombstoneBuilder start(Object... values);

            /**
             * Sets the end for the built range using the provided values.
             *
             * @param values the value for the end of the range. They act like the {@code clusteringValues} argument
             * of the {@link SimpleBuilder#row(Object...)} method, except that it doesn't have to be a full
             * clustering, it can only be a prefix.
             * @return this builder.
             */
            public RangeTombstoneBuilder end(Object... values);

            /**
             * Sets the start of this range as inclusive.
             * <p>
             * This is the default and don't need to be called, but can for explicitness.
             *
             * @return this builder.
             */
            public RangeTombstoneBuilder inclStart();

            /**
             * Sets the start of this range as exclusive.
             *
             * @return this builder.
             */
            public RangeTombstoneBuilder exclStart();

            /**
             * Sets the end of this range as inclusive.
             * <p>
             * This is the default and don't need to be called, but can for explicitness.
             *
             * @return this builder.
             */
            public RangeTombstoneBuilder inclEnd();

            /**
             * Sets the end of this range as exclusive.
             *
             * @return this builder.
             */
            public RangeTombstoneBuilder exclEnd();
        }
    }

    public static class PartitionUpdateSerializer
    {
        public void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
        {
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                assert !iter.isReverseOrder();

                if (version < MessagingService.VERSION_30)
                {
                    LegacyLayout.serializeAsLegacyPartition(null, iter, out, version);
                }
                else
                {
                    CFMetaData.serializer.serialize(update.metadata(), out, version);
                    UnfilteredRowIteratorSerializer.serializer.serialize(iter, null, out, version, update.rowCount());
                }
            }
        }

        public PartitionUpdate deserialize(DataInputPlus in, int version, SerializationHelper.Flag flag, ByteBuffer key) throws IOException
        {
            if (version >= MessagingService.VERSION_30)
            {
                assert key == null; // key is only there for the old format
                return deserialize30(in, version, flag);
            }
            else
            {
                assert key != null;
                return deserializePre30(in, version, flag, key);
            }
        }

        // Used to share same decorated key between updates.
        public PartitionUpdate deserialize(DataInputPlus in, int version, SerializationHelper.Flag flag, DecoratedKey key) throws IOException
        {
            if (version >= MessagingService.VERSION_30)
            {
                return deserialize30(in, version, flag);
            }
            else
            {
                assert key != null;
                return deserializePre30(in, version, flag, key.getKey());
            }
        }

        private static PartitionUpdate deserialize30(DataInputPlus in, int version, SerializationHelper.Flag flag) throws IOException
        {
            CFMetaData metadata = CFMetaData.serializer.deserialize(in, version);
            UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(metadata, null, in, version, flag);
            if (header.isEmpty)
                return emptyUpdate(metadata, header.key);

            assert !header.isReversed;
            assert header.rowEstimate >= 0;

            MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(header.partitionDeletion, metadata.comparator, false);
            BTree.Builder<Row> rows = BTree.builder(metadata.comparator, header.rowEstimate);
            rows.auto(false);

            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, metadata, flag, header))
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

            MutableDeletionInfo deletionInfo = deletionBuilder.build();
            return new PartitionUpdate(metadata,
                                       header.key,
                                       new Holder(header.sHeader.columns(), rows.build(), deletionInfo, header.staticRow, header.sHeader.stats()),
                                       deletionInfo,
                                       false);
        }

        private static PartitionUpdate deserializePre30(DataInputPlus in, int version, SerializationHelper.Flag flag, ByteBuffer key) throws IOException
        {
            try (UnfilteredRowIterator iterator = LegacyLayout.deserializeLegacyPartition(in, version, flag, key))
            {
                assert iterator != null; // This is only used in mutation, and mutation have never allowed "null" column families
                return PartitionUpdate.fromIterator(iterator, ColumnFilter.all(iterator.metadata()));
            }
        }

        public long serializedSize(PartitionUpdate update, int version)
        {
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                if (version < MessagingService.VERSION_30)
                    return LegacyLayout.serializedSizeAsLegacyPartition(null, iter, version);

                return CFMetaData.serializer.serializedSize(update.metadata(), version)
                     + UnfilteredRowIteratorSerializer.serializer.serializedSize(iter, null, version, update.rowCount());
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
            assert row instanceof BTreeRow;
            ((BTreeRow)row).setValue(column, path, value);
        }
    }
}
