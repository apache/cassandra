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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.CoordinatorBehindException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.db.SerializationHeader.StableHeaderSerializer.STABLE;
import static org.apache.cassandra.db.rows.UnfilteredRowIteratorSerializer.IS_EMPTY;

/**
 * Stores updates made on a partition.
 * <p>
 * A PartitionUpdate object requires that all writes/additions are performed before we
 * try to read the updates (attempts to write to the PartitionUpdate after a read method
 * has been called will result in an exception being thrown). In other words, a Partition
 * is mutable while it's written but becomes immutable as soon as it is read.
 * <p>
 * A typical usage is to create a new update ({@code PartitionUpdate.builder(metadata, key, columns, capacity)})
 * and then add rows and range tombstones through the {@code add()} methods (the partition
 * level deletion time can also be set with {@code addPartitionDeletion()}). However, there
 * is also a few static helper constructor methods for special cases ({@code emptyUpdate()},
 * {@code fullPartitionDelete} and {@code singleRowUpdate}).
 * <p>
 * The implementation of the update is chosen by the {@link Factory} of the table's memtable.
 */
public interface PartitionUpdate extends Partition
{
    PartitionUpdateSerializer serializer = new PartitionUpdateSerializer();

    DeletionInfo deletionInfo();

    /**
     * The number of "operations" contained in the update.
     * <p>
     * This is used by {@code Memtable} to approximate how much work this update does. In practice, this
     * count how many rows are updated and how many ranges are deleted by the partition update.
     *
     * @return the number of "operations" performed by the update.
     */
    int operationCount();

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    int dataSize();

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    long unsharedHeapSize();

    /**
     * The epoch of the table metadata used to serialize this update, if it was received from another node, or the
     * epoch of the metadata it was created with otherwise.
     */
    Epoch serializedAtEpoch();

    boolean canHaveShadowedData();

    /**
     * Validates the data contained in this update.
     *
     * @throws org.apache.cassandra.serializers.MarshalException if some of the data contained in this update is corrupted.
     */
    default void validate()
    {
        for (Row row : rows())
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
    long maxTimestamp();

    /**
     * For an update on a counter table, returns a list containing a {@code CounterMark} for
     * every counter contained in the update.
     *
     * @return a list with counter marks for every counter in this update.
     */
    List<CounterMark> collectCounterMarks();

    default void validateIndexedColumns(ClientState state)
    {
        IndexRegistry.obtain(metadata()).validate(this, state);
    }

    /**
     * Returns a copy of this update where every timestamp for live data is set to {@code newTimestamp} and
     * every deletion timestamp to {@code newTimestamp - 1}.
     * <p>
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
    PartitionUpdate withUpdatedTimestamps(long newTimestamp);

    PartitionUpdate withOnlyPresentColumns();

    static Builder builder(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, int initialRowCapacity)
    {
        return metadata.partitionUpdateFactory().builder(metadata, partitionKey, columns, initialRowCapacity);
    }

    static Builder builder(TableMetadata metadata, ByteBuffer partitionKey, RegularAndStaticColumns columns, int initialRowCapacity)
    {
        return builder(metadata, metadata.partitioner.decorateKey(partitionKey), columns, initialRowCapacity);
    }

    /**
     * Creates a empty immutable partition update.
     *
     * @param metadata the metadata for the created update.
     * @param partitionKey the partition key for the created update.
     *
     * @return the newly created empty (and immutable) update.
     */
    static PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey partitionKey)
    {
        return metadata.partitionUpdateFactory().emptyUpdate(metadata, partitionKey);
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
    static PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey key, Row row)
    {
        return metadata.partitionUpdateFactory().singleRowUpdate(metadata, key, row);
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
    static PartitionUpdate singleRowUpdate(TableMetadata metadata, ByteBuffer key, Row row)
    {
        return singleRowUpdate(metadata, metadata.partitioner.decorateKey(key), row);
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
    static PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, long nowInSec)
    {
        return metadata.partitionUpdateFactory().fullPartitionDelete(metadata, key, timestamp, nowInSec);
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
    static PartitionUpdate fullPartitionDelete(TableMetadata metadata, ByteBuffer key, long timestamp, long nowInSec)
    {
        return fullPartitionDelete(metadata, metadata.partitioner.decorateKey(key), timestamp, nowInSec);
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
    static PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter)
    {
        return iterator.metadata().partitionUpdateFactory().fromIterator(iterator, filter);
    }

    /**
     * Merges the provided updates, yielding a new update that incorporates all those updates.
     *
     * @param updates the collection of updates to merge. This shouldn't be empty.
     *
     * @return a partition update that include (merge) all the updates from {@code updates}.
     */
    static PartitionUpdate merge(List<? extends PartitionUpdate> updates)
    {
        assert !updates.isEmpty();
        return updates.get(0).metadata().partitionUpdateFactory().merge(updates);
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
    static SimpleBuilder simpleBuilder(TableMetadata metadata, Object... partitionKeyValues)
    {
        // Here we dereference the current version of the supplied TableMetadata. The reason for this is that in some
        // places we still reference static TableMetadata instances.
        // For instance, TraceKeyspace contains Sessions & Events static members which are created at startup when the
        // current epoch is Epoch.EMPTY. These are used to construct mutations when tracing is enabled and when the
        // mutations are serialised and sent between replica & coordinator the epoch comparisons in PartitionUpdate
        // deserializer trigger an IncompatibleSchemaException.
        // TODO ultimately remove the use of static TableMetadata instances in System/Tracing/Auth keyspaces.
        return new SimpleBuilders.PartitionUpdateBuilder(metadata.ref.get(), partitionKeyValues);
    }

    /**
     * Deserialize a partition update from a provided byte buffer.
     *
     * @param bytes the byte buffer that contains the serialized update.
     * @param version the version with which the update is serialized.
     *
     * @return the deserialized update or {@code null} if {@code bytes == null}.
     */
    static PartitionUpdate fromBytes(ByteBuffer bytes, int version)
    {
        if (bytes == null)
            return null;

        try
        {
            return serializer.deserialize(new DataInputBuffer(bytes, true),
                                          version,
                                          DeserializationHelper.Flag.LOCAL);
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
    static ByteBuffer toBytes(PartitionUpdate update, int version)
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
     *
     * @return the estimated number of rows affected by this mutation
     */
    default int affectedRowCount()
    {
        // If there is a partition-level deletion, we intend to delete at least one row.
        if (!partitionLevelDeletion().isLive())
            return 1;

        int count = 0;

        // Each range delete should correspond to at least one intended row deletion.
        if (deletionInfo().hasRanges())
            count += deletionInfo().rangeCount();

        count += rowCount();

        if (!staticRow().isEmpty())
            count++;

        return count;
    }

    /**
     *
     * @return the estimated total number of columns that either have live data or are covered by a delete
     */
    default int affectedColumnCount()
    {
        // If there is a partition-level deletion, we intend to delete at least the columns of one row.
        if (!partitionLevelDeletion().isLive())
            return metadata().regularAndStaticColumns().size();

        int count = 0;

        // Each range delete should correspond to at least one intended row deletion, and with it, its regular columns.
        if (deletionInfo().hasRanges())
            count += deletionInfo().rangeCount() * metadata().regularColumns().size();

        for (Row row : rows())
        {
            if (row.deletion().isLive())
                // If the row is live, this will include simple tombstones as well as cells w/ actual data.
                count += row.columnCount();
            else
                // We have a row deletion, so account for the columns that might be deleted.
                count += metadata().regularColumns().size();
        }

        if (!staticRow().isEmpty())
            count += staticRow().columnCount();

        return count;
    }

    /**
     * Interface for building partition updates geared towards human.
     * <p>
     * This should generally not be used when performance matters too much, but provides a more convenient interface to
     * build an update than using the class constructor when performance is not of the utmost importance.
     */
    interface SimpleBuilder
    {
        /**
         * The metadata of the table this is a builder on.
         */
        TableMetadata metadata();

        /**
         * Sets the timestamp to use for the following additions to this builder or any derived (row) builder.
         *
         * @param timestamp the timestamp to use for following additions. If that timestamp hasn't been set, the current
         * time in microseconds will be used.
         * @return this builder.
         */
        SimpleBuilder timestamp(long timestamp);

        /**
         * Sets the ttl to use for the following additions to this builder or any derived (row) builder.
         *
         * @param ttl the ttl to use for following additions. If that ttl hasn't been set, no ttl will be used.
         * @return this builder.
         */
        SimpleBuilder ttl(int ttl);

        /**
         * Sets the current time to use for the following additions to this builder or any derived (row) builder.
         *
         * @param nowInSec the current time to use for following additions. If the current time hasn't been set, the current
         * time in seconds will be used.
         * @return this builder.
         */
        SimpleBuilder nowInSec(long nowInSec);

        /**
         * Adds the row identifier by the provided clustering and return a builder for that row.
         *
         * @param clusteringValues the value for the clustering columns of the row to add to this build. There may be no
         * values if either the table has no clustering column, or if you want to edit the static row. Note that as a
         * shortcut it is also allowed to pass a {@code Clustering} object directly, in which case that should be the
         * only argument.
         * @return a builder for the row identified by {@code clusteringValues}.
         */
        Row.SimpleBuilder row(Object... clusteringValues);

        /**
         * Deletes the partition identified by this builder (using a partition level deletion).
         *
         * @return this builder.
         */
        SimpleBuilder delete();

        /**
         * Adds a new range tombstone to this update, returning a builder for that range.
         *
         * @return the range tombstone builder for the newly added range.
         */
        RangeTombstoneBuilder addRangeTombstone();

        /**
         * Adds a new range tombstone to this update
         *
         * @return this builder
         */
        SimpleBuilder addRangeTombstone(RangeTombstone rt);

        /**
         * Build the update represented by this builder.
         *
         * @return the built update.
         */
        PartitionUpdate build();

        /**
         * As shortcut for {@code new Mutation(build())}.
         *
         * @return the built update, wrapped in a {@code Mutation}.
         */
        Mutation buildAsMutation();

        /**
         * Interface to build range tombstone.
         *
         * By default, if no other methods are called, the represented range is inclusive of both start and end and
         * includes everything (its start is {@code BOTTOM} and it's end is {@code TOP}).
         */
        interface RangeTombstoneBuilder
        {
            /**
             * Sets the start for the built range using the provided values.
             *
             * @param values the value for the start of the range. They act like the {@code clusteringValues} argument
             * of the {@link SimpleBuilder#row(Object...)} method, except that it doesn't have to be a full
             * clustering, it can only be a prefix.
             * @return this builder.
             */
            RangeTombstoneBuilder start(Object... values);

            /**
             * Sets the end for the built range using the provided values.
             *
             * @param values the value for the end of the range. They act like the {@code clusteringValues} argument
             * of the {@link SimpleBuilder#row(Object...)} method, except that it doesn't have to be a full
             * clustering, it can only be a prefix.
             * @return this builder.
             */
            RangeTombstoneBuilder end(Object... values);

            /**
             * Sets the start of this range as inclusive.
             * <p>
             * This is the default and don't need to be called, but can for explicitness.
             *
             * @return this builder.
             */
            RangeTombstoneBuilder inclStart();

            /**
             * Sets the start of this range as exclusive.
             *
             * @return this builder.
             */
            RangeTombstoneBuilder exclStart();

            /**
             * Sets the end of this range as inclusive.
             * <p>
             * This is the default and don't need to be called, but can for explicitness.
             *
             * @return this builder.
             */
            RangeTombstoneBuilder inclEnd();

            /**
             * Sets the end of this range as exclusive.
             *
             * @return this builder.
             */
            RangeTombstoneBuilder exclEnd();
        }
    }

    class PartitionUpdateSerializer
    {
        public void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
        {
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                assert !iter.isReverseOrder();

                TableMetadata metadata = update.metadata();
                metadata.id.serialize(out);
                if (version >= MessagingService.VERSION_60)
                    Epoch.serializer.serialize(metadata.epoch != null ? metadata.epoch : Epoch.EMPTY, out);
                UnfilteredRowIteratorSerializer.serializer.serialize(iter, null, out, version, update.rowCount());
            }
        }

        public void serializeWithoutKey(PartitionUpdate update, TableMetadatas tables, DataOutputPlus out, int version) throws IOException
        {
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                TableMetadata metadata = update.metadata();
                tables.serialize(metadata, out);
                Epoch.serializer.serialize(metadata.epoch, out);
                SerializationHeader header = new SerializationHeader(false, metadata, iter.columns(), iter.stats());
                UnfilteredRowIteratorSerializer.serializer.serializeWithoutKey(iter, header, out, version, update.rowCount(), STABLE, null);
            }
        }

        public PartitionUpdate deserialize(DataInputPlus in, int version, DeserializationHelper.Flag flag) throws IOException
        {
            TableId tableId = TableId.deserialize(in);
            Epoch remoteVersion = null;
            if (version >= MessagingService.VERSION_60)
                remoteVersion = Epoch.serializer.deserialize(in);
            TableMetadata tableMetadata;
            try
            {
                tableMetadata = Schema.instance.getExistingTableMetadata(tableId);
            }
            catch (UnknownTableException e)
            {
                ClusterMetadata metadata = ClusterMetadata.current();
                Epoch localCurrentEpoch = metadata.epoch;
                if (remoteVersion != null && localCurrentEpoch.isAfter(remoteVersion))
                {
                    TCMMetrics.instance.coordinatorBehindSchema.mark();
                    throw new CoordinatorBehindException(e.getMessage(), e);
                }
                throw e;
            }
            UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeader(tableMetadata, null, in, version, flag);
            return deserialize(header, remoteVersion, tableMetadata, in, version, flag);
        }

        public PartitionUpdate deserialize(PartitionKey key, TableMetadatas tables, DataInputPlus in, int version, DeserializationHelper.Flag flag) throws IOException
        {
            TableMetadata tableMetadata = tables.deserialize(in);
            Epoch remoteVersion = Epoch.serializer.deserialize(in);
            UnfilteredRowIteratorSerializer.Header header = UnfilteredRowIteratorSerializer.serializer.deserializeHeaderWithoutKey(tableMetadata, key.partitionKey(), in, version, flag, STABLE, null);
            return deserialize(header, remoteVersion, tableMetadata, in, version, flag);
        }

        private PartitionUpdate deserialize(UnfilteredRowIteratorSerializer.Header header, Epoch remoteVersion, TableMetadata tableMetadata, DataInputPlus in, int version, DeserializationHelper.Flag flag) throws IOException
        {
            Factory factory = tableMetadata.partitionUpdateFactory();
            if (header.isEmpty)
                return factory.emptyUpdate(tableMetadata, header.key);

            assert !header.isReversed;
            assert header.rowEstimate >= 0;

            try (UnfilteredRowIterator partition = UnfilteredRowIteratorSerializer.serializer.deserialize(in, version, tableMetadata, flag, header))
            {
                return factory.fromDeserialized(partition, remoteVersion);
            }
        }

        public static boolean isEmpty(ByteBuffer in, DeserializationHelper.Flag flag, DecoratedKey key, int version) throws IOException
        {
            int position = in.position();
            position += 16; // CFMetaData.serializer.deserialize(in, version);
            if (position >= in.limit())
                throw new EOFException();

            if (version >= MessagingService.VERSION_60)
            {
                long epoch = VIntCoding.getUnsignedVInt(in, position);
                position += VIntCoding.computeUnsignedVIntSize(epoch);
            }

            // DecoratedKey key = metadata.decorateKey(ByteBufferUtil.readWithVIntLength(in));
            int keyLength = VIntCoding.getUnsignedVInt32(in, position);
            position += keyLength + VIntCoding.computeUnsignedVIntSize(keyLength);
            if (position >= in.limit())
                throw new EOFException();
            int flags = in.get(position) & 0xff;
            return (flags & IS_EMPTY) != 0;
        }

        public long serializedSize(PartitionUpdate update, int version)
        {
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                TableMetadata metadata = update.metadata();
                return metadata.id.serializedSize()
                     + (version >= MessagingService.VERSION_60 ? Epoch.serializer.serializedSize(metadata.epoch) : 0)
                     + UnfilteredRowIteratorSerializer.serializer.serializedSize(iter, null, version, update.rowCount());
            }
        }

        public long serializedSizeWithoutKey(PartitionUpdate update, TableMetadatas tables, int version)
        {
            try (UnfilteredRowIterator iter = update.unfilteredIterator())
            {
                TableMetadata metadata = update.metadata();
                long size = tables.serializedSize(metadata);
                size += Epoch.serializer.serializedSize(metadata.epoch);

                SerializationHeader header = new SerializationHeader(false, metadata, iter.columns(), iter.stats());
                return size + UnfilteredRowIteratorSerializer.serializer.serializedSizeWithoutKey(iter, header, version, update.rowCount(), STABLE, null);
            }
        }
    }

    /**
     * A counter mark is basically a pointer to a counter update inside this partition update. That pointer allows
     * us to update the counter value based on the pre-existing value read during the read-before-write that counters
     * do. See {@link CounterMutation} to understand how this is used.
     */
    class CounterMark
    {
        private final Row row;
        private final ColumnMetadata column;
        private final CellPath path;

        protected CounterMark(Row row, ColumnMetadata column, CellPath path)
        {
            this.row = row;
            this.column = column;
            this.path = path;
        }

        public Clustering<?> clustering()
        {
            return row.clustering();
        }

        public ColumnMetadata column()
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
                 ? row.getCell(column).buffer()
                 : row.getCell(column, path).buffer();
        }

        public void setValue(ByteBuffer value)
        {
            // This is a bit of a giant hack as this is the only place where we mutate a Row object. This makes it more efficient
            // for counters however and this won't be needed post-#6506 so that's probably fine.
            assert row instanceof BTreeRow;
            ((BTreeRow)row).setValue(column, path, value);
        }
    }

    /**
     * Builder for partition updates.
     */
    interface Builder
    {
        /**
         * Adds a row to this update.
         * <p>
         * There is no particular assumption made on the order of row added to a partition update. It is further
         * allowed to add the same row (more precisely, multiple row objects for the same clustering).
         * <p>
         * Note however that the columns contained in the added row must be a subset of the columns used when
         * creating this update.
         *
         * @param row the row to add.
         */
        void add(Row row);

        void addPartitionDeletion(DeletionTime deletionTime);

        void add(RangeTombstone range);

        DecoratedKey partitionKey();

        TableMetadata metadata();

        PartitionUpdate build();

        RegularAndStaticColumns columns();

        DeletionTime partitionLevelDeletion();
    }

    /**
     * Factory for partition updates, provided by the memtable implementation of a table
     * (see {@link org.apache.cassandra.db.memtable.Memtable.Factory#partitionUpdateFactory}).
     */
    interface Factory
    {
        Builder builder(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, int initialRowCapacity);

        PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey partitionKey);

        PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey key, Row row);

        PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, long nowInSec);

        /**
         * Turns the given iterator into an update.
         * <p>
         * Warning: this method does not close the provided iterator, it is up to the caller to close it.
         */
        PartitionUpdate fromIterator(UnfilteredRowIterator iterator);

        /**
         * Turns the given iterator into an update, using only the data selected by the given column filter.
         * <p>
         * Warning: this method does not close the provided iterator, it is up to the caller to close it.
         */
        PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter);

        /**
         * Turns the given deserialized partition into an update, recording the epoch at which it was serialized.
         * <p>
         * Warning: this method does not close the provided iterator, it is up to the caller to close it.
         */
        PartitionUpdate fromDeserialized(UnfilteredRowIterator iterator, Epoch serializedAtEpoch);

        default PartitionUpdate merge(List<? extends PartitionUpdate> updates)
        {
            assert !updates.isEmpty();
            final int size = updates.size();

            if (size == 1)
                return Iterables.getOnlyElement(updates);

            List<UnfilteredRowIterator> asIterators = Lists.transform(updates, Partition::unfilteredIterator);
            try (UnfilteredRowIterator merge = UnfilteredRowIterators.merge(asIterators))
            {
                return fromIterator(merge);
            }
        }
    }
}
