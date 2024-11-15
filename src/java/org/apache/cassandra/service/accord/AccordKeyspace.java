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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.impl.TimestampsForKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Row.Deletion;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.LocalCompositePrefixPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.accord.RouteIndex;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.LocalVersionedSerializer;
import org.apache.cassandra.io.MessageVersionProvider;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.service.accord.AccordConfigurationService.SyncStatus;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.serializers.AccordRoutingKeyByteSource;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock.Global;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSet;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.checkState;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.partitions.PartitionUpdate.singleRowUpdate;
import static org.apache.cassandra.db.rows.BTreeRow.singleCellRow;
import static org.apache.cassandra.db.rows.BufferCell.live;
import static org.apache.cassandra.db.rows.BufferCell.tombstone;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.serializers.AccordRoutingKeyByteSource.currentVersion;
import static org.apache.cassandra.service.accord.serializers.KeySerializers.blobMapToRanges;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class AccordKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(AccordKeyspace.class);

    public static final String JOURNAL = "journal";
    public static final String COMMANDS = "commands";
    public static final String TIMESTAMPS_FOR_KEY = "timestamps_for_key";
    public static final String COMMANDS_FOR_KEY = "commands_for_key";
    public static final String TOPOLOGIES = "topologies";
    public static final String EPOCH_METADATA = "epoch_metadata";

    public static final Set<String> TABLE_NAMES = ImmutableSet.of(COMMANDS, TIMESTAMPS_FOR_KEY, COMMANDS_FOR_KEY,
                                                                  TOPOLOGIES, EPOCH_METADATA,
                                                                  JOURNAL);

    // TODO (desired): implement a custom type so we can get correct sort order
    private static final TupleType TIMESTAMP_TYPE = new TupleType(Lists.newArrayList(LongType.instance, LongType.instance, Int32Type.instance));
    private static final String TIMESTAMP_TUPLE = TIMESTAMP_TYPE.asCQL3Type().toString();
    private static final TupleType KEY_TYPE = new TupleType(Arrays.asList(UUIDType.instance, BytesType.instance));
    private static final String KEY_TUPLE = KEY_TYPE.asCQL3Type().toString();

    private static final ClusteringIndexFilter FULL_PARTITION = new ClusteringIndexNamesFilter(BTreeSet.of(new ClusteringComparator(), Clustering.EMPTY), false);

    private static final ConcurrentMap<TableId, AccordRoutingKeyByteSource.Serializer> TABLE_SERIALIZERS = new ConcurrentHashMap<>();

    private static AccordRoutingKeyByteSource.Serializer getRoutingKeySerializer(AccordRoutingKey key)
    {
        return TABLE_SERIALIZERS.computeIfAbsent(key.table(), ignore -> {
            IPartitioner partitioner;
            if (key.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.TOKEN)
                partitioner = key.asTokenKey().token().getPartitioner();
            else
                partitioner = SchemaHolder.schema.getTablePartitioner(key.table());
            return AccordRoutingKeyByteSource.variableLength(partitioner);
        });
    }

    // Schema needs all system keyspace, and this is a system keyspace!  So can not touch schema in init
    private static class SchemaHolder
    {
        private static SchemaProvider schema = Objects.requireNonNull(Schema.instance);
    }

    private enum TokenType
    {
        Murmur3((byte) 1),
        ByteOrdered((byte) 2),
        ;

        private final byte value;

        TokenType(byte b)
        {
            this.value = b;
        }

        static TokenType valueOf(Token token)
        {
            if (token instanceof Murmur3Partitioner.LongToken)
                return Murmur3;
            if (token instanceof ByteOrderedPartitioner.BytesToken)
                return ByteOrdered;
            throw new IllegalArgumentException("Unexpected token type: " + token.getClass());
        }
    }

    public static TableMetadata journalMetadata(String tableName)
    {
        return parse(tableName,
                     "accord journal",
                     "CREATE TABLE %s ("
                     + "store_id int,"
                     + "type tinyint,"
                     + "id blob,"
                     + "descriptor bigint,"
                     + "offset int,"
                     + "user_version int,"
                     + "record blob,"
                     + "PRIMARY KEY((store_id, type, id), descriptor, offset)"
                     + ") WITH CLUSTERING ORDER BY (descriptor DESC, offset DESC)" +
                     " WITH compression = {'class':'NoopCompressor'};")
               .compaction(CompactionParams.lcs(emptyMap()))
               .bloomFilterFpChance(0.01)
               .partitioner(new LocalPartitioner(BytesType.instance))
               .build();
    }

    public static final TableMetadata Journal = journalMetadata(JOURNAL);

    // TODO: store timestamps as blobs (confirm there are no negative numbers, or offset)
    public static final TableMetadata Commands =
        parse(COMMANDS,
              "accord commands",
              "CREATE TABLE %s ("
              + "store_id int,"
              + "domain int," // this is stored as part of txn_id, used currently for cheaper scans of the table
              + format("txn_id %s,", TIMESTAMP_TUPLE)
              + "status int,"
              + "participants blob,"
              + "durability int,"
              + format("execute_at %s,", TIMESTAMP_TUPLE)
              + "PRIMARY KEY((store_id, domain, txn_id))"
              + ')')
        .partitioner(new LocalPartitioner(CompositeType.getInstance(Int32Type.instance, Int32Type.instance, TIMESTAMP_TYPE)))
        .indexes(Indexes.builder()
                        .add(IndexMetadata.fromSchemaMetadata("route", IndexMetadata.Kind.CUSTOM, ImmutableMap.of("class_name", RouteIndex.class.getCanonicalName(), "target", "participants")))
                        .build())
        .build();

    // TODO: naming is not very clearly distinct from the base serializers
    public static class LocalVersionedSerializers
    {
        static final LocalVersionedSerializer<StoreParticipants> participants = localSerializer(CommandSerializers.participants);

        private static <T> LocalVersionedSerializer<T> localSerializer(IVersionedSerializer<T> serializer)
        {
            return new LocalVersionedSerializer<>(AccordSerializerVersion.CURRENT, AccordSerializerVersion.serializer, serializer);
        }
    }

    private static ColumnMetadata getColumn(TableMetadata metadata, String name)
    {
        ColumnMetadata column = metadata.getColumn(new ColumnIdentifier(name, true));
        if (column == null)
            throw new IllegalArgumentException(format("Unknown column %s for %s.%s", name, metadata.keyspace, metadata.name));
        return column;
    }

    public static class CommandsColumns
    {
        static final ClusteringComparator keyComparator = Commands.partitionKeyAsClusteringComparator();
        static final CompositeType partitionKeyType = (CompositeType) Commands.partitionKeyType;
        public static final ColumnMetadata txn_id = getColumn(Commands, "txn_id");
        public static final ColumnMetadata store_id = getColumn(Commands, "store_id");
        public static final ColumnMetadata status = getColumn(Commands, "status");
        public static final ColumnMetadata participants = getColumn(Commands, "participants");
        public static final ColumnMetadata durability = getColumn(Commands, "durability");
        public static final ColumnMetadata execute_at = getColumn(Commands, "execute_at");

        public static final ColumnMetadata[] TRUNCATE_FIELDS = new ColumnMetadata[] { durability, execute_at, participants, status };
        public static final ColumnMetadata[] SAVE_STATUS_ONLY_FIELDS = new ColumnMetadata[] { status };

        static
        {
            for (int i = 1 ; i < TRUNCATE_FIELDS.length ; ++i)
                Invariants.checkState(TRUNCATE_FIELDS[i - 1].compareTo(TRUNCATE_FIELDS[i]) < 0);
        }
    }

    public static class CommandRows extends CommandsColumns
    {
        public static ByteBuffer[] splitPartitionKey(DecoratedKey key)
        {
            return partitionKeyType.split(key.getKey());
        }

        public static int getStoreId(ByteBuffer[] partitionKeyComponents)
        {
            return Int32Type.instance.compose(partitionKeyComponents[store_id.position()]);
        }

        public static int getStoreId(DecoratedKey pk)
        {
            var array = splitPartitionKey(pk);
            return getStoreId(array);
        }

        public static TxnId getTxnId(ByteBuffer[] partitionKeyComponents)
        {
            return deserializeTimestampOrNull(partitionKeyComponents[txn_id.position()], ByteBufferAccessor.instance, TxnId::fromBits);
        }

        @Nullable
        public static Timestamp getExecuteAt(Row row)
        {
            Cell cell = row.getCell(execute_at);
            if (cell == null)
                return null;
            return deserializeTimestampOrNull(cell.value(), cell.accessor(), Timestamp::fromBits);
        }

        @Nullable
        public static SaveStatus getStatus(Row row)
        {
            Cell cell = row.getCell(status);
            if (cell == null)
                return null;
            int ordinal = cell.accessor().getInt(cell.value(), 0);
            return CommandSerializers.saveStatus.forOrdinal(ordinal);
        }

        @Nullable
        public static Status.Durability getDurability(Row row)
        {
            Cell cell = row.getCell(durability);
            if (cell == null)
                return null;
            int ordinal = cell.accessor().getInt(cell.value(), 0);
            return CommandSerializers.durability.forOrdinal(ordinal);
        }

        @Nullable
        public static Route<?> getRoute(Row row)
        {
            Cell<?> cell = row.getCell(participants);
            StoreParticipants participants = deserializeParticipantsOrNull(cell);
            return participants == null ? null : participants.route();
        }

        private static Object[] truncatedApplyLeaf(long newTimestamp, SaveStatus newSaveStatus, @Nullable Cell<?> durabilityCell, @Nullable Cell<?> executeAtCell, @Nullable Cell<?> participantsCell, boolean updateTimestamps)
        {
            int count = 1 + (durabilityCell != null ? 1 : 0) + (executeAtCell != null ? 1 : 0) + (participantsCell != null ? 1 : 0);
            Object[] newLeaf = BTree.unsafeAllocateNonEmptyLeaf(count);
            int colIndex = 0;
            if (durabilityCell != null)
            {
                checkArgument(durabilityCell.column() == CommandsColumns.durability);
                newLeaf[colIndex++] = updateTimestamps ? durabilityCell.withUpdatedTimestamp(newTimestamp) : durabilityCell;
            }
            if (executeAtCell != null)
            {
                checkArgument(executeAtCell.column() == CommandsColumns.execute_at);
                newLeaf[colIndex++] = updateTimestamps ? executeAtCell.withUpdatedTimestamp(newTimestamp) : executeAtCell;
            }
            if (participantsCell != null)
            {
                checkArgument(participantsCell.column() == CommandsColumns.participants);
                newLeaf[colIndex++] = updateTimestamps ? participantsCell.withUpdatedTimestamp(newTimestamp) : participantsCell;
            }
            newLeaf[colIndex] = BufferCell.live(status, newTimestamp, ByteBufferAccessor.instance.valueOf(newSaveStatus.ordinal()));
            return newLeaf;
        }

        private static Object[] expungePartialLeaf(Cell<?> durabilityCell, Cell<?> executeAtCell, Cell<?> participantsCell)
        {
            int count = (durabilityCell != null ? 1 : 0) + (executeAtCell != null ? 1 : 0) + (participantsCell != null ? 1 : 0);
            if (count == 0)
                return null;

            Object[] newLeaf = BTree.unsafeAllocateNonEmptyLeaf(count);
            int colIndex = 0;
            if (durabilityCell != null)
            {
                checkArgument(durabilityCell.column() == CommandsColumns.durability);
                newLeaf[colIndex++] = durabilityCell;
            }
            if (executeAtCell != null)
            {
                checkArgument(executeAtCell.column() == CommandsColumns.execute_at);
                newLeaf[colIndex++] = executeAtCell;
            }
            if (participantsCell != null)
            {
                checkArgument(participantsCell.column() == CommandsColumns.participants);
                newLeaf[colIndex] = participantsCell;
            }
            return newLeaf;
        }

        public static Row saveStatusOnly(SaveStatus newSaveStatus, Row row, long nowInSec)
        {
            long oldTimestamp = row.primaryKeyLivenessInfo().timestamp();
            long newTimestamp = oldTimestamp + 1;

            Object[] newLeaf = saveStatusOnlyLeaf(newTimestamp, newSaveStatus);

            // Including a deletion allows future compactions to drop data before it gets to the purger
            // but it is pretty optional because maybeDropTruncatedCommandColumns will drop the extra columns
            // regardless
            Row.Deletion deletion = new Row.Deletion(DeletionTime.build(oldTimestamp, nowInSec), false);
            return BTreeRow.create(row.clustering(), LivenessInfo.create(newTimestamp, nowInSec), deletion, newLeaf);
        }

        private static Object[] saveStatusOnlyLeaf(long newTimestamp, SaveStatus newSaveStatus)
        {
            Object[] newLeaf = BTree.unsafeAllocateNonEmptyLeaf(SAVE_STATUS_ONLY_FIELDS.length);
            int colIndex = 0;
            // Status always needs to use the new timestamp since we are replacing the existing value
            // All the other columns are being retained unmodified with at most updated timestamps to accomdate deletion
            //noinspection UnusedAssignment
            newLeaf[colIndex++] = BufferCell.live(status, newTimestamp, ByteBufferAccessor.instance.valueOf(newSaveStatus.ordinal()));
            return newLeaf;
        }

        public static Row truncatedApply(SaveStatus newSaveStatus, Row row, long nowInSec, Durability durability, @Nullable Cell<?> durabilityCell, @Nullable Cell<?> executeAtCell, @Nullable Cell<?> participantsCell, boolean withOutcome)
        {
            long oldTimestamp = row.primaryKeyLivenessInfo().timestamp();
            long newTimestamp = oldTimestamp + 1;
            boolean doDeletion = durability == Durability.Universal;

            // We may not have what we need to generate a deletion and include the outcome in the truncated row
            // so need to wait until we can have the outcome to issue the deletion otherwise it would be shadowed and lost
            if (withOutcome)
                doDeletion = false;

            Object[] newLeaf = truncatedApplyLeaf(newTimestamp, newSaveStatus, durabilityCell, executeAtCell, participantsCell, doDeletion);

            // Including a deletion allows future compactions to drop data before it gets to the purger
            // but it is pretty optional because maybeDropTruncatedCommandColumns will drop the extra columns
            // regardless
            Row.Deletion deletion = doDeletion ? new Row.Deletion(DeletionTime.build(oldTimestamp, nowInSec), false) : Deletion.LIVE;
            return BTreeRow.create(row.clustering(), LivenessInfo.create(newTimestamp, nowInSec), deletion, newLeaf);
        }

        public static Row expungePartial(Row row, @Nullable Cell<?> durabilityCell, @Nullable Cell<?> executeAtCell, @Nullable Cell<?> participantsCell)
        {
            Object[] newLeaf = expungePartialLeaf(durabilityCell, executeAtCell, participantsCell);
            return BTreeRow.create(row.clustering(), row.primaryKeyLivenessInfo(), Deletion.LIVE, newLeaf);
        }
    }

    public static final TableMetadata TimestampsForKeys =
        parse(TIMESTAMPS_FOR_KEY,
              "accord timestamps per key",
              "CREATE TABLE %s ("
              + "store_id int, "
              + "routing_key blob, " // can't use "token" as this is restricted word in CQL
              + format("last_executed_timestamp %s, ", TIMESTAMP_TUPLE)
              + "last_executed_micros bigint, "
              + format("last_write_id %s, ", TIMESTAMP_TUPLE)
              + format("last_write_timestamp %s, ", TIMESTAMP_TUPLE)
              + "PRIMARY KEY((store_id, routing_key))"
              + ')')
        .partitioner(new LocalCompositePrefixPartitioner(Int32Type.instance, BytesType.instance))
        .build();

    public static class TimestampsForKeyColumns
    {
        static final ClusteringComparator keyComparator = TimestampsForKeys.partitionKeyAsClusteringComparator();
        static final CompositeType partitionKeyType = (CompositeType) TimestampsForKeys.partitionKeyType;
        static final ColumnMetadata store_id = getColumn(TimestampsForKeys, "store_id");
        static final ColumnMetadata routing_key = getColumn(TimestampsForKeys, "routing_key");
        public static final ColumnMetadata last_executed_timestamp = getColumn(TimestampsForKeys, "last_executed_timestamp");
        public static final ColumnMetadata last_executed_micros = getColumn(TimestampsForKeys, "last_executed_micros");
        public static final ColumnMetadata last_write_timestamp = getColumn(TimestampsForKeys, "last_write_timestamp");
        public static final ColumnMetadata last_write_id = getColumn(TimestampsForKeys, "last_write_id");
        static final Columns columns = Columns.from(Lists.newArrayList(last_executed_timestamp, last_executed_micros, last_write_timestamp));
        static final ColumnFilter allColumns = ColumnFilter.all(TimestampsForKeys);

        static DecoratedKey decorateKey(int storeId, RoutingKey key)
        {
            return TimestampsForKeys.partitioner.decorateKey(makeKey(storeId, key));
        }

        static ByteBuffer makeKey(int storeId, RoutingKey key)
        {
            TokenKey pk = (TokenKey) key;
            return keyComparator.make(storeId, serializeRoutingKey(pk)).serializeAsPartitionKey();
        }

        static ByteBuffer makeKey(int storeId, TimestampsForKey timestamps)
        {
            return makeKey(storeId, timestamps.key());
        }
    }

    public static class TimestampsForKeyRows extends TimestampsForKeyColumns
    {
        public static ByteBuffer[] splitPartitionKey(DecoratedKey key)
        {
            return partitionKeyType.split(key.getKey());
        }

        public static int getStoreId(ByteBuffer[] partitionKeyComponents)
        {
            return Int32Type.instance.compose(partitionKeyComponents[store_id.position()]);
        }

        public static TokenKey getKey(DecoratedKey key)
        {
            return getKey(splitPartitionKey(key));
        }

        public static TokenKey getKey(ByteBuffer[] partitionKeyComponents)
        {
            return (TokenKey) deserializeRoutingKey(partitionKeyComponents[routing_key.position()]);
        }

        @Nullable
        public static Timestamp getLastExecutedTimestamp(Row row)
        {
            Cell cell = row.getCell(last_executed_timestamp);
            if (cell == null)
                return null;
            return deserializeTimestampOrNull(cell.value(), cell.accessor(), Timestamp::fromBits);
        }

        public static long getLastExecutedMicros(Row row)
        {
            Cell cell = row.getCell(last_executed_micros);
            if (cell == null || cell.accessor().isEmpty(cell.value()))
                return Long.MIN_VALUE;
            return cell.accessor().getLong(cell.value(), 0);
        }

        @Nullable
        public static Timestamp getLastWriteTimestamp(Row row)
        {
            Cell cell = row.getCell(last_write_timestamp);
            if (cell == null)
                return null;
            return deserializeTimestampOrNull(cell.value(), cell.accessor(), Timestamp::fromBits);
        }

        public static Row truncateTimestampsForKeyRow(long nowInSec, Row row, Cell lastExecuteMicrosCell, Cell lastExecuteCell, Cell lastWriteCell)
        {
            checkArgument(lastExecuteMicrosCell == null || lastExecuteMicrosCell.column() == last_executed_micros);
            checkArgument(lastExecuteCell == null || lastExecuteCell.column() == last_executed_timestamp);
            checkArgument(lastWriteCell == null || lastWriteCell.column() == last_write_timestamp);

            long timestamp = row.primaryKeyLivenessInfo().timestamp();

            int colCount = 0;
            if (lastExecuteMicrosCell != null)
                colCount++;
            if (lastExecuteCell != null)
                colCount++;
            if (lastWriteCell != null)
                colCount++;

            checkState(columns.size() >= colCount, "CommandsForKeyColumns.static_columns_metadata should include all the columns");
            Object[] newLeaf = BTree.unsafeAllocateNonEmptyLeaf(colCount);
            int colIndex = 0;

            if (lastExecuteMicrosCell != null)
                newLeaf[colIndex++] = lastExecuteMicrosCell;
            if (lastExecuteCell != null)
                newLeaf[colIndex++] = lastExecuteCell;
            if (lastWriteCell != null)
                newLeaf[colIndex] = lastWriteCell;

            return BTreeRow.create(row.clustering(), LivenessInfo.create(timestamp, nowInSec),
                                   Deletion.LIVE, newLeaf);
        }
    }

    private static final LocalCompositePrefixPartitioner CFKPartitioner = new LocalCompositePrefixPartitioner(Int32Type.instance, UUIDType.instance, BytesType.instance);
    public static final TableMetadata CommandsForKeys = commandsForKeysTable(COMMANDS_FOR_KEY);

    private static TableMetadata commandsForKeysTable(String tableName)
    {
        return parse(tableName,
              "accord commands per key",
              "CREATE TABLE %s ("
              + "store_id int, "
              + "table_id uuid, "
              + "key_token blob, " // can't use "token" as this is restricted word in CQL
              + "data blob, "
              + "PRIMARY KEY((store_id, table_id, key_token))"
              + ')'
               + " WITH compression = {'class':'NoopCompressor'};")
        .partitioner(CFKPartitioner)
        .compaction(CompactionParams.lcs(emptyMap()))
        .bloomFilterFpChance(0.01)
        .build();
    }

    public static class CommandsForKeyAccessor
    {
        final TableMetadata table;
        final ClusteringComparator keyComparator;
        final CompositeType partitionKeyType;
        final ColumnFilter allColumns;
        final ColumnMetadata store_id;
        final ColumnMetadata table_id;
        final ColumnMetadata key_token;
        final ColumnMetadata data;

        final RegularAndStaticColumns columns;

        public CommandsForKeyAccessor(TableMetadata table)
        {
            this.table = table;
            this.keyComparator = table.partitionKeyAsClusteringComparator();
            this.partitionKeyType = (CompositeType) table.partitionKeyType;
            this.allColumns = ColumnFilter.all(table);
            this.store_id = getColumn(table, "store_id");
            this.table_id = getColumn(table, "table_id");
            this.key_token = getColumn(table, "key_token");
            this.data = getColumn(table, "data");
            this.columns = new RegularAndStaticColumns(Columns.NONE, Columns.from(Lists.newArrayList(data)));
        }

        public ByteBuffer[] splitPartitionKey(DecoratedKey key)
        {
            return partitionKeyType.split(key.getKey());
        }

        public int getStoreId(ByteBuffer[] partitionKeyComponents)
        {
            return Int32Type.instance.compose(partitionKeyComponents[store_id.position()]);
        }

        public TableId getTableId(ByteBuffer[] partitionKeyComponents)
        {
            return TableId.fromUUID(UUIDType.instance.compose(partitionKeyComponents[table_id.position()]));
        }

        public TokenKey getKey(DecoratedKey key)
        {
            return getKey(splitPartitionKey(key));
        }

        public TokenKey getKey(ByteBuffer[] partitionKeyComponents)
        {
            TableId tableId = TableId.fromUUID(UUIDSerializer.instance.deserialize(partitionKeyComponents[table_id.position()]));
            return deserializeTokenKeySeparateTable(tableId, partitionKeyComponents[key_token.position()]);
        }

        public CommandsForKey getCommandsForKey(TokenKey key, Row row)
        {
            Cell<?> cell = row.getCell(data);
            if (cell == null)
                return null;

            return CommandsForKeySerializer.fromBytes(key, cell.buffer());
        }

        @VisibleForTesting
        public ByteBuffer serializeKeyNoTable(AccordRoutingKey key)
        {
            byte[] bytes = getRoutingKeySerializer(key).serializeNoTable(key);
            return ByteBuffer.wrap(bytes);
        }

        // TODO (expected): garbage-free filtering, reusing encoding
        public Row withoutRedundantCommands(TokenKey key, Row row, RedundantBefore.Entry redundantBefore)
        {
            Invariants.checkState(row.columnCount() == 1);
            Cell<?> cell = row.getCell(data);
            if (cell == null)
                return row;

            CommandsForKey current = CommandsForKeySerializer.fromBytes(key, cell.buffer());
            if (current == null)
                return null;

            CommandsForKey updated = current.withRedundantBeforeAtLeast(redundantBefore.shardRedundantBefore());
            if (current == updated)
                return row;

            if (updated.size() == 0)
                return null;

            ByteBuffer buffer = CommandsForKeySerializer.toBytesWithoutKey(updated);
            return BTreeRow.singleCellRow(Clustering.EMPTY, BufferCell.live(data, cell.timestamp(), buffer));
        }

        public LocalCompositePrefixPartitioner.AbstractCompositePrefixToken getPrefixToken(int commandStore, AccordRoutingKey key)
        {
            if (key.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.TOKEN)
            {
                ByteBuffer tokenBytes = ByteBuffer.wrap(getRoutingKeySerializer(key).serializeNoTable(key));
                return CFKPartitioner.createPrefixToken(commandStore, key.table().asUUID(), tokenBytes);
            }
            else
            {
                return CFKPartitioner.createPrefixToken(commandStore, key.table().asUUID());
            }
        }
    }

    public static final CommandsForKeyAccessor CommandsForKeysAccessor = new CommandsForKeyAccessor(CommandsForKeys);

    public static final TableMetadata Topologies =
        parse(TOPOLOGIES,
              "accord topologies",
              "CREATE TABLE %s (" +
              "epoch bigint primary key, " +
              "sync_state int, " +
              "pending_sync_notify set<int>, " + // nodes that need to be told we're synced
              "remote_sync_complete set<int>, " +  // nodes that have told us they're synced
              "closed map<blob, blob>, " +
              "redundant map<blob, blob>" +
              ')').build();

    public static final TableMetadata EpochMetadata =
        parse(EPOCH_METADATA,
              "global epoch info",
              "CREATE TABLE %s (" +
              "key int primary key, " +
              "min_epoch bigint, " +
              "max_epoch bigint " +
              ')').build();

    private static TableMetadata.Builder parse(String name, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, name), ACCORD_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(ACCORD_KEYSPACE_NAME, name))
                                   .comment(description)
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90));
    }

    private static void flush(TableMetadata table)
    {
        Keyspace.open(table.keyspace).getColumnFamilyStore(table.id).forceBlockingFlush(ColumnFamilyStore.FlushReason.ACCORD);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(ACCORD_KEYSPACE_NAME, KeyspaceParams.local(), tables(), Views.none(), Types.none(), UserFunctions.none());
    }

    public static Tables tables = Tables.of(Commands, TimestampsForKeys, CommandsForKeys, Topologies, EpochMetadata, Journal);
    public static Tables tables()
    {
        return tables;
    }

    public static void truncateAllCaches()
    {
        Keyspace ks = Keyspace.open(ACCORD_KEYSPACE_NAME);
        for (String table : new String[]{ TimestampsForKeys.name, CommandsForKeys.name })
            ks.getColumnFamilyStore(table).truncateBlocking();
    }

    private static <T> ByteBuffer serialize(T obj, LocalVersionedSerializer<T> serializer) throws IOException
    {
        int size = (int) serializer.serializedSize(obj);
        try (DataOutputBuffer out = new DataOutputBuffer(size))
        {
            serializer.serialize(obj, out);
            ByteBuffer bb = out.buffer();
            assert size == bb.limit() : format("Expected to write %d but wrote %d", size, bb.limit());
            return bb;
        }
    }

    private static <T> ByteBuffer serializeOrNull(T obj, LocalVersionedSerializer<T> serializer) throws IOException
    {
        return obj != null ? serialize(obj, serializer) : EMPTY_BYTE_BUFFER;
    }

    private static <T> T deserialize(ByteBuffer bytes, LocalVersionedSerializer<T> serializer) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            return serializer.deserialize(in);
        }
    }

    private interface SerializeFunction<V>
    {
        ByteBuffer apply(V v) throws IOException;
    }

    private static <C, V> boolean valueModified(Function<C, V> get, C original, C current)
    {
        V prev = original != null ? get.apply(original) : null;
        V value = get.apply(current);

        return prev != value;
    }

    private static <C, V> void addCellIfModified(ColumnMetadata column, Function<C, V> get, SerializeFunction<V> serialize, Row.Builder builder, long timestampMicros, int nowInSeconds, C original, C current) throws IOException
    {
        if (valueModified(get, original, current))
        {
            V newValue = get.apply(current);
            if (newValue == null) builder.addCell(tombstone(column, timestampMicros, nowInSeconds));
            else builder.addCell(live(column, timestampMicros, serialize.apply(newValue)));
        }
    }

    private static <C, V> void addCell(ColumnMetadata column, Function<C, V> get, SerializeFunction<V> serialize, Row.Builder builder, long timestampMicros, int nowInSeconds, C current) throws IOException
    {
        V newValue = get.apply(current);
        if (newValue == null) builder.addCell(tombstone(column, timestampMicros, nowInSeconds));
        else builder.addCell(live(column, timestampMicros, serialize.apply(newValue)));
    }

    private static <C extends Command, V> void addCell(ColumnMetadata column, Function<C, V> get, LocalVersionedSerializer<V> serializer, Row.Builder builder, long timestampMicros, int nowInSeconds, C command) throws IOException
    {
        addCell(column, get, v -> serializeOrNull(v, serializer), builder, timestampMicros, nowInSeconds, command);
    }

    private static <C extends Command, V> void addCellIfModified(ColumnMetadata column, Function<C, V> get, LocalVersionedSerializer<V> serializer, Row.Builder builder, long timestampMicros, int nowInSeconds, C original, C command) throws IOException
    {
        addCellIfModified(column, get, v -> serializeOrNull(v, serializer), builder, timestampMicros, nowInSeconds, original, command);
    }

    private static <C extends Command, V extends Enum<V>> void addEnumCellIfModified(ColumnMetadata column, Function<C, V> get, Row.Builder builder, long timestampMicros, int nowInSeconds, C original, C command) throws IOException
    {
        // TODO: convert to byte arrays
        ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;
        addCellIfModified(column, get, v -> accessor.valueOf(v.ordinal()), builder, timestampMicros, nowInSeconds, original, command);
    }

    private static <C extends Command, V extends Enum<V>> void addEnumCell(ColumnMetadata column, Function<C, V> get, Row.Builder builder, long timestampMicros, int nowInSeconds, C command) throws IOException
    {
        // TODO: convert to byte arrays
        ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;
        addCell(column, get, v -> accessor.valueOf(v.ordinal()), builder, timestampMicros, nowInSeconds, command);
    }

    public static Mutation getCommandMutation(AccordCommandStore commandStore, AccordSafeCommand liveCommand, long timestampMicros)
    {
        return getCommandMutation(commandStore.id(), liveCommand.current(), timestampMicros);
    }

    public static Mutation getCommandMutation(int storeId, Command command, long timestampMicros)
    {
        if (command.saveStatus() == SaveStatus.Uninitialised)
            return null;

        try
        {
            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.EMPTY);
            int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestampMicros, nowInSeconds));

            addEnumCell(CommandsColumns.durability, Command::durability, builder, timestampMicros, nowInSeconds, command);
            addCell(CommandsColumns.participants, Command::participants, LocalVersionedSerializers.participants, builder, timestampMicros, nowInSeconds, command);
            addEnumCell(CommandsColumns.status, Command::saveStatus, builder, timestampMicros, nowInSeconds, command);
            addCell(CommandsColumns.execute_at, Command::executeAt, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, command);

            Row row = builder.build();
            if (row.columnCount() == 0)
                return null;

            ByteBuffer key = CommandsColumns.keyComparator.make(storeId,
                                                                command.txnId().domain().ordinal(),
                                                                serializeTimestamp(command.txnId())).serializeAsPartitionKey();
            PartitionUpdate update = singleRowUpdate(Commands, key, row);
            return new Mutation(update);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer serializeToken(Token token)
    {
        return serializeToken(token, ByteBufferAccessor.instance);
    }

    public static ByteBuffer serializeTableId(TableId tableId)
    {
        ByteBuffer buffer = ByteBuffer.allocate(tableId.serializedSize());
        tableId.serialize(buffer, ByteBufferAccessor.instance, 0);
        return buffer;
    }

    private static <V> V serializeToken(Token token, ValueAccessor<V> accessor)
    {
        TokenType type = TokenType.valueOf(token);
        byte[] ordered = token.getPartitioner().getTokenFactory().toOrderedByteArray(token, ByteComparable.Version.OSS50);
        V value = accessor.allocate(ordered.length + 1);
        accessor.putByte(value, 0, type.value);
        ByteArrayAccessor.instance.copyTo(ordered, 0, value, accessor, 1, ordered.length);
        return value;
    }

    public static ByteBuffer serializeTimestamp(Timestamp timestamp)
    {
        return TIMESTAMP_TYPE.pack(bytes(timestamp.msb), bytes(timestamp.lsb), bytes(timestamp.node.id));
    }

    public interface TimestampFactory<T extends Timestamp>
    {
        T create(long msb, long lsb, Node.Id node);
    }

    @Nullable
    public static <T extends Timestamp> T deserializeTimestampOrNull(ByteBuffer bytes, TimestampFactory<T> factory)
    {
        if (bytes == null || ByteBufferAccessor.instance.isEmpty(bytes))
            return null;
        List<ByteBuffer> split = TIMESTAMP_TYPE.unpack(bytes, ByteBufferAccessor.instance);
        return factory.create(split.get(0).getLong(), split.get(1).getLong(), new Node.Id(split.get(2).getInt()));
    }

    public static <V> Timestamp deserializeTimestampOrNull(Cell<V> cell)
    {
        if (cell == null)
            return null;
        ValueAccessor<V> accessor = cell.accessor();
        V value = cell.value();
        if (accessor.isEmpty(value))
            return null;
        List<V> split = TIMESTAMP_TYPE.unpack(value, accessor);
        return Timestamp.fromBits(accessor.getLong(split.get(0), 0), accessor.getLong(split.get(1), 0), new Node.Id(accessor.getInt(split.get(2), 0)));
    }

    public static <V, T extends Timestamp> T deserializeTimestampOrDefault(V value, ValueAccessor<V> accessor, TimestampFactory<T> factory, T defaultVal)
    {
        if (value == null || accessor.isEmpty(value))
            return defaultVal;
        List<V> split = TIMESTAMP_TYPE.unpack(value, accessor);
        return factory.create(accessor.getLong(split.get(0), 0), accessor.getLong(split.get(1), 0), new Node.Id(accessor.getInt(split.get(2), 0)));
    }

    public static <V, T extends Timestamp> T deserializeTimestampOrNull(V value, ValueAccessor<V> accessor, TimestampFactory<T> factory)
    {
        if (value == null || accessor.isEmpty(value))
            return null;
        List<V> split = TIMESTAMP_TYPE.unpack(value, accessor);
        return factory.create(accessor.getLong(split.get(0), 0), accessor.getLong(split.get(1), 0), new Node.Id(accessor.getInt(split.get(2), 0)));
    }

    private static <T extends Timestamp> T deserializeTimestampOrNull(UntypedResultSet.Row row, String name, TimestampFactory<T> factory)
    {
        return deserializeTimestampOrNull(row.getBlob(name), factory);
    }

    private static <T extends Timestamp> T deserializeTimestampOrDefault(UntypedResultSet.Row row, String name, TimestampFactory<T> factory, T defaultVal)
    {
        return deserializeTimestampOrDefault(row.getBlob(name), ByteBufferAccessor.instance, factory, defaultVal);
    }

    public static Durability deserializeDurabilityOrNull(Cell cell)
    {
        return cell == null ? null : CommandSerializers.durability.forOrdinal(cell.accessor().getInt(cell.value(), 0));
    }

    public static SaveStatus deserializeSaveStatusOrNull(Cell cell)
    {
        return cell == null ? null : CommandSerializers.saveStatus.forOrdinal(cell.accessor().getInt(cell.value(), 0));
    }

    @VisibleForTesting
    public static UntypedResultSet loadCommandRow(CommandStore commandStore, TxnId txnId)
    {
        String cql = "SELECT * FROM " + ACCORD_KEYSPACE_NAME + '.' + COMMANDS + ' ' +
                     "WHERE store_id = ? " +
                     "AND domain = ? " +
                     "AND txn_id=(?, ?, ?)";

        return executeInternal(cql,
                               commandStore.id(),
                               txnId.domain().ordinal(),
                               txnId.msb, txnId.lsb, txnId.node.id);
    }

    /**
     * Calculates token bounds based on key prefixes.
     */
    public static void findAllKeysBetween(int commandStore,
                                          AccordRoutingKey start, boolean startInclusive,
                                          AccordRoutingKey end, boolean endInclusive,
                                          Consumer<TokenKey> consumer)
    {

        Token startToken = CommandsForKeysAccessor.getPrefixToken(commandStore, start);
        Token endToken = CommandsForKeysAccessor.getPrefixToken(commandStore, end);

        if (start instanceof AccordRoutingKey.SentinelKey)
            startInclusive = true;
        if (end instanceof AccordRoutingKey.SentinelKey)
            endInclusive = true;

        PartitionPosition startPosition = startInclusive ? startToken.minKeyBound() : startToken.maxKeyBound();
        PartitionPosition endPosition = endInclusive ? endToken.maxKeyBound() : endToken.minKeyBound();
        AbstractBounds<PartitionPosition> bounds;
        if (startInclusive && endInclusive)
            bounds = new Bounds<>(startPosition, endPosition);
        else if (endInclusive)
            bounds = new Range<>(startPosition, endPosition);
        else if (startInclusive)
            bounds = new IncludingExcludingBounds<>(startPosition, endPosition);
        else
            bounds = new ExcludingBounds<>(startPosition, endPosition);

        ColumnFamilyStore baseCfs = AccordColumnFamilyStores.commandsForKey;
        try (OpOrder.Group baseOp = baseCfs.readOrdering.start();
             WriteContext writeContext = baseCfs.keyspace.getWriteHandler().createContextForRead();
             CloseableIterator<DecoratedKey> iter = LocalCompositePrefixPartitioner.keyIterator(CommandsForKeys, bounds))
        {
            // Need the second try to handle callback errors vs read errors.
            // Callback will see the read errors, but if the callback fails the outer try will see those errors
            while (iter.hasNext())
            {
                TokenKey pk = CommandsForKeysAccessor.getKey(iter.next());
                consumer.accept(pk);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static TxnId deserializeTxnId(UntypedResultSet.Row row)
    {
        return deserializeTimestampOrNull(row, "txn_id", TxnId::fromBits);
    }

    public static Status.Durability deserializeDurability(UntypedResultSet.Row row)
    {
        // TODO (performance, expected): something less brittle than ordinal, more efficient than values()
        return Status.Durability.values()[row.getInt("durability", 0)];
    }

    public static StoreParticipants deserializeParticipantsOrNull(ByteBuffer bytes) throws IOException
    {
        return bytes != null && !ByteBufferAccessor.instance.isEmpty(bytes) ? deserialize(bytes, LocalVersionedSerializers.participants) : null;
    }

    public static Route<?> deserializeParticipantsRouteOnlyOrNull(ByteBuffer bytes) throws IOException
    {
        if (bytes == null ||ByteBufferAccessor.instance.isEmpty(bytes))
            return null;

        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            MessageVersionProvider versionProvider = LocalVersionedSerializers.participants.deserializeVersion(in);
            return CommandSerializers.participants.deserializeRouteOnly(in, versionProvider.messageVersion());
        }

    }

    public static ByteBuffer serializeParticipants(StoreParticipants participants) throws IOException
    {
        return serialize(participants, LocalVersionedSerializers.participants);
    }

    public static StoreParticipants deserializeParticipantsOrNull(Cell<?> cell)
    {
        if (cell == null)
            return null;

        try
        {
            return deserializeParticipantsOrNull(cell.buffer());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static TokenKey deserializeTokenKeySeparateTable(TableId tableId, ByteBuffer tokenBytes)
    {
        return (TokenKey) AccordRoutingKeyByteSource.Serializer.fromComparableBytes(ByteBufferAccessor.instance, tokenBytes, tableId, currentVersion, null);
    }

    public static PartitionUpdate getTimestampsForKeyUpdate(int storeId, TimestampsForKey current, long timestampMicros)
    {
        try
        {
            // TODO: convert to byte arrays
            ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;

            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.EMPTY);
            int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);
            LivenessInfo livenessInfo = LivenessInfo.create(timestampMicros, nowInSeconds);
            builder.addPrimaryKeyLivenessInfo(livenessInfo);
            addCell(TimestampsForKeyColumns.last_executed_timestamp, TimestampsForKey::lastExecutedTimestamp, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, current);
            addCell(TimestampsForKeyColumns.last_executed_micros, TimestampsForKey::rawLastExecutedHlc, accessor::valueOf, builder, timestampMicros, nowInSeconds, current);
            addCell(TimestampsForKeyColumns.last_write_timestamp, TimestampsForKey::lastWriteTimestamp, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, current);

            Row row = builder.build();
            if (row.columnCount() == 0)
                return null;

            ByteBuffer key = TimestampsForKeyColumns.makeKey(storeId, current.key());
            return singleRowUpdate(TimestampsForKeys, key, row);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Runnable getTimestampsForKeyUpdater(AccordCommandStore commandStore, TimestampsForKey liveTimestamps, long timestampMicros)
    {
        PartitionUpdate upd = getTimestampsForKeyUpdate(commandStore.id(), liveTimestamps, timestampMicros);
        return () -> {
            ColumnFamilyStore cfs = AccordColumnFamilyStores.timestampsForKey;
            try (OpOrder.Group group = Keyspace.writeOrder.start())
            {
                cfs.getCurrentMemtable().put(upd, UpdateTransaction.NO_OP, group, true);
            }
        };
    }

    public static UntypedResultSet loadTimestampsForKeyRow(int commandStoreId, TokenKey key)
    {
        String cql = "SELECT * FROM " + ACCORD_KEYSPACE_NAME + '.' + TIMESTAMPS_FOR_KEY + ' ' +
                     "WHERE store_id = ? " +
                     "AND routing_key = ?";

        return executeInternal(cql, commandStoreId, serializeRoutingKey(key));
    }

    private static SinglePartitionReadCommand getTimestampsForKeyRead(int storeId, TokenKey key, long nowInSeconds)
    {
        return SinglePartitionReadCommand.create(TimestampsForKeys, nowInSeconds,
                                                 TimestampsForKeyColumns.allColumns,
                                                 RowFilter.none(),
                                                 DataLimits.NONE,
                                                 TimestampsForKeyColumns.decorateKey(storeId, key),
                                                 FULL_PARTITION);
    }

    public static TimestampsForKey loadTimestampsForKey(int commandStoreId, TokenKey key)
    {
        return unsafeLoadTimestampsForKey(commandStoreId, key);
    }

    public static TimestampsForKey unsafeLoadTimestampsForKey(int commandStoreId, TokenKey key)
    {
        long timestampMicros = TimeUnit.MILLISECONDS.toMicros(Global.currentTimeMillis());
        int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);

        SinglePartitionReadCommand command = getTimestampsForKeyRead(commandStoreId, key, nowInSeconds);
        try (ReadExecutionController controller = command.executionController();
             FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
        {
            if (!partitions.hasNext())
                return null;

            try (RowIterator partition = partitions.next())
            {
                Invariants.checkState(partition.hasNext());
                Row row = partition.next();
                TokenKey checkKey = TimestampsForKeyRows.getKey(partition.partitionKey());
                checkState(checkKey.equals(key));

                Timestamp lastExecutedTimestamp = deserializeTimestampOrDefault(cellValue(row, TimestampsForKeyColumns.last_executed_timestamp), ByteBufferAccessor.instance, Timestamp::fromBits, Timestamp.NONE);
                ByteBuffer lastExecutedMicrosBB = cellValue(row, TimestampsForKeyColumns.last_executed_micros);
                long lastExecutedMicros = lastExecutedMicrosBB == null || !lastExecutedMicrosBB.hasRemaining() ? 0 : lastExecutedMicrosBB.getLong(lastExecutedMicrosBB.position());
                TxnId lastWriteId = deserializeTimestampOrDefault(cellValue(row, TimestampsForKeyColumns.last_write_id), ByteBufferAccessor.instance, TxnId::fromBits, TxnId.NONE);
                Timestamp lastWriteTimestamp = deserializeTimestampOrDefault(cellValue(row, TimestampsForKeyColumns.last_write_timestamp), ByteBufferAccessor.instance, Timestamp::fromBits, Timestamp.NONE);
                return TimestampsForKey.SerializerSupport.create(key, lastExecutedTimestamp, lastExecutedMicros, lastWriteId, lastWriteTimestamp);
            }
        }
        catch (Throwable t)
        {
            logger.error("Exception loading AccordTimestampsForKey " + key, t);
            throw t;
        }
    }

    private static DecoratedKey makeKeySeparateTable(CommandsForKeyAccessor accessor, int commandStoreId, TokenKey key)
    {
        ByteBuffer pk = accessor.keyComparator.make(commandStoreId,
                                                    UUIDSerializer.instance.serialize(key.table().asUUID()),
                                                    serializeRoutingKeyNoTable(key)).serializeAsPartitionKey();
        return accessor.table.partitioner.decorateKey(pk);
    }

    @VisibleForTesting
    public static ByteBuffer serializeRoutingKey(AccordRoutingKey routingKey)
    {
        byte[] bytes = getRoutingKeySerializer(routingKey).serialize(routingKey);
        return ByteBuffer.wrap(bytes);
    }

    public static ByteBuffer serializeRoutingKeyNoTable(AccordRoutingKey key)
    {
        return CommandsForKeysAccessor.serializeKeyNoTable(key);
    }

    public static AccordRoutingKey deserializeRoutingKey(ByteBuffer buffer)
    {
        return AccordRoutingKeyByteSource.Serializer.fromComparableBytes(ByteBufferAccessor.instance, buffer, currentVersion, null);
    }

    private static AccordRoutingKeyByteSource.Serializer serializer(AccordRoutingKey routingKey)
    {
        return serializer(routingKey.table());
    }

    public static AccordRoutingKeyByteSource.Serializer serializer(TableId tableId)
    {
        return TABLE_SERIALIZERS.computeIfAbsent(tableId, id -> AccordRoutingKeyByteSource.variableLength(partitioner(tableId)));
    }

    public static IPartitioner partitioner(TableId tableId)
    {

        return SchemaHolder.schema.getTablePartitioner(tableId);
    }

    private static PartitionUpdate getCommandsForKeyPartitionUpdate(int storeId, TokenKey key, CommandsForKey commandsForKey, Object serialized, long timestampMicros)
    {
        ByteBuffer bytes;
        if (serialized instanceof ByteBuffer) bytes = (ByteBuffer) serialized;
        else bytes = CommandsForKeySerializer.toBytesWithoutKey(commandsForKey);
        return getCommandsForKeyPartitionUpdate(storeId, key, timestampMicros, bytes);
    }

    @VisibleForTesting
    public static PartitionUpdate getCommandsForKeyPartitionUpdate(int storeId, TokenKey key, long timestampMicros, ByteBuffer bytes)
    {
        return singleRowUpdate(CommandsForKeysAccessor.table,
                               makeKeySeparateTable(CommandsForKeysAccessor, storeId, key),
                               singleCellRow(Clustering.EMPTY, BufferCell.live(CommandsForKeysAccessor.data, timestampMicros, bytes)));
    }

    public static Runnable getCommandsForKeyUpdater(int storeId, TokenKey key, CommandsForKey update, Object serialized, long timestampMicros)
    {
        PartitionUpdate upd = getCommandsForKeyPartitionUpdate(storeId, key, update, serialized, timestampMicros);
        return () -> {
            ColumnFamilyStore cfs = AccordColumnFamilyStores.commandsForKey;
            try (OpOrder.Group group = Keyspace.writeOrder.start())
            {
                cfs.getCurrentMemtable().put(upd, UpdateTransaction.NO_OP, group, true);
            }
        };
    }

    private static <T> ByteBuffer cellValue(Cell<T> cell)
    {
        return cell.accessor().toBuffer(cell.value());
    }

    // TODO: convert to byte array
    private static ByteBuffer cellValue(Row row, ColumnMetadata column)
    {
        Cell<?> cell = row.getCell(column);
        return (cell != null && !cell.isTombstone()) ? cellValue(cell) : null;
    }

    private static <T> ByteBuffer clusteringValue(Clustering<T> clustering, int idx)
    {
        return clustering.accessor().toBuffer(clustering.get(idx));
    }

    private static SinglePartitionReadCommand getCommandsForKeyRead(CommandsForKeyAccessor accessor, int storeId, TokenKey key, long nowInSeconds)
    {
        return SinglePartitionReadCommand.create(accessor.table, nowInSeconds,
                                                 accessor.allColumns,
                                                 RowFilter.none(),
                                                 DataLimits.NONE,
                                                 makeKeySeparateTable(accessor, storeId, key),
                                                 FULL_PARTITION);
    }

    public static SinglePartitionReadCommand getCommandsForKeyRead(int storeId, TokenKey key, int nowInSeconds)
    {
        return getCommandsForKeyRead(CommandsForKeysAccessor, storeId, key, nowInSeconds);
    }

    static CommandsForKey unsafeLoadCommandsForKey(CommandsForKeyAccessor accessor, int commandStoreId, TokenKey key)
    {
        long timestampMicros = TimeUnit.MILLISECONDS.toMicros(Global.currentTimeMillis());
        int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);

        SinglePartitionReadCommand command = getCommandsForKeyRead(accessor, commandStoreId, key, nowInSeconds);

        try (ReadExecutionController controller = command.executionController();
             FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
        {
            if (!partitions.hasNext())
                return null;

            try (RowIterator partition = partitions.next())
            {
                Invariants.checkState(partition.hasNext());
                Row row = partition.next();
                ByteBuffer data = cellValue(row, accessor.data);
                return CommandsForKeySerializer.fromBytes(key, data);
            }
        }
        catch (Throwable t)
        {
            logger.error("Exception loading AccordCommandsForKey " + key, t);
            throw t;
        }
    }

    public static CommandsForKey unsafeLoadCommandsForKey(int commandStoreId, TokenKey key)
    {
        return unsafeLoadCommandsForKey(CommandsForKeysAccessor, commandStoreId, key);
    }

    public static CommandsForKey loadCommandsForKey(int commandStoreId, TokenKey key)
    {
        return unsafeLoadCommandsForKey(CommandsForKeysAccessor, commandStoreId, key);
    }

    public static class EpochDiskState
    {
        public static final EpochDiskState EMPTY = new EpochDiskState(0, 0);
        public final long minEpoch;
        public final long maxEpoch;

        private EpochDiskState(long minEpoch, long maxEpoch)
        {
            Invariants.checkArgument(minEpoch >= 0, "Min Epoch %d < 0", minEpoch);
            Invariants.checkArgument(maxEpoch >= minEpoch, "Max epoch %d < min %d", maxEpoch, minEpoch);
            this.minEpoch = minEpoch;
            this.maxEpoch = maxEpoch;
        }

        public static EpochDiskState create(long minEpoch, long maxEpoch)
        {
            if (minEpoch == maxEpoch && minEpoch == 0)
                return EMPTY;
            return new EpochDiskState(minEpoch, maxEpoch);
        }

        public static EpochDiskState create(long epoch)
        {
            return create(epoch, epoch);
        }

        public boolean isEmpty()
        {
            return minEpoch == maxEpoch && maxEpoch == 0;
        }

        @VisibleForTesting
        EpochDiskState withNewMaxEpoch(long epoch)
        {
            Invariants.checkArgument(epoch > maxEpoch, "Epoch %d <= %d (max)", epoch, maxEpoch);
            return EpochDiskState.create(Math.max(1, minEpoch), epoch);
        }

        private EpochDiskState withNewMinEpoch(long epoch)
        {
            Invariants.checkArgument(epoch > minEpoch, "epoch %d <= %d (min)", epoch, minEpoch);
            Invariants.checkArgument(epoch <= maxEpoch, "epoch %d > %d (max)", epoch, maxEpoch);
            return EpochDiskState.create(epoch, maxEpoch);
        }

        @Override
        public String toString()
        {
            return "EpochDiskState{" +
                   "minEpoch=" + minEpoch +
                   ", maxEpoch=" + maxEpoch +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EpochDiskState diskState = (EpochDiskState) o;
            return minEpoch == diskState.minEpoch && maxEpoch == diskState.maxEpoch;
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class JournalColumns
    {
        static final ClusteringComparator keyComparator = Journal.partitionKeyAsClusteringComparator();
        static final CompositeType partitionKeyType = (CompositeType) Journal.partitionKeyType;
        public static final ColumnMetadata store_id = getColumn(Journal, "store_id");
        public static final ColumnMetadata type = getColumn(Journal, "type");
        public static final ColumnMetadata id = getColumn(Journal, "id");
        public static final ColumnMetadata record = getColumn(Journal, "record");

        public static DecoratedKey decorate(JournalKey key)
        {
            ByteBuffer id = ByteBuffer.allocate(CommandSerializers.txnId.serializedSize());
            CommandSerializers.txnId.serialize(key.id, id);
            id.flip();
            ByteBuffer pk = keyComparator.make(key.commandStoreId, (byte)key.type.id, id).serializeAsPartitionKey();
            Invariants.checkState(getTxnId(splitPartitionKey(pk)).equals(key.id));
            return Journal.partitioner.decorateKey(pk);
        }

        public static ByteBuffer[] splitPartitionKey(DecoratedKey key)
        {
            return JournalColumns.partitionKeyType.split(key.getKey());
        }

        public static ByteBuffer[] splitPartitionKey(ByteBuffer key)
        {
            return JournalColumns.partitionKeyType.split(key);
        }

        public static int getStoreId(DecoratedKey pk)
        {
            return getStoreId(splitPartitionKey(pk));
        }

        public static int getStoreId(ByteBuffer[] partitionKeyComponents)
        {
            return Int32Type.instance.compose(partitionKeyComponents[store_id.position()]);
        }

        public static JournalKey.Type getType(ByteBuffer[] partitionKeyComponents)
        {
            return JournalKey.Type.fromId(ByteType.instance.compose(partitionKeyComponents[type.position()]));
        }

        public static TxnId getTxnId(DecoratedKey key)
        {
            return getTxnId(splitPartitionKey(key));
        }

        public static TxnId getTxnId(ByteBuffer[] partitionKeyComponents)
        {
            ByteBuffer buffer = partitionKeyComponents[id.position()];
            return CommandSerializers.txnId.deserialize(buffer, buffer.position());
        }

        public static JournalKey getJournalKey(DecoratedKey key)
        {
            ByteBuffer[] parts = splitPartitionKey(key);
            return new JournalKey(getTxnId(parts), getType(parts), getStoreId(parts));
        }
    }

    private static EpochDiskState saveEpochDiskState(EpochDiskState diskState)
    {
        String cql = "INSERT INTO " + ACCORD_KEYSPACE_NAME + '.' + EPOCH_METADATA + ' ' +
                     "(key, min_epoch, max_epoch) VALUES (0, ?, ?);";
        executeInternal(cql, diskState.minEpoch, diskState.maxEpoch);
        return diskState;
    }

    @Nullable
    @VisibleForTesting
    public static EpochDiskState loadEpochDiskState()
    {
        String cql = "SELECT * FROM " + ACCORD_KEYSPACE_NAME + '.' + EPOCH_METADATA + ' ' +
                     "WHERE key=0";
        UntypedResultSet result = executeInternal(format(cql, ACCORD_KEYSPACE_NAME, EPOCH_METADATA));
        if (result.isEmpty())
            return null;
        UntypedResultSet.Row row = result.one();
        return EpochDiskState.create(row.getLong("min_epoch"), row.getLong("max_epoch"));
    }

    /**
     * Update the disk state for this epoch, if it's higher than the one we have one disk.
     * <p>
     * This is meant to be called before any update involving the new epoch, not after. This way if the update
     * fails, we can detect and cleanup. If we updated disk state after an update and it failed, we could "forget"
     * about (now acked) topology updates after a restart.
     */
    private static EpochDiskState maybeUpdateMaxEpoch(EpochDiskState diskState, long epoch)
    {
        if (diskState.isEmpty())
            return saveEpochDiskState(EpochDiskState.create(epoch));
        Invariants.checkArgument(epoch >= diskState.minEpoch, "Epoch %d < %d (min)", epoch, diskState.minEpoch);
        if (epoch > diskState.maxEpoch)
        {
            diskState = diskState.withNewMaxEpoch(epoch);
            saveEpochDiskState(diskState);
        }
        return diskState;
    }

    public static EpochDiskState saveTopology(Topology topology, EpochDiskState diskState)
    {
        return maybeUpdateMaxEpoch(diskState, topology.epoch());
    }

    public static EpochDiskState markRemoteTopologySync(Node.Id node, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET remote_sync_complete = remote_sync_complete + ? WHERE epoch = ?";
        executeInternal(cql,
                        Collections.singleton(node.id), epoch);
        return diskState;
    }

    public static EpochDiskState markClosed(Ranges ranges, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET closed = closed + ? WHERE epoch = ?";
        executeInternal(cql,
                        KeySerializers.rangesToBlobMap(ranges), epoch);
        return diskState;
    }

    public static EpochDiskState markRedundant(Ranges ranges, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET redundant = redundant + ? WHERE epoch = ?";
        executeInternal(cql,
                        KeySerializers.rangesToBlobMap(ranges), epoch);
        flush(Topologies);
        return diskState;
    }

    public static EpochDiskState setNotifyingLocalSync(long epoch, Set<Node.Id> pending, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET sync_state = ?, pending_sync_notify = ? WHERE epoch = ?";
        executeInternal(cql,
                        SyncStatus.NOTIFYING.ordinal(),
                        pending.stream().map(i -> i.id).collect(Collectors.toSet()),
                        epoch);
        return diskState;
    }

    public static EpochDiskState markLocalSyncAck(Node.Id node, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET pending_sync_notify = pending_sync_notify - ? WHERE epoch = ?";
        executeInternal(cql,
                        Collections.singleton(node.id), epoch);
        return diskState;
    }

    public static EpochDiskState setCompletedLocalSync(long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "SET sync_state = ?, pending_sync_notify = {} WHERE epoch = ?";
        executeInternal(cql,
                        SyncStatus.COMPLETED.ordinal(),
                        epoch);
        return diskState;
    }

    public static EpochDiskState truncateTopologyUntil(final long epoch, EpochDiskState diskState)
    {
        while (diskState.minEpoch < epoch)
        {
            long delete = diskState.minEpoch;
            diskState = diskState.withNewMinEpoch(delete + 1);
            saveEpochDiskState(diskState);
            String cql = "DELETE FROM " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                         "WHERE epoch = ?";
            executeInternal(cql, delete);
        }
        return diskState;
    }

    public interface TopologyLoadConsumer
    {
        void load(long epoch, ClusterMetadata metadata, Topology topology, SyncStatus syncStatus, Set<Node.Id> pendingSyncNotify, Set<Node.Id> remoteSyncComplete, Ranges closed, Ranges redundant);
    }

    @VisibleForTesting
    public static void loadEpoch(long epoch, ClusterMetadata metadata, TopologyLoadConsumer consumer) throws IOException
    {
        Topology topology = AccordTopology.createAccordTopology(metadata);

        String cql = "SELECT * FROM " + ACCORD_KEYSPACE_NAME + '.' + TOPOLOGIES + ' ' +
                     "WHERE epoch=?";

        UntypedResultSet result = executeInternal(cql, epoch);
        if (result.isEmpty())
        {
            // topology updates disk state for epoch but doesn't save the topology to the table, so there maybe an epoch we know about, but no fields are present
            consumer.load(epoch, metadata, topology, SyncStatus.NOT_STARTED, Collections.emptySet(), Collections.emptySet(), Ranges.EMPTY, Ranges.EMPTY);
            return;
        }
        checkState(!result.isEmpty(), "Nothing found for epoch %d", epoch);
        UntypedResultSet.Row row = result.one();

        SyncStatus syncStatus = row.has("sync_state")
                                ? SyncStatus.values()[row.getInt("sync_state")]
                                : SyncStatus.NOT_STARTED;
        Set<Node.Id> pendingSyncNotify = row.has("pending_sync_notify")
                                         ? row.getSet("pending_sync_notify", Int32Type.instance).stream().map(Node.Id::new).collect(Collectors.toSet())
                                         : Collections.emptySet();
        Set<Node.Id> remoteSyncComplete = row.has("remote_sync_complete")
                                          ? row.getSet("remote_sync_complete", Int32Type.instance).stream().map(Node.Id::new).collect(Collectors.toSet())
                                          : Collections.emptySet();
        Ranges closed = row.has("closed") ? blobMapToRanges(row.getMap("closed", BytesType.instance, BytesType.instance)) : Ranges.EMPTY;
        Ranges redundant = row.has("redundant") ? blobMapToRanges(row.getMap("redundant", BytesType.instance, BytesType.instance)) : Ranges.EMPTY;

        consumer.load(epoch, metadata, topology, syncStatus, pendingSyncNotify, remoteSyncComplete, closed, redundant);
    }

    public static EpochDiskState loadTopologies(TopologyLoadConsumer consumer)
    {
        try
        {
            EpochDiskState diskState = loadEpochDiskState();
            if (diskState == null)
                return EpochDiskState.EMPTY;

            for (ClusterMetadata metadata : AccordService.tcmLoadRange(diskState.minEpoch, diskState.maxEpoch))
                loadEpoch(metadata.epoch.getEpoch(), metadata, consumer);

            return diskState;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    public static void unsafeSetSchema(SchemaProvider provider)
    {
        SchemaHolder.schema = provider;
    }

    @VisibleForTesting
    public static void unsafeClear()
    {
        for (var store : Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStores())
            store.truncateBlockingWithoutSnapshot();
        TABLE_SERIALIZERS.clear();
        SchemaHolder.schema = Schema.instance;
    }

    public static class AccordColumnFamilyStores
    {
        public static final ColumnFamilyStore journal = Schema.instance.getColumnFamilyStoreInstance(Journal.id);
        public static final ColumnFamilyStore commandsForKey = Schema.instance.getColumnFamilyStoreInstance(CommandsForKeys.id);
        public static final ColumnFamilyStore timestampsForKey = Schema.instance.getColumnFamilyStoreInstance(TimestampsForKeys.id);
    }
}
