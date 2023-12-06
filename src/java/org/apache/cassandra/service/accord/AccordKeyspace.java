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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.impl.CommandTimeseries;
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKeyUpdater;
import accord.impl.TimestampsForKey;
import accord.local.Command;
import accord.local.Command.WaitingOn;
import accord.local.CommandStore;
import accord.local.CommonAttributes;
import accord.local.DurableBefore;
import accord.local.Listeners;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.SaveStatus;
import accord.local.SerializerSupport;
import accord.local.SerializerSupport.MessageProvider;
import accord.local.SerializerSupport.WaitingOnProvider;
import accord.local.Status;
import accord.local.Status.Durability;
import accord.primitives.Ballot;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.async.Observable;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
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
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Row.Deletion;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.LocalVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.AccordConfigurationService.SyncStatus;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ListenerSerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.service.accord.serializers.WaitingOnSerializer;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.checkState;
import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.db.rows.BufferCell.live;
import static org.apache.cassandra.db.rows.BufferCell.tombstone;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.service.accord.serializers.KeySerializers.blobMapToRanges;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class AccordKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(AccordKeyspace.class);

    public static final String COMMANDS = "commands";
    public static final String TIMESTAMPS_FOR_KEY = "timestamps_for_key";
    public static final String DEPS_COMMANDS_FOR_KEY = "deps_commands_for_key";
    public static final String ALL_COMMANDS_FOR_KEY = "all_commands_for_key";
    public static final String TOPOLOGIES = "topologies";
    public static final String EPOCH_METADATA = "epoch_metadata";
    public static final String COMMAND_STORE_METADATA = "command_store_metadata";

    private static final TupleType TIMESTAMP_TYPE = new TupleType(Lists.newArrayList(LongType.instance, LongType.instance, Int32Type.instance));
    private static final String TIMESTAMP_TUPLE = TIMESTAMP_TYPE.asCQL3Type().toString();
    private static final TupleType KEY_TYPE = new TupleType(Arrays.asList(UUIDType.instance, BytesType.instance));
    private static final String KEY_TUPLE = KEY_TYPE.asCQL3Type().toString();

    private static final ClusteringIndexFilter FULL_PARTITION = new ClusteringIndexSliceFilter(Slices.ALL, false);

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

    // TODO: store timestamps as blobs (confirm there are no negative numbers, or offset)
    private static final TableMetadata Commands =
        parse(COMMANDS,
              "accord commands",
              "CREATE TABLE %s ("
              + "store_id int,"
              + "domain int," // this is stored as part of txn_id, used currently for cheaper scans of the table
              + format("txn_id %s,", TIMESTAMP_TUPLE)
              + "status int,"
              + "route blob,"
              + "durability int,"
              + format("execute_at %s,", TIMESTAMP_TUPLE)
              + format("promised_ballot %s,", TIMESTAMP_TUPLE)
              + format("accepted_ballot %s,", TIMESTAMP_TUPLE)
              + "waiting_on blob,"
              + "listeners set<blob>, "
              + "PRIMARY KEY((store_id, domain, txn_id))"
              + ')')
        .partitioner(new LocalPartitioner(CompositeType.getInstance(Int32Type.instance, Int32Type.instance, TIMESTAMP_TYPE)))
        .build();

    // TODO: naming is not very clearly distinct from the base serializers
    private static class LocalVersionedSerializers
    {
        static final LocalVersionedSerializer<Route<?>> route = localSerializer(KeySerializers.route);
        static final LocalVersionedSerializer<Command.DurableAndIdempotentListener> listeners = localSerializer(ListenerSerializers.listener);
        static final LocalVersionedSerializer<Topology> topology = localSerializer(TopologySerializers.topology);
        static final LocalVersionedSerializer<ReducingRangeMap<Timestamp>> rejectBefore = localSerializer(CommandStoreSerializers.rejectBefore);
        static final LocalVersionedSerializer<DurableBefore> durableBefore = localSerializer(CommandStoreSerializers.durableBefore);
        static final LocalVersionedSerializer<RedundantBefore> redundantBefore = localSerializer(CommandStoreSerializers.redundantBefore);
        static final LocalVersionedSerializer<NavigableMap<TxnId, Ranges>> bootstrapBeganAt = localSerializer(CommandStoreSerializers.bootstrapBeganAt);
        static final LocalVersionedSerializer<NavigableMap<Timestamp, Ranges>> safeToRead = localSerializer(CommandStoreSerializers.safeToRead);

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
        static final ColumnMetadata txn_id = getColumn(Commands, "txn_id");
        static final ColumnMetadata store_id = getColumn(Commands, "store_id");
        public static final ColumnMetadata status = getColumn(Commands, "status");
        public static final ColumnMetadata route = getColumn(Commands, "route");
        public static final ColumnMetadata durability = getColumn(Commands, "durability");
        public static final ColumnMetadata execute_at = getColumn(Commands, "execute_at");
        static final ColumnMetadata promised_ballot = getColumn(Commands, "promised_ballot");
        static final ColumnMetadata accepted_ballot = getColumn(Commands, "accepted_ballot");
        static final ColumnMetadata waiting_on = getColumn(Commands, "waiting_on");
        static final ColumnMetadata listeners = getColumn(Commands, "listeners");

        public static final ColumnMetadata[] TRUNCATE_FIELDS = new ColumnMetadata[] { durability, execute_at, route, status };

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
            Cell<?> cell = row.getCell(route);
            return deserializeRouteOrNull(cell);
        }

        private static Object[] truncatedApplyLeaf(long newTimestamp, SaveStatus newSaveStatus, Cell<?> durabilityCell, Cell<?> executeAtCell, Cell<?> routeCell, boolean updateTimestamps)
        {
            checkArgument(durabilityCell.column() == CommandsColumns.durability);
            checkArgument(executeAtCell.column() == CommandsColumns.execute_at);
            checkArgument(routeCell.column() == CommandsColumns.route);
            Object[] newLeaf = BTree.unsafeAllocateNonEmptyLeaf(TRUNCATE_FIELDS.length);
            int colIndex = 0;
            newLeaf[colIndex++] = updateTimestamps ? durabilityCell.withUpdatedTimestamp(newTimestamp) : durabilityCell;
            newLeaf[colIndex++] = updateTimestamps ? executeAtCell.withUpdatedTimestamp(newTimestamp) : executeAtCell;
            newLeaf[colIndex++] = updateTimestamps ? routeCell.withUpdatedTimestamp(newTimestamp) : routeCell;
            // Status always needs to use the new timestamp since we are replacing the existing value
            // All the other columns are being retained unmodified with at most updated timestamps to accomdate deletion
            //noinspection UnusedAssignment
            newLeaf[colIndex++] = BufferCell.live(status, newTimestamp, ByteBufferAccessor.instance.valueOf(newSaveStatus.ordinal()));
            return newLeaf;
        }

        public static Row truncatedApply(SaveStatus newSaveStatus, Row row, long nowInSec, Durability durability, Cell<?> durabilityCell, Cell<?> executeAtCell, Cell<?> routeCell, boolean withOutcome)
        {
            checkArgument(durabilityCell.column() == CommandsColumns.durability);
            checkArgument(executeAtCell.column() == CommandsColumns.execute_at);
            checkArgument(routeCell.column() == CommandsColumns.route);
            long oldTimestamp = row.primaryKeyLivenessInfo().timestamp();
            long newTimestamp = oldTimestamp + 1;
            // If durability is not universal we don't want to delete older versions of the row that might have recorded
            // a higher durability value. maybeDropTruncatedCommandColumns will take care of dropping things even if we don't drop via tombstones.
            // durability should be the only column that could have an older value that is insufficient for propagating forward
            // TODO (now): with UniversalOrInvalidated should this change?
            boolean doDeletion = durability == Durability.Universal;

            // We may not have what we need to generate a deletion and include the outcome in the truncated row
            // so need to wait until we can have the outcome to issue the deletion otherwise it would be shadowed and lost
            if (withOutcome)
                doDeletion = false;

            Object[] newLeaf = truncatedApplyLeaf(newTimestamp, newSaveStatus, durabilityCell, executeAtCell, routeCell, doDeletion);

            // Including a deletion allows future compactions to drop data before it gets to the purger
            // but it is pretty optional because maybeDropTruncatedCommandColumns will drop the extra columns
            // regardless
            Row.Deletion deletion = doDeletion ? new Row.Deletion(DeletionTime.build(oldTimestamp, nowInSec), false) : Deletion.LIVE;
            return BTreeRow.create(row.clustering(), LivenessInfo.create(newTimestamp, nowInSec), deletion, newLeaf);
        }

        public static Row maybeDropTruncatedCommandColumns(Row row, Cell<?> durabilityCell, Cell<?> executeAtCell, Cell<?> routeCell, Cell<?> statusCell)
        {
            checkArgument(durabilityCell.column() == CommandsColumns.durability);
            checkArgument(executeAtCell.column() == CommandsColumns.execute_at);
            checkArgument(routeCell.column() == CommandsColumns.route);
            checkArgument(statusCell.column() == CommandsColumns.status);
            int colCount = row.columnCount();
            // If it's the exact length of the post truncate column count without outcome fields
            // then it is exactly the columns needed for getting this far and withOutcome doesn't matter since
            // nothing additional is available to include anyway
            if (colCount == TRUNCATE_FIELDS.length)
                return row;

            // Construct a replacement with just the available columns that are still needed
            Object[] newLeaf = BTree.unsafeAllocateNonEmptyLeaf(TRUNCATE_FIELDS.length);
            int colIndex = 0;
            newLeaf[colIndex++] = durabilityCell;
            newLeaf[colIndex++] = executeAtCell;
            newLeaf[colIndex++] = routeCell;
            //noinspection UnusedAssignment
            newLeaf[colIndex++] = statusCell;

            return BTreeRow.create(row.clustering(), row.primaryKeyLivenessInfo(), row.deletion(), newLeaf);
        }
    }

    private static final TableMetadata TimestampsForKeys =
        parse(TIMESTAMPS_FOR_KEY,
              "accord timestamps per key",
              "CREATE TABLE %s ("
              + "store_id int, "
              + "key_token blob, " // can't use "token" as this is restricted word in CQL
              + format("key %s, ", KEY_TUPLE)
              + format("max_timestamp %s, ", TIMESTAMP_TUPLE)
              + format("last_executed_timestamp %s, ", TIMESTAMP_TUPLE)
              + "last_executed_micros bigint, "
              + format("last_write_timestamp %s, ", TIMESTAMP_TUPLE)
              + "PRIMARY KEY((store_id, key_token, key))"
              + ')')
        .partitioner(new LocalPartitioner(CompositeType.getInstance(Int32Type.instance, BytesType.instance, KEY_TYPE)))
        .build();

    public static class TimestampsForKeyColumns
    {
        static final ClusteringComparator keyComparator = TimestampsForKeys.partitionKeyAsClusteringComparator();
        static final CompositeType partitionKeyType = (CompositeType) TimestampsForKeys.partitionKeyType;
        static final ColumnFilter allColumns = ColumnFilter.all(TimestampsForKeys);
        static final ColumnMetadata store_id = getColumn(TimestampsForKeys, "store_id");
        static final ColumnMetadata key_token = getColumn(TimestampsForKeys, "key_token");
        static final ColumnMetadata key = getColumn(TimestampsForKeys, "key");
        public static final ColumnMetadata max_timestamp = getColumn(TimestampsForKeys, "max_timestamp");
        public static final ColumnMetadata last_executed_timestamp = getColumn(TimestampsForKeys, "last_executed_timestamp");
        public static final ColumnMetadata last_executed_micros = getColumn(TimestampsForKeys, "last_executed_micros");
        public static final ColumnMetadata last_write_timestamp = getColumn(TimestampsForKeys, "last_write_timestamp");

        static final Columns columns = Columns.from(Lists.newArrayList(max_timestamp, last_executed_timestamp, last_executed_micros, last_write_timestamp));

        static ByteBuffer makePartitionKey(int storeId, Key key)
        {
            PartitionKey pk = (PartitionKey) key;
            return keyComparator.make(storeId, serializeToken(pk.token()), serializeKey(pk)).serializeAsPartitionKey();
        }

        static ByteBuffer makePartitionKey(int storeId, TimestampsForKey timestamps)
        {
            return makePartitionKey(storeId, timestamps.key());
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

        public static PartitionKey getKey(ByteBuffer[] partitionKeyComponents)
        {
            return deserializeKey(partitionKeyComponents[key.position()]);
        }

        @Nullable
        public static Timestamp getMaxTimestamp(Row row)
        {
            Cell cell = row.getCell(max_timestamp);
            if (cell == null)
                return null;
            return deserializeTimestampOrNull(cell.value(), cell.accessor(), Timestamp::fromBits);
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

        public static Row truncateTimestampsForKeyRow(long nowInSec, Row row, Cell lastExecuteMicrosCell, Cell lastExecuteCell, Cell lastWriteCell, Cell maxTimestampCell)
        {
            checkArgument(lastExecuteMicrosCell == null || lastExecuteMicrosCell.column() == last_executed_micros);
            checkArgument(lastExecuteCell == null || lastExecuteCell.column() == last_executed_timestamp);
            checkArgument(lastWriteCell == null || lastWriteCell.column() == last_write_timestamp);
            checkArgument(maxTimestampCell == null || maxTimestampCell.column() == max_timestamp);

            long timestamp = row.primaryKeyLivenessInfo().timestamp();

            int colCount = 0;
            if (lastExecuteMicrosCell != null)
                colCount++;
            if (lastExecuteCell != null)
                colCount++;
            if (lastWriteCell != null)
                colCount++;
            if (maxTimestampCell != null)
                colCount++;

            checkState(columns.size() >= colCount, "CommandsForKeyColumns.static_columns_metadata should include all the columns");
            Object[] newLeaf = BTree.unsafeAllocateNonEmptyLeaf(colCount);
            int colIndex = 0;

            if (lastExecuteMicrosCell != null)
                newLeaf[colIndex++] = lastExecuteMicrosCell;
            if (lastExecuteCell != null)
                newLeaf[colIndex++] = lastExecuteCell;
            if (lastWriteCell != null)
                newLeaf[colIndex++] = lastWriteCell;
            if (maxTimestampCell != null)
                newLeaf[colIndex++] = maxTimestampCell;

            return BTreeRow.create(row.clustering(), LivenessInfo.create(timestamp, nowInSec),
                                   Deletion.LIVE, newLeaf);
        }
    }

    private static final TableMetadata DepsCommandsForKeys = commandsForKeysTable(DEPS_COMMANDS_FOR_KEY);
    private static final TableMetadata AllCommandsForKeys = commandsForKeysTable(ALL_COMMANDS_FOR_KEY);

    private static TableMetadata commandsForKeysTable(String tableName)
    {
        return parse(tableName,
              "accord commands per key",
              "CREATE TABLE %s ("
              + "store_id int, "
              + "key_token blob, " // can't use "token" as this is restricted word in CQL
              + format("key %s, ", KEY_TUPLE)
              + format("timestamp %s, ", TIMESTAMP_TUPLE)
              + "data blob, "
              + "PRIMARY KEY((store_id, key_token, key), timestamp)"
              + ')')
        .partitioner(new LocalPartitioner(CompositeType.getInstance(Int32Type.instance, BytesType.instance, KEY_TYPE)))
        .build();
    }

    public static class CommandsForKeyAccessor
    {
        final TableMetadata table;
        final ClusteringComparator keyComparator;
        final CompositeType partitionKeyType;
        final ColumnFilter allColumns;
        final ColumnMetadata store_id;
        final ColumnMetadata key_token;
        final ColumnMetadata key;
        final ColumnMetadata timestamp;

        final ColumnMetadata data;

        final RegularAndStaticColumns columns;

        public CommandsForKeyAccessor(TableMetadata table)
        {
            this.table = table;
            this.keyComparator = table.partitionKeyAsClusteringComparator();
            this.partitionKeyType = (CompositeType) table.partitionKeyType;
            this.allColumns = ColumnFilter.all(table);
            this.store_id = getColumn(table, "store_id");
            this.key_token = getColumn(table, "key_token");
            this.key = getColumn(table, "key");
            this.timestamp = getColumn(table, "timestamp");
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

        public PartitionKey getKey(ByteBuffer[] partitionKeyComponents)
        {
            return deserializeKey(partitionKeyComponents[key.position()]);
        }

        @Nullable
        public Timestamp getTimestamp(Row row)
        {
            return deserializeTimestampOrNull(row.clustering().bufferAt(timestamp.position()), Timestamp::fromBits);
        }
    }

    public static final CommandsForKeyAccessor DepsCommandsForKeysAccessor = new CommandsForKeyAccessor(DepsCommandsForKeys);
    public static final CommandsForKeyAccessor AllCommandsForKeysAccessor = new CommandsForKeyAccessor(AllCommandsForKeys);

    private static final TableMetadata Topologies =
        parse(TOPOLOGIES,
              "accord topologies",
              "CREATE TABLE %s (" +
              "epoch bigint primary key, " +
              "topology blob, " +
              "sync_state int, " +
              "pending_sync_notify set<int>, " + // nodes that need to be told we're synced
              "remote_sync_complete set<int>, " +  // nodes that have told us they're synced
              "closed map<blob, blob>, " +
              "redundant map<blob, blob>" +
              ')').build();

    private static final TableMetadata EpochMetadata =
        parse(EPOCH_METADATA,
              "global epoch info",
              "CREATE TABLE %s (" +
              "key int primary key, " +
              "min_epoch bigint, " +
              "max_epoch bigint " +
              ')').build();

    private static final TableMetadata CommandStoreMetadata =
        parse(COMMAND_STORE_METADATA,
              "command store state",
              "CREATE TABLE %s (" +
              "store_id int, " +
              "reject_before blob, " +
              "bootstrap_began_at blob, " +
              "safe_to_read blob, " +
              "redundant_before blob, " +
              "durable_before blob, " +
              "PRIMARY KEY(store_id)" +
              ')').build();

    private static final AtomicLong commandStoreMetadataTimestamp = new AtomicLong();

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

    public static Tables tables()
    {
        return Tables.of(Commands, TimestampsForKeys, DepsCommandsForKeys, AllCommandsForKeys, Topologies, EpochMetadata, CommandStoreMetadata);
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

    private static <C, V> void addSetChanges(ColumnMetadata column, Function<C, Set<V>> get, SerializeFunction<V> serialize, Row.Builder builder, long timestampMicros, int nowInSec, C original, C command) throws IOException
    {
        Set<V> prev = original != null ? get.apply(original) : Collections.emptySet();
        if (prev == null) prev = Collections.emptySet();
        Set<V> value = get.apply(command);
        if (value == null) value = Collections.emptySet();

        if (value.isEmpty() && !prev.isEmpty())
        {
            builder.addComplexDeletion(column, DeletionTime.build(timestampMicros, nowInSec));
            return;
        }

        for (V item : Sets.difference(value, prev))
            builder.addCell(live(column, timestampMicros, EMPTY_BYTE_BUFFER, CellPath.create(serialize.apply(item))));

        for (V item : Sets.difference(prev, value))
            builder.addCell(tombstone(column, timestampMicros, nowInSec, CellPath.create(serialize.apply(item))));
    }

    private static <C, K, V> void addMapChanges(ColumnMetadata column, Function<C, Map<K, V>> get, SerializeFunction<K> serializeKey, SerializeFunction<V> serializeVal, Row.Builder builder, long timestampMicros, int nowInSec, C original, C command) throws IOException
    {
        Map<K, V> prev = original != null ? get.apply(original) : Collections.emptyMap();
        if (prev == null) prev = Collections.emptyMap();
        Map<K, V> value = get.apply(command);
        if (value == null) value = Collections.emptyMap();

        if (value.isEmpty() && !prev.isEmpty())
        {
            builder.addComplexDeletion(column, DeletionTime.build(timestampMicros, nowInSec));
            return;
        }

        for (Map.Entry<K, V> entry : value.entrySet())
        {
            K key = entry.getKey();
            V pVal = prev.get(key);
            if (pVal != null && pVal.equals(entry.getValue()))
                continue;
            builder.addCell(live(column, timestampMicros, serializeVal.apply(entry.getValue()), CellPath.create(serializeKey.apply(key))));
        }
        for (K key : Sets.difference(prev.keySet(), value.keySet()))
            builder.addCell(tombstone(column, timestampMicros, nowInSec, CellPath.create(serializeKey.apply(key))));
    }

    private static <K, V> int estimateMapChanges(Map<K, V> prev, Map<K, V> value)
    {
        return Math.abs(prev.size() - value.size());
    }

    private static <C, K, V> int estimateMapChanges(Function<C, Map<K, V>> get, C original, C command)
    {
        Map<K, V> prev = original != null ? get.apply(original) : Collections.emptyMap();
        if (prev == null) prev = Collections.emptyMap();
        Map<K, V> value = get.apply(command);
        if (value == null) value = Collections.emptyMap();
        return estimateMapChanges(prev, value);
    }

    public static Mutation getCommandMutation(AccordCommandStore commandStore, AccordSafeCommand liveCommand, long timestampMicros)
    {
        return getCommandMutation(commandStore.id(), liveCommand.original(), liveCommand.current(), timestampMicros);
    }

    public static Mutation getCommandMutation(int storeId, Command original, Command command, long timestampMicros)
    {
        try
        {
            Invariants.checkArgument(original != command);

            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.EMPTY);
            int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestampMicros, nowInSeconds));

            addEnumCellIfModified(CommandsColumns.durability, Command::durability, builder, timestampMicros, nowInSeconds, original, command);
            addCellIfModified(CommandsColumns.route, Command::route, LocalVersionedSerializers.route, builder, timestampMicros, nowInSeconds, original, command);
            addSetChanges(CommandsColumns.listeners, Command::durableListeners, v -> serialize(v, LocalVersionedSerializers.listeners), builder, timestampMicros, nowInSeconds, original, command);
            addEnumCellIfModified(CommandsColumns.status, Command::saveStatus, builder, timestampMicros, nowInSeconds, original, command);
            addCellIfModified(CommandsColumns.execute_at, Command::executeAt, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, original, command);
            addCellIfModified(CommandsColumns.promised_ballot, Command::promised, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, original, command);
            addCellIfModified(CommandsColumns.accepted_ballot, Command::accepted, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, original, command);

            // TODO review this is just to work around Truncated not being committed but having a status after committed
            // so status claims it is committed.
            if (!command.isTruncated() && command.isCommitted())
            {
                Command.Committed committed = command.asCommitted();
                Command.Committed originalCommitted = original != null && original.isCommitted() ? original.asCommitted() : null;
                if (originalCommitted == null || committed.waitingOn != originalCommitted.waitingOn)
                    builder.addCell(live(CommandsColumns.waiting_on, timestampMicros, WaitingOnSerializer.serialize(committed.waitingOn)));
            }

            Row row = builder.build();
            if (row.columnCount() == 0)
                return null;

            ByteBuffer key = CommandsColumns.keyComparator.make(storeId,
                                                                command.txnId().domain().ordinal(),
                                                                serializeTimestamp(command.txnId())).serializeAsPartitionKey();
            PartitionUpdate update = PartitionUpdate.singleRowUpdate(Commands, key, row);
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

    private static <V> V serializeToken(Token token, ValueAccessor<V> accessor)
    {
        TokenType type = TokenType.valueOf(token);
        byte[] ordered = token.getPartitioner().getTokenFactory().toOrderedByteArray(token, ByteComparable.Version.OSS50);
        V value = accessor.allocate(ordered.length + 1);
        accessor.putByte(value, 0, type.value);
        ByteArrayAccessor.instance.copyTo(ordered, 0, value, accessor, 1, ordered.length);
        return value;
    }

    private static ByteBuffer serializeKey(PartitionKey key)
    {
        return TupleType.buildValue(UUIDSerializer.instance.serialize(key.tableId().asUUID()), key.partitionKey().getKey());
    }

    private static ByteBuffer serializeTimestamp(Timestamp timestamp)
    {
        return TupleType.buildValue(bytes(timestamp.msb), bytes(timestamp.lsb), bytes(timestamp.node.id));
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
        ByteBuffer[] split = TIMESTAMP_TYPE.split(ByteBufferAccessor.instance, bytes);
        return factory.create(split[0].getLong(), split[1].getLong(), new Node.Id(split[2].getInt()));
    }

    public static <V> Timestamp deserializeTimestampOrNull(Cell<V> cell)
    {
        if (cell == null)
            return null;
        ValueAccessor<V> accessor = cell.accessor();
        V value = cell.value();
        if (accessor.isEmpty(value))
            return null;
        V[] split = TIMESTAMP_TYPE.split(accessor, value);
        return Timestamp.fromBits(accessor.getLong(split[0], 0), accessor.getLong(split[1], 0), new Node.Id(accessor.getInt(split[2], 0)));
    }

    public static <V, T extends Timestamp> T deserializeTimestampOrDefault(V value, ValueAccessor<V> accessor, TimestampFactory<T> factory, T defaultVal)
    {
        if (value == null || accessor.isEmpty(value))
            return defaultVal;
        V[] split = TIMESTAMP_TYPE.split(accessor, value);
        return factory.create(accessor.getLong(split[0], 0), accessor.getLong(split[1], 0), new Node.Id(accessor.getInt(split[2], 0)));
    }

    public static <V, T extends Timestamp> T deserializeTimestampOrNull(V value, ValueAccessor<V> accessor, TimestampFactory<T> factory)
    {
        if (value == null || accessor.isEmpty(value))
            return null;
        V[] split = TIMESTAMP_TYPE.split(accessor, value);
        return factory.create(accessor.getLong(split[0], 0), accessor.getLong(split[1], 0), new Node.Id(accessor.getInt(split[2], 0)));
    }

    private static <T extends Timestamp> T deserializeTimestampOrNull(UntypedResultSet.Row row, String name, TimestampFactory<T> factory)
    {
        return deserializeTimestampOrNull(row.getBlob(name), factory);
    }

    private static <T extends Timestamp> T deserializeTimestampOrDefault(UntypedResultSet.Row row, String name, TimestampFactory<T> factory, T defaultVal)
    {
        return deserializeTimestampOrDefault(row.getBlob(name), ByteBufferAccessor.instance, factory, defaultVal);
    }

    private static ByteBuffer bytesOrNull(Row row, ColumnMetadata column)
    {
        Cell<?> cell = row.getCell(column);
        return cell != null && !cell.isTombstone() ? cell.buffer() : null;
    }

    private static <T extends Timestamp> T deserializeTimestampOrDefault(Row row, ColumnMetadata column, TimestampFactory<T> factory, T valIfNull)
    {
        ByteBuffer bytes = bytesOrNull(row, column);
        if (bytes == null)
            return valIfNull;
        T result = deserializeTimestampOrNull(bytes, factory);
        if (result == null)
            return valIfNull;
        return result;
    }

    public static Durability deserializeDurabilityOrNull(Cell cell)
    {
        return cell == null ? null : CommandSerializers.durability.forOrdinal(cell.accessor().getInt(cell.value(), 0));
    }

    public static SaveStatus deserializeSaveStatusOrNull(Cell cell)
    {
        return cell == null ? null : CommandSerializers.saveStatus.forOrdinal(cell.accessor().getInt(cell.value(), 0));
    }

    public static UntypedResultSet loadCommandRow(CommandStore commandStore, TxnId txnId)
    {
        String cql = "SELECT * FROM %s.%s " +
                     "WHERE store_id = ? " +
                     "AND domain = ? " +
                     "AND txn_id=(?, ?, ?)";

        return executeInternal(format(cql, ACCORD_KEYSPACE_NAME, COMMANDS),
                               commandStore.id(),
                               txnId.domain().ordinal(),
                               txnId.msb, txnId.lsb, txnId.node.id);
    }

    public static void findAllCommandsByDomain(int commandStore, Routable.Domain domain, Set<String> columns, Observable<UntypedResultSet.Row> callback)
    {
        WalkCommandsForDomain work = new WalkCommandsForDomain(commandStore, domain, columns, Stage.READ.executor(), callback);
        work.schedule();
    }

    private static abstract class TableWalk implements Runnable, DebuggableTask
    {
        private final long creationTimeNanos = Clock.Global.nanoTime();
        private final Executor executor;
        private final Observable<UntypedResultSet.Row> callback;
        private long startTimeNanos = -1;
        private int numQueries = 0;
        private UntypedResultSet.Row lastSeen = null;

        private TableWalk(Executor executor, Observable<UntypedResultSet.Row> callback)
        {
            this.executor = executor;
            this.callback = callback;
        }

        protected abstract UntypedResultSet query(UntypedResultSet.Row lastSeen);

        public final void schedule()
        {
            executor.execute(this);
        }

        @Override
        public final void run()
        {
            try
            {
                if (startTimeNanos == -1)
                    startTimeNanos = Clock.Global.nanoTime();
                numQueries++;
                UntypedResultSet result = query(lastSeen);
                if (result.isEmpty())
                {
                    callback.onCompleted();
                    return;
                }
                UntypedResultSet.Row lastRow = null;
                for (UntypedResultSet.Row row : result)
                {
                    callback.onNext(row);
                    lastRow = row;
                }
                lastSeen = lastRow;
                schedule();
            }
            catch (Throwable t)
            {
                callback.onError(t);
            }
        }

        @Override
        public long creationTimeNanos()
        {
            return creationTimeNanos;
        }

        @Override
        public long startTimeNanos()
        {
            return startTimeNanos;
        }

        @Override
        public String description()
        {
            return format("Table Walker for %s; queries = %d", getClass().getSimpleName(), numQueries);
        }
    }

    private static String selection(TableMetadata metadata, Set<String> requiredColumns, Set<String> forIteration)
    {
        StringBuilder selection = new StringBuilder();
        if (requiredColumns.isEmpty())
            selection.append("*");
        else
        {
            Sets.SetView<String> other = Sets.difference(requiredColumns, forIteration);
            for (String name : other)
            {
                ColumnMetadata meta = metadata.getColumn(new ColumnIdentifier(name, true));
                if (meta == null)
                    throw new IllegalArgumentException("Unknown column: " + name);
            }
            List<String> names = new ArrayList<>(forIteration.size() + other.size());
            names.addAll(forIteration);
            names.addAll(other);
            // this sort is to make sure the CQL is determanistic
            Collections.sort(names);
            for (int i = 0; i < names.size(); i++)
            {
                if (i > 0)
                    selection.append(", ");
                selection.append(names.get(i));
            }
        }
        return selection.toString();
    }

    private static class WalkCommandsForDomain extends TableWalk
    {
        private static final Set<String> COLUMNS_FOR_ITERATION = ImmutableSet.of("txn_id", "store_id", "domain");
        private final String cql;
        private final int storeId, domain;

        private WalkCommandsForDomain(int commandStore, Routable.Domain domain, Set<String> requiredColumns, Executor executor, Observable<UntypedResultSet.Row> callback)
        {
            super(executor, callback);
            this.storeId = commandStore;
            this.domain = domain.ordinal();
            cql = format("SELECT %s " +
                                "FROM %s " +
                                "WHERE store_id = ? " +
                                "      AND domain = ? " +
                                "      AND token(store_id, domain, txn_id) > token(?, ?, (?, ?, ?)) " +
                                "ALLOW FILTERING", selection(Commands, requiredColumns, COLUMNS_FOR_ITERATION), Commands);
        }

        @Override
        protected UntypedResultSet query(UntypedResultSet.Row lastSeen)
        {
            TxnId lastTxnId = lastSeen == null ?
                              new TxnId(0, 0, Txn.Kind.Read, Routable.Domain.Key, Node.Id.NONE)
                              : deserializeTxnId(lastSeen);
            return executeInternal(cql, storeId, domain, storeId, domain, lastTxnId.msb, lastTxnId.lsb, lastTxnId.node.id);
        }
    }

    public static void findAllKeysBetween(int commandStore,
                                          Token start, boolean startInclusive,
                                          Token end, boolean endInclusive,
                                          Observable<PartitionKey> callback)
    {
        //TODO (optimize) : CQL doesn't look smart enough to only walk Index.db, and ends up walking the Data.db file for each row in the partitions found (for frequent keys, this cost adds up)
        // it would be possible to find all SSTables that "could" intersect this range, then have a merge iterator over the Index.db (filtered to the range; index stores partition liveness)...
        KeysBetween work = new KeysBetween(commandStore,
                                           AccordKeyspace.serializeToken(start), startInclusive,
                                           AccordKeyspace.serializeToken(end), endInclusive,
                                           ImmutableSet.of("key"),
                                           Stage.READ.executor(), Observable.distinct(callback).map(AccordKeyspace::deserializeKey));
        work.schedule();
    }

    private static class KeysBetween extends TableWalk
    {
        private static final Set<String> COLUMNS_FOR_ITERATION = ImmutableSet.of("store_id", "key_token");

        private final int storeId;
        private final ByteBuffer start, end;
        private final String cqlFirst;
        private final String cqlContinue;

        private KeysBetween(int storeId,
                            ByteBuffer start, boolean startInclusive,
                            ByteBuffer end, boolean endInclusive,
                            Set<String> requiredColumns,
                            Executor executor, Observable<UntypedResultSet.Row> callback)
        {
            super(executor, callback);
            this.storeId = storeId;
            this.start = start;
            this.end = end;

            String selection = selection(TimestampsForKeys, requiredColumns, COLUMNS_FOR_ITERATION);
            this.cqlFirst = format("SELECT DISTINCT %s\n" +
                                          "FROM %s\n" +
                                          "WHERE store_id = ?\n" +
                                          (startInclusive ? "  AND key_token >= ?\n" : "  AND key_token > ?\n") +
                                          (endInclusive ? "  AND key_token <= ?\n" : "  AND key_token < ?\n") +
                                          "ALLOW FILTERING",
                                          selection, TimestampsForKeys);
            this.cqlContinue = format("SELECT DISTINCT %s\n" +
                                             "FROM %s\n" +
                                             "WHERE store_id = ?\n" +
                                             "  AND key_token > ?\n" +
                                             "  AND key > ?\n" +
                                             (endInclusive ? "  AND key_token <= ?\n" : "  AND key_token < ?\n") +
                                             "ALLOW FILTERING",
                                             selection, TimestampsForKeys);
        }

        @Override
        protected UntypedResultSet query(UntypedResultSet.Row lastSeen)
        {
            if (lastSeen == null)
            {
                return executeInternal(cqlFirst, storeId, start, end);
            }
            else
            {
                ByteBuffer previousToken = lastSeen.getBytes("key_token");
                ByteBuffer previousKey = lastSeen.getBytes("key");
                return executeInternal(cqlContinue, storeId, previousToken, previousKey, end);
            }
        }
    }

    public static Command loadCommand(AccordCommandStore commandStore, TxnId txnId)
    {
        commandStore.checkNotInStoreThread();
        return unsafeLoadCommand(commandStore, txnId);
    }

    static Command unsafeLoadCommand(AccordCommandStore commandStore, TxnId txnId)
    {
        UntypedResultSet rows = loadCommandRow(commandStore, txnId);
        if (rows.isEmpty())
            return null;
        UntypedResultSet.Row row = rows.one();

        try
        {
            checkState(deserializeTxnId(row).equals(txnId));

            CommonAttributes.Mutable attrs =
                new CommonAttributes.Mutable(txnId)
                                    .durability(deserializeDurability(row))
                                    .route(deserializeRouteOrNull(row))
                                    .setListeners(deserializeListeners(row));
            SaveStatus status = deserializeStatus(row);

            Timestamp executeAt = deserializeExecuteAtOrNull(row);
            Ballot promised = deserializePromisedOrNull(row);
            Ballot accepted = deserializeAcceptedOrNull(row);

            WaitingOnProvider waitingOn = deserializeWaitingOn(row);
            MessageProvider messages = commandStore.makeMessageProvider(txnId);

            return SerializerSupport.reconstruct(attrs, status, executeAt, promised, accepted, waitingOn, messages);
        }
        catch (Throwable t)
        {
            logger.error("Exception loading AccordCommand " + txnId, t);
            throw Throwables.unchecked(t);
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

    private static Route<?> deserializeRouteOrNull(ByteBuffer bytes) throws IOException
    {
        return bytes != null && !ByteBufferAccessor.instance.isEmpty(bytes) ? deserialize(bytes, LocalVersionedSerializers.route) : null;
    }

    private static Route<?> deserializeRouteOrNull(UntypedResultSet.Row row) throws IOException
    {
        return deserializeRouteOrNull(row.getBlob("route"));
    }

    public static Route<?> deserializeRouteOrNull(Cell<?> cell)
    {
        if (cell == null)
            return null;

        try
        {
            return deserializeRouteOrNull(cell.buffer());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static Listeners.Immutable deserializeListeners(UntypedResultSet.Row row) throws IOException
    {
        Set<ByteBuffer> serialized = row.getSet("listeners", BytesType.instance);
        if (serialized == null || serialized.isEmpty())
            return Listeners.Immutable.EMPTY;

        Listeners<Command.DurableAndIdempotentListener> result = new Listeners<>();
        for (ByteBuffer bytes : serialized)
            result.add(deserialize(bytes, LocalVersionedSerializers.listeners));
        return new Listeners.Immutable(result);
    }

    public static SaveStatus deserializeStatus(UntypedResultSet.Row row)
    {
        // TODO (performance, expected): something less brittle than ordinal, more efficient than values()
        return SaveStatus.values()[row.getInt("status")];
    }

    public static Timestamp deserializeExecuteAtOrNull(UntypedResultSet.Row row)
    {
        return deserializeTimestampOrNull(row, "execute_at", Timestamp::fromBits);
    }

    public static Ballot deserializePromisedOrNull(UntypedResultSet.Row row)
    {
        return deserializeTimestampOrNull(row.getBlob("promised_ballot"), Ballot::fromBits);
    }

    public static Ballot deserializeAcceptedOrNull(UntypedResultSet.Row row)
    {
        return deserializeTimestampOrNull(row.getBlob("accepted_ballot"), Ballot::fromBits);
    }

    private static WaitingOnProvider deserializeWaitingOn(UntypedResultSet.Row row)
    {
        ByteBuffer bytes = row.getBlob("waiting_on");

        return (deps) ->
        {
            if (bytes == null || !bytes.hasRemaining())
                return deps == null ? WaitingOn.EMPTY : WaitingOn.none(deps);

            try
            {
                return WaitingOnSerializer.deserialize(deps, new DataInputBuffer(bytes, false));
            }
            catch (IOException e)
            {
                throw Throwables.unchecked(e);
            }
        };
    }

    public static PartitionKey deserializeKey(ByteBuffer buffer)
    {
        ByteBuffer[] split = KEY_TYPE.split(ByteBufferAccessor.instance, buffer);
        TableId tableId = TableId.fromUUID(UUIDSerializer.instance.deserialize(split[0]));
        ByteBuffer key = split[1];

        TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
        if (metadata == null)
            throw new IllegalStateException("Table with id " + tableId + " could not be found; was it deleted?");
        return new PartitionKey(metadata.keyspace, tableId, metadata.partitioner.decorateKey(key));
    }

    public static PartitionKey deserializeKey(UntypedResultSet.Row row)
    {
        return deserializeKey(row.getBytes("key"));
    }

    public static Mutation getTimestampsForKeyMutation(int storeId, TimestampsForKey original, TimestampsForKey current, long timestampMicros)
    {
        try
        {
            Invariants.checkArgument(original != current);
            // TODO: convert to byte arrays
            ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;

            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.EMPTY);
            int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);
            LivenessInfo livenessInfo = LivenessInfo.create(timestampMicros, nowInSeconds);
            builder.addPrimaryKeyLivenessInfo(livenessInfo);
            addCellIfModified(TimestampsForKeyColumns.max_timestamp, TimestampsForKey::max, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, original, current);
            addCellIfModified(TimestampsForKeyColumns.last_executed_timestamp, TimestampsForKey::lastExecutedTimestamp, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, original, current);
            addCellIfModified(TimestampsForKeyColumns.last_executed_micros, TimestampsForKey::rawLastExecutedHlc, accessor::valueOf, builder, timestampMicros, nowInSeconds, original, current);
            addCellIfModified(TimestampsForKeyColumns.last_write_timestamp, TimestampsForKey::lastWriteTimestamp, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, original, current);

            Row row = builder.build();
            if (row.columnCount() == 0)
                return null;

            ByteBuffer key = TimestampsForKeyColumns.makePartitionKey(storeId, current.key());
            PartitionUpdate update = PartitionUpdate.singleRowUpdate(TimestampsForKeys, key, row);
            return new Mutation(update);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Mutation getTimestampsForKeyMutation(AccordCommandStore commandStore, AccordSafeTimestampsForKey liveTimestamps, long timestampMicros)
    {
        return getTimestampsForKeyMutation(commandStore.id(), liveTimestamps.original(), liveTimestamps.current(), timestampMicros);
    }

    public static UntypedResultSet loadTimestampsForKeyRow(CommandStore commandStore, PartitionKey key)
    {
        String cql = "SELECT * FROM %s.%s " +
                     "WHERE store_id = ? " +
                     "AND key_token = ? " +
                     "AND key=(?, ?)";

        return executeInternal(format(cql, ACCORD_KEYSPACE_NAME, TIMESTAMPS_FOR_KEY),
                               commandStore.id(),
                               serializeToken(key.token()),
                               key.tableId().asUUID(), key.partitionKey().getKey());
    }

    public static TimestampsForKey loadTimestampsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        commandStore.checkNotInStoreThread();
        return unsafeLoadTimestampsForKey(commandStore, key);
    }

    public static TimestampsForKey unsafeLoadTimestampsForKey(AccordCommandStore commandStore, PartitionKey key)
    {

        UntypedResultSet rows = loadTimestampsForKeyRow(commandStore, key);

        if (rows.isEmpty())
        {
            return null;
        }

        UntypedResultSet.Row row = rows.one();
        checkState(deserializeKey(row).equals(key));

        Timestamp max = deserializeTimestampOrDefault(row, "max_timestamp", Timestamp::fromBits, Timestamp.NONE);
        Timestamp lastExecutedTimestamp = deserializeTimestampOrDefault(row, "last_executed_timestamp", Timestamp::fromBits, Timestamp.NONE);
        long lastExecutedMicros = row.has("last_executed_micros") ? row.getLong("last_executed_micros") : 0;
        Timestamp lastWriteTimestamp = deserializeTimestampOrDefault(row, "last_write_timestamp", Timestamp::fromBits, Timestamp.NONE);

        return TimestampsForKey.SerializerSupport.create(key, max, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp);
    }

    private static <T extends Timestamp> void addSeriesMutations(CommandsForKeyAccessor accessor,
                                                                 CommandTimeseries.Update<T, ByteBuffer> update,
                                                                 PartitionUpdate.Builder partitionBuilder,
                                                                 Row.Builder rowBuilder,
                                                                 LivenessInfo livenessInfo,
                                                                 long timestampMicros,
                                                                 Row.Deletion deletion,
                                                                 Predicate<T> predicate)
    {
        if (update.isEmpty())
            return;

        update.forEachWrite((timestamp, bytes) -> {
            if (!predicate.test(timestamp))
                return;
            rowBuilder.newRow(Clustering.make(serializeTimestamp(timestamp)));
            rowBuilder.addCell(live(accessor.data, timestampMicros, bytes));
            rowBuilder.addPrimaryKeyLivenessInfo(livenessInfo);
            partitionBuilder.add(rowBuilder.build());
        });
        update.forEachDelete(timestamp -> {
            if (!predicate.test(timestamp))
                return;
            rowBuilder.newRow(Clustering.make(serializeTimestamp(timestamp)));
            rowBuilder.addRowDeletion(deletion);
            partitionBuilder.add(rowBuilder.build());
        });
    }

    private static <T extends Timestamp> void addSeriesMutations(CommandsForKeyAccessor accessor,
                                                                 CommandTimeseries.Update<T, ByteBuffer> common,
                                                                 CommandTimeseries.Update<T, ByteBuffer> update,
                                                                 PartitionUpdate.Builder partitionBuilder,
                                                                 Row.Builder rowBuilder,
                                                                 LivenessInfo livenessInfo,
                                                                 int nowInSeconds)
    {

        long timestampMicros = livenessInfo.timestamp();

        Row.Deletion deletion = common.numDeletes() + update.numDeletes() > 0 ?
                                Row.Deletion.regular(DeletionTime.build(timestampMicros, nowInSeconds)) :
                                null;

        addSeriesMutations(accessor, common, partitionBuilder, rowBuilder, livenessInfo, timestampMicros, deletion, ts -> !update.contains(ts));
        addSeriesMutations(accessor, update, partitionBuilder, rowBuilder, livenessInfo, timestampMicros, deletion, ts -> true);
    }

    private static DecoratedKey makeKey(CommandsForKeyAccessor accessor, int storeId, PartitionKey key)
    {
        Token token = key.token();
        ByteBuffer pk = accessor.keyComparator.make(storeId,
                                                    serializeToken(token),
                                                    serializeKey(key)).serializeAsPartitionKey();
        return accessor.table.partitioner.decorateKey(pk);
    }

    private static PartitionUpdate getCommandsForKeyPartitionUpdate(CommandsForKeyAccessor accessor, int storeId, PartitionKey key, CommandsForKeyUpdater<ByteBuffer> common, CommandsForKeyUpdater<ByteBuffer> update, long timestampMicros)
    {

        int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);
        LivenessInfo livenessInfo = LivenessInfo.create(timestampMicros, nowInSeconds);

        int expectedRows = common.totalChanges() + update.totalChanges();

        PartitionUpdate.Builder partitionBuilder = new PartitionUpdate.Builder(accessor.table,
                                                                               makeKey(accessor, storeId, key),
                                                                               accessor.columns,
                                                                               expectedRows);

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();

        addSeriesMutations(accessor, common.commands(), update.commands(), partitionBuilder, rowBuilder, livenessInfo, nowInSeconds);

        PartitionUpdate partitionUpdate = partitionBuilder.build();
        if (partitionUpdate.isEmpty())
            return null;
        return partitionUpdate;
    }

    public static Mutation getCommandsForKeyMutation(int storeId, CommandsForKeyUpdate update, long timestampMicros)
    {
        PartitionUpdate depsUpdate = getCommandsForKeyPartitionUpdate(DepsCommandsForKeysAccessor,
                                                                      storeId,
                                                                      update.key(),
                                                                      update.common(),
                                                                      update.deps(),
                                                                      timestampMicros);
        PartitionUpdate allUpdate = getCommandsForKeyPartitionUpdate(AllCommandsForKeysAccessor,
                                                                     storeId,
                                                                     update.key(),
                                                                     update.common(),
                                                                     update.all(),
                                                                     timestampMicros);

        if (depsUpdate == null && allUpdate == null)
            return null;
        if (depsUpdate == null)
            return new Mutation(allUpdate);
        else if (allUpdate == null)
            return new Mutation(depsUpdate);

        return new Mutation(ACCORD_KEYSPACE_NAME, depsUpdate.partitionKey(),
                            ImmutableMap.of(depsUpdate.metadata().id, depsUpdate, allUpdate.metadata().id, allUpdate),
                            MonotonicClock.Global.approxTime.now(), false);
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

    private static SinglePartitionReadCommand getCommandsForKeyRead(CommandsForKeyAccessor accessor, int storeId, PartitionKey key, long nowInSeconds)
    {
        return SinglePartitionReadCommand.create(accessor.table, nowInSeconds,
                                                 accessor.allColumns,
                                                 RowFilter.NONE,
                                                 DataLimits.NONE,
                                                 makeKey(accessor, storeId, key),
                                                 FULL_PARTITION);
    }

    public static SinglePartitionReadCommand getDepsCommandsForKeyRead(int storeId, PartitionKey key, int nowInSeconds)
    {
        return getCommandsForKeyRead(DepsCommandsForKeysAccessor, storeId, key, nowInSeconds);
    }

    public static SinglePartitionReadCommand getAllCommandsForKeyRead(int storeId, PartitionKey key, int nowInSeconds)
    {
        return getCommandsForKeyRead(AllCommandsForKeysAccessor, storeId, key, nowInSeconds);
    }

    static CommandsForKey unsafeLoadCommandsForKey(CommandsForKeyAccessor accessor, AccordCommandStore commandStore, PartitionKey key)
    {
        long timestampMicros = TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
        int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);

        SinglePartitionReadCommand command = getCommandsForKeyRead(accessor, commandStore.id(), key, nowInSeconds);


        ImmutableSortedMap.Builder<Timestamp, ByteBuffer> commands = new ImmutableSortedMap.Builder<>(Comparator.naturalOrder());

        try (ReadExecutionController controller = command.executionController();
             FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
        {
            if (!partitions.hasNext())
            {
                return null;
            }

            try (RowIterator partition = partitions.next())
            {
                while (partition.hasNext())
                {
                    Row row = partition.next();
                    Clustering<?> clustering = row.clustering();
                    Timestamp timestamp = deserializeTimestampOrNull(clusteringValue(clustering, 0), Timestamp::fromBits);
                    ByteBuffer data = cellValue(row, accessor.data);
                    if (data == null)
                        continue;
                    commands.put(timestamp, data);
                }
            }
            checkState(!partitions.hasNext());

            return CommandsForKey.SerializerSupport.create(key, CommandsForKeySerializer.loader, commands.build());
        }
        catch (Throwable t)
        {
            logger.error("Exception loading AccordCommandsForKey " + key, t);
            throw t;
        }
    }

    public static CommandsForKey unsafeLoadDepsCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        return unsafeLoadCommandsForKey(DepsCommandsForKeysAccessor, commandStore, key);
    }

    public static CommandsForKey unsafeLoadAllCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        return unsafeLoadCommandsForKey(AllCommandsForKeysAccessor, commandStore, key);
    }

    public static CommandsForKey loadDepsCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        commandStore.checkNotInStoreThread();
        return unsafeLoadCommandsForKey(DepsCommandsForKeysAccessor, commandStore, key);
    }

    public static CommandsForKey loadAllCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        commandStore.checkNotInStoreThread();
        return unsafeLoadCommandsForKey(AllCommandsForKeysAccessor, commandStore, key);
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

        private EpochDiskState withNewMaxEpoch(long epoch)
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

    private static EpochDiskState saveEpochDiskState(EpochDiskState diskState)
    {
        String cql = "INSERT INTO %s.%s (key, min_epoch, max_epoch) VALUES (0, ?, ?);";
        executeInternal(format(cql, ACCORD_KEYSPACE_NAME, EPOCH_METADATA),
                        diskState.minEpoch, diskState.maxEpoch);
        return diskState;
    }

    @Nullable
    @VisibleForTesting
    public static EpochDiskState loadEpochDiskState()
    {
        String cql = "SELECT * FROM %s.%s WHERE key=0";
        UntypedResultSet result = executeInternal(format(cql, ACCORD_KEYSPACE_NAME, EPOCH_METADATA));
        if (result.isEmpty())
            return null;
        UntypedResultSet.Row row = result.one();
        return EpochDiskState.create(row.getLong("min_epoch"), row.getLong("max_epoch"));
    }

    /**
     * Update the disk state for this epoch, if it's higher than the one we have one disk.
     *
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
        diskState = maybeUpdateMaxEpoch(diskState, topology.epoch());

        try
        {
            String cql = "UPDATE %s.%s SET topology=? WHERE epoch=?";
            executeInternal(format(cql, ACCORD_KEYSPACE_NAME, TOPOLOGIES),
                            serialize(topology, LocalVersionedSerializers.topology), topology.epoch());
            flush(Topologies);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }

        return diskState;
    }

    public static EpochDiskState markRemoteTopologySync(Node.Id node, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE %s.%s SET remote_sync_complete = remote_sync_complete + ? WHERE epoch = ?";
        executeInternal(format(cql, ACCORD_KEYSPACE_NAME, TOPOLOGIES),
                        Collections.singleton(node.id), epoch);
        flush(Topologies);
        return diskState;
    }

    public static EpochDiskState markClosed(Ranges ranges, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE %s.%s SET closed = closed + ? WHERE epoch = ?";
        executeInternal(String.format(cql, ACCORD_KEYSPACE_NAME, TOPOLOGIES),
                        KeySerializers.rangesToBlobMap(ranges), epoch);
        flush(Topologies);
        return diskState;
    }

    public static EpochDiskState markRedundant(Ranges ranges, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE %s.%s SET redundant = redundant + ? WHERE epoch = ?";
        executeInternal(String.format(cql, ACCORD_KEYSPACE_NAME, TOPOLOGIES),
                        KeySerializers.rangesToBlobMap(ranges), epoch);
        flush(Topologies);
        return diskState;
    }

    public static EpochDiskState setNotifyingLocalSync(long epoch, Set<Node.Id> pending, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE %s.%s SET sync_state = ?, pending_sync_notify = ? WHERE epoch = ?";
        executeInternal(format(cql, ACCORD_KEYSPACE_NAME, TOPOLOGIES),
                        SyncStatus.NOTIFYING.ordinal(),
                        pending.stream().map(i -> i.id).collect(Collectors.toSet()),
                        epoch);
        return diskState;
    }

    public static EpochDiskState markLocalSyncAck(Node.Id node, long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE %s.%s SET pending_sync_notify = pending_sync_notify - ? WHERE epoch = ?";
        executeInternal(format(cql, ACCORD_KEYSPACE_NAME, TOPOLOGIES),
                        Collections.singleton(node.id), epoch);
        return diskState;
    }

    public static EpochDiskState setCompletedLocalSync(long epoch, EpochDiskState diskState)
    {
        diskState = maybeUpdateMaxEpoch(diskState, epoch);
        String cql = "UPDATE %s.%s SET sync_state = ?, pending_sync_notify = {} WHERE epoch = ?";
        executeInternal(format(cql, ACCORD_KEYSPACE_NAME, TOPOLOGIES),
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
            String cql = "DELETE FROM %s.%s WHERE epoch = ?";
            executeInternal(format(cql, ACCORD_KEYSPACE_NAME, TOPOLOGIES), delete);
        }
        return diskState;
    }

    public interface TopologyLoadConsumer
    {
        void load(long epoch, Topology topology, SyncStatus syncStatus, Set<Node.Id> pendingSyncNotify, Set<Node.Id> remoteSyncComplete, Ranges closed, Ranges redundant);
    }

    @VisibleForTesting
    public static void loadEpoch(long epoch, TopologyLoadConsumer consumer) throws IOException
    {
        String cql = format("SELECT * FROM %s.%s WHERE epoch=?", ACCORD_KEYSPACE_NAME, TOPOLOGIES);

        UntypedResultSet result = executeInternal(cql, epoch);
        checkState(!result.isEmpty(), "Nothing found for epoch %d", epoch);
        UntypedResultSet.Row row = result.one();
        Topology topology = row.has("topology")
                            ? deserialize(row.getBytes("topology"), LocalVersionedSerializers.topology)
                            : null;

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

        consumer.load(epoch, topology, syncStatus, pendingSyncNotify, remoteSyncComplete, closed, redundant);

    }

    public static EpochDiskState loadTopologies(TopologyLoadConsumer consumer)
    {
        try
        {
            EpochDiskState diskState = loadEpochDiskState();
            if (diskState == null)
                return EpochDiskState.EMPTY;

            for (long epoch=diskState.minEpoch; epoch<=diskState.maxEpoch; epoch++)
                loadEpoch(epoch, consumer);

            return diskState;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static IMutation getCommandStoreMetadataMutation(String cql, ByteBuffer... values)
    {
        ClientState clientState = ClientState.forInternalCalls();
        ModificationStatement statement = (ModificationStatement) QueryProcessor.parseStatement(cql).prepare(ClientState.forInternalCalls());
        QueryOptions options = QueryOptions.forInternalCalls(Arrays.asList(values));

        long tsMicros = TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());

        while (true)
        {
            long prev = commandStoreMetadataTimestamp.get();
            if (prev >= tsMicros)
                tsMicros = prev + 1;

            if (commandStoreMetadataTimestamp.compareAndSet(prev, tsMicros))
                break;
        }

        return Iterables.getOnlyElement(statement.getMutations(clientState, options, true, tsMicros, (int) TimeUnit.MICROSECONDS.toSeconds(tsMicros), Clock.Global.nanoTime(), false));
    }


    private static <T> Future<?> updateCommandStoreMetadata(CommandStore commandStore, String column, T value, LocalVersionedSerializer<T> serializer)
    {
        String cql = format("UPDATE %s.%s SET %s=? WHERE store_id=?", ACCORD_KEYSPACE_NAME, COMMAND_STORE_METADATA, column);
        try
        {
            IMutation mutation = getCommandStoreMetadataMutation(cql, serialize(value, serializer), bytes(commandStore.id()));
            return Stage.MUTATION.submit(mutation::apply);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public static Future<?> updateRejectBefore(CommandStore commandStore, ReducingRangeMap<Timestamp> rejectBefore)
    {
        return updateCommandStoreMetadata(commandStore, "reject_before", rejectBefore, LocalVersionedSerializers.rejectBefore);
    }

    public static Future<?> updateDurableBefore(CommandStore commandStore, DurableBefore durableBefore)
    {
        return updateCommandStoreMetadata(commandStore, "durable_before", durableBefore, LocalVersionedSerializers.durableBefore);
    }

    public static Future<?> updateRedundantBefore(CommandStore commandStore, RedundantBefore redundantBefore)
    {
        return updateCommandStoreMetadata(commandStore, "redundant_before", redundantBefore, LocalVersionedSerializers.redundantBefore);
    }

    public static Future<?> updateBootstrapBeganAt(CommandStore commandStore, NavigableMap<TxnId, Ranges> bootstrapBeganAt)
    {
        return updateCommandStoreMetadata(commandStore, "bootstrap_began_at", bootstrapBeganAt, LocalVersionedSerializers.bootstrapBeganAt);
    }

    public static Future<?> updateSafeToRead(CommandStore commandStore, NavigableMap<Timestamp, Ranges> safeToRead)
    {
        return updateCommandStoreMetadata(commandStore, "safe_to_read", safeToRead, LocalVersionedSerializers.safeToRead);
    }

    public interface CommandStoreMetadataConsumer
    {
        void accept(ReducingRangeMap<Timestamp> rejectBefore, DurableBefore durableBefore, RedundantBefore redundantBefore, NavigableMap<TxnId, Ranges> bootstrapBeganAt, NavigableMap<Timestamp, Ranges> safeToRead);

    }
    public static void loadCommandStoreMetadata(int id, CommandStoreMetadataConsumer consumer)
    {
        UntypedResultSet result = executeOnceInternal(format("SELECT * FROM %s.%s WHERE store_id=?", ACCORD_KEYSPACE_NAME, COMMAND_STORE_METADATA), id);
        ReducingRangeMap<Timestamp> rejectBefore = null;
        DurableBefore durableBefore = null;
        RedundantBefore redundantBefore = null;
        NavigableMap<TxnId, Ranges> bootstrapBeganAt = null;
        NavigableMap<Timestamp, Ranges> safeToRead = null;
        if (!result.isEmpty())
        {
            UntypedResultSet.Row row = Iterables.getOnlyElement(result);
            try
            {
                if (row.has("reject_before"))
                    rejectBefore = deserialize(row.getBlob("reject_before"), LocalVersionedSerializers.rejectBefore);
                if (row.has("durable_before"))
                    durableBefore = deserialize(row.getBlob("durable_before"), LocalVersionedSerializers.durableBefore);
                if (row.has("redundant_before"))
                    redundantBefore = deserialize(row.getBlob("redundant_before"), LocalVersionedSerializers.redundantBefore);
                if (row.has("bootstrap_began_at"))
                    bootstrapBeganAt = deserialize(row.getBlob("bootstrap_began_at"), LocalVersionedSerializers.bootstrapBeganAt);
                if (row.has("safe_to_read"))
                    safeToRead = deserialize(row.getBlob("safe_to_read"), LocalVersionedSerializers.safeToRead);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
        consumer.accept(rejectBefore, durableBefore, redundantBefore, bootstrapBeganAt, safeToRead);
    }

}
