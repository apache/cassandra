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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.impl.CommandsForKey;
import accord.impl.CommandTimeseries;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommonAttributes;
import accord.local.Listeners;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
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
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
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
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.serializers.CommandStoreSerializers;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;
import org.apache.cassandra.service.accord.serializers.DepsSerializer;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.serializers.ListenerSerializers;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.db.rows.BufferCell.live;
import static org.apache.cassandra.db.rows.BufferCell.tombstone;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class AccordKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(AccordKeyspace.class);

    public static final String COMMANDS = "commands";
    public static final String COMMANDS_FOR_KEY = "commands_for_key";
    public static final String TOPOLOGIES = "topologies";
    public static final String EPOCH_METADATA = "epoch_metadata";
    public static final String COMMAND_STORE_METADATA = "command_store_metadata";

    private static final TupleType TIMESTAMP_TYPE = new TupleType(Lists.newArrayList(LongType.instance, LongType.instance, Int32Type.instance));
    private static final String TIMESTAMP_TUPLE = TIMESTAMP_TYPE.asCQL3Type().toString();
    private static final TupleType KEY_TYPE = new TupleType(Arrays.asList(UUIDType.instance, BytesType.instance));
    private static final String KEY_TUPLE = KEY_TYPE.asCQL3Type().toString();

    private static final ClusteringIndexFilter FULL_PARTITION = new ClusteringIndexSliceFilter(Slices.ALL, false);

    public enum SeriesKind
    {
        BY_ID(CommandsForKey::byId),
        BY_EXECUTE_AT(CommandsForKey::byExecuteAt);

        private final Function<CommandsForKey, CommandTimeseries<?>> getSeries;

        SeriesKind(Function<CommandsForKey, CommandTimeseries<?>> getSeries)
        {
            this.getSeries = getSeries;
        }

        ImmutableSortedMap<Timestamp, ByteBuffer> getValues(CommandsForKey cfk)
        {
            if (cfk == null)
                return ImmutableSortedMap.of();

            CommandTimeseries<?> series = getSeries.apply(cfk);
            return (ImmutableSortedMap<Timestamp, ByteBuffer>) series.commands;
        }
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

    // TODO: store timestamps as blobs (confirm there are no negative numbers, or offset)
    private static final TableMetadata Commands =
        parse(COMMANDS,
              "accord commands",
              "CREATE TABLE %s ("
              + "store_id int,"
              + "domain int," // this is stored as part of txn_id, used currently for more cheaper scans of the table
              + format("txn_id %s,", TIMESTAMP_TUPLE)
              + "status int,"
              + "home_key blob,"
              + "progress_key blob,"
              + "route blob,"
              + "durability int,"
              + "txn blob,"
              + format("execute_at %s,", TIMESTAMP_TUPLE)
              + format("promised_ballot %s,", TIMESTAMP_TUPLE)
              + format("accepted_ballot %s,", TIMESTAMP_TUPLE)
              + "dependencies blob,"
              + "writes blob,"
              + "result blob,"
              + format("waiting_on_commit set<%s>,", TIMESTAMP_TUPLE)
              + format("waiting_on_apply map<%s, blob>,", TIMESTAMP_TUPLE)
              + "listeners set<blob>, "
              + "PRIMARY KEY((store_id, domain, txn_id))"
              + ')')
        .partitioner(new LocalPartitioner(CompositeType.getInstance(Int32Type.instance, Int32Type.instance, TIMESTAMP_TYPE)))
        .build();

    // TODO: naming is not very clearly distinct from the base serializers
    private static class LocalVersionedSerializers
    {
        static final LocalVersionedSerializer<Route<?>> route = localSerializer(KeySerializers.route);
        static final LocalVersionedSerializer<AccordRoutingKey> routingKey = localSerializer(AccordRoutingKey.serializer);
        static final LocalVersionedSerializer<PartialTxn> partialTxn = localSerializer(CommandSerializers.partialTxn);
        static final LocalVersionedSerializer<PartialDeps> partialDeps = localSerializer(DepsSerializer.partialDeps);
        static final LocalVersionedSerializer<Writes> writes = localSerializer(CommandSerializers.writes);
        static final LocalVersionedSerializer<TxnData> result = localSerializer(TxnData.serializer);
        static final LocalVersionedSerializer<Command.DurableAndIdempotentListener> listeners = localSerializer(ListenerSerializers.listener);
        static final LocalVersionedSerializer<Topology> topology = localSerializer(TopologySerializers.topology);
        static final LocalVersionedSerializer<ReducingRangeMap<Timestamp>> rejectBefore = localSerializer(CommandStoreSerializers.rejectBefore);
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

    private static class CommandsColumns
    {
        static final ClusteringComparator keyComparator = Commands.partitionKeyAsClusteringComparator();
        static final ColumnMetadata status = getColumn(Commands, "status");
        static final ColumnMetadata home_key = getColumn(Commands, "home_key");
        static final ColumnMetadata progress_key = getColumn(Commands, "progress_key");
        static final ColumnMetadata route = getColumn(Commands, "route");
        static final ColumnMetadata durability = getColumn(Commands, "durability");
        static final ColumnMetadata txn = getColumn(Commands, "txn");
        static final ColumnMetadata execute_at = getColumn(Commands, "execute_at");
        static final ColumnMetadata promised_ballot = getColumn(Commands, "promised_ballot");
        static final ColumnMetadata accepted_ballot = getColumn(Commands, "accepted_ballot");
        static final ColumnMetadata dependencies = getColumn(Commands, "dependencies");
        static final ColumnMetadata writes = getColumn(Commands, "writes");
        static final ColumnMetadata result = getColumn(Commands, "result");
        static final ColumnMetadata waiting_on_commit = getColumn(Commands, "waiting_on_commit");
        static final ColumnMetadata waiting_on_apply = getColumn(Commands, "waiting_on_apply");
        static final ColumnMetadata listeners = getColumn(Commands, "listeners");
    }

    private static final TableMetadata CommandsForKeys =
        parse(COMMANDS_FOR_KEY,
              "accord commands per key",
              "CREATE TABLE %s ("
              + "store_id int, "
              + "key_token blob, " // can't use "token" as this is restricted word in CQL
              + format("key %s, ", KEY_TUPLE)
              + format("max_timestamp %s static, ", TIMESTAMP_TUPLE)
              + format("last_executed_timestamp %s static, ", TIMESTAMP_TUPLE)
              + "last_executed_micros bigint static, "
              + format("last_write_timestamp %s static, ", TIMESTAMP_TUPLE)
              + format("blind_witnessed set<%s> static, ", TIMESTAMP_TUPLE)
              + "series int, "
              + format("timestamp %s, ", TIMESTAMP_TUPLE)
              + "data blob, "
              + "PRIMARY KEY((store_id, key_token, key), series, timestamp)"
              + ')')
        .partitioner(new LocalPartitioner(CompositeType.getInstance(Int32Type.instance, BytesType.instance, KEY_TYPE)))
        .build();

    private static class CommandsForKeyColumns
    {
        static final ClusteringComparator keyComparator = CommandsForKeys.partitionKeyAsClusteringComparator();
        static final ColumnFilter allColumns = ColumnFilter.all(CommandsForKeys);
        static final ColumnMetadata max_timestamp = getColumn(CommandsForKeys, "max_timestamp");
        static final ColumnMetadata last_executed_timestamp = getColumn(CommandsForKeys, "last_executed_timestamp");
        static final ColumnMetadata last_executed_micros = getColumn(CommandsForKeys, "last_executed_micros");
        static final ColumnMetadata last_write_timestamp = getColumn(CommandsForKeys, "last_write_timestamp");
        static final ColumnMetadata blind_witnessed = getColumn(CommandsForKeys, "blind_witnessed");

        static final ColumnMetadata series = getColumn(CommandsForKeys, "series");
        static final ColumnMetadata timestamp = getColumn(CommandsForKeys, "timestamp");
        static final ColumnMetadata data = getColumn(CommandsForKeys, "data");

        static final Columns statics = Columns.from(Lists.newArrayList(max_timestamp, last_executed_timestamp, last_executed_micros, last_write_timestamp, blind_witnessed));
        static final Columns regulars = Columns.from(Lists.newArrayList(data));
        private static final RegularAndStaticColumns all = new RegularAndStaticColumns(statics, regulars);
        private static final RegularAndStaticColumns justStatic = new RegularAndStaticColumns(statics, Columns.NONE);
        private static final RegularAndStaticColumns justRegular = new RegularAndStaticColumns(Columns.NONE, regulars);

        static boolean hasStaticChanges(CommandsForKey original, CommandsForKey current)
        {
            return valueModified(CommandsForKey::max, original, current)
                   || valueModified(CommandsForKey::lastExecutedTimestamp, original, current)
                   || valueModified(CommandsForKey::lastWriteTimestamp, original, current);
        }

        private static boolean hasRegularChanges(CommandsForKey original, CommandsForKey current)
        {
            return valueModified(CommandsForKey::byId, original, current)
                   || valueModified(CommandsForKey::byExecuteAt, original, current);
        }

        static RegularAndStaticColumns columnsFor(CommandsForKey original, CommandsForKey current)
        {
            boolean hasStaticChanges = hasStaticChanges(original, current);
            boolean hasRegularChanges = hasRegularChanges(original, current);

            if (hasStaticChanges && hasRegularChanges)
                return all;
            else if (hasStaticChanges)
                return justStatic;
            else if (hasRegularChanges)
                return justRegular;
            else
                throw new IllegalArgumentException("No Static or Regular columns changed for CFK " + current.key());
        }
    }

    private static final TableMetadata Topologies =
        parse(TOPOLOGIES,
              "accord topologies",
              "CREATE TABLE %s (" +
              "epoch bigint primary key, " +
              "topology blob, " +
              "sync_state int, " +
              "pending_sync_notify set<int>, " + // nodes that need to be told we're synced
              "remote_sync_complete set<int> " +  // nodes that have told us they're synced
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

    private static Tables tables()
    {
        return Tables.of(Commands, CommandsForKeys, Topologies, EpochMetadata, CommandStoreMetadata);
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

    private static <T> T deserializeOrNull(ByteBuffer bytes, LocalVersionedSerializer<T> serializer) throws IOException
    {
        return bytes != null && !ByteBufferAccessor.instance.isEmpty(bytes) ? deserialize(bytes, serializer) : null;
    }

    private static ImmutableSortedMap<Timestamp, TxnId> deserializeWaitingOnApply(Map<ByteBuffer, ByteBuffer> serialized)
    {
        if (serialized == null || serialized.isEmpty())
            return ImmutableSortedMap.of();

        NavigableMap<Timestamp, TxnId> result = new TreeMap<>();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : serialized.entrySet())
            result.put(deserializeTimestampOrNull(entry.getKey(), Timestamp::fromBits), deserializeTimestampOrNull(entry.getValue(), TxnId::fromBits));
        return ImmutableSortedMap.copyOf(result);
    }

    private static <T extends Timestamp> ImmutableSortedSet<T> deserializeTimestampSet(Set<ByteBuffer> serialized, TimestampFactory<T> timestampFactory)
    {
        if (serialized == null || serialized.isEmpty())
            return ImmutableSortedSet.of();

        List<T> result = new ArrayList<>(serialized.size());
        for (ByteBuffer bytes : serialized)
            result.add(deserializeTimestampOrNull(bytes, timestampFactory));

        return ImmutableSortedSet.copyOf(result);
    }

    private static ImmutableSortedSet<TxnId> deserializeTxnIdNavigableSet(UntypedResultSet.Row row, String name)
    {
        return deserializeTimestampSet(row.getSet(name, BytesType.instance), TxnId::fromBits);
    }

    private static Listeners.Immutable deserializeListeners(Set<ByteBuffer> serialized) throws IOException
    {
        if (serialized == null || serialized.isEmpty())
            return Listeners.Immutable.EMPTY;
        Listeners result = new Listeners();
        for (ByteBuffer bytes : serialized)
        {
            result.add(deserialize(bytes, LocalVersionedSerializers.listeners));
        }
        return new Listeners.Immutable(result);
    }

    private static Listeners.Immutable deserializeListeners(UntypedResultSet.Row row, String name) throws IOException
    {
        return deserializeListeners(row.getSet(name, BytesType.instance));
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

    private static <C, V> void addCellIfModified(ColumnMetadata column, Function<C, V> get, SerializeFunction<V> serialize, Row.Builder builder, long timestampMicros, C original, C current) throws IOException
    {
        if (valueModified(get, original, current))
            builder.addCell(live(column, timestampMicros, serialize.apply(get.apply(current))));
    }

    private static <C extends Command, V> void addCellIfModified(ColumnMetadata column, Function<C, V> get, LocalVersionedSerializer<V> serializer, Row.Builder builder, long timestampMicros, C original, C command) throws IOException
    {
        addCellIfModified(column, get, v -> serializeOrNull(v, serializer), builder, timestampMicros, original, command);
    }

    private static <C extends Command, V> void addKeyCellIfModified(ColumnMetadata column, Function<C, V> get, Row.Builder builder, long timestampMicros, C original, C command) throws IOException
    {
        addCellIfModified(column, get, v -> serializeOrNull((AccordRoutingKey) v, LocalVersionedSerializers.routingKey), builder, timestampMicros, original, command);
    }

    private static <C extends Command, V extends Enum<V>> void addEnumCellIfModified(ColumnMetadata column, Function<C, V> get, Row.Builder builder, long timestampMicros, C original, C command) throws IOException
    {
        // TODO: convert to byte arrays
        ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;
        addCellIfModified(column, get, v -> accessor.valueOf(v.ordinal()), builder, timestampMicros, original, command);
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

            addEnumCellIfModified(CommandsColumns.status, Command::saveStatus, builder, timestampMicros, original, command);
            addKeyCellIfModified(CommandsColumns.home_key, Command::homeKey, builder, timestampMicros, original, command);
            addKeyCellIfModified(CommandsColumns.progress_key, Command::progressKey, builder, timestampMicros, original, command);
            addCellIfModified(CommandsColumns.route, Command::route, LocalVersionedSerializers.route, builder, timestampMicros, original, command);
            addEnumCellIfModified(CommandsColumns.durability, Command::durability, builder, timestampMicros, original, command);
            addCellIfModified(CommandsColumns.txn, Command::partialTxn, LocalVersionedSerializers.partialTxn, builder, timestampMicros, original, command);

            addCellIfModified(CommandsColumns.execute_at, Command::executeAt, AccordKeyspace::serializeTimestamp, builder, timestampMicros, original, command);
            addCellIfModified(CommandsColumns.promised_ballot, Command::promised, AccordKeyspace::serializeTimestamp, builder, timestampMicros, original, command);
            addCellIfModified(CommandsColumns.accepted_ballot, Command::accepted, AccordKeyspace::serializeTimestamp, builder, timestampMicros, original, command);

            addCellIfModified(CommandsColumns.dependencies, Command::partialDeps, LocalVersionedSerializers.partialDeps, builder, timestampMicros, original, command);

            addSetChanges(CommandsColumns.listeners, Command::durableListeners, v -> serialize(v, LocalVersionedSerializers.listeners), builder, timestampMicros, nowInSeconds, original, command);

            if (command.isCommitted())
            {
                Command.Committed committed = command.asCommitted();
                Command.Committed originalCommitted = original != null && original.isCommitted() ? original.asCommitted() : null;
                addSetChanges(CommandsColumns.waiting_on_commit, Command.Committed::waitingOnCommit, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, originalCommitted, committed);
                addMapChanges(CommandsColumns.waiting_on_apply, Command.Committed::waitingOnApply, AccordKeyspace::serializeTimestamp, AccordKeyspace::serializeTimestamp, builder, timestampMicros, nowInSeconds, originalCommitted, committed);
                if (command.isExecuted())
                {
                    Command.Executed executed = command.asExecuted();
                    Command.Executed originalExecuted = original != null && original.isExecuted() ? original.asExecuted() : null;
                    addCellIfModified(CommandsColumns.writes, Command.Executed::writes, v -> serialize(v, LocalVersionedSerializers.writes), builder, timestampMicros, originalExecuted, executed);
                    addCellIfModified(CommandsColumns.result, Command.Executed::result, v -> serialize((TxnData) v, LocalVersionedSerializers.result), builder, timestampMicros, originalExecuted, executed);
                }
            }

            Row row = builder.build();
            if (row.isEmpty())
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

    public static <T extends Timestamp> T deserializeTimestampOrNull(ByteBuffer bytes, TimestampFactory<T> factory)
    {
        if (bytes == null || ByteBufferAccessor.instance.isEmpty(bytes))
            return null;
        ByteBuffer[] split = TIMESTAMP_TYPE.split(ByteBufferAccessor.instance, bytes);
        return factory.create(split[0].getLong(), split[1].getLong(), new Node.Id(split[2].getInt()));
    }

    private static <T extends Timestamp> T deserializeTimestampOrNull(UntypedResultSet.Row row, String name, TimestampFactory<T> factory)
    {
        return deserializeTimestampOrNull(row.getBlob(name), factory);
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

    private static <T> T deserializeWithVersionOr(UntypedResultSet.Row row, String dataColumn, LocalVersionedSerializer<T> serializer, Supplier<T> defaultSupplier) throws IOException
    {
        if (!row.has(dataColumn))
            return defaultSupplier.get();

        return deserialize(row.getBlob(dataColumn), serializer);
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
                                           Stage.READ.executor(), Observable.distinct(callback).map(value -> AccordKeyspace.deserializeKey(value)));
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

            String selection = selection(CommandsForKeys, requiredColumns, COLUMNS_FOR_ITERATION);
            this.cqlFirst = format("SELECT DISTINCT %s\n" +
                                          "FROM %s\n" +
                                          "WHERE store_id = ?\n" +
                                          (startInclusive ? "  AND key_token >= ?\n" : "  AND key_token > ?\n") +
                                          (endInclusive ? "  AND key_token <= ?\n" : "  AND key_token < ?\n") +
                                          "ALLOW FILTERING",
                                          selection, CommandsForKeys);
            this.cqlContinue = format("SELECT DISTINCT %s\n" +
                                             "FROM %s\n" +
                                             "WHERE store_id = ?\n" +
                                             "  AND key_token > ?\n" +
                                             "  AND key > ?\n" +
                                             (endInclusive ? "  AND key_token <= ?\n" : "  AND key_token < ?\n") +
                                             "ALLOW FILTERING",
                                             selection, CommandsForKeys);
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

        UntypedResultSet rows = loadCommandRow(commandStore, txnId);

        if (rows.isEmpty())
        {
            return null;
        }

        try
        {
            UntypedResultSet.Row row = rows.one();
            Invariants.checkState(deserializeTxnId(row).equals(txnId));
            SaveStatus status = deserializeStatus(row);
            CommonAttributes.Mutable attributes = new CommonAttributes.Mutable(txnId);
            // TODO: something less brittle than ordinal, more efficient than values()
            attributes.durability(Status.Durability.values()[row.getInt("durability", 0)]);
            attributes.homeKey(deserializeOrNull(row.getBlob("home_key"), LocalVersionedSerializers.routingKey));
            attributes.progressKey(deserializeOrNull(row.getBlob("progress_key"), LocalVersionedSerializers.routingKey));
            attributes.route(deserializeOrNull(row.getBlob("route"), LocalVersionedSerializers.route));
            attributes.partialTxn(deserializeTxn(row));
            attributes.partialDeps(deserializeDependencies(row));
            attributes.setListeners(deserializeListeners(row, "listeners"));

            Timestamp executeAt = deserializeExecuteAt(row);
            Ballot promised = deserializeTimestampOrNull(row, "promised_ballot", Ballot::fromBits);
            Ballot accepted = deserializeTimestampOrNull(row, "accepted_ballot", Ballot::fromBits);
            ImmutableSortedSet<TxnId> waitingOnCommit = deserializeTxnIdNavigableSet(row, "waiting_on_commit");
            ImmutableSortedMap<Timestamp, TxnId> waitingOnApply = deserializeWaitingOnApply(row.getMap("waiting_on_apply", BytesType.instance, BytesType.instance));
            Writes writes = deserializeWithVersionOr(row, "writes", LocalVersionedSerializers.writes, () -> null);
            Result result = deserializeWithVersionOr(row, "result", LocalVersionedSerializers.result, () -> null);

            switch (status.status)
            {
                case NotWitnessed:
                    return Command.SerializerSupport.notWitnessed(attributes, promised);
                case PreAccepted:
                    return Command.SerializerSupport.preaccepted(attributes, executeAt, promised);
                case AcceptedInvalidate:
                case Accepted:
                case PreCommitted:
                    return Command.SerializerSupport.accepted(attributes, status, executeAt, promised, accepted);
                case Committed:
                case ReadyToExecute:
                    return Command.SerializerSupport.committed(attributes, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
                case PreApplied:
                case Applied:
                case Invalidated:
                    return Command.SerializerSupport.executed(attributes, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply, writes, result);
                default:
                    throw new IllegalStateException("Unhandled status " + status);
            }
        }
        catch (IOException e)
        {
            logger.error("Exception loading AccordCommand " + txnId, e);
            throw new RuntimeException(e);
        }
        catch (Throwable t)
        {
            logger.error("Exception loading AccordCommand " + txnId, t);
            throw t;
        }
    }

    public static PartialDeps deserializeDependencies(UntypedResultSet.Row row) throws IOException
    {
        return deserializeOrNull(row.getBlob("dependencies"), LocalVersionedSerializers.partialDeps);
    }

    public static Timestamp deserializeExecuteAt(UntypedResultSet.Row row)
    {
        return deserializeTimestampOrNull(row, "execute_at", Timestamp::fromBits);
    }

    public static SaveStatus deserializeStatus(UntypedResultSet.Row row)
    {
        return SaveStatus.values()[row.getInt("status")];
    }

    public static TxnId deserializeTxnId(UntypedResultSet.Row row)
    {
        return deserializeTimestampOrNull(row, "txn_id", TxnId::fromBits);
    }

    public static PartialTxn deserializeTxn(UntypedResultSet.Row row) throws IOException
    {
        return deserializeOrNull(row.getBlob("txn"), LocalVersionedSerializers.partialTxn);
    }

    public static PartitionKey deserializeKey(UntypedResultSet.Row row)
    {
        ByteBuffer[] split = KEY_TYPE.split(ByteBufferAccessor.instance, row.getBytes("key"));
        TableId tableId = TableId.fromUUID(UUIDSerializer.instance.deserialize(split[0]));
        ByteBuffer key = split[1];

        TableMetadata metadata = Schema.instance.getTableMetadata(tableId);
        if (metadata == null)
            throw new IllegalStateException("Table with id " + tableId + " could not be found; was it deleted?");
        return new PartitionKey(metadata.keyspace, tableId, metadata.partitioner.decorateKey(key));
    }

    private static void addSeriesMutations(ImmutableSortedMap<Timestamp, ByteBuffer> prev,
                                           ImmutableSortedMap<Timestamp, ByteBuffer> value,
                                           SeriesKind kind,
                                           PartitionUpdate.Builder partitionBuilder,
                                           Row.Builder rowBuilder,
                                           long timestampMicros,
                                           int nowInSeconds)
    {
        if (prev == value)
            return;

        Set<Timestamp> deletions = Sets.difference(prev.keySet(), value.keySet());

        Row.Deletion deletion = !deletions.isEmpty() ?
                                Row.Deletion.regular(DeletionTime.build(timestampMicros, nowInSeconds)) :
                                null;
        ByteBuffer ordinalBytes = bytes(kind.ordinal());
        value.forEach((timestamp, bytes) -> {
            if (bytes.equals(prev.get(timestamp)))
                return;
            rowBuilder.newRow(Clustering.make(ordinalBytes, serializeTimestamp(timestamp)));
            rowBuilder.addCell(live(CommandsForKeyColumns.data, timestampMicros, bytes));
            partitionBuilder.add(rowBuilder.build());
        });
        deletions.forEach(timestamp -> {
            rowBuilder.newRow(Clustering.make(ordinalBytes, serializeTimestamp(timestamp)));
            rowBuilder.addRowDeletion(deletion);
            partitionBuilder.add(rowBuilder.build());
        });
    }

    private static void addSeriesMutations(CommandsForKey original,
                                           CommandsForKey cfk,
                                           SeriesKind kind,
                                           PartitionUpdate.Builder partitionBuilder,
                                           Row.Builder rowBuilder,
                                           long timestampMicros,
                                           int nowInSeconds)
    {
        addSeriesMutations(kind.getValues(original), kind.getValues(cfk), kind, partitionBuilder, rowBuilder, timestampMicros, nowInSeconds);
    }

    private static DecoratedKey makeKey(int storeId, PartitionKey key)
    {
        Token token = key.token();
        ByteBuffer pk = CommandsForKeyColumns.keyComparator.make(storeId,
                                                                 serializeToken(token),
                                                                 serializeKey(key)).serializeAsPartitionKey();
        return CommandsForKeys.partitioner.decorateKey(pk);
    }

    private static DecoratedKey makeKey(int storeId, CommandsForKey cfk)
    {
        return makeKey(storeId, (PartitionKey) cfk.key());
    }

    public static Mutation getCommandsForKeyMutation(AccordCommandStore commandStore, AccordSafeCommandsForKey liveCfk, long timestampMicros)
    {
        return getCommandsForKeyMutation(commandStore.id(), liveCfk.original(), liveCfk.current(), timestampMicros);
    }

    public static Mutation getCommandsForKeyMutation(int storeId, CommandsForKey original, CommandsForKey cfk, long timestampMicros)
    {
        try
        {
            Invariants.checkArgument(original != cfk);
            // TODO: convert to byte arrays
            ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;

            int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);

            boolean hasStaticChanges = CommandsForKeyColumns.hasStaticChanges(original, cfk);
            int expectedRows = (hasStaticChanges ? 1 : 0)
                               + estimateMapChanges(c -> c.byId().commands, original, cfk)
                               + estimateMapChanges(c -> c.byExecuteAt().commands, original, cfk);

            PartitionUpdate.Builder partitionBuilder = new PartitionUpdate.Builder(CommandsForKeys,
                                                                                   makeKey(storeId, cfk),
                                                                                   CommandsForKeyColumns.columnsFor(original, cfk),
                                                                                   expectedRows);

            Row.Builder rowBuilder = BTreeRow.unsortedBuilder();

            if (hasStaticChanges)
            {
                rowBuilder.newRow(Clustering.STATIC_CLUSTERING);
                addCellIfModified(CommandsForKeyColumns.max_timestamp, CommandsForKey::max, AccordKeyspace::serializeTimestamp, rowBuilder, timestampMicros, original, cfk);
                addCellIfModified(CommandsForKeyColumns.last_executed_timestamp, CommandsForKey::lastExecutedTimestamp, AccordKeyspace::serializeTimestamp, rowBuilder, timestampMicros, original, cfk);
                addCellIfModified(CommandsForKeyColumns.last_executed_micros, CommandsForKey::lastExecutedMicros, accessor::valueOf, rowBuilder, timestampMicros, original, cfk);
                addCellIfModified(CommandsForKeyColumns.last_write_timestamp, CommandsForKey::lastWriteTimestamp, AccordKeyspace::serializeTimestamp, rowBuilder, timestampMicros, original, cfk);
                Row row = rowBuilder.build();
                if (!row.isEmpty())
                    partitionBuilder.add(row);
            }

            addSeriesMutations(original, cfk, SeriesKind.BY_ID, partitionBuilder, rowBuilder, timestampMicros, nowInSeconds);
            addSeriesMutations(original, cfk, SeriesKind.BY_EXECUTE_AT, partitionBuilder, rowBuilder, timestampMicros, nowInSeconds);

            PartitionUpdate update = partitionBuilder.build();
            if (update.isEmpty())
                return null;
            return new Mutation(update);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
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

    public static SinglePartitionReadCommand getCommandsForKeyRead(int storeId, PartitionKey key, long nowInSeconds)
    {
        return SinglePartitionReadCommand.create(CommandsForKeys, nowInSeconds,
                                                 CommandsForKeyColumns.allColumns,
                                                 RowFilter.NONE,
                                                 DataLimits.NONE,
                                                 makeKey(storeId, key),
                                                 FULL_PARTITION);
    }

    public static CommandsForKey loadCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        commandStore.checkNotInStoreThread();
        long timestampMicros = TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
        int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);

        SinglePartitionReadCommand command = getCommandsForKeyRead(commandStore.id(), key, nowInSeconds);

        EnumMap<SeriesKind, ImmutableSortedMap.Builder<Timestamp, ByteBuffer>> seriesMaps = new EnumMap<>(SeriesKind.class);
        for (SeriesKind kind : SeriesKind.values())
            seriesMaps.put(kind, new ImmutableSortedMap.Builder<>(Comparator.naturalOrder()));

        try (ReadExecutionController controller = command.executionController();
             FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
        {
            if (!partitions.hasNext())
            {
                return null;
            }

            Timestamp max = Timestamp.NONE;
            Timestamp lastExecutedTimestamp = Timestamp.NONE;
            long lastExecutedMicros = 0;
            Timestamp lastWriteTimestamp = Timestamp.NONE;

            try (RowIterator partition = partitions.next())
            {
                // empty static row will be interpreted as all null cells which will cause everything to be initialized
                Row staticRow = partition.staticRow();
                max = deserializeTimestampOrDefault(staticRow, CommandsForKeyColumns.max_timestamp, Timestamp::fromBits, max);
                lastExecutedTimestamp = deserializeTimestampOrDefault(staticRow, CommandsForKeyColumns.last_executed_timestamp, Timestamp::fromBits, lastExecutedTimestamp);

                ByteBuffer microsBytes = bytesOrNull(staticRow, CommandsForKeyColumns.last_executed_micros);
                if (microsBytes != null)
                    lastExecutedMicros = microsBytes.getLong(microsBytes.position());

                lastWriteTimestamp = deserializeTimestampOrDefault(staticRow, CommandsForKeyColumns.last_write_timestamp, Timestamp::fromBits, lastWriteTimestamp);

                while (partition.hasNext())
                {
                    Row row = partition.next();
                    Clustering<?> clustering = row.clustering();
                    int ordinal = Int32Type.instance.compose(clusteringValue(clustering, 0));
                    Timestamp timestamp = deserializeTimestampOrNull(clusteringValue(clustering, 1), Timestamp::fromBits);
                    ByteBuffer data = cellValue(row, CommandsForKeyColumns.data);
                    if (data == null)
                        continue;
                    seriesMaps.get(SeriesKind.values()[ordinal]).put(timestamp, data);
                }
            }
            Invariants.checkState(!partitions.hasNext());

            return CommandsForKey.SerializerSupport.create(key, max, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp,
                                                           CommandsForKeySerializer.loader,
                                                           seriesMaps.get(SeriesKind.BY_ID).build(),
                                                           seriesMaps.get(SeriesKind.BY_EXECUTE_AT).build());
        }
        catch (Throwable t)
        {
            logger.error("Exception loading AccordCommandsForKey " + key, t);
            throw t;
        }
    }

    public static class EpochDiskState
    {
        public static final EpochDiskState EMPTY = new EpochDiskState(0, 0);
        public final long minEpoch;
        public final long maxEpoch;

        public EpochDiskState(long minEpoch, long maxEpoch)
        {
            Invariants.checkArgument(minEpoch >= 0, "Min Epoch %d < 0", minEpoch);
            Invariants.checkArgument(maxEpoch >= minEpoch, "Max epoch %d < min %d", maxEpoch, minEpoch);
            this.minEpoch = minEpoch;
            this.maxEpoch = maxEpoch;
        }

        private EpochDiskState withNewMaxEpoch(long epoch)
        {
            Invariants.checkArgument(epoch > maxEpoch, "Epoch %d <= %d (max)", epoch, maxEpoch);
            return new EpochDiskState(Math.max(1, minEpoch), epoch);
        }

        private EpochDiskState withNewMinEpoch(long epoch)
        {
            Invariants.checkArgument(epoch > minEpoch, "epoch %d <= %d (min)", epoch, minEpoch);
            Invariants.checkArgument(epoch <= maxEpoch, "epoch %d > %d (max)", epoch, maxEpoch);
            return new EpochDiskState(epoch, maxEpoch);
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

    private static void saveEpochDiskState(EpochDiskState diskState)
    {
        String cql = "INSERT INTO %s.%s (key, min_epoch, max_epoch) VALUES (0, ?, ?);";
        executeInternal(format(cql, ACCORD_KEYSPACE_NAME, EPOCH_METADATA),
                        diskState.minEpoch, diskState.maxEpoch);
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
        return new EpochDiskState(row.getLong("min_epoch"), row.getLong("max_epoch"));
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
        void load(long epoch, Topology topology, SyncStatus syncStatus, Set<Node.Id> pendingSyncNotify, Set<Node.Id> remoteSyncComplete);
    }

    @VisibleForTesting
    public static void loadEpoch(long epoch, TopologyLoadConsumer consumer) throws IOException
    {
        String cql = format("SELECT * FROM %s.%s WHERE epoch=?", ACCORD_KEYSPACE_NAME, TOPOLOGIES);

        UntypedResultSet result = executeInternal(cql, epoch);
        Invariants.checkState(!result.isEmpty(), "Nothing found for epoch %d", epoch);
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

        consumer.load(epoch, topology, syncStatus, pendingSyncNotify, remoteSyncComplete);

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

        return Iterables.getOnlyElement(statement.getMutations(clientState, options, true, tsMicros, (int) TimeUnit.MICROSECONDS.toSeconds(tsMicros), Clock.Global.nanoTime()));
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
        void accept(ReducingRangeMap<Timestamp> rejectBefore, NavigableMap<TxnId, Ranges> bootstrapBeganAt, NavigableMap<Timestamp, Ranges> safeToRead);

    }
    public static void loadCommandStoreMetadata(int id, CommandStoreMetadataConsumer consumer)
    {
        UntypedResultSet result = executeOnceInternal(format("SELECT * FROM %s.%s WHERE store_id=?", ACCORD_KEYSPACE_NAME, COMMAND_STORE_METADATA), id);
        ReducingRangeMap<Timestamp> rejectBefore = null;
        NavigableMap<TxnId, Ranges> bootstrapBeganAt = null;
        NavigableMap<Timestamp, Ranges> safeToRead = null;
        if (!result.isEmpty())
        {
            UntypedResultSet.Row row = Iterables.getOnlyElement(result);
            try
            {
                if (row.has("reject_before"))
                    rejectBefore = deserialize(row.getBlob("reject_before"), LocalVersionedSerializers.rejectBefore);
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
        consumer.accept(rejectBefore, bootstrapBeganAt, safeToRead);
    }
}
