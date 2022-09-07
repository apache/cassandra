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
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Status;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import accord.utils.DeterministicIdentitySet;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
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
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.service.accord.AccordCommandsForKey.SeriesKind;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.db.rows.BufferCell.*;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class AccordKeyspace
{
    public static final String COMMANDS = "commands";
    public static final String COMMAND_SERIES = "command_series";
    public static final String COMMANDS_FOR_KEY = "commands_for_key";

    private static final String TIMESTAMP_TUPLE = "tuple<bigint, bigint, int, bigint>";
    private static final TupleType TIMESTAMP_TYPE = new TupleType(Lists.newArrayList(LongType.instance, LongType.instance, Int32Type.instance, LongType.instance));
    private static final String KEY_TUPLE = "tuple<uuid, blob>";

    private static final ClusteringIndexFilter FULL_PARTITION = new ClusteringIndexSliceFilter(Slices.ALL, false);

    // TODO: store timestamps as blobs (confirm there are no negative numbers, or offset)
    private static final TableMetadata Commands =
        parse(COMMANDS,
              "accord commands",
              "CREATE TABLE %s ("
              + "store_generation int,"
              + "store_index int,"
              + format("txn_id %s,", TIMESTAMP_TUPLE)
              + "status int,"
              + "txn_version int,"
              + "txn blob,"
              + format("execute_at %s,", TIMESTAMP_TUPLE)
              + format("promised_ballot %s,", TIMESTAMP_TUPLE)
              + format("accepted_ballot %s,", TIMESTAMP_TUPLE)
              + "dependencies_version int,"
              + "dependencies blob,"
              + "writes_version int,"
              + "writes blob,"
              + "result_version int,"
              + "result blob,"
              + format("waiting_on_commit map<%s, %s>,", TIMESTAMP_TUPLE, TIMESTAMP_TUPLE)
              + format("waiting_on_apply map<%s, %s>,", TIMESTAMP_TUPLE, TIMESTAMP_TUPLE)
              + "listeners set<blob>, "
              + format("blocking_commit_on set<%s>, ", TIMESTAMP_TUPLE)
              + format("blocking_apply_on set<%s>, ", TIMESTAMP_TUPLE)
              + "PRIMARY KEY((store_generation, store_index, txn_id))"
              + ')');

    private static ColumnMetadata getColumn(TableMetadata metadata, String name)
    {
        ColumnMetadata column = metadata.getColumn(new ColumnIdentifier(name, true));
        if (column == null)
            throw new IllegalArgumentException(String.format("Unknown column %s for %s.%s", name, metadata.keyspace, metadata.name));
        return column;
    }

    private static class CommandsColumns
    {
        static final ClusteringComparator keyComparator = Commands.partitionKeyAsClusteringComparator();
        static final ColumnMetadata status = getColumn(Commands, "status");
        static final ColumnMetadata txn_version = getColumn(Commands, "txn_version");
        static final ColumnMetadata txn = getColumn(Commands, "txn");
        static final ColumnMetadata execute_at = getColumn(Commands, "execute_at");
        static final ColumnMetadata promised_ballot = getColumn(Commands, "promised_ballot");
        static final ColumnMetadata accepted_ballot = getColumn(Commands, "accepted_ballot");
        static final ColumnMetadata dependencies_version = getColumn(Commands, "dependencies_version");
        static final ColumnMetadata dependencies = getColumn(Commands, "dependencies");
        static final ColumnMetadata writes_version = getColumn(Commands, "writes_version");
        static final ColumnMetadata writes = getColumn(Commands, "writes");
        static final ColumnMetadata result_version = getColumn(Commands, "result_version");
        static final ColumnMetadata result = getColumn(Commands, "result");
        static final ColumnMetadata waiting_on_commit = getColumn(Commands, "waiting_on_commit");
        static final ColumnMetadata waiting_on_apply = getColumn(Commands, "waiting_on_apply");
        static final ColumnMetadata listeners = getColumn(Commands, "listeners");
        static final ColumnMetadata blocking_commit_on = getColumn(Commands, "blocking_commit_on");
        static final ColumnMetadata blocking_apply_on = getColumn(Commands, "blocking_apply_on");
    }

    private static final TableMetadata CommandsForKey =
        parse(COMMANDS_FOR_KEY,
              "accord commands per key",
              "CREATE TABLE %s ("
              + "store_generation int,"
              + "store_index int,"
              + format("key %s,", KEY_TUPLE)
              + format("max_timestamp %s static,", TIMESTAMP_TUPLE)
              + format("blind_witnessed set<%s> static, ", TIMESTAMP_TUPLE)
              + "series int,"
              + format("timestamp %s,", TIMESTAMP_TUPLE)
              + "data blob, "
              + "PRIMARY KEY((store_generation, store_index, key), series, timestamp)"
              + ')');

    private static class CommandsForKeyColumns
    {
        static final ClusteringComparator keyComparator = CommandsForKey.partitionKeyAsClusteringComparator();
        static final ColumnFilter allColumns = ColumnFilter.all(CommandsForKey);
        static final ColumnMetadata max_timestamp = getColumn(CommandsForKey, "max_timestamp");
        static final ColumnMetadata blind_witnessed = getColumn(CommandsForKey, "blind_witnessed");

        static final ColumnMetadata series = getColumn(CommandsForKey, "series");
        static final ColumnMetadata timestamp = getColumn(CommandsForKey, "timestamp");
        static final ColumnMetadata data = getColumn(CommandsForKey, "data");

        static final Columns statics = Columns.from(Lists.newArrayList(max_timestamp, blind_witnessed));
        static final Columns regulars = Columns.from(Lists.newArrayList(series, timestamp, data));
        private static final RegularAndStaticColumns all = new RegularAndStaticColumns(statics, regulars);
        private static final RegularAndStaticColumns justStatic = new RegularAndStaticColumns(statics, Columns.NONE);
        private static final RegularAndStaticColumns justRegular = new RegularAndStaticColumns(Columns.NONE, regulars);

        static boolean hasStaticChanges(AccordCommandsForKey commandsForKey)
        {
            return commandsForKey.maxTimestamp.hasModifications() || commandsForKey.blindWitnessed.hasModifications();
        }

        static RegularAndStaticColumns columnsFor(AccordCommandsForKey commandsForKey)
        {
            boolean hasStaticChanges = hasStaticChanges(commandsForKey);
            boolean hasRegularChanges = commandsForKey.uncommitted.map.hasAdditions()
                                        || commandsForKey.committedById.map.hasAdditions()
                                        || commandsForKey.committedByExecuteAt.map.hasAdditions();

            if (hasStaticChanges && hasRegularChanges)
                return all;
            else if (hasStaticChanges)
                return justStatic;
            else if (hasRegularChanges)
                return justRegular;
            else
                throw new IllegalArgumentException();
        }
    }

    private static TableMetadata parse(String name, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, name), ACCORD_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(ACCORD_KEYSPACE_NAME, name))
                                   .comment(description)
                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90))
                                   .build();
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(ACCORD_KEYSPACE_NAME, KeyspaceParams.local(), tables(), Views.none(), Types.none(), Functions.none());
    }

    private static Tables tables()
    {
        return Tables.of(Commands, CommandsForKey);
    }

    private static <T> ByteBuffer serialize(T obj, IVersionedSerializer<T> serializer, int version) throws IOException
    {
        int size = (int) serializer.serializedSize(obj, version);
        try (DataOutputBuffer out = new DataOutputBuffer(size))
        {
            serializer.serialize(obj, out, version);
            assert size == out.buffer().limit();
            return out.buffer();
        }
    }

    private static <T> ByteBuffer serializeOrNull(T obj, IVersionedSerializer<T> serializer, int version) throws IOException
    {
        return obj != null ? serialize(obj, serializer, version) : EMPTY_BYTE_BUFFER;
    }

    private static <T> T deserialize(ByteBuffer bytes, IVersionedSerializer<T> serializer, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            return serializer.deserialize(in, version);
        }
    }

    private static <T> T deserializeOrNull(ByteBuffer bytes, IVersionedSerializer<T> serializer, int version) throws IOException
    {
        return bytes != null && ! ByteBufferAccessor.instance.isEmpty(bytes) ? deserialize(bytes, serializer, version) : null;
    }

    private static Map<ByteBuffer, ByteBuffer> serializeWaitingOn(Map<TxnId, ByteBuffer> waitingOn)
    {
        Map<ByteBuffer, ByteBuffer> result = Maps.newHashMapWithExpectedSize(waitingOn.size());
        for (Map.Entry<TxnId, ByteBuffer> entry : waitingOn.entrySet())
            result.put(serializeTimestamp(entry.getKey()), entry.getValue());
        return result;
    }

    private static NavigableMap<TxnId, ByteBuffer> deserializeWaitingOn(Map<ByteBuffer, ByteBuffer> serialized)
    {
        if (serialized == null || serialized.isEmpty())
            return new TreeMap<>();

        NavigableMap<TxnId, ByteBuffer> result = new TreeMap<>();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : serialized.entrySet())
            result.put(deserializeTimestampOrNull(entry.getKey(), TxnId::new), entry.getValue());
        return result;
    }

    private static NavigableMap<TxnId, ByteBuffer> deserializeWaitingOn(UntypedResultSet.Row row, String name)
    {
        return deserializeWaitingOn(row.getMap(name, BytesType.instance, BytesType.instance));
    }

    private static <T extends Timestamp, S extends Set<T>> S deserializeTimestampSet(Set<ByteBuffer> serialized, Supplier<S> setFactory, TimestampFactory<T> timestampFactory)
    {
        S result = setFactory.get();
        if (serialized == null || serialized.isEmpty())
            return result;

        for (ByteBuffer bytes : serialized)
            result.add(deserializeTimestampOrNull(bytes, timestampFactory));

        return result;
    }

    private static NavigableSet<TxnId> deserializeBlocking(UntypedResultSet.Row row, String name)
    {
        return deserializeTimestampSet(row.getSet(name, BytesType.instance), TreeSet::new, TxnId::new);
    }

    public static Set<ByteBuffer> serializeListeners(Set<ListenerProxy> listeners)
    {
        Set<ByteBuffer> result = Sets.newHashSetWithExpectedSize(listeners.size());
        for (ListenerProxy listener : listeners)
        {
            result.add(listener.identifier());
        }
        return result;
    }

    private static DeterministicIdentitySet<ListenerProxy> deserializeListeners(CommandStore commandStore, Set<ByteBuffer> serialized) throws IOException
    {
        if (serialized == null || serialized.isEmpty())
            return new DeterministicIdentitySet<>();
        DeterministicIdentitySet<ListenerProxy> result = new DeterministicIdentitySet<>();
        for (ByteBuffer bytes : serialized)
        {
            result.add(ListenerProxy.deserialize(commandStore, bytes, ByteBufferAccessor.instance, 0));
        }
        return result;
    }

    private static DeterministicIdentitySet<ListenerProxy> deserializeListeners(CommandStore commandStore, UntypedResultSet.Row row, String name) throws IOException
    {
        return deserializeListeners(commandStore, row.getSet(name, BytesType.instance));
    }

    private static <K extends Comparable<?>, V> void addStoredMapChanges(Row.Builder builder,
                                                                         ColumnMetadata column,
                                                                         long timestamp,
                                                                         int nowInSec,
                                                                         StoredNavigableMap<K, V> map,
                                                                         Function<K, ByteBuffer> serializeKey,
                                                                         Function<V, ByteBuffer> serializeVal)
    {
        if (map.wasCleared())
        {
            if (!map.hasAdditions())
            {
                builder.addComplexDeletion(column, new DeletionTime(timestamp, nowInSec));
                return;
            }
            else
                builder.addComplexDeletion(column, new DeletionTime(timestamp - 1, nowInSec));
        }

        map.forEachAddition((k, v) -> builder.addCell(live(column, timestamp, serializeVal.apply(v), CellPath.create(serializeKey.apply(k)))));

        if (!map.wasCleared())
            map.forEachDeletion(k -> builder.addCell(tombstone(column, timestamp, nowInSec, CellPath.create(serializeKey.apply(k)))));
    }

    private static <T extends Comparable<?>> void addStoredSetChanges(Row.Builder builder,
                                                                      ColumnMetadata column,
                                                                      long timestamp,
                                                                      int nowInSec,
                                                                      StoredSet<T, ?> map,
                                                                      Function<T, ByteBuffer> serialize)
    {
        if (map.wasCleared())
        {
            if (!map.hasAdditions())
            {
                builder.addComplexDeletion(column, new DeletionTime(timestamp, nowInSec));
                return;
            }
            else
                builder.addComplexDeletion(column, new DeletionTime(timestamp - 1, nowInSec));
        }

        map.forEachAddition(i -> builder.addCell(live(column, timestamp, EMPTY_BYTE_BUFFER, CellPath.create(serialize.apply(i)))));

        if (!map.wasCleared())
            map.forEachDeletion(k -> builder.addCell(tombstone(column, timestamp, nowInSec, CellPath.create(serialize.apply(k)))));
    }

    public static Mutation getCommandMutation(AccordCommand command, long timestampMicros)
    {
        try
        {
            Preconditions.checkArgument(command.hasModifications());

            // TODO: convert to byte arrays
            ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;

            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(Clustering.EMPTY);
            int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);
            int version = MessagingService.current_version;
            ByteBuffer versionBytes = accessor.valueOf(version);

            if (command.status.hasModifications())
                builder.addCell(live(CommandsColumns.status, timestampMicros, accessor.valueOf(command.status.get().ordinal())));

            if (command.txn.hasModifications())
            {
                builder.addCell(live(CommandsColumns.txn_version, timestampMicros, versionBytes));
                builder.addCell(live(CommandsColumns.txn, timestampMicros, serializeOrNull(command.txn.get(), CommandSerializers.txn, version)));
            }

            if (command.executeAt.hasModifications())
                builder.addCell(live(CommandsColumns.execute_at, timestampMicros, serializeTimestamp(command.executeAt.get())));

            if (command.promised.hasModifications())
                builder.addCell(live(CommandsColumns.promised_ballot, timestampMicros, serializeTimestamp(command.promised.get())));

            if (command.accepted.hasModifications())
                builder.addCell(live(CommandsColumns.accepted_ballot, timestampMicros, serializeTimestamp(command.accepted.get())));

            if (command.deps.hasModifications())
            {
                builder.addCell(live(CommandsColumns.dependencies_version, timestampMicros, versionBytes));
                builder.addCell(live(CommandsColumns.dependencies, timestampMicros, serialize(command.deps.get(), CommandSerializers.deps, version)));
            }

            if (command.writes.hasModifications())
            {
                builder.addCell(live(CommandsColumns.writes_version, timestampMicros, versionBytes));
                builder.addCell(live(CommandsColumns.writes, timestampMicros, serialize(command.writes.get(), CommandSerializers.writes, version)));
            }

            if (command.result.hasModifications())
            {
                builder.addCell(live(CommandsColumns.result_version, timestampMicros, versionBytes));
                builder.addCell(live(CommandsColumns.result, timestampMicros, serialize((AccordData) command.result.get(), AccordData.serializer, version)));
            }

            if (command.waitingOnCommit.hasModifications())
            {
                addStoredMapChanges(builder, CommandsColumns.waiting_on_commit,
                                    timestampMicros, nowInSeconds, command.waitingOnCommit,
                                    AccordKeyspace::serializeTimestamp, bytes -> bytes);
            }

            if (command.blockingCommitOn.hasModifications())
            {
                addStoredSetChanges(builder, CommandsColumns.blocking_commit_on,
                                    timestampMicros, nowInSeconds, command.blockingApplyOn,
                                    AccordKeyspace::serializeTimestamp);
            }

            if (command.waitingOnApply.hasModifications())
            {
                addStoredMapChanges(builder, CommandsColumns.waiting_on_apply,
                                    timestampMicros, nowInSeconds, command.waitingOnApply,
                                    AccordKeyspace::serializeTimestamp, bytes -> bytes);
            }

            if (command.blockingApplyOn.hasModifications())
            {
                addStoredSetChanges(builder, CommandsColumns.blocking_apply_on,
                                    timestampMicros, nowInSeconds, command.blockingApplyOn,
                                    AccordKeyspace::serializeTimestamp);
            }

            if (command.storedListeners.hasModifications())
            {
                addStoredSetChanges(builder, CommandsColumns.listeners,
                                    timestampMicros, nowInSeconds, command.storedListeners,
                                    ListenerProxy::identifier);
            }
            ByteBuffer key = CommandsColumns.keyComparator.make(command.commandStore().generation(),
                                                                command.commandStore().index(),
                                                                serializeTimestamp(command.txnId())).serializeAsPartitionKey();
            PartitionUpdate update = PartitionUpdate.singleRowUpdate(Commands, key, builder.build());
            return new Mutation(update);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void saveCommand(AccordCommand command) throws IOException
    {
        int version = MessagingService.current_version;
        TxnId txnId = command.txnId();
        Timestamp executeAt = command.executeAt();
        Ballot promised = command.promised();
        Ballot accepted = command.accepted();
        String cql = "UPDATE %s.%s " +
                     "SET status=?, " +
                     "txn_version=?, " +
                     "txn=?, " +
                     "execute_at=(?, ?, ?, ?), " +
                     "promised_ballot=(?, ?, ?, ?), " +
                     "accepted_ballot=(?, ?, ?, ?), " +
                     "dependencies_version=?, " +
                     "dependencies=?, " +
                     "writes_version=?, " +
                     "writes=?, " +
                     "result_version=?, " +
                     "result=?, " +
                     "waiting_on_commit=?, " +
                     "waiting_on_apply=?, " +
                     "listeners=? " +
                     "WHERE store_generation=? AND store_index=? AND txn_id=(?, ?, ?, ?)";
        executeOnceInternal(String.format(cql, ACCORD_KEYSPACE_NAME, COMMANDS),
                            command.status().ordinal(),
                            version,
                            serializeOrNull(command.txn(), CommandSerializers.txn, version),
                            executeAt.epoch, executeAt.real, executeAt.logical, executeAt.node.id,
                            promised.epoch, promised.real, promised.logical, promised.node.id,
                            accepted.epoch, accepted.real, accepted.logical, accepted.node.id,
                            version,
                            serialize(command.savedDeps(), CommandSerializers.deps, version),
                            version,
                            serializeOrNull(command.writes(), CommandSerializers.writes, version),
                            version,
                            serializeOrNull((AccordData) command.result(), AccordData.serializer, version),
                            serializeWaitingOn(command.waitingOnCommit.getView()),
                            serializeWaitingOn(command.waitingOnApply.getView()),
                            serializeListeners(command.storedListeners.getView()),
                            command.commandStore().generation(),
                            command.commandStore().index(),
                            txnId.epoch, txnId.real, txnId.logical, txnId.node.id);
    }

    private static ByteBuffer serializeKey(PartitionKey key)
    {
        UUIDSerializer.instance.serialize(key.tableId().asUUID());
        return TupleType.buildValue(new ByteBuffer[]{UUIDSerializer.instance.serialize(key.tableId().asUUID()),
                                                     key.partitionKey().getKey()});
    }

    private static ByteBuffer serializeTimestamp(Timestamp timestamp)
    {
        return TupleType.buildValue(new ByteBuffer[]{bytes(timestamp.epoch), bytes(timestamp.real), bytes(timestamp.logical), bytes(timestamp.node.id)});
    }

    private interface TimestampFactory<T extends Timestamp>
    {
        T create(long epoch, long real, int logical, Node.Id node);
    }

    private static <T extends Timestamp> T deserializeTimestampOrNull(ByteBuffer bytes, TimestampFactory<T> factory)
    {
        if (bytes == null || ByteBufferAccessor.instance.isEmpty(bytes))
            return null;
        ByteBuffer[] split = TIMESTAMP_TYPE.split(bytes);
        return factory.create(split[0].getLong(), split[1].getLong(), split[2].getInt(), new Node.Id(split[3].getLong()));
    }

    private static <T extends Timestamp> T deserializeTimestampOrNull(UntypedResultSet.Row row, String name, TimestampFactory<T> factory)
    {
        return deserializeTimestampOrNull(row.getBlob(name), factory);
    }

    public static AccordCommand loadCommand(AccordCommandStore commandStore, TxnId txnId)
    {
        AccordCommand command = new AccordCommand(commandStore, txnId);
        loadCommand(command);
        return command;
    }

    private static <T> T deserializeWithVersionOr(UntypedResultSet.Row row, String dataColumn, String versionColumn, IVersionedSerializer<T> serializer, Supplier<T> defaultSupplier) throws IOException
    {
        if (!row.has(versionColumn))
            return defaultSupplier.get();

        return deserialize(row.getBlob(dataColumn), serializer, row.getInt(versionColumn));
    }

    public static void loadCommand(AccordCommand command)
    {
        Preconditions.checkArgument(!command.isLoaded());
        TxnId txnId = command.txnId();
        AccordCommandStore commandStore = command.commandStore();
        commandStore.checkNotInStoreThread();

        String cql = "SELECT * FROM %s.%s " +
                     "WHERE store_generation=? " +
                     "AND store_index=? " +
                     "AND txn_id=(?, ?, ?, ?)";

        UntypedResultSet result = executeOnceInternal(String.format(cql, ACCORD_KEYSPACE_NAME, COMMANDS),
                                                      commandStore.generation(),
                                                      commandStore.index(),
                                                      txnId.epoch, txnId.real, txnId.logical, txnId.node.id);

        if (result.isEmpty())
        {
            command.initialize();
            return;
        }

        try
        {
            UntypedResultSet.Row row = result.one();
            Preconditions.checkState(deserializeTimestampOrNull(row, "txn_id", TxnId::new).equals(txnId));
            command.status.load(Status.values()[row.getInt("status")]);
            command.txn.load(deserializeOrNull(row.getBlob("txn"), CommandSerializers.txn, row.getInt("txn_version")));
            command.executeAt.load(deserializeTimestampOrNull(row, "execute_at", Timestamp::new));
            command.promised.load(deserializeTimestampOrNull(row, "promised_ballot", Ballot::new));
            command.accepted.load(deserializeTimestampOrNull(row, "accepted_ballot", Ballot::new));
            command.deps.load(deserializeWithVersionOr(row, "dependencies", "dependencies_version", CommandSerializers.deps, Dependencies::new));
            command.writes.load(deserializeWithVersionOr(row, "writes", "writes_version", CommandSerializers.writes, () -> null));
            command.result.load(deserializeWithVersionOr(row, "result", "result_version", AccordData.serializer, () -> null));
            command.waitingOnCommit.load(deserializeWaitingOn(row, "waiting_on_commit"));
            command.blockingCommitOn.load(deserializeBlocking(row, "blocking_commit_on"));
            command.waitingOnApply.load(deserializeWaitingOn(row, "waiting_on_apply"));
            command.blockingApplyOn.load(deserializeBlocking(row, "blocking_apply_on"));
            command.storedListeners.load(deserializeListeners(commandStore, row, "listeners"));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void addSeriesMutations(AccordCommandsForKey.Series series,
                                           PartitionUpdate.Builder partitionBuilder,
                                           Row.Builder rowBuilder,
                                           long timestampMicros,
                                           int nowInSeconds)
    {
        if (!series.map.hasModifications())
            return;

        Row.Deletion deletion = series.map.hasDeletions() ?
                                Row.Deletion.regular(new DeletionTime(timestampMicros, nowInSeconds)) :
                                null;
        ByteBuffer ordinalBytes = bytes(series.kind.ordinal());
        series.map.forEachAddition((timestamp, bytes) -> {
            rowBuilder.newRow(Clustering.make(ordinalBytes, serializeTimestamp(timestamp)));
            rowBuilder.addCell(live(CommandsForKeyColumns.data, timestampMicros, bytes));
            partitionBuilder.add(rowBuilder.build());
        });
        series.map.forEachDeletion(timestamp -> {
            rowBuilder.newRow(Clustering.make(ordinalBytes, serializeTimestamp(timestamp)));
            rowBuilder.addRowDeletion(deletion);
            partitionBuilder.add(rowBuilder.build());
        });
    }

    private static DecoratedKey makeKey(AccordCommandsForKey cfk)
    {
        ByteBuffer key = CommandsForKeyColumns.keyComparator.make(cfk.commandStore().generation(),
                                                                  cfk.commandStore().index(),
                                                                  serializeKey(cfk.key())).serializeAsPartitionKey();
        return CommandsForKey.partitioner.decorateKey(key);
    }

    public static Mutation getCommandsForKeyMutation(AccordCommandsForKey cfk, long timestampMicros)
    {
        Preconditions.checkArgument(cfk.hasModifications());

        int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);

        int expectedRows = (CommandsForKeyColumns.hasStaticChanges(cfk) ? 1 : 0)
                           + cfk.uncommitted.map.totalModifications()
                           + cfk.committedById.map.totalModifications()
                           + cfk.committedByExecuteAt.map.totalModifications();

        PartitionUpdate.Builder partitionBuilder = new PartitionUpdate.Builder(CommandsForKey,
                                                                               makeKey(cfk),
                                                                               CommandsForKeyColumns.columnsFor(cfk),
                                                                               expectedRows);

        Row.Builder rowBuilder = BTreeRow.unsortedBuilder();
        boolean updateStaticRow = cfk.maxTimestamp.hasModifications() || cfk.blindWitnessed.hasModifications();
        if (updateStaticRow)
            rowBuilder.newRow(Clustering.STATIC_CLUSTERING);

        if (cfk.maxTimestamp.hasModifications())
            rowBuilder.addCell(live(CommandsForKeyColumns.max_timestamp, timestampMicros, serializeTimestamp(cfk.maxTimestamp.get())));

        if (cfk.blindWitnessed.hasModifications())
            addStoredSetChanges(rowBuilder, CommandsForKeyColumns.blind_witnessed,
                                timestampMicros, nowInSeconds, cfk.blindWitnessed,
                                AccordKeyspace::serializeTimestamp);

        if (updateStaticRow)
            partitionBuilder.add(rowBuilder.build());

        addSeriesMutations(cfk.uncommitted, partitionBuilder, rowBuilder, timestampMicros, nowInSeconds);
        addSeriesMutations(cfk.committedById, partitionBuilder, rowBuilder, timestampMicros, nowInSeconds);
        addSeriesMutations(cfk.committedByExecuteAt, partitionBuilder, rowBuilder, timestampMicros, nowInSeconds);

        return new Mutation(partitionBuilder.build());
    }

    public static AccordCommandsForKey loadCommandsForKey(AccordCommandStore commandStore, PartitionKey key)
    {
        AccordCommandsForKey commandsForKey = new AccordCommandsForKey(commandStore, key);
        loadCommandsForKey(commandsForKey);
        return commandsForKey;
    }

    private static <T> ByteBuffer cellValue(Cell<T> cell)
    {
        return cell.accessor().toBuffer(cell.value());
    }

    // TODO: convert to byte array
    private static ByteBuffer cellValue(Row row, ColumnMetadata column)
    {
        Cell<?> cell = row.getCell(column);
        return  (cell != null && !cell.isTombstone()) ? cellValue(cell) : null;
    }

    private static <T> ByteBuffer clusteringValue(Clustering<T> clustering, int idx)
    {
        return clustering.accessor().toBuffer(clustering.get(idx));
    }

    public static void loadCommandsForKey(AccordCommandsForKey cfk)
    {
        Preconditions.checkArgument(!cfk.isLoaded());
        ((AccordCommandStore) cfk.commandStore()).checkNotInStoreThread();
        long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        int nowInSeconds = (int) TimeUnit.MICROSECONDS.toSeconds(timestampMicros);

        SinglePartitionReadCommand command = SinglePartitionReadCommand.create(CommandsForKey, nowInSeconds,
                                                                               CommandsForKeyColumns.allColumns,
                                                                               RowFilter.NONE,
                                                                               DataLimits.NONE,
                                                                               makeKey(cfk),
                                                                               FULL_PARTITION);

        EnumMap<SeriesKind, TreeMap<Timestamp, ByteBuffer>> seriesMaps = new EnumMap<>(SeriesKind.class);
        for (SeriesKind kind : SeriesKind.values())
            seriesMaps.put(kind, new TreeMap<>());

        try(ReadExecutionController controller = command.executionController();
            FilteredPartitions partitions = FilteredPartitions.filter(command.executeLocally(controller), nowInSeconds))
        {
            if (!partitions.hasNext())
            {
                cfk.initialize();
                return;
            }

            try (RowIterator partition = partitions.next())
            {
                Row staticRow = partition.staticRow();
                if (!staticRow.isEmpty())
                {
                    Cell<?> cell = staticRow.getCell(CommandsForKeyColumns.max_timestamp);
                    if (cell == null || cell.isTombstone())
                        cfk.maxTimestamp.load(Timestamp.NONE);
                    else
                        cfk.maxTimestamp.load(deserializeTimestampOrNull(cellValue(cell), Timestamp::new));

                    TreeSet<Timestamp> blindWitnessed = new TreeSet<>();
                    ComplexColumnData cmplx = staticRow.getComplexColumnData(CommandsForKeyColumns.blind_witnessed);
                    if (cmplx != null)
                        cmplx.forEach(c -> blindWitnessed.add(deserializeTimestampOrNull(c.path().get(0), Timestamp::new)));
                    cfk.blindWitnessed.load(blindWitnessed);
                }

                while (partition.hasNext())
                {
                    Row row = partition.next();
                    Clustering<?> clustering = row.clustering();
                    int ordinal = Int32Type.instance.compose(clusteringValue(clustering, 0));
                    Timestamp timestamp = deserializeTimestampOrNull(clusteringValue(clustering, 1), Timestamp::new);
                    ByteBuffer data = cellValue(row, CommandsForKeyColumns.data);
                    if (data == null)
                        continue;
                    seriesMaps.get(SeriesKind.values()[ordinal]).put(timestamp, data);
                }
            }
            Preconditions.checkState(!partitions.hasNext());
        }

        cfk.uncommitted.map.load(seriesMaps.get(SeriesKind.UNCOMMITTED));
        cfk.committedById.map.load(seriesMaps.get(SeriesKind.COMMITTED_BY_ID));
        cfk.committedByExecuteAt.map.load(seriesMaps.get(SeriesKind.COMMITTED_BY_EXECUTE_AT));
    }
}
