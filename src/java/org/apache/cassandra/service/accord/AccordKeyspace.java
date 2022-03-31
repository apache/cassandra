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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Status;
import accord.txn.Ballot;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import com.beust.jcommander.internal.Lists;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
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
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.service.accord.serializers.CommandSerializers;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredNavigableSet;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.db.rows.BufferCell.*;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;
import static org.apache.cassandra.utils.ByteBufferUtil.EMPTY_BYTE_BUFFER;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

// TODO: rework all of the io methods to emit/accumulate mutations
public class AccordKeyspace
{
    public static final String COMMANDS = "commands";
    public static final String COMMAND_SERIES = "command_series";
    public static final String COMMANDS_FOR_KEY = "commands_for_key";

    private static final String TIMESTAMP_TUPLE = "tuple<bigint, bigint, int, bigint>";
    private static final TupleType TIMESTAMP_TYPE = new TupleType(Lists.newArrayList(LongType.instance, LongType.instance, Int32Type.instance, LongType.instance));
    private static final String KEY_TUPLE = "tuple<uuid, blob>";
    private static final TupleType KEY_TYPE = new TupleType(Lists.newArrayList(UUIDType.instance, BytesType.instance));

    // TODO: store timestamps as blobs (confirm there are no negative numbers, or offset)
    private static final TableMetadata Commands =
        parse(COMMANDS,
              "accord commands",
              "CREATE TABLE %s ("
              + "store_generation int,"
              + "store_index int,"
              + format("txn_id %s,", TIMESTAMP_TUPLE)
              + "status int,"
              + "serializer_version int,"  // TODO: remove serializer_version
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
              + "listeners set<blob>,"
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
        static final ColumnMetadata store_generation = getColumn(Commands, "store_generation");
        static final ColumnMetadata store_index = getColumn(Commands, "store_index");
        static final ColumnMetadata txn_id = getColumn(Commands, "txn_id");

        static final ColumnMetadata status = getColumn(Commands, "status");
        static final ColumnMetadata serializer_version = getColumn(Commands, "serializer_version");
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
    }

    private static final TableMetadata CommandsForKey =
        parse(COMMANDS_FOR_KEY,
              "accord commands per key",
              "CREATE TABLE %s ("
              + "store_generation int,"
              + "store_index int,"
              + format("key %s,", KEY_TUPLE)
              + format("max_timestamp %s static,", TIMESTAMP_TUPLE)
              + "series int,"
              + format("timestamp %s,", TIMESTAMP_TUPLE)
              + format("txn_id %s,", TIMESTAMP_TUPLE)
              + "PRIMARY KEY((store_generation, store_index, key), series, timestamp)"
              + ')');

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
        return obj != null ? serialize(obj, serializer, version) : null;
    }

    private static <T> T deserialize(ByteBuffer bytes, IVersionedSerializer<T> serializer, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes, false))
        {
            return serializer.deserialize(in, version);
        }
    }

    private static <T> T deserializeOrNull(ByteBuffer bytes, IVersionedSerializer<T> serializer, int version) throws IOException
    {
        return bytes != null ? deserialize(bytes, serializer, version) : null;
    }

    private static Map<ByteBuffer, ByteBuffer> serializeWaitingOn(Map<? extends Timestamp, TxnId> waitingOn)
    {
        Map<ByteBuffer, ByteBuffer> result = Maps.newHashMapWithExpectedSize(waitingOn.size());
        for (Map.Entry<? extends Timestamp, TxnId> entry : waitingOn.entrySet())
            result.put(serializeTimestamp(entry.getKey()), serializeTimestamp(entry.getValue()));
        return result;
    }

    private static <T extends Timestamp> NavigableMap<T, TxnId> deserializeWaitingOn(Map<ByteBuffer, ByteBuffer> serialized, TimestampFactory<T> factory)
    {
        if (serialized == null || serialized.isEmpty())
            return null;

        NavigableMap<T, TxnId> result = new TreeMap<>();
        for (Map.Entry<ByteBuffer, ByteBuffer> entry : serialized.entrySet())
            result.put(deserializeTimestamp(entry.getKey(), factory), deserializeTimestamp(entry.getValue(), TxnId::new));
        return result;
    }

    private static <T extends Timestamp> NavigableMap<T, TxnId> deserializeWaitingOn(UntypedResultSet.Row row, String name, TimestampFactory<T> factory)
    {
        return deserializeWaitingOn(row.getMap(name, BytesType.instance, BytesType.instance), factory);
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

    private static NavigableSet<ListenerProxy> deserializeListeners(CommandStore commandStore, Set<ByteBuffer> serialized) throws IOException
    {
        NavigableSet<ListenerProxy> result = new TreeSet<>();
        for (ByteBuffer bytes : serialized)
        {
            result.add(ListenerProxy.deserialize(commandStore, bytes, ByteBufferAccessor.instance, 0));
        }
        return result;
    }

    private static NavigableSet<ListenerProxy> deserializeListeners(CommandStore commandStore, UntypedResultSet.Row row, String name) throws IOException
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
                                                                      StoredNavigableSet<T> map,
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

    public static Mutation getCommandMutation(AccordCommand command) throws IOException
    {
        Preconditions.checkArgument(command.hasModifications());

        // TODO: convert to byte arrays
        ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;

        Row.Builder builder = BTreeRow.unsortedBuilder();
        long timestampMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
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
                                AccordKeyspace::serializeTimestamp, AccordKeyspace::serializeTimestamp);
        }

        if (command.waitingOnApply.hasModifications())
        {
            addStoredMapChanges(builder, CommandsColumns.waiting_on_apply,
                                timestampMicros, nowInSeconds, command.waitingOnApply,
                                AccordKeyspace::serializeTimestamp, AccordKeyspace::serializeTimestamp);
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
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(Commands, Commands.partitioner.decorateKey(key), builder.build(), Rows.EMPTY_STATIC_ROW);
        return new Mutation(update);
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
                     "serializer_version=?, " +
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

    private static ByteBuffer serializeTimestamp(Timestamp timestamp)
    {
        return TupleType.buildValue(new ByteBuffer[]{bytes(timestamp.epoch), bytes(timestamp.real), bytes(timestamp.logical), bytes(timestamp.node.id)});
    }

    private interface TimestampFactory<T extends Timestamp>
    {
        T create(long epoch, long real, int logical, Node.Id node);
    }

    private static <T extends Timestamp> T deserializeTimestamp(ByteBuffer bytes, TimestampFactory<T> factory)
    {
        ByteBuffer[] split = TIMESTAMP_TYPE.split(bytes);
        return factory.create(split[0].getLong(), split[1].getLong(), split[2].getInt(), new Node.Id(split[3].getLong()));
    }

    private static <T extends Timestamp> T deserializeTimestamp(UntypedResultSet.Row row, String name, TimestampFactory<T> factory)
    {
        return deserializeTimestamp(row.getBlob(name), factory);
    }

    public static AccordCommand loadCommand(CommandStore commandStore, TxnId txnId)
    {
        AccordCommand command = new AccordCommand(commandStore, txnId);
        loadCommand(command);
        return command;
    }

    // FIXME: indicate which fields need to be loaded
    public static void loadCommand(AccordCommand command)
    {
        TxnId txnId = command.txnId();
        CommandStore commandStore = command.commandStore();

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
            command.loadEmpty();
            return;
        }

        try
        {
            UntypedResultSet.Row row = result.one();
            Preconditions.checkState(deserializeTimestamp(row, "txn_id", TxnId::new).equals(txnId));
            int version = row.getInt("serializer_version");
            command.status.load(Status.values()[row.getInt("status")]);
            command.txn.load(deserializeOrNull(row.getBlob("txn"), CommandSerializers.txn, version));
            command.executeAt.load(deserializeTimestamp(row, "execute_at", Timestamp::new));
            command.promised.load(deserializeTimestamp(row, "promised_ballot", Ballot::new));
            command.accepted.load(deserializeTimestamp(row, "accepted_ballot", Ballot::new));
            command.deps.load(deserialize(row.getBlob("dependencies"), CommandSerializers.deps, version));
            command.writes.load(deserializeOrNull(row.getBlob("writes"), CommandSerializers.writes, version));
            command.result.load(deserializeOrNull(row.getBlob("result"), AccordData.serializer, version));
            command.waitingOnCommit.load(deserializeWaitingOn(row, "waiting_on_commit", TxnId::new));
            command.waitingOnApply.load(deserializeWaitingOn(row, "waiting_on_apply", Timestamp::new));
            command.storedListeners.load(deserializeListeners(commandStore, row, "listeners"));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static AccordCommandsForKey loadCommandsForKey(CommandStore commandStore, AccordKey.PartitionKey key)
    {
        String cql = "SELECT max_timestamp FROM %s.%s " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?) " +
                     "LIMIT 1";
        UntypedResultSet result = executeOnceInternal(String.format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                                                      commandStore.generation(), commandStore.index(),
                                                      key.tableId(), key.partitionKey().getKey());

        if (result.isEmpty())
            return new AccordCommandsForKey(commandStore, key, Timestamp.NONE);

        return new AccordCommandsForKey(commandStore, key, deserializeTimestamp(result.one(), "max_timestamp", Timestamp::new));
    }

    public static void updateCommandsForKeyMaxTimestamp(CommandStore commandStore, AccordKey.PartitionKey key, Timestamp time)
    {
        String cql = "UPDATE %s.%s " +
                     "SET max_timestamp=(?, ?, ?, ?) " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?)";
        executeOnceInternal(String.format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                            time.epoch, time.real, time.logical, time.node.id,
                            commandStore.generation(), commandStore.index(),
                            key.tableId(), key.partitionKey().getKey());
    }

    public static TxnId getCommandForKeySeriesEntry(CommandStore commandStore, AccordKey.PartitionKey key, AccordCommandsForKey.SeriesKind kind, Timestamp time)
    {

        String cql = "SELECT txn_id FROM %s.%s " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?), " +
                     "series=?, " +
                     "timestamp=(?, ?, ?, ?)";
        UntypedResultSet result = executeOnceInternal(format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                                                      commandStore.generation(), commandStore.index(),
                                                      key.tableId(), key.partitionKey().getKey(),
                                                      kind.ordinal(),
                                                      time.epoch, time.real, time.logical, time.node.id);

        if (result.isEmpty())
            return null;

        return deserializeTimestamp(result.one(), "txn_id", TxnId::new);
    }

    public static void addCommandForKeySeriesEntry(CommandStore commandStore, AccordKey.PartitionKey key, AccordCommandsForKey.SeriesKind kind, Timestamp time, TxnId txnId)
    {

        String cql = "UPDATE %s.%s " +
                     "SET txn_id=(?, ?, ?, ?) " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?), " +
                     "series=?, " +
                     "timestamp=(?, ?, ?, ?)";
        executeOnceInternal(String.format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                            txnId.epoch, txnId.real, txnId.logical, txnId.node.id,
                            commandStore.generation(), commandStore.index(),
                            key.tableId(), key.partitionKey().getKey(),
                            kind.ordinal(),
                            time.epoch, time.real, time.logical, time.node.id);
    }

    public static void removeCommandForKeySeriesEntry(CommandStore commandStore, AccordKey.PartitionKey key, AccordCommandsForKey.SeriesKind kind, Timestamp time)
    {

        String cql = "DELETE FROM %s.%s " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?), " +
                     "series=?, " +
                     "timestamp=(?, ?, ?, ?)";
        executeOnceInternal(String.format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                            commandStore.generation(), commandStore.index(),
                            key.tableId(), key.partitionKey().getKey(),
                            kind.ordinal(),
                            time.epoch, time.real, time.logical, time.node.id);
    }

    private static List<TxnId> createTxnIdList(UntypedResultSet result)
    {
        if (result.isEmpty())
            return Collections.emptyList();

        List<TxnId> ids = new ArrayList<>(result.size());
        for (UntypedResultSet.Row row : result)
            ids.add(deserializeTimestamp(row, "txn_id", TxnId::new));
        return ids;
    }

    public static List<TxnId> commandsForKeysSeriesEntriesBefore(CommandStore commandStore, AccordKey.PartitionKey key, AccordCommandsForKey.SeriesKind kind, Timestamp time)
    {
        String cql = "SELECT txn_id FROM %s.%s " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?), " +
                     "series=?, " +
                     "timestamp < (?, ?, ?, ?)";
        UntypedResultSet result = executeOnceInternal(format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                                                      commandStore.generation(), commandStore.index(),
                                                      key.tableId(), key.partitionKey().getKey(),
                                                      kind.ordinal(),
                                                      time.epoch, time.real, time.logical, time.node.id);
        return createTxnIdList(result);
    }

    public static List<TxnId> commandsForKeysSeriesEntriesAfter(CommandStore commandStore, AccordKey.PartitionKey key, AccordCommandsForKey.SeriesKind kind, Timestamp time)
    {
        String cql = "SELECT txn_id FROM %s.%s " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?), " +
                     "series=?, " +
                     "timestamp > (?, ?, ?, ?)";
        UntypedResultSet result = executeOnceInternal(format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                                                      commandStore.generation(), commandStore.index(),
                                                      key.tableId(), key.partitionKey().getKey(),
                                                      kind.ordinal(),
                                                      time.epoch, time.real, time.logical, time.node.id);
        return createTxnIdList(result);
    }

    public static List<TxnId> commandsForKeysSeriesEntriesBetween(CommandStore commandStore, AccordKey.PartitionKey key, AccordCommandsForKey.SeriesKind kind, Timestamp min, Timestamp max)
    {
        String cql = "SELECT txn_id FROM %s.%s " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?), " +
                     "series=?, " +
                     "timestamp >= (?, ?, ?, ?), " +
                     "timestamp <= (?, ?, ?, ?)";
        UntypedResultSet result = executeOnceInternal(format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                                                      commandStore.generation(), commandStore.index(),
                                                      key.tableId(), key.partitionKey().getKey(),
                                                      kind.ordinal(),
                                                      min.epoch, min.real, min.logical, min.node.id,
                                                      max.epoch, max.real, max.logical, max.node.id);
        return createTxnIdList(result);
    }

    public static List<TxnId> allCommandsForKeysSeriesEntries(CommandStore commandStore, AccordKey.PartitionKey key, AccordCommandsForKey.SeriesKind kind)
    {
        String cql = "SELECT txn_id FROM %s.%s " +
                     "WHERE store_generation=?, " +
                     "store_index=?, " +
                     "key=(?, ?), " +
                     "series=?";
        UntypedResultSet result = executeOnceInternal(format(cql, ACCORD_KEYSPACE_NAME, COMMANDS_FOR_KEY),
                                                      commandStore.generation(), commandStore.index(),
                                                      key.tableId(), key.partitionKey().getKey(),
                                                      kind.ordinal());
        return createTxnIdList(result);
    }
}
