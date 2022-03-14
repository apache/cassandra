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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import accord.impl.InMemoryCommand;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Status;
import accord.txn.Ballot;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import com.beust.jcommander.internal.Lists;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
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

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeOnceInternal;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;

// TODO: rework all of the io methods to emit/accumulate mutations
public class AccordKeyspace
{
    public static final String COMMANDS = "commands";
    public static final String COMMANDS_FOR_KEY = "commands_for_key";

    private static final String TIMESTAMP_TUPLE = "tuple<bigint, bigint, int, bigint>";
    private static final TupleType TIMESTAMP_TYPE = new TupleType(Lists.newArrayList(LongType.instance, LongType.instance, Int32Type.instance, LongType.instance));
    private static final String KEY_TUPLE = "tuple<uuid, blob>";
    private static final TupleType KEY_TYPE = new TupleType(Lists.newArrayList(UUIDType.instance, BytesType.instance));

    private static final TableMetadata Commands =
        parse(COMMANDS,
              "accord commands",
              "CREATE TABLE %s ("
              + "store_generation int,"
              + "store_index int,"
              + format("txn_id %s,", TIMESTAMP_TUPLE)
              + "status int,"
              + "serializer_version int,"
              + "txn blob,"
              + format("execute_at %s,", TIMESTAMP_TUPLE)
              + format("promised_ballot %s,", TIMESTAMP_TUPLE)
              + format("accepted_ballot %s,", TIMESTAMP_TUPLE)
              + "dependencies blob,"
              + "writes blob,"
              + "result blob,"
              + "PRIMARY KEY((store_generation, store_index, txn_id))"
              + ')');

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

    public static void saveCommand(Command command) throws IOException
    {
        int version = MessagingService.current_version;
        TxnId txnId = command.txnId();
        Timestamp executeAt = command.executeAt();
        Ballot promised = command.promised();
        Ballot accepted = command.accepted();
        String cql = "UPDATE %s.%s " +
                     "SET status=?, " +
                     "txn=?, " +
                     "execute_at=(?, ?, ?, ?), " +
                     "promised_ballot=(?, ?, ?, ?), " +
                     "accepted_ballot=(?, ?, ?, ?), " +
                     "serializer_version=?, " +
                     "dependencies=?, " +
                     "writes=?, " +
                     "result=? " +
                     "WHERE store_generation=? AND store_index=? AND txn_id=(?, ?, ?, ?)";
        executeOnceInternal(String.format(cql, ACCORD_KEYSPACE_NAME, COMMANDS),
                            command.status().ordinal(),
                            serializeOrNull(command.txn(), CommandSerializers.txn, version),
                            executeAt.epoch, executeAt.real, executeAt.logical, executeAt.node.id,
                            promised.epoch, promised.real, promised.logical, promised.node.id,
                            accepted.epoch, accepted.real, accepted.logical, accepted.node.id,
                            version,
                            serialize(command.savedDeps(), CommandSerializers.deps, version),
                            serializeOrNull(command.writes(), CommandSerializers.writes, version),
                            serializeOrNull((AccordData) command.result(), AccordData.serializer, version),
                            command.commandStore().generation(),
                            command.commandStore().index(),
                            txnId.epoch, txnId.real, txnId.logical, txnId.node.id);
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

    public static AccordCommand loadCommand(CommandStore commandStore, TxnId txnId) throws IOException
    {
        String cql = "SELECT * FROM %s.%s " +
                     "WHERE store_generation=? " +
                     "AND store_index=? " +
                     "AND txn_id=(?, ?, ?, ?)";

        UntypedResultSet result = executeOnceInternal(String.format(cql, ACCORD_KEYSPACE_NAME, COMMANDS),
                                                      commandStore.generation(),
                                                      commandStore.index(),
                                                      txnId.epoch, txnId.real, txnId.logical, txnId.node.id);

        if (result.isEmpty())
            return null;

        UntypedResultSet.Row row = result.one();
        Preconditions.checkState(deserializeTimestamp(row, "txn_id", TxnId::new).equals(txnId));
        AccordCommand command = new AccordCommand(commandStore, txnId);
        int version = row.getInt("serializer_version");
        command.status(Status.values()[row.getInt("status")]);
        command.txn(deserializeOrNull(row.getBlob("txn"), CommandSerializers.txn, version));
        command.executeAt(deserializeTimestamp(row, "execute_at", Timestamp::new));
        command.promised(deserializeTimestamp(row, "promised_ballot", Ballot::new));
        command.accepted(deserializeTimestamp(row, "accepted_ballot", Ballot::new));
        command.savedDeps(deserialize(row.getBlob("dependencies"), CommandSerializers.deps, version));
        command.writes(deserializeOrNull(row.getBlob("writes"), CommandSerializers.writes, version));
        command.result(deserializeOrNull(row.getBlob("result"), AccordData.serializer, version));
        return command;
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
