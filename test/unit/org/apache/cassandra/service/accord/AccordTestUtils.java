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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;

import accord.api.Data;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.impl.CommandsForKey;
import accord.impl.InMemoryCommandStore;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.SaveStatus.LocalExecution;
import accord.primitives.Ballot;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.ManualExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.primitives.Routable.Domain.Key;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static java.lang.String.format;

public class AccordTestUtils
{
    public static final TableId TABLE_ID1 = TableId.fromString("00000000-0000-0000-0000-000000000001");

    public static class Commands
    {
        public static Command notDefined(TxnId txnId, PartialTxn txn)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
            attrs.partialTxn(txn);
            return Command.SerializerSupport.notDefined(attrs, Ballot.ZERO);
        }

        public static Command preaccepted(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
            attrs.partialTxn(txn);
            attrs.route(route(txn));
            return Command.SerializerSupport.preaccepted(attrs, executeAt, Ballot.ZERO);
        }

        public static Command committed(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId).partialDeps(PartialDeps.NONE);
            attrs.partialTxn(txn);
            attrs.route(route(txn));
            return Command.SerializerSupport.committed(attrs,
                                                       SaveStatus.Committed,
                                                       executeAt,
                                                       Ballot.ZERO,
                                                       Ballot.ZERO,
                                                       null);
        }

        public static Command stable(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId).partialDeps(PartialDeps.NONE);
            attrs.partialTxn(txn);
            attrs.route(route(txn));
            return Command.SerializerSupport.committed(attrs,
                                                       SaveStatus.Stable,
                                                       executeAt,
                                                       Ballot.ZERO,
                                                       Ballot.ZERO,
                                                       Command.WaitingOn.EMPTY);
        }

        private static FullRoute<?> route(PartialTxn txn)
        {
            Seekable key = txn.keys().get(0);
            RoutingKey routingKey = key.asKey().toUnseekable();
            return new FullKeyRoute(routingKey, true, new RoutingKey[]{ routingKey });
        }
    }

    public static CommandsForKey commandsForKey(Key key)
    {
        return new CommandsForKey(key, CommandsForKeySerializer.loader);
    }

    public static <K, V> AccordCachingState<K, V> loaded(K key, V value, int index)
    {
        AccordCachingState<K, V> global = new AccordCachingState<>(key, index);
        global.load(ImmediateExecutor.INSTANCE, k -> {
            Assert.assertEquals(key, k);
            return value;
        });
        Assert.assertEquals(AccordCachingState.Status.LOADED, global.status());
        return global;
    }

    public static <K, V> AccordCachingState<K, V> loaded(K key, V value)
    {
        return loaded(key, value, 0);
    }

    public static AccordSafeCommand safeCommand(Command command)
    {
        AccordCachingState<TxnId, Command> global = loaded(command.txnId(), command);
        return new AccordSafeCommand(global);
    }

    public static <K, V> Function<K, V> testableLoad(K key, V val)
    {
        return k -> {
            Assert.assertEquals(key, k);
            return val;
        };
    }

    public static <K, V> void testLoad(ManualExecutor executor, AccordSafeState<K, V> safeState, V val)
    {
        Assert.assertEquals(AccordCachingState.Status.LOADING, safeState.globalStatus());
        executor.runOne();
        Assert.assertEquals(AccordCachingState.Status.LOADED, safeState.globalStatus());
        safeState.preExecute();
        Assert.assertEquals(val, safeState.current());
    }

    public static final ProgressLog NOOP_PROGRESS_LOG = new ProgressLog()
    {
        @Override public void unwitnessed(TxnId txnId, ProgressShard progressShard) {}
        @Override public void preaccepted(Command command, ProgressShard progressShard) {}
        @Override public void accepted(Command command, ProgressShard progressShard) {}
        @Override public void precommitted(Command command) {}
        @Override public void stable(Command command, ProgressShard progressShard) {}
        @Override public void readyToExecute(Command command) {}
        @Override public void executed(Command command, ProgressShard progressShard) {}
        @Override public void clear(TxnId txnId) {}
        @Override public void durable(Command command) {}
        @Override
        public void waiting(SafeCommand blockedBy, LocalExecution blockedUntil, Route<?> blockedOnRoute, Participants<?> blockedOnParticipants) {}
    };

    public static TxnId txnId(long epoch, long hlc, int node)
    {
        return txnId(epoch, hlc, node, Txn.Kind.Write);
    }

    public static TxnId txnId(long epoch, long hlc, int node, Txn.Kind kind)
    {
        return new TxnId(epoch, hlc, kind, Key, new Node.Id(node));
    }

    public static Timestamp timestamp(long epoch, long hlc, int node)
    {
        return Timestamp.fromValues(epoch, hlc, new Node.Id(node));
    }

    public static Ballot ballot(long epoch, long hlc, int node)
    {
        return Ballot.fromValues(epoch, hlc, new Node.Id(node));
    }

    public static Pair<Writes, Result> processTxnResult(AccordCommandStore commandStore, TxnId txnId, PartialTxn txn, Timestamp executeAt) throws Throwable
    {
        AtomicReference<Pair<Writes, Result>> result = new AtomicReference<>();
        getUninterruptibly(commandStore.execute(PreLoadContext.contextFor(txn.keys()),
                           safeStore -> result.set(processTxnResultDirect(safeStore, txnId, txn, executeAt))));
        return result.get();
    }

    public static Pair<Writes, Result> processTxnResultDirect(SafeCommandStore safeStore, TxnId txnId, PartialTxn txn, Timestamp executeAt)
    {
        TxnRead read = (TxnRead) txn.read();
        Data readData = read.keys().stream().map(key -> {
                                try
                                {
                                    return AsyncChains.getBlocking(read.read(key, safeStore, executeAt, null));
                                }
                                catch (InterruptedException e)
                                {
                                    throw new UncheckedInterruptedException(e);
                                }
                                catch (ExecutionException e)
                                {
                                    throw new RuntimeException(e);
                                }
                            })
                            .reduce(null, TxnData::merge);
        return Pair.create(txn.execute(txnId, executeAt, readData),
                           txn.query().compute(txnId, executeAt, txn.keys(), readData, txn.read(), txn.update()));

    }

    public static String wrapInTxn(String query)
    {
        if (!query.endsWith(";"))
            query += ";";
        return "BEGIN TRANSACTION\n" +
               query +
               "\nCOMMIT TRANSACTION";
    }

    public static Txn createTxn(String query)
    {
        return createTxn(query, QueryOptions.DEFAULT);
    }

    public static Txn createTxn(String query, List<Object> binds)
    {
        TransactionStatement statement = parse(query);
        QueryOptions options = QueryProcessor.makeInternalOptions(statement, binds.toArray(new Object[binds.size()]));
        return statement.createTxn(ClientState.forInternalCalls(), options);
    }

    public static Txn createTxn(String query, QueryOptions options)
    {
        TransactionStatement statement = parse(query);
        return statement.createTxn(ClientState.forInternalCalls(), options);
    }

    public static TransactionStatement parse(String query)
    {
        TransactionStatement.Parsed parsed = (TransactionStatement.Parsed) QueryProcessor.parseStatement(query);
        Assert.assertNotNull(parsed);
        TransactionStatement statement = (TransactionStatement) parsed.prepare(ClientState.forInternalCalls());
        return statement;
    }

    public static Txn createTxn(int readKey, int... writeKeys)
    {
        StringBuilder sb = new StringBuilder("BEGIN TRANSACTION\n");
        sb.append(format("LET row1 = (SELECT * FROM ks.tbl WHERE k=%s AND c=0);\n", readKey));
        sb.append("SELECT row1.v;\n");
        if (writeKeys.length > 0)
        {
            sb.append("IF row1 IS NULL THEN\n");
            for (int key : writeKeys)
                sb.append(format("INSERT INTO ks.tbl (k, c, v) VALUES (%s, 0, 1);\n", key));
            sb.append("END IF\n");
        }
        sb.append("COMMIT TRANSACTION");
        return createTxn(sb.toString());
    }

    public static Txn createWriteTxn(int key)
    {
        return createTxn(key, key);
    }

    public static Ranges fullRange(Txn txn)
    {
        return fullRange(txn.keys());
    }

    public static Ranges fullRange(Seekables<?, ?> keys)
    {
        PartitionKey key = (PartitionKey) keys.get(0);
        return Ranges.of(TokenRange.fullRange(key.table()));
    }

    public static PartialTxn createPartialTxn(int key)
    {
        Txn txn = createTxn(key, key);
        Ranges ranges = fullRange(txn);
        return new PartialTxn.InMemory(ranges, txn.kind(), txn.keys(), txn.read(), txn.query(), txn.update());
    }

    private static class SingleEpochRanges extends CommandStore.EpochUpdateHolder
    {
        private final Ranges ranges;

        public SingleEpochRanges(Ranges ranges)
        {
            this.ranges = ranges;
        }

        private void set(CommandStore store)
        {
            add(1, new CommandStores.RangesForEpoch(1, ranges, store), ranges);
        }
    }

    public static InMemoryCommandStore.Synchronized createInMemoryCommandStore(LongSupplier now, String keyspace, String table)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        TokenRange range = TokenRange.fullRange(metadata.id);
        Node.Id node = new Id(1);
        Topology topology = new Topology(1, new Shard(range, Lists.newArrayList(node), Sets.newHashSet(node), Collections.emptySet()));
        NodeTimeService time = new NodeTimeService()
        {
            @Override public Id id() { return node;}
            @Override public long epoch() {return 1; }
            @Override public long now() {return now.getAsLong(); }
            @Override public Timestamp uniqueNow(Timestamp atLeast) { return Timestamp.fromValues(1, now.getAsLong(), node); }
            @Override
            public long unix(TimeUnit timeUnit) { return NodeTimeService.unixWrapper(TimeUnit.MICROSECONDS, this::now).applyAsLong(timeUnit); }
        };

        SingleEpochRanges holder = new SingleEpochRanges(Ranges.of(range));
        InMemoryCommandStore.Synchronized result = new InMemoryCommandStore.Synchronized(0,
                                                     time,
                                                     new AccordAgent(),
                                                     null,
                                                     cs -> null, holder);
        holder.set(result);
        return result;
    }

    public static AccordCommandStore createAccordCommandStore(
        Node.Id node, LongSupplier now, Topology topology, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor)
    {
        NodeTimeService time = new NodeTimeService()
        {
            @Override public Id id() { return node;}
            @Override public long epoch() {return 1; }
            @Override public long now() {return now.getAsLong(); }
            @Override public Timestamp uniqueNow(Timestamp atLeast) { return Timestamp.fromValues(1, now.getAsLong(), node); }
            @Override
            public long unix(TimeUnit timeUnit) { return NodeTimeService.unixWrapper(TimeUnit.MICROSECONDS, this::now).applyAsLong(timeUnit); }
        };

        AccordJournal journal = new AccordJournal(null);
        journal.start(null);

        SingleEpochRanges holder = new SingleEpochRanges(topology.rangesForNode(node));
        AccordCommandStore result = new AccordCommandStore(0,
                                                           time,
                                                           new AccordAgent(),
                                                           null,
                                                           cs -> NOOP_PROGRESS_LOG,
                                                           holder,
                                                           journal,
                                                           loadExecutor,
                                                           saveExecutor,
                                                           new AccordStateCacheMetrics(AccordCommandStores.ACCORD_STATE_CACHE + System.currentTimeMillis()));
        holder.set(result);
        result.updateRangesForEpoch();
        return result;
    }

    public static AccordCommandStore createAccordCommandStore(Node.Id node, LongSupplier now, Topology topology)
    {
        return createAccordCommandStore(node, now, topology, Stage.READ.executor(), Stage.MUTATION.executor());
    }

    public static AccordCommandStore createAccordCommandStore(
        LongSupplier now, String keyspace, String table, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        TokenRange range = TokenRange.fullRange(metadata.id);
        Node.Id node = new Id(1);
        Topology topology = new Topology(1, new Shard(range, Lists.newArrayList(node), Sets.newHashSet(node), Collections.emptySet()));
        AccordCommandStore store = createAccordCommandStore(node, now, topology, loadExecutor, saveExecutor);
        store.execute(PreLoadContext.empty(), safeStore -> ((AccordCommandStore)safeStore.commandStore()).setCapacity(1 << 20));
        return store;
    }

    public static AccordCommandStore createAccordCommandStore(LongSupplier now, String keyspace, String table)
    {
        return createAccordCommandStore(now, keyspace, table, Stage.READ.executor(), Stage.MUTATION.executor());
    }

    public static void execute(AccordCommandStore commandStore, Runnable runnable)
    {
        try
        {
            commandStore.executor().submit(runnable).get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    public static PartitionKey key(TableMetadata table, int key)
    {
        DecoratedKey dk = table.partitioner.decorateKey(Int32Type.instance.decompose(key));
        return new PartitionKey(table.id, dk);
    }

    public static Keys keys(TableMetadata table, int... keys)
    {
        return Keys.of(IntStream.of(keys).mapToObj(key -> key(table, key)).collect(Collectors.toList()));
    }

    public static Node.Id id(int id)
    {
        return new Node.Id(id);
    }

    public static List<Node.Id> idList(int... ids)
    {
        return Arrays.stream(ids).mapToObj(AccordTestUtils::id).collect(Collectors.toList());
    }

    public static Set<Id> idSet(int... ids)
    {
        return Arrays.stream(ids).mapToObj(AccordTestUtils::id).collect(Collectors.toSet());
    }
}
