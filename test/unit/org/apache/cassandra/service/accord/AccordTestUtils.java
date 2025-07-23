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
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;
import org.junit.Assert;

import accord.api.AsyncExecutor;
import accord.api.Data;
import accord.api.Journal;
import accord.api.ProgressLog.NoOpProgressLog;
import accord.api.RemoteListeners.NoOpRemoteListeners;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Timeouts;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultLocalListeners.NotifySink.NoOpNotifySink;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.SequentialAsyncExecutor;
import accord.local.StoreParticipants;
import accord.local.TimeService;
import accord.local.durability.DurabilityService;
import accord.primitives.Ballot;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.SaveStatus;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ManualExecutor;
import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.AccordCacheEntry.LoadExecutor;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.SaveStatus.NotDefined;
import static accord.primitives.SaveStatus.PreAccepted;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static java.lang.String.format;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;

public class AccordTestUtils
{
    public static final TableId TABLE_ID1 = TableId.fromString("00000000-0000-0000-0000-000000000001");

    public static class Commands
    {
        public static Command notDefined(TxnId txnId, PartialTxn txn)
        {
            return Command.NotDefined.notDefined(txnId, NotDefined, NotDurable, StoreParticipants.empty(txnId), Ballot.ZERO);
        }

        public static Command preaccepted(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            return Command.PreAccepted.preaccepted(txnId, PreAccepted, NotDurable, StoreParticipants.all(route(txn)), Ballot.ZERO, executeAt, txn, null);
        }

        public static Command committed(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            return Command.Committed.committed(txnId, SaveStatus.Committed, NotDurable, StoreParticipants.all(route(txn)),
                                               Ballot.ZERO, executeAt, txn, PartialDeps.NONE, Ballot.ZERO, null);
        }

        public static Command stable(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            return Command.Committed.committed(txnId, SaveStatus.Stable, NotDurable, StoreParticipants.all(route(txn)),
                                               Ballot.ZERO, executeAt, txn, PartialDeps.NONE, Ballot.ZERO, Command.WaitingOn.empty(txnId.domain()));
        }

        private static FullRoute<?> route(PartialTxn txn)
        {
            Seekable key = txn.keys().get(0);
            RoutingKey routingKey = key.asKey().toUnseekable();
            return new FullKeyRoute(routingKey, new RoutingKey[]{ routingKey });
        }
    }

    public static <K, V> AccordCacheEntry<K, V> loaded(K key, V value)
    {
        AccordCacheEntry<K, V> global = new AccordCacheEntry<>(key, null);
        global.initialize(value);
        return global;
    }

    public static AccordSafeCommand safeCommand(Command command)
    {
        AccordCacheEntry<TxnId, Command> global = loaded(command.txnId(), command);
        return new AccordSafeCommand(global);
    }

    public static <K, V> Function<K, V> testableLoad(K key, V val)
    {
        return k -> {
            Assert.assertEquals(key, k);
            return val;
        };
    }

    private static <P1, P2> LoadExecutor<P1, P2> loadExecutor(ExecutorPlus executor)
    {
        return new LoadExecutor<>()
        {
            @Override
            public <K, V> Cancellable load(P1 p1, P2 p2, AccordCacheEntry<K, V> entry)
            {
                Future<?> future = executor.submit(() -> {
                    V v;
                    try { v = entry.owner.parent().adapter().load(entry.owner.commandStore, entry.key()); }
                    catch (Throwable t)
                    {
                        entry.failedToLoad();
                        throw t;
                    }
                    entry.loaded(v);
                });
                return () -> future.cancel(true);
            }
        };
    }

    public static <K, V> void testLoad(ManualExecutor executor, AccordSafeState<K, V> safeState, V val)
    {
        Assert.assertEquals(AccordCacheEntry.Status.WAITING_TO_LOAD, safeState.global().status());
        safeState.global().load(loadExecutor(executor), null, null);
        Assert.assertEquals(AccordCacheEntry.Status.LOADING, safeState.global().status());
        executor.runOne();
        Assert.assertEquals(AccordCacheEntry.Status.LOADED, safeState.global().status());
        safeState.preExecute();
        Assert.assertEquals(val, safeState.current());
    }

    public static TxnId txnId(long epoch, long hlc, int node)
    {
        return txnId(epoch, hlc, node, Write);
    }

    public static TxnId txnId(long epoch, long hlc, int node, Txn.Kind kind)
    {
        return new TxnId(epoch, hlc, kind, Key, new Node.Id(node));
    }

    public static TxnId txnId(long epoch, long hlc, int node, Txn.Kind kind, Routable.Domain domain)
    {
        return new TxnId(epoch, hlc, kind, domain, new Node.Id(node));
    }

    public static Timestamp timestamp(long epoch, long hlc, int node)
    {
        return Timestamp.fromValues(epoch, hlc, new Node.Id(node));
    }

    public static Ballot ballot(long epoch, long hlc, int node)
    {
        return Ballot.fromValues(epoch, hlc, new Node.Id(node));
    }

    public static AsyncChain<Pair<Writes, Result>> processTxnResult(AccordCommandStore commandStore, TxnId txnId, PartialTxn txn, Timestamp executeAt) throws Throwable
    {
        AtomicReference<AsyncChain<Pair<Writes, Result>>> result = new AtomicReference<>();
        getUninterruptibly(commandStore.execute((PreLoadContext.Empty)() -> "Test",
                           safeStore -> result.set(processTxnResultDirect(safeStore, txnId, txn, executeAt))));
        return result.get();
    }

    public static AsyncChain<Pair<Writes, Result>> processTxnResultDirect(SafeCommandStore safeStore, TxnId txnId, PartialTxn txn, Timestamp executeAt)
    {
        TxnRead read = (TxnRead) txn.read();
        return AsyncChains.allOf(read.keys().stream().map(key -> read.read(safeStore, key, executeAt))
                                                          .collect(Collectors.toList()))
                                               .map(list -> {
                                                   Data data = list.stream().reduce(Data::merge).orElse(new TxnData());
                                                   return Pair.create(txnId.is(Write) ? txn.execute(txnId, executeAt, data) : null,
                                                                      txn.query().compute(txnId, executeAt, txn.keys(), data, txn.read(), txn.update()));
                                               });
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

    public static Txn createTxn(String query, Object... binds)
    {
        return createTxn(query, Arrays.asList(binds));
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

    public static Txn createTxn(Txn.Kind kind, Seekables<?, ?> seekables)
    {
        TableMetadatas.Collector tables = new TableMetadatas.Collector();
        for (Seekable seekable : seekables)
            tables.add(TableMetadata.minimal("", "", (TableId)seekable.prefix()));
        return new Txn.InMemory(kind, seekables, TxnRead.empty(seekables.domain()), TxnQuery.NONE, null, new TableMetadatasAndKeys(tables.build(), seekables));
    }

    public static Ranges fullRange(Txn txn)
    {
        return fullRange(txn.keys());
    }

    public static Ranges fullRange(Seekables<?, ?> keys)
    {
        PartitionKey key = (PartitionKey) keys.get(0);
        return Ranges.of(TokenRange.fullRange(key.table(), DatabaseDescriptor.getPartitioner()));
    }

    public static PartialTxn createPartialTxn(int key)
    {
        Txn txn = createTxn(key, key);
        TableMetadatas.Collector tables = new TableMetadatas.Collector();
        for (Seekable seekable : txn.keys())
            tables.add(TableMetadata.minimal("", "", (TableId)seekable.prefix()));
        return new PartialTxn.InMemory(txn.kind(), txn.keys(), txn.read(), txn.query(), txn.update(), new TableMetadatasAndKeys(tables.build(), txn.keys()));
    }

    public static AccordCommandStore createAccordCommandStore(
        Node.Id node, LongSupplier now, Topology topology)
    {
        AccordExecutor executor = new AccordExecutorSyncSubmit(0, RUN_WITH_LOCK, CommandStore.class.getSimpleName() + '[' + 0 + ']', new AccordCacheMetrics("test"), new AccordAgent());
        return createAccordCommandStore(node, now, topology, executor);
    }

    public static AccordCommandStore createAccordCommandStore(
        Node.Id node, LongSupplier now, Topology topology, AccordExecutor executor)
    {
        NodeCommandStoreService time = new NodeCommandStoreService()
        {
            @Override
            public AsyncExecutor someExecutor()
            {
                return null;
            }

            @Override
            public SequentialAsyncExecutor someSequentialExecutor()
            {
                return null;
            }

            private ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, this::now);
            private long stamp = 0;

            @Override public Timeouts timeouts() { return null; }
            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }
            @Override public DurabilityService durability() { return null; }
            @Override public Id id() { return node;}
            @Override public long epoch() {return 1; }
            @Override public long now() {return now.getAsLong(); }
            @Override public long uniqueNow(long atLeast) { return now.getAsLong(); }
            @Override public long elapsed(TimeUnit timeUnit) { return elapsed.applyAsLong(timeUnit); }
            @Override public TopologyManager topology() { throw new UnsupportedOperationException(); }
            @Override public long currentStamp() { return stamp; }
            @Override public void updateStamp() {++stamp;}
        };

        AccordAgent agent = new AccordAgent();
        if (new File(DatabaseDescriptor.getAccordJournalDirectory()).exists())
            ServerTestUtils.cleanupDirectory(DatabaseDescriptor.getAccordJournalDirectory());
        AccordSpec.JournalSpec spec = new AccordSpec.JournalSpec();
        spec.flushPeriod = new DurationSpec.IntSecondsBound(1);
        AccordJournal journal = new AccordJournal(spec);
        journal.start(null);

        CommandStore.EpochUpdateHolder holder = new CommandStore.EpochUpdateHolder();
        Ranges ranges = topology.rangesForNode(node);
        holder.add(1, new CommandStores.RangesForEpoch(1, ranges), ranges);
        AccordCommandStore result = new AccordCommandStore(0, time, agent, null,
                                                           cs -> new NoOpProgressLog(),
                                                           cs -> new DefaultLocalListeners(new NoOpRemoteListeners(), new NoOpNotifySink()),
                                                           holder, journal, executor);
        result.unsafeUpdateRangesForEpoch();
        return result;
    }

    public static AccordCommandStore createAccordCommandStore(
        LongSupplier now, String keyspace, String table)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        TokenRange range = TokenRange.fullRange(metadata.id, metadata.partitioner);
        Node.Id node = new Id(1);
        Topology topology = new Topology(1, Shard.create(range, new SortedArrayList<>(new Id[] { node }), Sets.newHashSet(node), Collections.emptySet()));
        AccordCommandStore store = createAccordCommandStore(node, now, topology);
        store.execute((PreLoadContext.Empty)()->"Test", safeStore -> ((AccordCommandStore)safeStore.commandStore()).executor().cacheUnsafe().setCapacity(1 << 20));
        return store;
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

    public static SortedArrayList<Id> idList(int... ids)
    {
        return new SortedArrayList<>(Arrays.stream(ids).mapToObj(AccordTestUtils::id).toArray(Id[]::new));
    }

    public static Set<Id> idSet(int... ids)
    {
        return Arrays.stream(ids).mapToObj(AccordTestUtils::id).collect(Collectors.toSet());
    }

    public static Token token(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    public static Range<Token> range(Token left, Token right)
    {
        return new Range<>(left, right);
    }

    public static Range<Token> range(long left, long right)
    {
        return range(token(left), token(right));
    }

    public static void appendCommandsBlocking(AccordCommandStore commandStore, Command before, Command after)
    {
        Journal.CommandUpdate diff = new Journal.CommandUpdate(before, after);
        Condition condition = Condition.newOneTimeCondition();
        commandStore.appendCommands(Collections.singletonList(diff), condition::signal);
        condition.awaitUninterruptibly(30, TimeUnit.SECONDS);
    }
}
