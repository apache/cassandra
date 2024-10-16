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

import accord.api.Data;
import accord.api.LocalListeners;
import accord.api.ProgressLog.NoOpProgressLog;
import accord.api.RemoteListeners;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.impl.DefaultLocalListeners;
import accord.impl.InMemoryCommandStore;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommonAttributes;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeCommandStoreService;
import accord.local.TimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
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
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.ManualExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.AccordSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnKeyRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.primitives.Routable.Domain.Key;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static java.lang.String.format;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class AccordTestUtils
{
    private static final AccordAgent AGENT = new AccordAgent();
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
            attrs.setParticipants(StoreParticipants.all(route(txn)));
            attrs.durability(Status.Durability.NotDurable);
            return Command.SerializerSupport.preaccepted(attrs, executeAt, Ballot.ZERO);
        }

        public static Command committed(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId).partialDeps(PartialDeps.NONE);
            attrs.partialTxn(txn);
            attrs.setParticipants(StoreParticipants.all(route(txn)));
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
            attrs.setParticipants(StoreParticipants.all(route(txn)));
            return Command.SerializerSupport.committed(attrs,
                                                       SaveStatus.Stable,
                                                       executeAt,
                                                       Ballot.ZERO,
                                                       Ballot.ZERO,
                                                       Command.WaitingOn.empty(txnId.domain()));
        }

        private static FullRoute<?> route(PartialTxn txn)
        {
            Seekable key = txn.keys().get(0);
            RoutingKey routingKey = key.asKey().toUnseekable();
            return new FullKeyRoute(routingKey, new RoutingKey[]{ routingKey });
        }
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

    public static TxnId txnId(long epoch, long hlc, int node)
    {
        return txnId(epoch, hlc, node, Txn.Kind.Write);
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

    public static Pair<Writes, Result> processTxnResult(AccordCommandStore commandStore, TxnId txnId, PartialTxn txn, Timestamp executeAt) throws Throwable
    {
        AtomicReference<Pair<Writes, Result>> result = new AtomicReference<>();
        getUninterruptibly(commandStore.execute(PreLoadContext.contextFor(txn.keys().toParticipants()),
                           safeStore -> result.set(processTxnResultDirect(safeStore, txnId, txn, executeAt))));
        return result.get();
    }

    public static Pair<Writes, Result> processTxnResultDirect(SafeCommandStore safeStore, TxnId txnId, PartialTxn txn, Timestamp executeAt)
    {
        TxnKeyRead read = (TxnKeyRead) txn.read();
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
        return new Txn.InMemory(kind, seekables, TxnKeyRead.EMPTY, TxnQuery.NONE, null);
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
        return new PartialTxn.InMemory(txn.kind(), txn.keys(), txn.read(), txn.query(), txn.update());
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
        NodeCommandStoreService time = new NodeCommandStoreService()
        {
            private ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, this::now);

            @Override public Id id() { return node;}
            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }

            @Override public long epoch() {return 1; }
            @Override public long now() {return now.getAsLong(); }
            @Override public Timestamp uniqueNow() { return uniqueNow(Timestamp.NONE); }
            @Override public Timestamp uniqueNow(Timestamp atLeast) { return Timestamp.fromValues(1, now.getAsLong(), node); }
            @Override public long elapsed(TimeUnit timeUnit) { return elapsed.applyAsLong(timeUnit); }
        };

        SingleEpochRanges holder = new SingleEpochRanges(Ranges.of(range));
        InMemoryCommandStore.Synchronized result = new InMemoryCommandStore.Synchronized(0, time, new AccordAgent(),
                                                     null, null, cs -> null, holder);
        holder.set(result);
        return result;
    }

    public static AccordCommandStore createAccordCommandStore(
        Node.Id node, LongSupplier now, Topology topology, ExecutorPlus loadExecutor, ExecutorPlus saveExecutor)
    {
        NodeCommandStoreService time = new NodeCommandStoreService()
        {
            private ToLongFunction<TimeUnit> elapsed = TimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, this::now);

            @Override public DurableBefore durableBefore() { return DurableBefore.EMPTY; }
            @Override public Id id() { return node;}
            @Override public long epoch() {return 1; }
            @Override public long now() {return now.getAsLong(); }
            @Override public Timestamp uniqueNow() { return uniqueNow(Timestamp.NONE); }
            @Override public Timestamp uniqueNow(Timestamp atLeast) { return Timestamp.fromValues(1, now.getAsLong(), node); }
            @Override
            public long elapsed(TimeUnit timeUnit) { return elapsed.applyAsLong(timeUnit); }
        };


        if (new File(DatabaseDescriptor.getAccordJournalDirectory()).exists())
            ServerTestUtils.cleanupDirectory(DatabaseDescriptor.getAccordJournalDirectory());
        AccordJournal journal = new AccordJournal(new AccordSpec.JournalSpec());
        journal.start(null);

        AccordStateCache stateCache = new AccordStateCache(loadExecutor, saveExecutor, 8 << 20, new AccordStateCacheMetrics("test"));
        SingleEpochRanges holder = new SingleEpochRanges(topology.rangesForNode(node));
        AccordCommandStore result = new AccordCommandStore(0,
                                                           time,
                                                           new AccordAgent(),
                                                           null,
                                                           cs -> new NoOpProgressLog(),
                                                           cs -> new DefaultLocalListeners(new RemoteListeners.NoOpRemoteListeners(), new DefaultLocalListeners.NotifySink()
                                                           {
                                                               @Override public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listener) {}
                                                               @Override public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, LocalListeners.ComplexListener listener) { return false; }
                                                           }),
                                                           holder,
                                                           journal,
                                                           new AccordCommandStore.CommandStoreExecutor(stateCache, executorFactory().sequential(CommandStore.class.getSimpleName() + '[' + 0 + ']')));
        holder.set(result);

        // TODO: CompactionAccordIteratorsTest relies on this
        result.execute(PreLoadContext.empty(),
                       result::updateRangesForEpoch)
              .beginAsResult();
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
        Topology topology = new Topology(1, new Shard(range, new SortedArrayList<>(new Id[] { node }), Sets.newHashSet(node), Collections.emptySet()));
        AccordCommandStore store = createAccordCommandStore(node, now, topology, loadExecutor, saveExecutor);
        store.execute(PreLoadContext.empty(), safeStore -> ((AccordCommandStore)safeStore.commandStore()).cache().setCapacity(1 << 20));
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

    public static void appendCommandsBlocking(AccordCommandStore commandStore, Command after)
    {
        appendCommandsBlocking(commandStore, null, after);
    }

    public static void appendCommandsBlocking(AccordCommandStore commandStore, Command before, Command after)
    {
        SavedCommand.DiffWriter diff = SavedCommand.diff(before, after);
        if (diff == null) return;
        Condition condition = Condition.newOneTimeCondition();
        commandStore.appendCommands(Collections.singletonList(diff), condition::signal);
        condition.awaitUninterruptibly(30, TimeUnit.SECONDS);
    }
}
