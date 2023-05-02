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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;

import accord.api.Data;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Write;
import accord.impl.CommandsForKey;
import accord.impl.InMemoryCommandStore;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SaveStatus;
import accord.local.Status.Known;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.TransactionStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.primitives.Routable.Domain.Key;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static java.lang.String.format;

public class AccordTestUtils
{
    public static Id localNodeId()
    {
        return EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
    }

    public static class Commands
    {
        public static Command notWitnessed(TxnId txnId, PartialTxn txn)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
            attrs.partialTxn(txn);
            return Command.SerializerSupport.notWitnessed(attrs, Ballot.ZERO);
        }

        public static Command preaccepted(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
            attrs.partialTxn(txn);
            return Command.SerializerSupport.preaccepted(attrs, executeAt, Ballot.ZERO);
        }

        public static Command committed(TxnId txnId, PartialTxn txn, Timestamp executeAt)
        {
            CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
            attrs.partialTxn(txn);
            return Command.SerializerSupport.committed(attrs,
                                                       SaveStatus.Committed,
                                                       executeAt,
                                                       Ballot.ZERO,
                                                       Ballot.ZERO,
                                                       ImmutableSortedSet.of(),
                                                       ImmutableSortedMap.of());
        }
    }

    public static CommandsForKey commandsForKey(Key key)
    {
        return new CommandsForKey(key, CommandsForKeySerializer.loader);
    }

    public static <K, V> AccordLoadingState<K, V> loaded(K key, V value)
    {
        AccordLoadingState<K, V> global = new AccordLoadingState<>(key);
        global.load(k -> {
            Assert.assertEquals(key, k);
            return value;
        }).run();
        Assert.assertEquals(AccordLoadingState.LoadingState.LOADED, global.state());
        return global;
    }

    public static AccordSafeCommand safeCommand(Command command)
    {
        AccordLoadingState<TxnId, Command> global = loaded(command.txnId(), command);
        return new AccordSafeCommand(global);
    }

    public static <K, V> Function<K, V> testableLoad(K key, V val)
    {
        return k -> {
            Assert.assertEquals(key, k);
            return val;
        };
    }

    public static <K, V> void testLoad(AccordSafeState<K, V> safeState, V val)
    {
        Assert.assertEquals(AccordLoadingState.LoadingState.UNINITIALIZED, safeState.loadingState());
        Runnable load = safeState.load(testableLoad(safeState.key(), val));
        Assert.assertEquals(AccordLoadingState.LoadingState.PENDING, safeState.loadingState());
        load.run();
        Assert.assertEquals(AccordLoadingState.LoadingState.LOADED, safeState.loadingState());
        safeState.preExecute();
        Assert.assertEquals(val, safeState.current());
    }

    public static final ProgressLog NOOP_PROGRESS_LOG = new ProgressLog()
    {
        @Override public void unwitnessed(TxnId txnId, RoutingKey routingKey, ProgressShard progressShard) {}
        @Override public void preaccepted(Command command, ProgressShard progressShard) {}
        @Override public void accepted(Command command, ProgressShard progressShard) {}
        @Override public void committed(Command command, ProgressShard progressShard) {}
        @Override public void readyToExecute(Command command, ProgressShard progressShard) {}
        @Override public void executed(Command command, ProgressShard progressShard) {}
        @Override public void invalidated(Command command, ProgressShard progressShard) {}
        @Override public void durableLocal(TxnId txnId) {}
        @Override public void durable(Command command, @Nullable Set<Id> set) {}
        @Override public void durable(TxnId txnId, @Nullable Unseekables<?, ?> unseekables, ProgressShard progressShard) {}
        @Override public void waiting(TxnId txnId, Known known, Unseekables<?, ?> unseekables) {}
    };

    public static TxnId txnId(long epoch, long hlc, int node)
    {
        return new TxnId(epoch, hlc, Txn.Kind.Write, Key, new Node.Id(node));
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
        getUninterruptibly(commandStore.execute(PreLoadContext.contextFor(Collections.emptyList(), txn.keys()),
                              safeStore -> {
                                  TxnRead read = (TxnRead) txn.read();
                                  Data readData = read.keys().stream().map(key -> {
                                                          try
                                                          {
                                                              return AsyncChains.getBlocking(read.read(key, txn.kind(), safeStore, executeAt, null));
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
                                  Write write = txn.update().apply(readData);
                                  result.set(Pair.create(new Writes(executeAt, (Keys)txn.keys(), write),
                                                         txn.query().compute(txnId, readData, txn.read(), txn.update())));
                              }));
        return result.get();
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
        sb.append("IF row1 IS NULL THEN\n");
        for (int key : writeKeys)
            sb.append(format("INSERT INTO ks.tbl (k, c, v) VALUES (%s, 0, 1);\n", key));
        sb.append("END IF\n");
        sb.append("COMMIT TRANSACTION");
        return createTxn(sb.toString());
    }

    public static Txn createTxn(int key)
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
        return Ranges.of(TokenRange.fullRange(key.keyspace()));
    }

    public static PartialTxn createPartialTxn(int key)
    {
        Txn txn = createTxn(key, key);
        Ranges ranges = fullRange(txn);
        return new PartialTxn.InMemory(ranges, txn.kind(), txn.keys(), txn.read(), txn.query(), txn.update());
    }

    private static class SingleEpochRanges extends CommandStores.RangesForEpochHolder
    {
        private final Ranges ranges;

        public SingleEpochRanges(Ranges ranges)
        {
            this.ranges = ranges;
            this.current = new CommandStores.RangesForEpoch(1, ranges);
        }
    }

    public static InMemoryCommandStore.Synchronized createInMemoryCommandStore(LongSupplier now, String keyspace, String table)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        TokenRange range = TokenRange.fullRange(metadata.keyspace);
        Node.Id node = EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
        Topology topology = new Topology(1, new Shard(range, Lists.newArrayList(node), Sets.newHashSet(node), Collections.emptySet()));
        NodeTimeService time = new NodeTimeService()
        {
            @Override public Id id() { return node;}
            @Override public long epoch() {return 1; }
            @Override public long now() {return now.getAsLong(); }
            @Override public Timestamp uniqueNow(Timestamp atLeast) { return Timestamp.fromValues(1, now.getAsLong(), node); }
        };
        return new InMemoryCommandStore.Synchronized(0,
                                                     time,
                                                     new AccordAgent(),
                                                     null,
                                                     cs -> null,
                                                     new SingleEpochRanges(Ranges.of(range)));
    }

    public static AccordCommandStore createAccordCommandStore(Node.Id node, LongSupplier now, Topology topology)
    {
        NodeTimeService time = new NodeTimeService()
        {
            @Override public Id id() { return node;}
            @Override public long epoch() {return 1; }
            @Override public long now() {return now.getAsLong(); }
            @Override public Timestamp uniqueNow(Timestamp atLeast) { return Timestamp.fromValues(1, now.getAsLong(), node); }
        };
        return new AccordCommandStore(0,
                                      time,
                                      new AccordAgent(),
                                      null,
                                      cs -> NOOP_PROGRESS_LOG,
                                      new SingleEpochRanges(topology.rangesForNode(node)));
    }

    public static AccordCommandStore createAccordCommandStore(LongSupplier now, String keyspace, String table)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        TokenRange range = TokenRange.fullRange(metadata.keyspace);
        Node.Id node = EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
        Topology topology = new Topology(1, new Shard(range, Lists.newArrayList(node), Sets.newHashSet(node), Collections.emptySet()));
        AccordCommandStore store = createAccordCommandStore(node, now, topology);
        store.execute(PreLoadContext.empty(), safeStore -> ((AccordCommandStore)safeStore.commandStore()).setCacheSize(1 << 20));
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
        return new PartitionKey(table.keyspace, table.id, dk);
    }

    public static Keys keys(TableMetadata table, int... keys)
    {
        return Keys.of(IntStream.of(keys).mapToObj(key -> key(table, key)).collect(Collectors.toList()));
    }
}
