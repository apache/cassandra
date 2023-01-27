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
import java.util.function.LongSupplier;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;

import accord.api.Data;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.api.Write;
import accord.impl.InMemoryCommandStore;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.Status.Known;
import accord.primitives.Ballot;
import accord.primitives.Ranges;
import accord.primitives.Keys;
import accord.primitives.PartialTxn;
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
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static accord.primitives.Routable.Domain.Key;
import static accord.utils.async.AsyncChains.awaitUninterruptibly;
import static java.lang.String.format;

public class AccordTestUtils
{
    public static Id localNodeId()
    {
        return EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
    }

    public static final ProgressLog NOOP_PROGRESS_LOG = new ProgressLog()
    {
        @Override public void unwitnessed(TxnId txnId, RoutingKey homeKey, ProgressShard shard) {}
        @Override public void preaccepted(Command command, ProgressShard progressShard) {}
        @Override public void accepted(Command command, ProgressShard progressShard) {}
        @Override public void committed(Command command, ProgressShard progressShard) {}
        @Override public void readyToExecute(Command command, ProgressShard progressShard) {}
        @Override public void executed(Command command, ProgressShard progressShard) {}
        @Override public void invalidated(Command command, ProgressShard progressShard) {}
        @Override public void durable(Command command, Set<Id> persistedOn) {}
        @Override public void durable(TxnId txnId, @Nullable Unseekables<?, ?> someKeys, ProgressShard shard) {}
        @Override public void durableLocal(TxnId txnId) {}
        @Override public void waiting(TxnId blockedBy, Known blockedUntil, Unseekables<?, ?> blockedOn) {}
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

    /**
     * does the reads, writes, and results for a command without the consensus
     */
    public static void processCommandResult(AccordCommandStore commandStore, Command command) throws Throwable
    {

        awaitUninterruptibly(commandStore.execute(PreLoadContext.contextFor(Collections.emptyList(), command.partialTxn().keys()),
                                       instance -> {
            PartialTxn txn = command.partialTxn();
            TxnRead read = (TxnRead) txn.read();
            Data readData = read.keys().stream()
                                .map(key -> {
                                    try
                                    {
                                        return AsyncChains.getBlocking(read.read(key, command.txnId().rw(), instance, command.executeAt(), null));
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
            ((AccordCommand)command).setWrites(new Writes(command.executeAt(), (Keys)txn.keys(), write));
            ((AccordCommand)command).setResult(txn.query().compute(command.txnId(), readData, txn.read(), txn.update()));
        }));
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
        PartitionKey key = (PartitionKey) txn.keys().get(0);
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
}
