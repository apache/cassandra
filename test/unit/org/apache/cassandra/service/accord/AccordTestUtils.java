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
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import accord.api.Data;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.api.Write;
import accord.impl.InMemoryCommandStore;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.Status.Known;
import accord.primitives.AbstractKeys;
import accord.primitives.Ballot;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.PartialTxn;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.service.accord.db.AccordRead;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static java.lang.String.format;
import static org.apache.cassandra.service.accord.db.AccordUpdate.UpdatePredicate.Type.NOT_EXISTS;

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
        @Override public void durable(TxnId txnId, @Nullable RoutingKeys someKeys, ProgressShard shard) {}
        @Override public void durableLocal(TxnId txnId) {}
        @Override public void waiting(TxnId blockedBy, Known blockedUntil, RoutingKeys blockedOnKeys) {}
    };

    public static Topology simpleTopology(TableId... tables)
    {
        Arrays.sort(tables, Comparator.naturalOrder());
        Id node = localNodeId();
        Shard[] shards = new Shard[tables.length];

        List<Id> nodes = Lists.newArrayList(node);
        Set<Id> fastPath = Sets.newHashSet(node);
        for (int i=0; i<tables.length; i++)
        {
            KeyRange range = TokenRange.fullRange(tables[i]);
            shards[i] = new Shard(range, nodes, fastPath, Collections.emptySet());
        }

        return new Topology(1, shards);
    }

    public static AccordTxnBuilder txnBuilder()
    {
        return new AccordTxnBuilder();
    }

    public static TxnId txnId(long epoch, long real, int logical, long node)
    {
        return new TxnId(epoch, real, logical, new Node.Id(node));
    }

    public static Timestamp timestamp(long epoch, long real, int logical, long node)
    {
        return new Timestamp(epoch, real, logical, new Node.Id(node));
    }

    public static Ballot ballot(long epoch, long real, int logical, long node)
    {
        return new Ballot(epoch, real, logical, new Node.Id(node));
    }

    /**
     * does the reads, writes, and results for a command without the consensus
     */
    public static void processCommandResult(AccordCommandStore commandStore, Command command) throws Throwable
    {

        commandStore.execute(PreLoadContext.contextFor(Collections.emptyList(), command.partialTxn().keys()),
                                       instance -> {
            PartialTxn txn = command.partialTxn();
            AccordRead read = (AccordRead) txn.read();
            Data readData = read.keys().stream()
                                .map(key -> {
                                    try
                                    {
                                        return read.read(key, command.kind(), commandStore, command.executeAt(), null).get();
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
                                .reduce(null, AccordData::merge);
            Write write = txn.update().apply(readData);
            ((AccordCommand)command).setWrites(new Writes(command.executeAt(), txn.keys(), write));
            ((AccordCommand)command).setResult(txn.query().compute(command.txnId(), readData, txn.read(), txn.update()));
        }).get();
    }

    public static Txn createTxn(int readKey, int... writeKeys)
    {
        AccordTxnBuilder builder = txnBuilder().withRead(format("SELECT * FROM ks.tbl WHERE k=%s AND c=0", readKey));
        for (int key : writeKeys)
            builder.withWrite(format("INSERT INTO ks.tbl (k, c, v) VALUES (%s, 0, 1)", key));
        builder.withCondition("ks", "tbl", readKey, 0, NOT_EXISTS).build();
        return builder.build();
    }

    public static Txn createTxn(int key)
    {
        return createTxn(key, key);
    }

    public static KeyRanges fullRange(Txn txn)
    {
        TableId tableId = ((AccordKey)txn.keys().get(0)).tableId();
        return KeyRanges.of(TokenRange.fullRange(tableId));
    }

    public static PartialTxn createPartialTxn(int key)
    {
        Txn txn = createTxn(key, key);
        KeyRanges ranges = fullRange(txn);
        return new PartialTxn.InMemory(ranges, txn.kind(), txn.keys(), txn.read(), txn.query(), txn.update());
    }

    private static class SingleEpochRanges implements CommandStore.RangesForEpoch
    {
        private final KeyRanges ranges;

        public SingleEpochRanges(KeyRanges ranges)
        {
            this.ranges = ranges;
        }

        @Override
        public KeyRanges at(long epoch)
        {
            assert epoch == 1;
            return ranges;
        }

        @Override
        public KeyRanges between(long fromInclusive, long toInclusive)
        {
            return ranges;
        }

        @Override
        public KeyRanges since(long epoch)
        {
            assert epoch == 1;
            return ranges;
        }

        @Override
        public boolean owns(long epoch, RoutingKey key)
        {
            return ranges.contains(key);
        }

        @Override
        public boolean intersects(long epoch, AbstractKeys<?, ?> keys)
        {
            assert epoch == 1;
            return ranges.intersects(keys);
        }
    }

    public static InMemoryCommandStore.Synchronized createInMemoryCommandStore(LongSupplier now, String keyspace, String table)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        TokenRange range = TokenRange.fullRange(metadata.id);
        Node.Id node = EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
        Topology topology = new Topology(1, new Shard(range, Lists.newArrayList(node), Sets.newHashSet(node), Collections.emptySet()));
        NodeTimeService time = new NodeTimeService()
        {
            @Override public Id id() { return node;}
            @Override public long epoch() {return 1; }
            @Override public Timestamp uniqueNow(Timestamp atLeast) { return new Timestamp(1, now.getAsLong(), 0, node); }
        };
        return new InMemoryCommandStore.Synchronized(0, 0, 1, 8,
                                                     time,
                                                     new AccordAgent(),
                                                     null,
                                                     cs -> null,
                                                     new SingleEpochRanges(KeyRanges.of(range)));
    }

    public static AccordCommandStore createAccordCommandStore(Node.Id node, LongSupplier now, Topology topology)
    {
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName(CommandStore.class.getSimpleName() + '[' + node + ':' + 0 + ']');
            return thread;
        });
        NodeTimeService time = new NodeTimeService()
        {
            @Override public Id id() { return node;}
            @Override public long epoch() {return 1; }
            @Override public Timestamp uniqueNow(Timestamp atLeast) { return new Timestamp(1, now.getAsLong(), 0, node); }
        };
        return new AccordCommandStore(0, 0, 0, 1,
                                      time,
                                      new AccordAgent(),
                                      null,
                                      cs -> NOOP_PROGRESS_LOG,
                                      new SingleEpochRanges(topology.rangesForNode(node)),
                                      executor);
    }
    public static AccordCommandStore createAccordCommandStore(LongSupplier now, String keyspace, String table)
    {
        TableMetadata metadata = Schema.instance.getTableMetadata(keyspace, table);
        TokenRange range = TokenRange.fullRange(metadata.id);
        Node.Id node = EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
        Topology topology = new Topology(1, new Shard(range, Lists.newArrayList(node), Sets.newHashSet(node), Collections.emptySet()));
        return createAccordCommandStore(node, now, topology);
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
