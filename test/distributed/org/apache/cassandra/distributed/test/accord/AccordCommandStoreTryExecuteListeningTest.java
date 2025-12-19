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

package org.apache.cassandra.distributed.test.accord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.api.Journal;
import accord.local.Command;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.PreLoadContext.Empty;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyException;
import accord.utils.ImmutableBitSet;
import accord.utils.LargeBitSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.ResultSerializers;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class AccordCommandStoreTryExecuteListeningTest extends TestBaseImpl
{
    private static DecoratedKey dk(int key)
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        return partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }

    private static PartitionKey pk(int key, String keyspace, String table)
    {
        TableId tid = Schema.instance.getTableMetadata(keyspace, table).id;
        return new PartitionKey(tid, dk(key));
    }

    @Test
    public void testTryExecuteListening() throws Throwable
    {
        try (Cluster cluster = Cluster.build().withNodes(1)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(1))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(1, "dc0", "rack0"))
                                      .withConfig(config -> config.set("accord.command_store_shard_count", 1)
                                                                  .set("accord.queue_shard_count", 1)
                                                                  .with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':1}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key(k, c)) WITH transactional_mode='full'");

            cluster.get(1).runOnInstance(() -> {
                AccordService service = (AccordService) AccordService.instance();

                Node node = service.node();
                PartitionKey key = pk(1, "ks", "tbl");
                AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().unsafeForKey(key.toUnseekable());

                Command txn1a = executed(node, SaveStatus.Applied, 1);
                Command txn1b = executed(node, SaveStatus.PreApplied, 2);
                Command txn2a = executed(node, SaveStatus.PreApplied, 1, txn1a.txnId());
                Command txn2b = executed(node, SaveStatus.PreApplied, 2, txn1b.txnId());
                Command txn3  = executed(node, SaveStatus.PreApplied,1,  txn1a.txnId(), 2, txn1b.txnId(), txn2b.txnId());
                    Command txn4  = executed(node, SaveStatus.PreApplied, 1, txn1a.txnId(), txn3.txnId(), 2, txn1b.txnId(), txn3.txnId());
                Command[] commands = new Command[] { txn1a, txn1b, txn2a, txn2b, txn3, txn4 };

                AccordService.getBlocking(commandStore.chain((Empty)() -> "Test", safeStore -> {
                    for (Command command : commands)
                        commandStore.journal.saveCommand(commandStore.id(), new Journal.CommandUpdate(null, command), () -> {});

                    commandStore.unsafeGetListeners().register(txn1a.txnId(), SaveStatus.Applied, txn2a.txnId());
                    commandStore.unsafeGetListeners().register(txn3.txnId(), SaveStatus.Applied, txn4.txnId());
                }));

                AccordService.getBlocking(commandStore.tryToExecuteListeningTxns(true));

                for (Command command : commands)
                {
                    Command cmd = AccordService.getBlocking(commandStore.submit(PreLoadContext.contextFor(command.txnId(), "Test"), safeStore -> safeStore.unsafeGet(command.txnId()).current()));
                    Assertions.assertThat(cmd.saveStatus()).isEqualTo(SaveStatus.Applied);
                }
            });
        }
    }

    private static Command executed(Node node, SaveStatus saveStatus, Object ... inputs)
    {
        int depCount;
        Map<PartitionKey, List<TxnId>> depsByInputKey = new TreeMap<>();
        TxnId[] txnIds;
        {
            PartitionKey k = null;
            for (Object input : inputs)
            {
                if (input instanceof Integer)
                {
                    k = keyN((Integer) input, node);
                    depsByInputKey.put(k, new ArrayList<>());
                }
                else depsByInputKey.get(k).add((TxnId)input);
            }
            txnIds = depsByInputKey.values().stream().flatMap(Collection::stream).distinct().sorted().toArray(TxnId[]::new);
            depCount = depsByInputKey.values().stream().mapToInt(Collection::size).sum();
        }
        PartitionKey[] keys = depsByInputKey.keySet().toArray(PartitionKey[]::new);
        Range[] ranges = Stream.of(keys).map(PartitionKey::asRange).toArray(Range[]::new);

        PartitionKey key = keys[0];
        AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().unsafeForKey(key.toUnseekable());

        Txn txn = node.agent().emptySystemTxn(Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range);
        TxnId txnId = node.nextTxnId(txn);
        FullRoute<?> route;
        try { route = node.computeRoute(txnId, Ranges.of(ranges)); }
        catch (TopologyException e) { throw new RuntimeException(e); }
        int[] rangesToTxnIds = new int[depCount + ranges.length];
        {
            int offset = ranges.length;
            for (int i = 0 ; i < ranges.length ; ++i)
            {
                for (TxnId dep : depsByInputKey.get(keys[i]))
                    rangesToTxnIds[offset++] = Arrays.binarySearch(txnIds, dep);
                rangesToTxnIds[i] = offset;
            }
        }
        Deps deps = new Deps(KeyDeps.NONE, RangeDeps.SerializerSupport.create(ranges, txnIds, rangesToTxnIds, null));
        Command.WaitingOn waitingOn; {
            LargeBitSet waitingOnBits = new LargeBitSet(txnIds.length);
            waitingOnBits.setRange(0, txnIds.length);
            waitingOn = new Command.WaitingOn(RoutingKeys.EMPTY, deps.rangeDeps, new ImmutableBitSet(waitingOnBits), new ImmutableBitSet(txnIds.length));
        }
        return Command.Executed.executed(txnId, saveStatus, Status.Durability.NotDurable, StoreParticipants.execute(commandStore.unsafeGetRangesForEpoch(), route, txnId, txnId.epoch()), Ballot.ZERO, txnId, txn.intersecting(route, true), deps.intersecting(route), Ballot.ZERO, waitingOn, null, ResultSerializers.APPLIED);
    }

    private static PartitionKey keyN(int n, Node node)
    {
        PartitionKey first = pk(1, "ks", "tbl");
        if (n == 1)
            return first;

        AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().unsafeForKey(first.toUnseekable());

        int i = 2;
        while (true)
        {
            PartitionKey next = pk(i, "ks", "tbl");
            if (commandStore.unsafeGetRangesForEpoch().all().contains(next) && --n == 0)
                return next;
        }
    }

}
