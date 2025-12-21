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

                Command txn1a = executed(node, SaveStatus.Applied);
                Command txn1b = executed(node, SaveStatus.PreApplied);
                Command txn2a = executed(node, SaveStatus.PreApplied, txn1a.txnId());
                Command txn2b = executed(node, SaveStatus.PreApplied, txn1b.txnId());
                Command txn3 = executed(node, SaveStatus.PreApplied, txn1a.txnId(), txn1b.txnId(), txn2b.txnId());
                Command txn4  = executed(node, SaveStatus.PreApplied, txn1a.txnId(), txn1b.txnId(), txn3.txnId());
                Command[] commands = new Command[] { txn1a, txn1b, txn2a, txn2b, txn3, txn4 };

                AccordService.getBlocking(commandStore.chain((Empty)() -> "Test", safeStore -> {
                    for (Command command : commands)
                        commandStore.journal.saveCommand(commandStore.id(), new Journal.CommandUpdate(null, command), () -> {});

                    commandStore.unsafeGetListeners().register(txn1a.txnId(), SaveStatus.Applied, txn2a.txnId());
                    commandStore.unsafeGetListeners().register(txn3.txnId(), SaveStatus.Applied, txn4.txnId());
                }));

                AccordService.getBlocking(commandStore.operatorTryToExecuteListeningTxns());

                for (Command command : commands)
                {
                    Command cmd = AccordService.getBlocking(commandStore.submit(PreLoadContext.contextFor(command.txnId(), "Test"), safeStore -> safeStore.unsafeGet(command.txnId()).current()));
                    Assertions.assertThat(cmd.saveStatus()).isEqualTo(SaveStatus.Applied);
                }
            });
        }
    }

    private static Command executed(Node node, SaveStatus saveStatus, TxnId ... dependencies)
    {
        PartitionKey key = pk(1, "ks", "tbl");
        Txn txn = node.agent().emptySystemTxn(Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range);
        TxnId txnId = node.nextTxnId(txn);
        FullRoute<?> route;
        try { route = node.computeRoute(txnId, Ranges.of(key.asRange())); }
        catch (TopologyException e) { throw new RuntimeException(e); }
        AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().unsafeForKey(key.toUnseekable());
        int[] rangesToTxnIds = new int[dependencies.length + 1];
        rangesToTxnIds[0] = rangesToTxnIds.length;
        for (int i = 1; i < rangesToTxnIds.length ; ++i)
            rangesToTxnIds[i] = i - 1;
        Deps deps = new Deps(KeyDeps.NONE, RangeDeps.SerializerSupport.create(new accord.primitives.Range[] { key.asRange() }, dependencies, rangesToTxnIds, null));
        Command.WaitingOn waitingOn; {
            LargeBitSet waitingOnBits = new LargeBitSet(dependencies.length);
            waitingOnBits.setRange(0, dependencies.length);
            waitingOn = new Command.WaitingOn(RoutingKeys.EMPTY, deps.rangeDeps, new ImmutableBitSet(waitingOnBits), new ImmutableBitSet(dependencies.length));
        }
        return Command.Executed.executed(txnId, saveStatus, Status.Durability.NotDurable, StoreParticipants.execute(commandStore.unsafeGetRangesForEpoch(), route, txnId, txnId.epoch()), Ballot.ZERO, txnId, txn.intersecting(route, true), deps.intersecting(route), Ballot.ZERO, waitingOn, null, ResultSerializers.APPLIED);
    }

}
