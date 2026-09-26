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

package org.apache.cassandra.distributed.test.accord.journal;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.api.Journal;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.ExecuteSyncPoint;
import accord.local.Command;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.RedundantStatus;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.SyncPoint;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.TopologyException;
import accord.utils.ImmutableBitSet;
import accord.utils.LargeBitSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnDataResult;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.spinUntilTrue;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class AccordJournalReplayTest extends TestBaseImpl
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
    public void replayCommandWithOnlyDurableSyncPointDependency() throws Throwable
    {
        try (Cluster cluster = Cluster.build().withNodes(1)
                                      .withoutVNodes()
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(1))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(1, "dc0", "rack0"))
                                      .withConfig(config -> config.set("accord.command_store_shard_count", 2)
                                                                  .set("accord.queue_shard_count", 2)
                                                                  .set("accord.shard_durability_cycle", "20s")
                                                                  .set("accord.shard_durability_target_splits", "1")
                                                                  .set("accord.retry_syncpoint", "1s*attempts")
                                                                  .set("accord.retry_durability", "1s*attempts")
                                                                  .set("accord.journal.replay_save_point", "NO")
                                                                  .set("accord.catchup_on_start", "false")
                                                                  .with(NETWORK, GOSSIP))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor':1}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key(k, c)) WITH transactional_mode='full'");

            String txnIdStr = cluster.get(1).callOnInstance(() -> {
                AccordService service = (AccordService) AccordService.instance();
                PartitionKey key = pk(1, "ks", "tbl");

                Node node = service.node();
                Txn txn = AccordTestUtils.createTxn("BEGIN TRANSACTION\n" +
                                                    "INSERT INTO ks.tbl (k, c, v) VALUES (?, ?, ?);\n" +
                                                    "COMMIT TRANSACTION", 1, 1, 1);

                Txn syncPointTxn = node.agent().emptySystemTxn(Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range);
                TxnId syncPointId = node.nextTxnId(syncPointTxn);
                TxnId txnId = node.nextTxnId(txn);
                FullRoute<?> route;
                try { route = node.computeRoute(txnId, txn.keys()); }
                catch (TopologyException e) { throw new RuntimeException(e); }
                AccordCommandStore commandStore = (AccordCommandStore) node.commandStores().unsafeForKey(key.toUnseekable());
                Deps deps = new Deps(KeyDeps.NONE, RangeDeps.SerializerSupport.create(new accord.primitives.Range[] { key.asRange() }, new TxnId[] { syncPointId }, new int[] { 2, 0 }, new int[] { 2, 0 }));
                Command.WaitingOn waitingOn; {
                    LargeBitSet waitingOnBits = new LargeBitSet(1);
                    waitingOnBits.set(0);
                    waitingOn = new Command.WaitingOn(RoutingKeys.EMPTY, deps.rangeDeps, new ImmutableBitSet(waitingOnBits), null);
                }
                Writes writes = new Writes(txnId, txnId, Keys.of(key), null);
                Command command = Command.Executed.executed(txnId, SaveStatus.PreApplied, Status.Durability.NotDurable, StoreParticipants.execute(commandStore.unsafeGetRangesForEpoch(), route, txnId, txnId.epoch()), Ballot.ZERO, txnId, txn.intersecting(route, true), deps.intersecting(route), Ballot.ZERO, waitingOn, writes, TxnDataResult.PERSISTABLE);
                commandStore.journal.saveCommand(commandStore.id(), new Journal.CommandUpdate(null, command), () -> {});

                SyncPoint syncPoint = AccordService.getBlocking(CoordinateSyncPoint.exclusive(node, syncPointId, Ranges.of(key.asRange())));
                AccordService.getBlocking(ExecuteSyncPoint.coordinate(node, syncPoint, 1).onQuorumOrDone());
                Keyspace.open("ks").getColumnFamilyStore("tbl").forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                Keyspace.open("system_accord").getColumnFamilyStore("commands_for_key").forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
                spinUntilTrue(() -> {
                    RedundantBefore.Bounds bounds = commandStore.unsafeGetRedundantBefore().get(key);
                    return bounds.maxBound(RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE).equals(syncPointId)
                        && bounds.maxBound(RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE).equals(syncPointId);
                });
                return txnId.toString();
            });

            cluster.get(1).shutdown(false).get();
            cluster.get(1).startup();
            cluster.get(1).runOnInstance(() -> {
                AccordService service = (AccordService) AccordService.instance();
                PartitionKey key = pk(1, "ks", "tbl");
                AccordCommandStore commandStore = (AccordCommandStore) service.node().commandStores().unsafeForKey(key.toUnseekable());
                TxnId txnId = TxnId.parse(txnIdStr);
                Command command = commandStore.loadCommand(txnId);
                Assertions.assertThat(command.saveStatus()).isEqualTo(SaveStatus.Applied);
            });
        }
    }

}
