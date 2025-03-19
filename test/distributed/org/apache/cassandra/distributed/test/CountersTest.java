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

package org.apache.cassandra.distributed.test;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CountersTest extends TestBaseImpl
{
    @Test
    public void testUpdateCounter() throws Throwable
    {
        testUpdateCounter(false);
    }

    @Test
    public void testUpdateCounterWithDroppedCompactStorage() throws Throwable
    {
        testUpdateCounter(true);
    }

    @Test
    public void testCountersFromCoordinatorNodes() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withNodeIdTopology(singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withTokenSupplier(evenlyDistributedTokens(3, 1))
                                      .withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL))
                                      .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance coordinatorNode = cluster.bootstrap(config);

            // make coordinator only node from the third node where transport will be active
            try (WithProperties wp = new WithProperties().set(JOIN_RING, false))
            {
                coordinatorNode.startup(cluster);
            }

            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}", true, coordinatorNode);
            cluster.schemaChange("CREATE TABLE k.t ( k int, c int, total counter, PRIMARY KEY (k, c))", true, coordinatorNode);

            // stop native transports on the first two (storage) nodes
            cluster.get(1).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.stopNativeTransport());
            cluster.get(2).runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.stopNativeTransport());

            IIsolatedExecutor.SerializableCallable<Boolean> nativeTransportRunning = () -> StorageService.instance.isNativeTransportRunning();
            IIsolatedExecutor.SerializableCallable<Boolean> rpcReady = () -> StorageService.instance.isRpcReady(FBUtilities.getBroadcastAddressAndPort());

            // verify it is stopped on storage nodes but transport is running on the coordinator
            assertFalse(cluster.get(1).callOnInstance(nativeTransportRunning));
            assertFalse(cluster.get(2).callOnInstance(nativeTransportRunning));
            assertTrue(cluster.get(3).callOnInstance(nativeTransportRunning));

            // check RPC too
            assertFalse(cluster.get(1).callOnInstance(rpcReady));
            assertFalse(cluster.get(2).callOnInstance(rpcReady));
            assertTrue(cluster.get(3).callOnInstance(rpcReady));

            // even we turned off transports on the two storage nodes, counter mutation is still working fine, because we wait on JOINED state instead on RPC_READY from Gossip
            cluster.coordinator(3).execute("UPDATE k.t SET total = total + 1 WHERE k = 0 AND c = 0", ConsistencyLevel.ALL);
            assertRows(cluster.coordinator(3).execute("SELECT total FROM k.t WHERE k = 0 AND c = 0", ConsistencyLevel.ALL), row(1L));
        }
    }

    private static void testUpdateCounter(boolean droppedCompactStorage) throws Throwable
    {
        try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL).set("drop_compact_storage_enabled", true)).start())
        {
            cluster.schemaChange("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

            String createTable = "CREATE TABLE k.t ( k int, c int, total counter, PRIMARY KEY (k, c))";
            if (droppedCompactStorage)
            {
                cluster.schemaChange(createTable + " WITH COMPACT STORAGE");
                cluster.schemaChange("ALTER TABLE k.t DROP COMPACT STORAGE");
            }
            else
            {
                cluster.schemaChange(createTable);
            }

            ConsistencyLevel cl = ConsistencyLevel.ONE;
            String select = "SELECT total FROM k.t WHERE k = 1 AND c = ?";

            for (int i = 1; i <= cluster.size(); i++)
            {
                ICoordinator coordinator = cluster.coordinator(i);

                coordinator.execute("UPDATE k.t SET total = total + 1 WHERE k = 1 AND c = ?", cl, i);
                assertRows(coordinator.execute(select, cl, i), row(1L));

                coordinator.execute("UPDATE k.t SET total = total - 4 WHERE k = 1 AND c = ?", cl, i);
                assertRows(coordinator.execute(select, cl, i), row(-3L));
            }
        }
    }
}
