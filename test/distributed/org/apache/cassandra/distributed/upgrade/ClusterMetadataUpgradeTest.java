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

package org.apache.cassandra.distributed.upgrade;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.monitoring.runtime.instrumentation.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.tcm.membership.NodeId;
import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.psjava.util.AssertStatus.assertTrue;

public class ClusterMetadataUpgradeTest extends UpgradeTestBase
{

    @Test
    public void simpleUpgradeTest() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .singleUpgradeToCurrentFrom(v41.toStrict())
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        })
        .runAfterClusterUpgrade((cluster) -> {
            cluster.get(1).nodetoolResult("addtocms").asserts().success();
            cluster.forEach(i ->
            {
                // The cast is unpleasant, but safe to do so as the upgraded instance is running the current version.
                assertFalse("node " + i.config().num() + " is still in MIGRATING STATE",
                            ClusterUtils.isMigrating((IInvokableInstance) i));
            });
            cluster.get(2).nodetoolResult("addtocms").asserts().success();
            cluster.get(1).nodetoolResult("addtocms").asserts().failure();
            cluster.schemaChange(withKeyspace("create table %s.xyz (id int primary key)"));
            cluster.forEach(i -> {
                Object [][] res = i.executeInternal("select host_id from system.local");
                assertTrue(NodeId.isValidNodeId(UUID.fromString(res[0][0].toString())));
            });
        }).run();
    }

    @Test
    public void upgradeWithHintsTest() throws Throwable
    {
        final int rowCount = 50;
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
        .singleUpgradeToCurrentFrom(v41.toStrict())
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':3}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int, v int, PRIMARY KEY (k))");
            cluster.get(2).nodetoolResult("pausehandoff").asserts().success();

            // insert some data while node1 is down so that hints are written
            cluster.get(1).shutdown().get();
            for (int i = 0; i < rowCount; i++)
                cluster.coordinator(2).execute("INSERT INTO " + KEYSPACE + ".tbl(k,v) VALUES (?, ?)", ConsistencyLevel.ANY, i, i);
            cluster.get(2).flush(KEYSPACE);
            cluster.get(3).flush(KEYSPACE);
            cluster.get(1).startup();

            // Check that none of the writes got to node1
            SimpleQueryResult rows = cluster.get(1).executeInternalWithResult("SELECT * FROM " + KEYSPACE + ".tbl");
            assertFalse(rows.hasNext());
        })
        .runAfterClusterUpgrade((cluster) -> {
            Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(() -> {
                SimpleQueryResult rows = cluster.get(1).executeInternalWithResult("SELECT * FROM " + KEYSPACE + ".tbl");
                return rows.toObjectArrays().length == rowCount;
            });

            IInvokableInstance inst = (IInvokableInstance)cluster.get(2);
            long hintsDelivered = inst.callOnInstance(() -> {
                return (long)HintsServiceMetrics.hintsSucceeded.getCount();
            });
            assertEquals(rowCount, hintsDelivered);
        }).run();
    }

    @Test
    public void upgradeIgnoreHostsTest() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .singleUpgradeToCurrentFrom(v41.toStrict())
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        })
        .runAfterClusterUpgrade((cluster) -> {
            // todo; isolate node 3 - actually shutting it down makes us throw exceptions when test finishes
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            cluster.get(1).nodetoolResult("addtocms").asserts().failure(); // node3 unreachable
            cluster.get(1).nodetoolResult("addtocms", "--ignore", "127.0.0.1").asserts().failure(); // can't ignore localhost
            cluster.get(1).nodetoolResult("addtocms", "--ignore", "127.0.0.3").asserts().success();
        }).run();
    }

    @Test
    public void upgradeHostIdUpdateTest() throws Throwable
    {
        Map<InetAddressAndPort, UUID> expectedUUIDs = new HashMap<>();
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .singleUpgradeToCurrentFrom(v41.toStrict())
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            expectedUUIDs.putAll(getHostIds(cluster, 1));
            for (UUID hostId : expectedUUIDs.values())
                assertFalse(NodeId.isValidNodeId(hostId));
        })
        .runAfterClusterUpgrade((cluster) -> {
            for (int i = 1; i <= 3; i++)
                assertEquals(expectedUUIDs, getHostIds(cluster, i));

            cluster.get(1).nodetoolResult("addtocms").asserts().success();

            Map<InetAddressAndPort, UUID> postUpgradeUUIDs = new HashMap<>(getHostIds(cluster, 1));
            boolean found = false;
            for (int i = 0; i < 20; i++)
            {
                if (postUpgradeUUIDs.values().stream().allMatch(NodeId::isValidNodeId))
                {
                    found = true;
                    break;
                }
                System.out.println("NOT ALL VALID: "+postUpgradeUUIDs);
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                postUpgradeUUIDs = new HashMap<>(getHostIds(cluster, 1));
            }
            assertTrue(found);

            for (int i = 2; i <= 3; i++)
                while(!getHostIds(cluster, i).equals(postUpgradeUUIDs))
                    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }).run();
    }

    private static Map<InetAddressAndPort, UUID> getHostIds(UpgradeableCluster cluster, int instance)
    {
        String gossipInfo = cluster.get(instance).nodetoolResult("gossipinfo").getStdout();
        InetAddressAndPort host = null;
        Map<InetAddressAndPort, UUID> hostIds = new HashMap<>();
        for (String l : gossipInfo.split("\n"))
        {
            if (l.startsWith("/"))
                host = InetAddressAndPort.getByNameUnchecked(l.replace("/", ""));

            if (l.contains("HOST_ID"))
            {
                String hostIdStr = l.split(":")[2];
                hostIds.put(host, UUID.fromString(hostIdStr));
            }
        }
        return hostIds;
    }
}
