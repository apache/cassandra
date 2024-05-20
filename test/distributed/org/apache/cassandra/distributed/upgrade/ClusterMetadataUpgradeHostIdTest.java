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

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.psjava.util.AssertStatus.assertTrue;

public class ClusterMetadataUpgradeHostIdTest extends UpgradeTestBase
{
    @Test
    public void upgradeHostIdUpdateTest() throws Throwable
    {
        Map<InetAddressAndPort, UUID> expectedUUIDs = new HashMap<>();
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .upgradesToCurrentFrom(v41)
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

            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();

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
