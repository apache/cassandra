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

import org.junit.Test;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.impl.TestEndpointCache;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ClusterMetadataUpgradePeersHostIdsTest extends UpgradeTestBase
{
    @Test
    public void upgradeHostIdUpdateTest() throws Throwable
    {
        Map<InetAddressAndPort, UUID> preUpgradeIDs = new HashMap<>();
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .singleUpgradeToCurrentFrom(v50)
        .setup((cluster) -> {
            // combine the system.peers_v2 entries from each instance pre-upgrade
            for (int i = 1; i <= 3; i++)
                preUpgradeIDs.putAll(getHostIdsFromSystemPeersV2(cluster.get(i)));

            assertEquals(3, preUpgradeIDs.size());

            for (UUID hostId : preUpgradeIDs.values())
                assertFalse(NodeId.isValidNodeId(hostId));
        })
        .runAfterClusterUpgrade((cluster) -> {
            for (int i = 1; i <= 3; i++)
            {
                // system.peers/peers_v2 should still contain the pre-upgrade ids
                IUpgradeableInstance inst = cluster.get(i);
                Map<InetAddressAndPort, UUID> expected = new HashMap<>(preUpgradeIDs);
                expected.remove(TestEndpointCache.toCassandraInetAddressAndPort(inst.config().broadcastAddress()));
                assertEquals(expected, getHostIdsFromSystemPeersV2(cluster.get(i)));
            }

            // initialize the CMS and fetch new ids from the ClusterMetadata Directory
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
            Map<InetAddressAndPort, UUID> postUpgradeIDs = getHostIdsFromClusterMetadata(cluster.get(1));

            for (int i = 1; i <= 3; i++)
            {
                // assert system.peers/peers_v2 now contain the post-upgrade ids
                IUpgradeableInstance inst = cluster.get(i);
                Map<InetAddressAndPort, UUID> expected = new HashMap<>(postUpgradeIDs);
                expected.remove(TestEndpointCache.toCassandraInetAddressAndPort(inst.config().broadcastAddress()));
                assertEquals(expected, getHostIdsFromSystemPeersV2(cluster.get(i)));
            }
        }).run();
    }

    private static Map<InetAddressAndPort, UUID> getHostIdsFromSystemPeersV2(IUpgradeableInstance instance)
    {
        Map<InetAddressAndPort, UUID> hostIds = new HashMap<>();
        SimpleQueryResult res = instance.executeInternalWithResult("select peer, peer_port, host_id from system.peers_v2");
        while(res.hasNext())
        {
            Row row = res.next();
            hostIds.put(InetAddressAndPort.getByAddressOverrideDefaults(row.get(0), row.getInteger(1)), row.getUUID(2));
        }
        return hostIds;
    }

    private static Map<InetAddressAndPort, UUID> getHostIdsFromClusterMetadata(IUpgradeableInstance instance)
    {
        Map<InetAddressAndPort, UUID> hostIds = new HashMap<>();
        SimpleQueryResult res = instance.executeInternalWithResult("select broadcast_address, broadcast_port, host_id from system_views.cluster_metadata_directory");
        while(res.hasNext())
        {
            Row row = res.next();
            hostIds.put(InetAddressAndPort.getByAddressOverrideDefaults(row.get(0), row.getInteger(1)), row.getUUID(2));
        }
        return hostIds;
    }
}
