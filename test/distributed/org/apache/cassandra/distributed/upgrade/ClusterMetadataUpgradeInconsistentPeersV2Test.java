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

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.METADATA_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterMetadataUpgradeInconsistentPeersV2Test extends UpgradeTestBase
{
    @Test
    public void upgradeWithInconsistentSystemPeersV2Test() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .withNodeIdTopology(NetworkTopology.networkTopology(3, (i) -> NetworkTopology.dcAndRack("datacenter0" + i % 2, "rack0" + i)))
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {
            // insert mismatching entries into system.peers_v2 table
            for (int i = 1; i <= 3; i++)
            {
                cluster.get(i).executeInternal(format("insert into system.peers_v2 (peer, peer_port, data_center) " +
                                                      "values ('10.10.10.10', 7000, 'a%s')", i));
                cluster.get(i).flush("system");
            }
        })
        .runAfterClusterUpgrade((cluster) -> {
            // The system_cluster_metadata keyspace shouldn't be created until the CMS is intialized
            assertNoMetaKeyspace(cluster);

            cluster.get(3).nodetoolResult("cms", "initialize").asserts().success();

            // post-initialization, the replication params for system_cluster_metadata should be RF 1 in the DC of the
            // first CMS member - node3 / datacenter01
            Map<String, String> actualReplication = Map.of("class", MetaStrategy.class.getName(), "datacenter01", "1");
            Map<String, String> fromSystemTable = Map.of("class", NetworkTopologyStrategy.class.getName(), "datacenter01", "1");
            assertReplicationParams(cluster, actualReplication, fromSystemTable);
        }).run();
    }

    private static void assertNoMetaKeyspace(UpgradeableCluster cluster)
    {
        for (IUpgradeableInstance inst : cluster)
        {
            IInvokableInstance i = (IInvokableInstance) inst;
            boolean found = i.callOnInstance(() -> ClusterMetadata.current().schema.getKeyspaces().containsKeyspace(METADATA_KEYSPACE_NAME));
            assertFalse("Metadata keyspace present on node" + i + " when it should not be", found);

            SimpleQueryResult res = inst.executeInternalWithResult("select replication from system_schema.keyspaces " +
                                                                   "where keyspace_name = ?",
                                                                   METADATA_KEYSPACE_NAME);
            assertFalse(res.hasNext());
        }
    }

    private static void assertReplicationParams(UpgradeableCluster cluster,
                                                Map<String, String> expectedActual,
                                                Map<String, String> expectedInSystemTable)
    {
        for (IUpgradeableInstance inst : cluster)
        {
            IInvokableInstance i = (IInvokableInstance) inst;
            Map<String, String> rs = i.callOnInstance(() -> {
                ReplicationParams r = ClusterMetadata.current().schema.getKeyspaceMetadata("system_cluster_metadata").params.replication;
                return r.asMap();
            });
            assertEquals(rs, expectedActual);

            SimpleQueryResult res = inst.executeInternalWithResult("select replication from system_schema.keyspaces " +
                                                                   "where keyspace_name = ?",
                                                                   METADATA_KEYSPACE_NAME);
            assertTrue(res.hasNext());
            Map<String, String> replication = res.next().get("replication");
            assertEquals(replication, expectedInSystemTable);
        }
    }
}
