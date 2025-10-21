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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.gms.ApplicationState;

import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.upgrade.ClusterMetadataUpgradeAssassinateTest.checkPlacements;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterMetadataUpgradeHibernateTest extends UpgradeTestBase
{
    @Test
    public void hibernateUpgradeTest() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2) // not node3 - we manually upgrade that below
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.get(3).shutdown().get();
            // stopping a fully joined node and then starting it with join_ring=false puts it in hibernate status - it still owns tokens:
            withProperty(CassandraRelevantProperties.JOIN_RING, false, () -> cluster.get(3).startup());
            assertTrue(hibernating(cluster.get(1), "127.0.0.3"));
        })
        .runAfterClusterUpgrade((cluster) -> {
            // manually upgrade node3 to be able to keep join_ring=false
            cluster.get(3).shutdown().get();
            cluster.get(3).setVersion(Versions.find().getLatest(v51));
            assertTrue(hibernating(cluster.get(1), "127.0.0.3"));
            withProperty(CassandraRelevantProperties.JOIN_RING, false, () -> cluster.get(3).startup());
            cluster.forEach(i -> checkPlacements(i, true));
            assertTrue(hibernating(cluster.get(1), "127.0.0.3"));
            cluster.forEach(i -> checkPlacements(i, true));
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
            assertTrue(hibernating(cluster.get(1), "127.0.0.3"));
            cluster.forEach(i -> checkPlacements(i, true));

            // and remove join_ring=false and make sure it is no longer hibernating
            cluster.get(3).shutdown().get();
            cluster.get(3).startup();
            assertFalse(hibernating(cluster.get(1), "127.0.0.3"));
            cluster.forEach(i -> checkPlacements(i, true));
        }).run();
    }

    @Test
    public void hibernateBadGossipUpgradeTest() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP))
        .upgradesToCurrentFrom(v50)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.get(3).shutdown().get();
            withProperty(CassandraRelevantProperties.JOIN_RING, false, () -> cluster.get(3).startup());
            cluster.get(3).shutdown();
            assertTrue(hibernating(cluster.get(1), "127.0.0.3"));
            assertTrue(hibernating(cluster.get(2), "127.0.0.3"));
            // terrible - we might have old hibernating nodes in gossip which don't exist in peers_v2 - this
            // is an approximation of that state to be able to upgrade and ignore these nodes
            for (int i = 1; i <= 2; i++)
                cluster.get(i).executeInternal("delete from system.peers_v2 where peer = '127.0.0.3'");
        })
        .runAfterClusterUpgrade((cluster) -> {
            checkPlacements(cluster.get(1), false);
            checkPlacements(cluster.get(2), false);
            // 127.0.0.3 should have been ignored on upgrade:
            assertFalse(hibernating(cluster.get(1), "127.0.0.3"));
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
            assertFalse(hibernating(cluster.get(1), "127.0.0.3"));
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();
            assertFalse(hibernating(cluster.get(2), "127.0.0.3"));
            checkPlacements(cluster.get(1), false);
            checkPlacements(cluster.get(2), false);
        }).run();
    }

    private static boolean hibernating(IInstance instance, String host)
    {
        Map<String, Map<String, String>> states = ClusterUtils.gossipInfo(instance);
        Map<String, String> state = states.get('/'+host);
        if (state == null)
            return false;
        String status = state.get(ApplicationState.STATUS_WITH_PORT.name());
        return status != null && status.contains("hibernate");
    }
}
