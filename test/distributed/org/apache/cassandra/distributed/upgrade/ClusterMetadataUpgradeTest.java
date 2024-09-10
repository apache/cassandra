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

import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.tcm.membership.NodeId;

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
        .upgradesToCurrentFrom(v41)
        .setup((cluster) -> {
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        })
        .runAfterClusterUpgrade((cluster) -> {
            cluster.get(1).nodetoolResult("cms","initialize").asserts().success();
            cluster.forEach(i ->
            {
                // The cast is unpleasant, but safe to do so as the upgraded instance is running the current version.
                assertFalse("node " + i.config().num() + " is still in MIGRATING STATE",
                            ClusterUtils.isMigrating((IInvokableInstance) i));
            });
            cluster.get(2).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.schemaChange(withKeyspace("create table %s.xyz (id int primary key)"));
            cluster.forEach(i -> {
                Object [][] res = i.executeInternal("select host_id from system.local");
                assertTrue(NodeId.isValidNodeId(UUID.fromString(res[0][0].toString())));
            });
        }).run();
    }

    @Test
    public void upgradeSystemKeyspaces() throws Throwable
    {
        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .singleUpgradeToCurrentFrom(v50)
        .setup((cluster) -> cluster.schemaChange(withKeyspace("ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter0':3}")))
        .runAfterNodeUpgrade((cluster, i) -> {
            Object [][] desc = cluster.get(1).executeInternal("describe keyspace system_auth");
            assertTrue(Arrays.toString(desc[0]).contains("NetworkTopologyStrategy"));
        }).run();
    }
}
