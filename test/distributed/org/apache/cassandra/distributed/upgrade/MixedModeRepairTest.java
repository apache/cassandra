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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertEquals;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertNotEquals;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.ClusterUtils.awaitRingJoin;
import static org.apache.cassandra.distributed.shared.Versions.Major;

/**
 * Tests that repair only works on mixed mode clusters if there isn't a major upgrade. Otherwise, repair attempts will
 * be rejected with an informative message.
 */
public class MixedModeRepairTest extends UpgradeTestBase
{
    public static final String CREATE_TABLE = withKeyspace("CREATE TABLE %s.t (k uuid, c int, v int, PRIMARY KEY (k, c))");
    public static final String INSERT = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
    public static final String SELECT = withKeyspace("SELECT * FROM %s.t WHERE k=?");

    @Test
    public void testMixedModeRepair() throws Throwable
    {
        new UpgradeTestBase.TestCase()
        .nodes(2)
        .nodesToUpgrade(2)
        .upgrade(Major.v30, Major.v4)
        .upgrade(Major.v3X, Major.v4)
        .withConfig(config -> config.with(NETWORK, GOSSIP))
        .setup(cluster -> cluster.schemaChange(withKeyspace(CREATE_TABLE)))
        .runAfterNodeUpgrade((cluster, node) -> {

            awaitRingJoin(cluster.get(1), cluster.get(2));

            // run the repair scenario in both the upgraded and the not upgraded node
            for (int repairedNode = 1; repairedNode <= cluster.size(); repairedNode++)
            {
                UUID key = UUID.randomUUID();

                // only in node 1, create a sstable with a version of a partition
                Object[] row1 = row(key, 10, 100);
                cluster.get(1).executeInternal(INSERT, row1);
                cluster.get(1).flush(KEYSPACE);

                // only in node 2, create a sstable with another version of the partition
                Object[] row2 = row(key, 20, 200);
                cluster.get(2).executeInternal(INSERT, row2);
                cluster.get(2).flush(KEYSPACE);

                // in case of a major upgrade repair should be rejected and the data should remain unrepaired
                try
                {
                    IUpgradeableInstance instance = cluster.get(repairedNode);
                    String errorMessage = "Repair is not supported in mixed major version clusters";
                    CompletableFuture.supplyAsync(() -> instance.nodetoolResult("repair", "--full", KEYSPACE)
                                                                .asserts()
                                                                .errorContains(errorMessage))
                                     .get(10, TimeUnit.SECONDS);
                    assertEquals("Repair should cleanly fail in the upgraded node", node, repairedNode);
                    assertLogHas(cluster, 2, errorMessage);
                }
                // if the node issuing the repair is the not updated node we don't have specific error management,
                // and nodetool repair doesn't end waiting
                catch (TimeoutException e)
                {
                    assertNotEquals("Repair should time out in the not upgraded node", node, repairedNode);
                    assertLogHas(cluster, 2, "unexpected exception caught while processing inbound messages");
                }

                // verify that the previous failed repair hasn't repaired the data
                assertRows(cluster.get(1).executeInternal(SELECT, key), row1);
                assertRows(cluster.get(2).executeInternal(SELECT, key), row2);
            }
        })
        .run();
    }

    private static void assertLogHas(UpgradeableCluster cluster, int node, String msg)
    {
        Assert.assertFalse("Unable to find '" + msg + "' in the logs of node " + node,
                           cluster.get(node).logs().grep(msg).getResult().isEmpty());
    }
}
