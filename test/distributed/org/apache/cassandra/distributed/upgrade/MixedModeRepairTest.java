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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.fail;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class MixedModeRepairTest extends UpgradeTestBase
{
    public static final int UPGRADED_NODE = 1;
    public static final String CREATE_TABLE = withKeyspace("CREATE TABLE %s.t (k uuid, c int, v int, PRIMARY KEY (k, c))");
    public static final String INSERT = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
    public static final String SELECT = withKeyspace("SELECT * FROM %s.t WHERE k=?");

    /**
     * Test that repairs fail during a major upgrade. If the repaired node is >= 4.0 thanks to CASSANDRA-13944 there
     * will be an informative message. Otherwise, if the repaired node is below 4.0, there won't be such an informative
     * message and the repair will take very long to timeout.
     */
    @Test
    public void testRepairDuringMajorUpgrade() throws Throwable
    {
        new UpgradeTestBase.TestCase()
        .nodes(2)
        .nodesToUpgrade(UPGRADED_NODE)
        .upgradesToCurrentFrom(v40)
        .withConfig(config -> config.with(NETWORK, GOSSIP))
        .setup(cluster -> {
            cluster.schemaChange(CREATE_TABLE);
            cluster.setUncaughtExceptionsFilter(throwable -> throwable instanceof RejectedExecutionException);
        })
        .runAfterNodeUpgrade((cluster, node) -> {

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

                // in case of repairing the upgraded node the repair should be rejected with a decriptive error in both
                // nodetool output and logs (see CASSANDRA-13944), if MessagingService.currentVersion has changed
                if (cluster.get(1).getMessagingVersion() != cluster.get(2).getMessagingVersion())
                {
                    if (repairedNode == UPGRADED_NODE)
                    {
                        // Repair is only not supported when MessagingService.current_version don't match
                        //  but we keep the error message simple and user-facing
                        String errorMessage = "Repair is not supported in mixed major version clusters";
                        cluster.get(repairedNode)
                               .nodetoolResult("repair", "--full", KEYSPACE)
                               .asserts()
                               .errorContains(errorMessage);
                        assertLogHas(cluster, repairedNode, errorMessage);
                    }
                    // if the node issuing the repair is the not updated node we don't have specific error management,
                    // so the repair will produce a failure in the upgraded node, and it will take one hour to time out in
                    // the not upgraded node. Since we don't want to wait that long, we only wait a few seconds for the
                    // repair before verifying the "unknown verb id" error in the upgraded node.
                    else
                    {
                        try
                        {
                            IUpgradeableInstance instance = cluster.get(repairedNode);
                            CompletableFuture.supplyAsync(() -> instance.nodetoolResult("repair", "--full", KEYSPACE))
                                             .get(10, TimeUnit.SECONDS);
                            fail("Repair in the not upgraded node should have timed out");
                        }
                        catch (TimeoutException e)
                        {
                            assertLogHas(cluster, UPGRADED_NODE, "unexpected exception caught while processing inbound messages");
                            assertLogHas(cluster, UPGRADED_NODE, "java.lang.IllegalArgumentException: Unknown verb id");
                        }
                    }

                    // verify that the previous failed repair hasn't repaired the data
                    assertRows(cluster.get(1).executeInternal(SELECT, key), row1);
                    assertRows(cluster.get(2).executeInternal(SELECT, key), row2);
                }
                else
                {
                    cluster.get(repairedNode).nodetoolResult("repair", "--full", KEYSPACE);
                    // verify that the repair repaired the data
                    assertRows(cluster.get(1).executeInternal(SELECT, key), row1, row2);
                    assertRows(cluster.get(2).executeInternal(SELECT, key), row1, row2);
                }
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
