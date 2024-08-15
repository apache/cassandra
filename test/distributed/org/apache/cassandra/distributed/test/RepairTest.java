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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.service.StorageService;

import static com.google.common.collect.ImmutableList.of;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.test.ExecUtil.rethrow;
import static org.apache.cassandra.service.StorageService.instance;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.apache.cassandra.utils.progress.ProgressEventType.COMPLETE;

public class RepairTest extends TestBaseImpl
{
    private static ICluster<IInvokableInstance> cluster;

    private static void insert(ICluster<IInvokableInstance> cluster, String keyspace, int start, int end, int ... nodes)
    {
        String insert = String.format("INSERT INTO %s.test (k, c1, c2) VALUES (?, 'value1', 'value2');", keyspace);
        for (int i = start ; i < end ; ++i)
            for (int node : nodes)
                cluster.get(node).executeInternal(insert, Integer.toString(i));
    }

    private static void verify(ICluster<IInvokableInstance> cluster, String keyspace, int start, int end, int ... nodes)
    {
        String query = String.format("SELECT k, c1, c2 FROM %s.test WHERE k = ?;", keyspace);
        for (int i = start ; i < end ; ++i)
        {
            for (int node = 1 ; node <= cluster.size() ; ++node)
            {
                Object[][] rows = cluster.get(node).executeInternal(query, Integer.toString(i));
                if (Arrays.binarySearch(nodes, node) >= 0)
                    assertRows(rows, new Object[] { Integer.toString(i), "value1", "value2" });
                else
                    assertRows(rows);
            }
        }
    }

    private static void flush(ICluster<IInvokableInstance> cluster, String keyspace, int ... nodes)
    {
        for (int node : nodes)
            cluster.get(node).runOnInstance(rethrow(() -> StorageService.instance.forceKeyspaceFlush(keyspace,
                                                                                                     ColumnFamilyStore.FlushReason.UNIT_TESTS)));
    }

    private static ICluster create(Consumer<IInstanceConfig> configModifier) throws IOException
    {
        configModifier = configModifier.andThen(
        config -> config.set("hinted_handoff_enabled", false)
                        .with(NETWORK)
                        .with(GOSSIP)
        );

        return init(Cluster.build().withNodes(3).withConfig(configModifier).start());
    }

    static void repair(ICluster<IInvokableInstance> cluster, String keyspace, Map<String, String> options)
    {
        cluster.get(1).runOnInstance(rethrow(() -> {
            Condition await = newOneTimeCondition();
            instance.repair(keyspace, options, of((tag, event) -> {
                if (event.getType() == COMPLETE)
                    await.signalAll();
            })).right.get();
            await.await(1L, MINUTES);
        }));
    }

    static void populate(ICluster<IInvokableInstance> cluster, String keyspace, String compression) throws Exception
    {
        try
        {
            cluster.schemaChange(String.format("DROP TABLE IF EXISTS %s.test;", keyspace));
            cluster.schemaChange(String.format("CREATE TABLE %s.test (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compression = %s", keyspace, compression));

            insert(cluster, keyspace, 0, 1000, 1, 2, 3);
            flush(cluster, keyspace, 1);
            insert(cluster, keyspace, 1000, 1001, 1, 2);
            insert(cluster, keyspace, 1001, 2001, 1, 2, 3);
            flush(cluster, keyspace, 1, 2, 3);

            verify(cluster, keyspace, 0, 1000, 1, 2, 3);
            verify(cluster, keyspace, 1000, 1001, 1, 2);
            verify(cluster, keyspace, 1001, 2001, 1, 2, 3);
        }
        catch (Throwable t)
        {
            cluster.close();
            throw t;
        }

    }

    void repair(ICluster<IInvokableInstance> cluster, boolean sequential, String compression) throws Exception
    {
        populate(cluster, KEYSPACE, compression);
        repair(cluster, KEYSPACE, ImmutableMap.of("parallelism", sequential ? "sequential" : "parallel"));
        verify(cluster, KEYSPACE, 0, 2001, 1, 2, 3);
    }

    void shutDownNodesAndForceRepair(ICluster<IInvokableInstance> cluster, String keyspace, int downNode) throws Exception
    {
        populate(cluster, keyspace, "{'enabled': false}");
        ClusterUtils.stopUnchecked(cluster.get(downNode));
        repair(cluster, keyspace, ImmutableMap.of("forceRepair", "true"));
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = create(config -> {});
    }

    @AfterClass
    public static void closeCluster() throws Exception
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testSequentialRepairWithDefaultCompression() throws Exception
    {
        repair(cluster, true, "{'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testParallelRepairWithDefaultCompression() throws Exception
    {
        repair(cluster, false, "{'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}");
    }

    @Test
    public void testSequentialRepairWithMinCompressRatio() throws Exception
    {
        repair(cluster, true, "{'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'min_compress_ratio': 4.0}");
    }

    @Test
    public void testParallelRepairWithMinCompressRatio() throws Exception
    {
        repair(cluster, false, "{'class': 'org.apache.cassandra.io.compress.LZ4Compressor', 'min_compress_ratio': 4.0}");
    }

    @Test
    public void testSequentialRepairWithoutCompression() throws Exception
    {
        repair(cluster, true, "{'enabled': false}");
    }

    @Test
    public void testParallelRepairWithoutCompression() throws Exception
    {
        repair(cluster, false, "{'enabled': false}");
    }

    @Test
    public void testForcedNormalRepairWithOneNodeDown() throws Exception
    {
        // The test uses its own keyspace with rf == 2
        String forceRepairKeyspace = "test_force_repair_keyspace";
        int rf = 2;
        int tokenCount = ClusterUtils.getTokenCount(cluster.get(1));
        cluster.schemaChange("CREATE KEYSPACE " + forceRepairKeyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf + "};");

        try
        {
            shutDownNodesAndForceRepair(cluster, forceRepairKeyspace, 3); // shutdown node 3 after inserting
            DistributedRepairUtils.assertParentRepairSuccess(cluster, 1, forceRepairKeyspace, "test", row -> {
                Set<String> successfulRanges = row.getSet("successful_ranges");
                Set<String> requestedRanges = row.getSet("requested_ranges");
                Assert.assertNotNull("Found no successful ranges", successfulRanges);
                Assert.assertNotNull("Found no requested ranges", requestedRanges);
                Assert.assertEquals("Requested ranges count should equals to replication factor", rf * tokenCount, requestedRanges.size());
                Assert.assertTrue("Given clusterSize = 3, RF = 2 and 1 node down in the replica set, it should yield only " + tokenCount + " successful repaired range.",
                                  successfulRanges.size() == tokenCount && !successfulRanges.contains("")); // the successful ranges set should not only contain empty string
            });
        }
        finally
        {
            // bring the node 3 back up
            if (cluster.get(3).isShutdown())
                cluster.get(3).startup(cluster);
        }
    }
}
