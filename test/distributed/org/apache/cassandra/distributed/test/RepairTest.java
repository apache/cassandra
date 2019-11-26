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
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.SimpleCondition;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.impl.ExecUtil.rethrow;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class RepairTest extends DistributedTestBase
{
    private static final String insert = withKeyspace("INSERT INTO %s.test (k, c1, c2) VALUES (?, 'value1', 'value2');");
    private static final String query = withKeyspace("SELECT k, c1, c2 FROM %s.test WHERE k = ?;");
    private static Cluster cluster;

    private static void insert(Cluster cluster, int start, int end, int ... nodes)
    {
        for (int i = start ; i < end ; ++i)
            for (int node : nodes)
                cluster.get(node).executeInternal(insert, Integer.toString(i));
    }

    private static void verify(Cluster cluster, int start, int end, int ... nodes)
    {
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

    private static void flush(Cluster cluster, int ... nodes)
    {
        for (int node : nodes)
            cluster.get(node).runOnInstance(rethrow(() -> StorageService.instance.forceKeyspaceFlush(KEYSPACE)));
    }

    private static Cluster create(Consumer<InstanceConfig> configModifier) throws IOException
    {
        configModifier = configModifier.andThen(
        config -> config.set("hinted_handoff_enabled", false)
                        .set("commitlog_sync_batch_window_in_ms", 5)
                        .with(NETWORK)
                        .with(GOSSIP)
        );

        Cluster cluster = init(Cluster.build(3).withConfig(configModifier).start());
        return cluster;
    }

    private void repair(Cluster cluster, Map<String, String> options)
    {
        cluster.get(1).runOnInstance(rethrow(() -> {
            SimpleCondition await = new SimpleCondition();
            StorageService.instance.repair(KEYSPACE, options, ImmutableList.of((tag, event) -> {
                if (event.getType() == ProgressEventType.COMPLETE)
                    await.signalAll();
            })).right.get();
            await.await(1L, MINUTES);
        }));
    }

    void populate(Cluster cluster, boolean compression)
    {
        try
        {
            cluster.schemaChange(withKeyspace("DROP TABLE IF EXISTS %s.test;"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.test (k text, c1 text, c2 text, PRIMARY KEY (k))") +
                                 (compression == false ? " WITH compression = {'enabled' : false};" : ";"));

            insert(cluster,    0, 1000, 1, 2, 3);
            flush(cluster, 1);
            insert(cluster, 1000, 1001, 1, 2);
            insert(cluster, 1001, 2001, 1, 2, 3);
            flush(cluster, 1, 2, 3);

            verify(cluster,    0, 1000, 1, 2, 3);
            verify(cluster, 1000, 1001, 1, 2);
            verify(cluster, 1001, 2001, 1, 2, 3);
        }
        catch (Throwable t)
        {
            cluster.close();
            throw t;
        }

    }

    void simpleRepair(Cluster cluster, boolean sequential, boolean compression) throws IOException
    {
        populate(cluster, compression);
        repair(cluster, ImmutableMap.of("parallelism", sequential ? "sequential" : "parallel"));
        verify(cluster, 0, 2001, 1, 2, 3);
    }

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = create(config -> {});
    }

    @AfterClass
    public static void closeCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Ignore("Test requires CASSANDRA-13938 to be merged")
    public void testSimpleSequentialRepairDefaultCompression() throws IOException
    {
        simpleRepair(cluster, true, true);
    }

    @Test
    public void testSimpleSequentialRepairCompressionOff() throws IOException
    {
        simpleRepair(cluster, true, false);
    }
}
