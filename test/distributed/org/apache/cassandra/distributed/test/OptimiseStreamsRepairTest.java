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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;

import java.util.concurrent.TimeoutException;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.AsymmetricRemoteSyncTask;
import org.apache.cassandra.repair.LocalSyncTask;
import org.apache.cassandra.repair.RepairJob;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SyncTask;
import org.apache.cassandra.repair.TreeResponse;
import org.apache.cassandra.streaming.PreviewKind;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OptimiseStreamsRepairTest extends TestBaseImpl
{
    @Test
    public void testBasic() throws Exception
    {
        try(Cluster cluster = init(Cluster.build(3)
                                          .withInstanceInitializer(BBHelper::install)
                                          .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int) with compaction={'class': 'SizeTieredCompactionStrategy'}");
            for (int i = 0; i < 10000; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, t) values (?,?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach((i) -> i.flush(KEYSPACE));

            cluster.get(2).shutdown().get();

            for (int i = 0; i < 2000; i++)
                cluster.coordinator(1).execute("INSERT INTO "+KEYSPACE+".tbl (id, t) values (?,?)", ConsistencyLevel.QUORUM, i, i * 2 + 2);

            cluster.get(2).startup();
            Thread.sleep(1000);
            cluster.forEach(c -> c.flush(KEYSPACE));
            cluster.forEach(c -> c.forceCompact(KEYSPACE, "tbl"));

            long [] marks = PreviewRepairTest.logMark(cluster);
            NodeToolResult res = cluster.get(1).nodetoolResult("repair", KEYSPACE, "-os");
            res.asserts().success();

            PreviewRepairTest.waitLogsRepairFullyFinished(cluster, marks);

            res = cluster.get(1).nodetoolResult("repair", KEYSPACE, "-vd");
            res.asserts().success();
            res.asserts().notificationContains("Repaired data is in sync");

            res = cluster.get(1).nodetoolResult("repair", KEYSPACE, "--preview", "--full");
            res.asserts().success();
            res.asserts().notificationContains("Previewed data was in sync");
        }
    }

    public static class BBHelper
    {
        public static void install(ClassLoader cl, int id)
        {
            new ByteBuddy().rebase(RepairJob.class)
                           .method(named("createOptimisedSyncingSyncTasks"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static List<SyncTask> createOptimisedSyncingSyncTasks(SharedContext ctx,
                                                                     RepairJobDesc desc,
                                                                     List<TreeResponse> trees,
                                                                     InetAddressAndPort local,
                                                                     Predicate<InetAddressAndPort> isTransient,
                                                                     Function<InetAddressAndPort, String> getDC,
                                                                     boolean isIncremental,
                                                                     PreviewKind previewKind,
                                                                     @SuperCall Callable<List<SyncTask>> zuperCall)
        {
            List<SyncTask> tasks = null;
            try
            {
                tasks = zuperCall.call();
                verifySyncTasks(tasks);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            return tasks;
        }

        private static void verifySyncTasks(List<SyncTask> tasks) throws UnknownHostException
        {
            Map<InetAddressAndPort, Map<InetAddressAndPort, List<Range<Token>>>> fetching = new HashMap<>();
            for (SyncTask task : tasks)
            {
                if (task instanceof LocalSyncTask)
                {
                    assertFalse(((LocalSyncTask)task).transferRanges);
                    assertTrue(((LocalSyncTask)task).requestRanges);
                }
                else
                    assertTrue(task instanceof AsymmetricRemoteSyncTask);

                Map<InetAddressAndPort, List<Range<Token>>> fetch = fetching.computeIfAbsent(task.nodePair().coordinator, k -> new HashMap<>());
                fetch.computeIfAbsent(task.nodePair().peer, k -> new ArrayList<>()).addAll(task.rangesToSync);
            }
            // 127.0.0.2 is the node out of sync - make sure it does not receive multiple copies of the same range from the other nodes;
            Map<InetAddressAndPort, List<Range<Token>>> node2 = fetching.get(InetAddressAndPort.getByName("127.0.0.2"));
            Set<Range<Token>> allRanges = new HashSet<>();
            node2.values().forEach(ranges -> ranges.forEach(r -> assertTrue(allRanges.add(r))));

            // 127.0.0.2 should stream the same ranges to .1 and .3
            Set<Range<Token>> node2ToNode1 = new HashSet<>(fetching.get(InetAddressAndPort.getByName("127.0.0.1")).get(InetAddressAndPort.getByName("127.0.0.2")));
            Set<Range<Token>> node2ToNode3 = new HashSet<>(fetching.get(InetAddressAndPort.getByName("127.0.0.3")).get(InetAddressAndPort.getByName("127.0.0.2")));
            assertEquals(node2ToNode1, allRanges);
            assertEquals(node2ToNode3, allRanges);
        }
    }

    @Test
    public void randomTest() throws IOException, TimeoutException
    {
        try(Cluster cluster = init(Cluster.build(3)
                                          .withConfig(config -> config.set("hinted_handoff_enabled", false)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK))
                                          .start()))
        {
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int) with compaction={'class': 'SizeTieredCompactionStrategy'}");
            for (int i = 0; i < 1000; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, t) values (?,?)", ConsistencyLevel.ALL, i, i);
            cluster.forEach((i) -> i.flush(KEYSPACE));

            Random r = new Random();
            for (int i = 0; i < 500; i++)
                for (int j = 1; j <= 3; j++)
                    cluster.get(j).executeInternal("INSERT INTO "+KEYSPACE+".tbl (id, t) values (?,?)", r.nextInt(), i * 2 + 2);

            long [] marks = PreviewRepairTest.logMark(cluster);
            NodeToolResult res = cluster.get(1).nodetoolResult("repair", KEYSPACE, "-os");
            res.asserts().success();
            PreviewRepairTest.waitLogsRepairFullyFinished(cluster, marks);
            res = cluster.get(1).nodetoolResult("repair", KEYSPACE, "-vd");
            res.asserts().success();
            res.asserts().notificationContains("Repaired data is in sync");

            res = cluster.get(1).nodetoolResult("repair", KEYSPACE, "--preview", "--full");
            res.asserts().success();
            res.asserts().notificationContains("Previewed data was in sync");
        }
    }
}
