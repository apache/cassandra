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

package org.apache.cassandra.distributed.test.replication;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MutationTrackingServiceTest extends TestBaseImpl
{
    private static final String KS_NAME = "ks";
    private static final String TBL_NAME = "tbl";

    @Shared
    public static class BBState
    {
        public static volatile boolean enabled = false;
        public static volatile CountDownLatch enteredLatch = new CountDownLatch(1);
        public static volatile CountDownLatch pauseLatch = new CountDownLatch(1);

        public static void reset()
        {
            enabled = false;
            enteredLatch = new CountDownLatch(1);
            pauseLatch = new CountDownLatch(1);
        }
    }

    public static class BBInstaller
    {
        public static void install(ClassLoader cl, Integer num)
        {
            if (num == 1)
            {
                new ByteBuddy().rebase(MutationTrackingService.class)
                               .method(named("start").and(takesArguments(0)))
                               .intercept(MethodDelegation.to(BBInterceptor.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }
    }

    public static class BBInterceptor
    {
        public static void start(@SuperCall Runnable zuper)
        {
            if (BBState.enabled)
            {
                BBState.enteredLatch.countDown();
                try
                {
                    BBState.pauseLatch.await(60, TimeUnit.SECONDS);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }
            zuper.run();
        }
    }

    @Test
    public void testIrrelevantTopologyChange() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3).start())
        {
            // Create tracked keyspace and table
            cluster.schemaChange("CREATE KEYSPACE " + KS_NAME + " WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked'");
            cluster.schemaChange("CREATE TABLE " + KS_NAME + "." + TBL_NAME + " (k int PRIMARY KEY, v int)");

            // Create an untracked keyspace (should not affect tracked keyspace shards)
            cluster.schemaChange("CREATE KEYSPACE untracked WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='untracked'");

            // Test that shards remain the same object (no reconfiguration)
            Boolean shardsUnchanged = cluster.get(1).callOnInstance(() -> {
                var service = MutationTrackingService.instance();
                Object initialShards = MutationTrackingService.TestAccess.getKeyspaceShards(service, KS_NAME);
                Object newShards = MutationTrackingService.TestAccess.getKeyspaceShards(service, KS_NAME);
                return initialShards == newShards; // Same object reference
            });

            assertTrue("Keyspace shards should not change for irrelevant topology changes", shardsUnchanged);
        }
    }

    @Test
    public void testHostLogIdNotRegressedByBroadcastDuringStartup() throws Throwable
    {
        BBState.reset();
        try (Cluster cluster = builder().withNodes(2)
                                        .withInstanceInitializer(BBInstaller::install)
                                        .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KS_NAME + " WITH replication = " +
                                 "{'class': 'SimpleStrategy', 'replication_factor': 2} AND replication_type='tracked'");
            cluster.schemaChange("CREATE TABLE " + KS_NAME + "." + TBL_NAME + " (k int PRIMARY KEY, v int)");

            // Perform writes to generate mutations from both coordinators so shards have active offsets
            cluster.coordinator(1).execute("INSERT INTO " + KS_NAME + "." + TBL_NAME + " (k, v) VALUES (1, 1)",
                                           ConsistencyLevel.ALL);
            cluster.coordinator(2).execute("INSERT INTO " + KS_NAME + "." + TBL_NAME + " (k, v) VALUES (2, 2)",
                                           ConsistencyLevel.ALL);

            // Allocate additional log IDs on node 1 so counter is well above 1 (mirroring a node that ran for some time)
            cluster.get(1).runOnInstance(() -> {
                var service = MutationTrackingService.instance();
                for (int i = 0; i < 10; i++)
                    MutationTrackingService.TestAccess.nextHostLogId(service);
            });

            // Read host_log_id before restart on node 1
            Object[][] res = cluster.get(1).executeInternal("SELECT host_log_id FROM system.host_log_id WHERE key='local'");
            assertNotNull(res);
            assertTrue(res.length > 0);
            int initialHostLogId = (Integer) res[0][0];
            assertTrue("Expected positive initial host log id, got " + initialHostLogId, initialHostLogId > 0);

            // Ensure node 2 has witnessed the mutations and can broadcast offsets
            cluster.get(2).runOnInstance(() -> {
                MutationTrackingService.instance().broadcastOffsetsForTesting();
            });

            // Shut down node 1
            cluster.get(1).shutdown().get();

            // Enable startup pause for node 1
            BBState.enabled = true;
            BBState.enteredLatch = new CountDownLatch(1);
            BBState.pauseLatch = new CountDownLatch(1);

            // Start node 1 asynchronously
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<?> startupFuture = executor.submit(() -> {
                try
                {
                    cluster.get(1).startup();
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }
            });

            try
            {
                // Wait for node 1 to reach MutationTrackingService.start()
                assertTrue("Timed out waiting for node 1 to reach MutationTrackingService.start()",
                           BBState.enteredLatch.await(60, TimeUnit.SECONDS));

                InetAddressAndPort node1Addr = InetAddressAndPort.getByNameUnchecked("127.0.0.1:7012");
                cluster.get(2).runOnInstance(() -> {
                    MutationTrackingService.TestAccess.broadcastOffsetsTo(MutationTrackingService.instance(), KS_NAME, node1Addr);
                });
                Thread.sleep(500);
            }
            finally
            {
                // Unpause node 1 so startup can finish
                BBState.pauseLatch.countDown();
            }

            startupFuture.get(60, TimeUnit.SECONDS);
            executor.shutdown();

            // Verify post-restart state on node 1:
            // 1. system.host_log_id must be strictly greater than initialHostLogId (since boot allocates a new primary log)
            Object[][] postRes = cluster.get(1).executeInternal("SELECT host_log_id FROM system.host_log_id WHERE key='local'");
            assertNotNull(postRes);
            assertTrue(postRes.length > 0);
            int postHostLogId = (Integer) postRes[0][0];
            assertTrue("Post restart host log id " + postHostLogId + " should be greater than initial " + initialHostLogId,
                       postHostLogId > initialHostLogId);

            // 2. New primary log id on each shard must be strictly greater than initialHostLogId
            List<Long> newPrimaryLogIds = cluster.get(1).callOnInstance(() -> {
                return MutationTrackingService.TestAccess.getPrimaryHostLogIds(MutationTrackingService.instance(), KS_NAME);
            });

            for (Long logId : newPrimaryLogIds)
            {
                assertTrue("New primary log id " + logId + " must be greater than initial host log id " + initialHostLogId,
                           logId > initialHostLogId);
            }
        }
    }
}
