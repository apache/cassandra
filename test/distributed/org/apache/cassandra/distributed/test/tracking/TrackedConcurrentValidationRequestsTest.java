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

package org.apache.cassandra.distributed.test.tracking;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.MutationTrackingValidationCoordinator;
import org.apache.cassandra.utils.concurrent.Future;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class TrackedConcurrentValidationRequestsTest extends TestBaseImpl
{
    private static final int NODES = 3;
    private static final int RF = 3;
    private static final int TABLES = 5;
    private static final int ROWS = 10;
    private static final int MAX_REQUESTS = 1;

    @Test
    public void testConcurrentTrackedValidations() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(NODES)
                                             .withInstanceInitializer(BBHelper::install)
                                             .withConfig(c -> c.set("concurrent_merkle_tree_requests", MAX_REQUESTS).with(NETWORK, GOSSIP))
                                             .start(), RF))
        {
            String keyspace = "test_concurrent_tracked_validations";
            cluster.schemaChange("CREATE KEYSPACE " + keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + RF + "} AND replication_type='tracked'");

            for (int t = 1; t <= TABLES; t++)
                cluster.schemaChange("CREATE TABLE " + keyspace + ".t" + t + " (k int PRIMARY KEY, v int)");

            for (int t = 1; t <= TABLES; t++)
                for (int k = 0; k < ROWS; k++)
                    cluster.coordinator(1).execute("INSERT INTO " + keyspace + ".t" + t + " (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, k, k);

            MutationTrackingUtils.settleReconciliation(cluster);

            NodeToolResult result = cluster.get(1).nodetoolResult(true, "repair", "--validate", keyspace);
            result.asserts().success();
        }
    }

    public static class BBHelper
    {
        public static volatile AtomicInteger requests = new AtomicInteger(0);

        public static void install(ClassLoader cl, int node)
        {
            // MutationTrackingValidationCoordinator only runs on the node that initiated the repair.
            if (node != 1)
                return;

            new ByteBuddy().rebase(MutationTrackingValidationCoordinator.class)
                           .method(named("start"))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings({ "unchecked", "rawtypes", "unused" })
        public static Future start(@SuperCall Callable<Future> zuper)
        {
            int inFlight = requests.incrementAndGet();
            if (inFlight > MAX_REQUESTS)
                throw new AssertionError("Too many concurrent tracked validation requests: " + inFlight);

            Future future;
            try
            {
                future = zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            future.addCallback(result -> requests.decrementAndGet(), failure -> requests.decrementAndGet());
            return future;
        }
    }
}
