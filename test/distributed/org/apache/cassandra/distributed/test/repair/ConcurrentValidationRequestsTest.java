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

package org.apache.cassandra.distributed.test.repair;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.repair.ValidationTask;
import org.apache.cassandra.utils.MerkleTrees;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

/**
 * Verifies that the config property {@code concurrent_merkle_tree_requests} limits the number of concurrent validation
 * requests.
 */
public class ConcurrentValidationRequestsTest extends TestBaseImpl
{
    private static final int NODES = 4;
    private static final int RF = 3;
    private static final int TABLES = 5;
    private static final int ROWS = 100;
    private static final int MAX_REQUESTS = 1;

    @Test
    public void testConcurrentValidations() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(NODES)
                                             .withInstanceInitializer(ConcurrentValidationRequestsTest.BBHelper::install)
                                             .withConfig(c -> c.set("concurrent_merkle_tree_requests", MAX_REQUESTS)
                                                               .with(NETWORK, GOSSIP))
                                             .start(), RF))
        {
            for (int t = 1; t <= TABLES; t++)
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.t" + t + " (k int PRIMARY KEY, v int)"));

            int v = 0;
            for (int k = 0; k < ROWS; k++)
            {
                v++;
                for (int t = 1; t <= TABLES; t++)
                {
                    String insert = withKeyspace("INSERT INTO %s.t" + t + " (k, v) VALUES (?, ?)");
                    cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, k, v);
                }
            }
            cluster.forEach(x -> x.flush(KEYSPACE));

            NodeToolResult res = cluster.get(1).nodetoolResult("repair", "-j=4", KEYSPACE);
            res.asserts().success();
        }
    }

    public static class BBHelper
    {
        /**
         * Keeps track of the number of concurrent validation requests, which should never be greater than RF times
         * the value of the {@code concurrent_merkle_tree_requests} config property.
         */
        public static volatile AtomicInteger requests = new AtomicInteger(0);

        public static void install(ClassLoader cl, int node)
        {
            if (node != 1)
                return;

            new ByteBuddy().rebase(ValidationTask.class)
                           .method(named("run"))
                           .intercept(MethodDelegation.to(ConcurrentValidationRequestsTest.BBHelper.class))
                           .method(named("treesReceived"))
                           .intercept(MethodDelegation.to(ConcurrentValidationRequestsTest.BBHelper.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static void run(@SuperCall Callable<Void> zuper)
        {
            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            int requests = BBHelper.requests.incrementAndGet();
            if (requests > MAX_REQUESTS * RF)
                throw new AssertionError("Too many concurrent validation requests: " + requests);
        }

        @SuppressWarnings("unused")
        public static void treesReceived(MerkleTrees trees, @SuperCall Callable<Void> zuperCall)
        {
            requests.decrementAndGet();

            try
            {
                zuperCall.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}