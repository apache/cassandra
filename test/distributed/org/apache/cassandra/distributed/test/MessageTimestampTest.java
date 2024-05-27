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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.transport.Dispatcher;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class MessageTimestampTest extends TestBaseImpl
{
    @Test
    public void testFinishInProgressQueries() throws Throwable
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try (Cluster cluster = init(Cluster.build().withNodes(2)
                                           .withInstanceInitializer(EnqueuedInThePast::install)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                       .set("native_transport_max_threads", 2)
                                                                       .set("read_request_timeout_in_ms", 5000)
                                                                       .set("cql_start_time", Config.CQLStartTime.QUEUE)
                                           )
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));

            long expiredBefore = cluster.get(1).callsOnInstance(() -> InternodeOutboundMetrics.totalExpiredCallbacks.getCount()).call();

            cluster.get(1).runOnInstance(() -> {
                Assert.assertTrue(EnqueuedInThePast.enabled.compareAndSet(false, true));
            });
            CompletableFuture.supplyAsync(() -> cluster.coordinator(1).execute("select * from " + KEYSPACE + ".tbl where pk = " + 1, ConsistencyLevel.ALL), executor);
            CompletableFuture.supplyAsync(() -> cluster.coordinator(1).execute("select * from " + KEYSPACE + ".tbl where pk = " + 2, ConsistencyLevel.ALL), executor) ;

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            long expiredAfter = 0;
            while (System.nanoTime() < deadline) {
                long computed = cluster.get(1).callsOnInstance(() -> InternodeOutboundMetrics.totalExpiredCallbacks.getCount()).call();
                if (computed > expiredBefore)
                {
                    expiredAfter = computed;
                    break;
                }
            }

            Assert.assertTrue(String.format("%d should be larger than %d", expiredAfter, expiredBefore),
                              expiredAfter > expiredBefore);


        }
    }

    public static class EnqueuedInThePast
    {
        static AtomicBoolean enabled = new AtomicBoolean(false);

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(Dispatcher.RequestTime.class)
                           .method(named("enqueuedAtNanos"))
                           .intercept(MethodDelegation.to(EnqueuedInThePast.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static long enqueuedAtNanos(@SuperCall Callable<Long> r) throws Exception
        {
            if (enabled.get())
            {
                return System.nanoTime() - TimeUnit.SECONDS.toNanos(30);
            }
            return r.call();
        }
    }
}
