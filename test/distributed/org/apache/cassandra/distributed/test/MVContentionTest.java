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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.db.view.ViewManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;

public class MVContentionTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MVContentionTest.class);

    @Test
    public void concurrentMVLockTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build().withNodes(3)
                                      .withTokenSupplier(evenlyDistributedTokens(3, 1))
                                      .withNodeIdTopology(singleDcNetworkTopology(3, "dc0", "rack0"))
                                      .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                  .set("materialized_views_enabled", "true")
                                                                  .set("concurrent_writes", 3)) // fewer writers will more likely to reproduce
                                      .withInstanceInitializer(BBViewManagerAcquireLockFor::install)
                                      .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (id int PRIMARY KEY, v1 int, v2 int)"));
            cluster.schemaChange("create materialized view " + KEYSPACE + ".tbl_v1 as SELECT id, v1, v2 FROM " + KEYSPACE + ".tbl WHERE v1 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (v1, id)");

            // let node 2 very likely fail to aquire MV lock
            cluster.get(2).runOnInstance(() -> BBViewManagerAcquireLockFor.failAquireLockPercentage.set(0.9));


            ExecutorService executor = Executors.newFixedThreadPool(10);
            CountDownLatch start = new CountDownLatch(1);    // released to start all threads at once
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < 10; i++)
            {
                final int id = i;
                futures.add(executor.submit(() -> {
                    try
                    {
                        start.await();
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                    for (int j = 0; j < 10; j++)
                    {
                        int v1 = id * 100 + j;
                        int v2 = v1 + 1;
                        try
                        {
                            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (id, v1, v2) VALUES (1000, ?, ?)", ConsistencyLevel.LOCAL_QUORUM, v1, v2);
                        }
                        catch (Throwable e)
                        {
                            logger.info("Thread" + id + " failed to insert v1=" + v1 + ", v2=" + v2 + " due to " + e.getMessage());
                        }
                    }

                }));
            }

            start.countDown();
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            for (Future<?> f : futures)
            {
                f.cancel(true);
            }

            // wait long enough to verify that node 2 can drain the mutation stage queue on high MV contention
            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);

            int pending1 = cluster.get(1).callOnInstance(() -> {
                SEPExecutor mutationSEPTP = SharedExecutorPool.SHARED.getExecutor("MutationStage");
                return mutationSEPTP.getPendingTaskCount();
            });
            int pending2 = cluster.get(2).callOnInstance(() -> {
                SEPExecutor mutationSEPTP = SharedExecutorPool.SHARED.getExecutor("MutationStage");
                return mutationSEPTP.getPendingTaskCount();
            });
            int pending3 = cluster.get(3).callOnInstance(() -> {
                SEPExecutor mutationSEPTP = SharedExecutorPool.SHARED.getExecutor("MutationStage");
                return mutationSEPTP.getPendingTaskCount();
            });
            Assert.assertEquals(0, pending1);
            Assert.assertEquals(0, pending2);
            Assert.assertEquals(0, pending3);
        }
    }

    public static class BBViewManagerAcquireLockFor
    {
        public static final AtomicDouble failAquireLockPercentage = new AtomicDouble();
        public static final Random random = new Random();
        public static void install(ClassLoader cl, Integer i)
        {
            new ByteBuddy().rebase(ViewManager.class)
                           .method(named("acquireLockFor"))
                           .intercept(MethodDelegation.to(MVContentionTest.BBViewManagerAcquireLockFor.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static Lock acquireLockFor(int keyAndCfidHash, @SuperCall Callable<Lock> zuper) throws Exception
        {
            if (random.nextDouble() < failAquireLockPercentage.get())
            {
                logger.info("Contention, fail to acquire lock");
                return null;
            }
            logger.info("lock acquired");
            return zuper.call();
        }
    }
}
