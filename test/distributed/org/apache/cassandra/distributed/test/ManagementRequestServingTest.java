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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that management requests are served while the native transport is saturated with SELECT
 * requests from the driver. The test blocks those SELECTs and checks that management requests still
 * get processed.
 */
public class ManagementRequestServingTest extends TestBaseImpl
{
    private static final int MANAGEMENT_PORT = 11211;
    private static final int NATIVE_PORT = 9042;
    private static final int SELECT_REQUESTS_NUM = 10;

    @Test
    public void testManagementRequestsServedUnderRegularPortLoad() throws Exception
    {
        ExecutorService executor = Executors.newFixedThreadPool(SELECT_REQUESTS_NUM);
        try (Cluster cluster = init(Cluster.build(1)
                                           .withInstanceInitializer(BlockingSelectByteBuddy::install)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                       .set("start_native_transport_management", true)
                                                                       .set("native_transport_management_port", MANAGEMENT_PORT))
                                           .start());
             com.datastax.driver.core.Cluster driver = com.datastax.driver.core.Cluster.builder()
                                                                                       .addContactPoint("127.0.0.1")
                                                                                       .withPort(NATIVE_PORT)
                                                                                       .build();
             Session session = driver.connect())
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));

            long mgmtBefore = cluster.get(1).callOnInstance(() -> getCompletedTaskCountByExecutor("Native-Transport-Management-Tasks"));
            long reqBefore = cluster.get(1).callOnInstance(() -> getCompletedTaskCountByExecutor("Native-Transport-Requests"));

            cluster.get(1).runOnInstance(() -> assertTrue(BlockingSelectByteBuddy.enabled.compareAndSet(false, true)));

            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < SELECT_REQUESTS_NUM; i++)
                futures.add(CompletableFuture.supplyAsync(() -> session.execute("SELECT * FROM " + KEYSPACE + ".tbl"), executor));

            cluster.get(1).runOnInstance(() -> {
                try
                {
                    BlockingSelectByteBuddy.latch.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });

            try (com.datastax.driver.core.Cluster mgmt = com.datastax.driver.core.Cluster.builder()
                                                                                         .addContactPoint("127.0.0.1")
                                                                                         .withPort(MANAGEMENT_PORT)
                                                                                         .build();
                 Session mgmtSession = mgmt.connect("system"))
            {
                ResultSet rs = mgmtSession.execute("SELECT * FROM system.local");
                assertFalse("Management query should return data", rs.all().isEmpty());
            }

            long mgmtAfter = cluster.get(1).callOnInstance(() -> getCompletedTaskCountByExecutor("Native-Transport-Management-Tasks"));
            assertTrue("Management executor should have processed requests", mgmtAfter > mgmtBefore);

            cluster.get(1).runOnInstance(() -> BlockingSelectByteBuddy.signal.countDown());

            for (Future<?> f : futures)
                f.get();

            long reqAfter = cluster.get(1).callOnInstance(() -> getCompletedTaskCountByExecutor("Native-Transport-Requests"));
            assertTrue("Request executor should have processed requests", reqAfter > reqBefore);
        }
        finally
        {
            executor.shutdown();
        }
    }

    private static long getCompletedTaskCountByExecutor(String name)
    {
        return SharedExecutorPool.SHARED.executors.stream()
                                                  .filter(e -> e.name.endsWith(name))
                                                  .findFirst()
                                                  .map(SEPExecutor::getCompletedTaskCount)
                                                  .orElse(0L);
    }

    public static class BlockingSelectByteBuddy
    {
        static CountDownLatch latch = new CountDownLatch(SELECT_REQUESTS_NUM);
        static CountDownLatch signal = new CountDownLatch(1);
        static AtomicBoolean enabled = new AtomicBoolean(false);

        static void install(ClassLoader cl, int ignored)
        {
            new ByteBuddy().rebase(SelectStatement.class)
                           .method(named("execute")
                                   .and(takesArguments(QueryState.class, QueryOptions.class, Dispatcher.RequestTime.class)))
                           .intercept(MethodDelegation.to(BlockingSelectByteBuddy.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static ResultMessage.Rows execute(QueryState state,
                                                 QueryOptions options,
                                                 Dispatcher.RequestTime requestTime,
                                                 @SuperCall Callable<ResultMessage.Rows> r) throws Exception
        {
            if (enabled.get() && !state.getClientState().isInternal && !state.getClientState().isManagement())
            {
                latch.countDown();
                signal.await();
            }
            return r.call();
        }
    }
}
