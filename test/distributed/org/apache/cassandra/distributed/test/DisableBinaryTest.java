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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.OverloadedException;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.fail;

public class DisableBinaryTest extends TestBaseImpl
{
    private static int REQUESTS = 100;

    private static com.datastax.driver.core.Cluster driver(Cluster sut)
    {
        return com.datastax.driver.core.Cluster.builder().addContactPoint(sut.get(1).broadcastAddress().getHostString())
                                               .withSocketOptions(new SocketOptions().setReadTimeoutMillis(Integer.MAX_VALUE))
                                               .withPoolingOptions(new PoolingOptions().setHeartbeatIntervalSeconds(Integer.MAX_VALUE))
                                               .build();
    }

    @Test
    public void testFinishInProgressQueries() throws Throwable
    {
        ExecutorService executor = Executors.newFixedThreadPool(REQUESTS);
        try (Cluster control = init(Cluster.build().withNodes(1)
                                           .withInstanceInitializer(BlockingSelect::install)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)).start());
             com.datastax.driver.core.Cluster cluster = driver(control);
             Session session = cluster.connect(KEYSPACE))
        {
            control.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            control.get(1).runOnInstance(() -> {
                Assert.assertTrue(BlockingSelect.enabled.compareAndSet(false, true));
            });
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < REQUESTS; i++)
                futures.add(CompletableFuture.supplyAsync(() -> session.execute("select * from tbl").one(), executor));

            control.get(1).runOnInstance(() -> {
                try
                {
                    BlockingSelect.latch.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
            CompletableFuture<NodeToolResult> result = CompletableFuture.supplyAsync(() -> control.get(1).nodetoolResult("disablebinary"));

            control.get(1).runOnInstance(() -> BlockingSelect.signal.countDown());
            result.get();
            for (Future<?> future : futures)
                future.get();
            // There are no assertions in this test as we simply expect there were no transport exceptions, and all queries succeed.
        }
        finally
        {
            executor.shutdown();
        }
    }


    @Test
    public void testDisallowsNewRequests() throws Throwable
    {
        ExecutorService executor = Executors.newFixedThreadPool(REQUESTS);
        try (Cluster control = init(Cluster.build().withNodes(1)
                                           .withInstanceInitializer(BlockingSelect::install)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)).start());
             com.datastax.driver.core.Cluster cluster = driver(control);
             Session session = cluster.connect(KEYSPACE))
        {
            control.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            control.get(1).runOnInstance(() -> {
                Assert.assertTrue(BlockingSelect.enabled.compareAndSet(false, true));
            });
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < REQUESTS; i++)
                futures.add(CompletableFuture.supplyAsync(() -> session.execute("select * from tbl").one(), executor));

            control.get(1).runOnInstance(() -> {
                try
                {
                    BlockingSelect.latch.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
            CompletableFuture<NodeToolResult> result = CompletableFuture.supplyAsync(() -> control.get(1).nodetoolResult("disablebinary"));

            control.get(1).runOnInstance(() -> {
                while(CassandraDaemon.getInstanceForTesting().nativeTransportService().isRunning()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                }
            });

            Future<?> afterShutdown = CompletableFuture.supplyAsync(() -> session.execute("select * from tbl").one());
            try
            {
                session.execute("select * from tbl").one();
                fail("Should have thrown OverloadedException");
            }
            catch (OverloadedException e) {}

            control.get(1).runOnInstance(() -> BlockingSelect.signal.countDown());
            result.get();
            for (Future<?> future : futures)
                future.get();

            try
            {
                afterShutdown.get();
                fail("Should have thrown OverloadedException");
            }
            catch (ExecutionException e)
            {
                Assert.assertTrue(e.getCause() instanceof OverloadedException);
            }
        }
        finally
        {
            executor.shutdown();
        }
    }

    public static class BlockingSelect
    {
        static CountDownLatch latch = new CountDownLatch(REQUESTS);
        static CountDownLatch signal = new CountDownLatch(1);
        static AtomicBoolean enabled  = new AtomicBoolean(false);

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(SelectStatement.class)
                           .method(named("execute").and(takesArguments(QueryState.class, QueryOptions.class, Dispatcher.RequestTime.class)))
                           .intercept(MethodDelegation.to(BlockingSelect.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static ResultMessage.Rows execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime, @SuperCall Callable<ResultMessage.Rows> r) throws Exception
        {
            if (enabled.get() && !state.getClientState().isInternal)
            {
                latch.countDown();
                signal.await();
                // Sleep for one more second to make sure disable binary reacts
                Thread.sleep(1000);
            }
            return r.call();
        }
    }

}
