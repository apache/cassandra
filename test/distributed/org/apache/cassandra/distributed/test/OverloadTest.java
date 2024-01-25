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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class OverloadTest extends TestBaseImpl
{

    @Test
    public void testFinishInProgressQueries() throws Throwable
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try (Cluster control = init(Cluster.build().withNodes(1)
                                           .withInstanceInitializer(SlowSelect::install)
                                           .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL)
                                                                       .set("read_request_timeout_in_ms", 1000)
                                                                       .set("write_request_timeout_in_ms", 1000)
                                                                       .set("range_request_timeout_in_ms", 1000)
                                                                       .set("counter_write_request_timeout_in_ms", 1000)
                                           )
                                           .start());
             com.datastax.driver.core.Cluster cluster = driver(control);
             Session session = cluster.connect(KEYSPACE))
        {
            control.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, v int)"));
            session.execute("select * from tbl").one();
            control.get(1).runOnInstance(() -> {
                Assert.assertTrue(SlowSelect.enabled.compareAndSet(false, true));
            });
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 1000; i++)
                futures.add(CompletableFuture.supplyAsync(() -> session.execute("select * from tbl").one(), executor));

            int success = 0;
            int timedOut = 0;
            int overloaded = 0;
            for (Future<?> future : futures)
            {
                try
                {
                    future.get();
                    success++;
                }
                catch (ExecutionException e)
                {
                    if (e.getCause() instanceof OperationTimedOutException)
                        timedOut++;
                    else if (e.getCause() instanceof ReadTimeoutException)
                        timedOut++;
                    else if (e.getCause() instanceof OverloadedException)
                        overloaded++;
                    else throw e;
                }
            }

            Assert.assertEquals(1000, success + timedOut + overloaded);
        }
        finally
        {
            executor.shutdown();
        }
    }

    public static com.datastax.driver.core.Cluster driver(Cluster sut)
    {
        return com.datastax.driver.core.Cluster.builder().addContactPoint(sut.get(1).broadcastAddress().getHostString())
                                               .withSocketOptions(new SocketOptions().setReadTimeoutMillis(2000))
                                               .withPoolingOptions(new PoolingOptions().setHeartbeatIntervalSeconds(5000))
                                               .build();
    }

    public static class SlowSelect
    {
        static AtomicBoolean enabled = new AtomicBoolean(false);

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(SelectStatement.class)
                           .method(named("execute").and(takesArguments(QueryState.class, QueryOptions.class, long.class)))
                           .intercept(MethodDelegation.to(SlowSelect.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static ResultMessage.Rows execute(QueryState state, QueryOptions options, long queryStartNanoTime, @SuperCall Callable<ResultMessage.Rows> r) throws Exception
        {
            if (enabled.get() && !state.getClientState().isInternal)
            {
                Thread.sleep(1100);
            }
            return r.call();
        }
    }
}