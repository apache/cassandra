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
package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class StreamFailedAfterReceivingTest extends TestBaseImpl
{
    @Test
    public void zcsTest() throws IOException, ExecutionException, InterruptedException
    {
        leftoverFilesTest(true);
    }

    @Test
    public void nozcsTest() throws IOException, ExecutionException, InterruptedException
    {
        leftoverFilesTest(false);
    }

    public void leftoverFilesTest(boolean zcs) throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withInstanceInitializer(BBHelper::install)
                                      .withConfig(c -> c.with(Feature.values())
                                                        .set("stream_entire_sstables", zcs)
                                                        .set("autocompaction_on_startup_enabled", false)
                                                        .set("disk_failure_policy", "die"))
                                      .start())
        {
            init(cluster);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, x int)"));
            cluster.forEach(i -> i.nodetoolResult("disableautocompaction").asserts().success());
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            for (int i = 1; i <= 1000000; i++)
            {
                node1.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, x) VALUES (?,?)"), i, i);
                if (i % 100000 == 0)
                    node1.flush(KEYSPACE);
            }
            node1.flush(KEYSPACE);
            node2.runOnInstance(() -> BBHelper.enabled.set(true));
            cluster.setUncaughtExceptionsFilter((e) -> e.getClass().getName().contains("TransactionAlreadyCompletedException"));
            node1.nodetoolResult("repair", "-pr", "-full", KEYSPACE, "tbl").asserts().failure();
            node2.runOnInstance(() -> BBHelper.cdl.awaitUninterruptibly());
            node2.runOnInstance(() -> BBHelper.enabled.set(false));
            node2.shutdown().get();
            node2.startup();
        }
    }


    public static class BBHelper
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num == 2)
            {
                // in this case we need to throw after trackNew:ing the sstable, but before it is finished
                new ByteBuddy().rebase(LifecycleTransaction.class)
                               .method(named("trackNew").and(takesArguments(1)))
                               .intercept(to(StreamFailedAfterReceivingTest.BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        static AtomicInteger waiting = new AtomicInteger();
        static AtomicBoolean enabled = new AtomicBoolean();
        static CountDownLatch cdl = CountDownLatch.newCountDownLatch(1);

        public static void trackNew(SSTable sstable, @SuperCall Callable<Void> zuper) throws Exception
        {
            zuper.call();
            if (enabled.get())
            {
                if (waiting.incrementAndGet() > 4)
                    throw new RuntimeException();

                // using a sleep instead of a horrible nesting of latches - this should
                // not make the test flaky, just might flakily pass without hitting the
                // right condition
                Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
                cdl.decrement();
            }
        }
    }

    @Test
    public void basicStreamTest() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                      .withConfig(c -> c.with(Feature.values())
                                                        .set("stream_entire_sstables", false)
                                                        .set("autocompaction_on_startup_enabled", false)
                                                        .set("disk_failure_policy", "die"))
                                      .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY, x int)"));
            cluster.forEach(i -> i.nodetoolResult("disableautocompaction").asserts().success());
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            for (int i = 1; i <= 1000; i++)
            {
                node1.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, x) VALUES (?,?)"), i, i);
                if (i % 100 == 0)
                    node1.flush(KEYSPACE);
            }
            node1.flush(KEYSPACE);

            node1.nodetoolResult("repair", "-pr", "-full", KEYSPACE, "tbl").asserts().success();
        }
    }
}
