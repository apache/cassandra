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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.service.ActiveRepairService;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertFalse;

public class ClearSnapshotTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ClearSnapshotTest.class);

    @Test
    public void clearSnapshotSlowTest() throws IOException, InterruptedException, ExecutionException
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(config ->
                                                                config.with(GOSSIP)
                                                                      .with(NETWORK))
                                          .withInstanceInitializer(BB::install)
                                          .start()))
        {
            int tableCount = 50;
            for (int i = 0; i < tableCount; i++)
            {
                String ksname = "ks"+i;
                cluster.schemaChange("create keyspace "+ksname+" with replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
                cluster.schemaChange("create table "+ksname+".tbl (id int primary key, t int)");
                cluster.get(1).executeInternal("insert into "+ksname+".tbl (id , t) values (?, ?)", i, i);
                cluster.forEach((node) -> node.flush(ksname));
            }
            List<Thread> repairThreads = new ArrayList<>();
            for (int i = 0; i < tableCount; i++)
            {
                String ksname = "ks"+i;
                Thread t = new Thread(() -> cluster.get(1).nodetoolResult("repair", "-full", ksname).asserts().success());
                t.start();
                repairThreads.add(t);
            }
            AtomicBoolean gotExc = new AtomicBoolean(false);
            AtomicBoolean exit = new AtomicBoolean(false);
            Thread reads = new Thread(() -> {
                while (!exit.get())
                {
                    try
                    {
                        cluster.coordinator(1).execute("select * from ks1.tbl where id = 5", ConsistencyLevel.QUORUM);
                        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                    }
                    catch (Exception e)
                    {
                        if (!gotExc.get())
                            logger.error("Unexpected exception querying table ks1.tbl", e);
                        gotExc.set(true);
                    }
                }
            });

            reads.start();
            long activeRepairs;
            do
            {
                activeRepairs = cluster.get(1).callOnInstance(() -> ActiveRepairService.instance().parentRepairSessionCount());
                Thread.sleep(50);
            }
            while (activeRepairs < 35);

            cluster.setUncaughtExceptionsFilter((t) -> t.getMessage() != null && t.getMessage().contains("Parent repair session with id") );
            cluster.get(2).shutdown().get();
            repairThreads.forEach(t -> {
                try
                {
                    t.join();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
            exit.set(true);
            reads.join();

            assertFalse(gotExc.get());
        }
    }

    public static class BB
    {
        public static void install(ClassLoader classLoader, Integer num)
        {
            new ByteBuddy().rebase(Directories.class)
                           .method(named("snapshotExists"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

        }

        @SuppressWarnings("unused")
        public static boolean snapshotExists(String name, @SuperCall Callable<Boolean> zuper)
        {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testSeqClearsSnapshot() throws IOException, TimeoutException
    {
        try(Cluster cluster = init(Cluster.build(3).withConfig(config ->
                                                               config.with(GOSSIP)
                                                                     .with(NETWORK))
                                          .withInstanceInitializer(BB::install)
                                          .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key, x int)"));
            for (int i = 0; i < 10; i++)
                cluster.get(1).executeInternal(withKeyspace("insert into %s.tbl (id, x) values (?, ?)"), i, i);
            cluster.get(1).nodetoolResult("repair", "-seq", "-full", KEYSPACE).asserts().success();
            cluster.get(1).logs().watchFor("Clearing snapshot");
        }
    }
}
