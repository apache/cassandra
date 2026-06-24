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

package org.apache.cassandra.distributed.upgrade;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;

public class ClusterMetadata2iInitializeTest extends UpgradeTestBase
{
    @Test
    public void initializeCMSWithConcurrentIndexReadsTest() throws Throwable
    {
        Consumer<UpgradeableCluster.Builder > builderUpdater = builder -> builder.withInstanceInitializer(BBInstaller::install);
        new TestCase()
        .nodes(3)
        .withConfig((cfg) -> cfg.with(Feature.GOSSIP))
        .withBuilder(builderUpdater)
        .upgradesToCurrentFrom(v41)
        .setup((cluster) -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.schemaChange(withKeyspace("create index iii2 on %s.tbl (v)"));
            for (int i = 0; i < 10000; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v) values (?, ?, ?)"), ConsistencyLevel.ALL, 1, i, i%5);
        })
        .runAfterClusterUpgrade((cluster) -> {
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger queryFailures = new AtomicInteger();
            Thread t = new Thread(() -> {
                while (!stop.get())
                {
                    try
                    {
                        cluster.coordinator(1).execute(withKeyspace("select * from %s.tbl where pk=1 and v=4"), ConsistencyLevel.ALL);
                    }
                    catch (Throwable e)
                    {
                        queryFailures.incrementAndGet();
                    }
                }
            });
            t.start();
            cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
            stop.set(true);
            t.join();
            assertEquals(0, queryFailures.get());
        }).run();
    }

    public static class BBInstaller
    {
        public static void install(ClassLoader classLoader, int inst)
        {
            if (inst == 1)
                return;
            new ByteBuddy().rebase(CassandraIndexSearcher.class)
                           .method(named("queryIndex"))
                           .intercept(MethodDelegation.to(BBInterceptor.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }
    }

    public static class BBInterceptor
    {
        public static UnfilteredRowIterator queryIndex(DecoratedKey indexKey, ReadCommand command, ReadExecutionController executionController, @SuperCall Callable<UnfilteredRowIterator> zuper) throws Exception
        {
            // this makes it more likely that we decorate the key with one partitioner and execute the SinglePartitionReadCommand with another
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            return zuper.call();
        }
    }
}
