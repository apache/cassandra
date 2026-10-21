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

package org.apache.cassandra.distributed.test.tcm;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.Epoch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

public class IndexTablePartitionerAfterForceSnapshotTest extends TestBaseImpl
{
    @Test
    public void indexQueryAfterSnapshotTest() throws IOException, InterruptedException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withInstanceInitializer(BBInstaller::install)
                                      .withConfig(config -> config.with(GOSSIP, NETWORK))
                                      .start())
        {
            init(cluster);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.schemaChange(withKeyspace("create index iii2 on %s.tbl (v)"));
            for (int i = 0; i < 10000; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (pk, ck, v) values (?, ?, ?)"), ConsistencyLevel.ALL, 1, i, i % 5);

            // drop metadata change replication messages from node 1 to nodes 2 & 3
            cluster.filters().verbs(Verb.TCM_REPLICATION.id,
                                    Verb.TCM_FETCH_CMS_LOG_RSP.id,
                                    Verb.TCM_FETCH_PEER_LOG_RSP.id,
                                    Verb.TCM_CURRENT_EPOCH_REQ.id)
                   .from(1)
                   .drop()
                   .on();

            // node1 makes some metadata changes interspersed with snapshots. When the message filters are dropped, this
            // will cause nodes 2 & 3 to try and catchup. We want multiple snapshots to be taken to ensure that the
            // catchup responses contain one, rather than just a list of entries.
            cluster.get(1).nodetoolResult("cms", "snapshot").asserts().success();
            cluster.get(1).nodetoolResult("cms", "snapshot").asserts().success();
            final Epoch epoch = ClusterUtils.getCurrentEpoch(cluster.get(1));
            // start executing
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
            TimeUnit.SECONDS.sleep(1);
            // drop the filters and wait for nodes 2 & 3 to catch up
            cluster.filters().reset();
            Awaitility.waitAtMost(30, TimeUnit.SECONDS)
                      .pollInterval(1, TimeUnit.SECONDS)
                      .until(() -> ClusterUtils.getCurrentEpoch(cluster.get(2)).isEqualOrAfter(epoch) &&
                               ClusterUtils.getCurrentEpoch(cluster.get(3)).isEqualOrAfter(epoch));

            // give it another second of querying
            TimeUnit.SECONDS.sleep(1);
            stop.set(true);
            t.join();
            assertEquals(0, queryFailures.get());
        }
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
