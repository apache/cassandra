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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import harry.core.Configuration;
import harry.ddl.SchemaSpec;
import harry.visitors.MutatingVisitor;
import harry.visitors.RandomPartitionValidator;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.harry.ClusterState;
import org.apache.cassandra.distributed.harry.ExistingClusterSUT;
import org.apache.cassandra.distributed.harry.FlaggedRunner;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static harry.core.Configuration.VisitorPoolConfiguration.pool;
import static harry.ddl.ColumnSpec.asciiType;
import static harry.ddl.ColumnSpec.ck;
import static harry.ddl.ColumnSpec.int64Type;
import static harry.ddl.ColumnSpec.pk;
import static harry.ddl.ColumnSpec.regularColumn;
import static harry.ddl.ColumnSpec.staticColumn;
import static java.util.Arrays.asList;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.tcm.log.SystemKeyspaceStorage.NAME;
import static org.junit.Assert.assertEquals;

public class ClusterMetadataUpgradeHarryTest extends UpgradeTestBase
{
    @Test
    public void simpleUpgradeTest() throws Throwable
    {
        ExecutorService es = executorFactory().pooled("harry", 1);
        Listener listener = new Listener();
        CountDownLatch stopLatch = CountDownLatch.newCountDownLatch(1);
        AtomicReference<Future<?>> harryRunner = new AtomicReference<>();
        new UpgradeTestBase.TestCase()
        .nodes(3)
        .nodesToUpgrade(1, 2, 3)
        .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                .set(Constants.KEY_DTEST_FULL_STARTUP, true))
        .upgradesFrom(v41, v50)
        .withUpgradeListener(listener)
        .setup((cluster) -> {
            SchemaSpec schema = new SchemaSpec("harry", "test_table",
                                               asList(pk("pk1", asciiType), pk("pk1", int64Type)),
                                               asList(ck("ck1", asciiType), ck("ck1", int64Type)),
                                               asList(regularColumn("regular1", asciiType), regularColumn("regular1", int64Type)),
                                               asList(staticColumn("static1", asciiType), staticColumn("static1", int64Type)));

            Configuration config = Configuration.fromFile("conf/example.yaml")
                                                .unbuild()
                                                .setKeyspaceDdl(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d};", schema.keyspace, 3))
                                                .setSUT(new ExistingClusterSUT(cluster, listener))
                                                .build();

            Future<?> f = es.submit(() -> {
                try
                {
                    new FlaggedRunner(config.createRun(),
                                      config,
                                      asList(pool("Writer", 1, MutatingVisitor::new),
                                             pool("Reader", 1, RandomPartitionValidator::new)),
                                      stopLatch).run();
                }
                catch (Throwable e)
                {
                    throw new RuntimeException(e);
                }
              });
            harryRunner.set(f);
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS); // make sure harry executes in mixed mode
        })
        .runAfterClusterUpgrade((cluster) -> {

            // make sure we haven't persisted any events;
            cluster.stream().forEach((i) -> {
                Object[][] res = i.executeInternal(String.format("select * from %s.%s", SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME));
                assertEquals(0, res.length);
            });

            cluster.get(1).nodetoolResult("addtocms").asserts().success();
            cluster.get(2).nodetoolResult("addtocms").asserts().success();
            cluster.get(1).nodetoolResult("addtocms").asserts().failure();
            cluster.schemaChange(withKeyspace("create table %s.xyz (id int primary key)"));
            stopLatch.decrement();
            harryRunner.get().get();
        }).run();
    }


    private static class Listener implements UpgradeListener, ClusterState
    {
        // only ever one node down here.
        public final AtomicInteger downNode = new AtomicInteger(0);
        @Override
        public void shutdown(int i)
        {
            downNode.set(i);
        }

        @Override
        public void startup(int i)
        {
            downNode.set(0);
        }

        @Override
        public boolean isDown(int i)
        {
            return downNode.get() == i;
        }
    }
}
