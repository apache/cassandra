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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.schema.SchemaConstants;

import static java.util.Arrays.asList;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.UNSAFE;
import static org.apache.cassandra.harry.ColumnSpec.asciiType;
import static org.apache.cassandra.harry.ColumnSpec.ck;
import static org.apache.cassandra.harry.ColumnSpec.int64Type;
import static org.apache.cassandra.harry.ColumnSpec.pk;
import static org.apache.cassandra.harry.ColumnSpec.regularColumn;
import static org.apache.cassandra.harry.ColumnSpec.staticColumn;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.apache.cassandra.tcm.log.SystemKeyspaceStorage.NAME;
import static org.junit.Assert.assertEquals;

public class ClusterMetadataUpgradeHarryTest extends UpgradeTestBase
{
    @Test
    public void simpleUpgradeTest() throws Throwable
    {
        AtomicReference<Interruptible> executor = new AtomicReference<>();
        Listener listener = new Listener();
        withRandom(rng -> {
            new TestCase()
            .nodes(3)
            .nodesToUpgrade(1, 2, 3)
            .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                    .set(Constants.KEY_DTEST_FULL_STARTUP, true))
            .upgradesToCurrentFrom(v41)
            .withUpgradeListener(listener)
            .setup((cluster) -> {
                SchemaSpec schema = new SchemaSpec(rng.next(),
                                                   10_000,
                                                   "harry", "test_table",
                                                   asList(pk("pk1", asciiType), pk("pk2", int64Type)),
                                                   asList(ck("ck1", asciiType, false), ck("ck2", int64Type, false)),
                                                   asList(regularColumn("regular1", asciiType), regularColumn("regular2", int64Type)),
                                                   asList(staticColumn("static1", asciiType), staticColumn("static2", int64Type)));

                HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                     hb -> InJvmDTestVisitExecutor.builder()
                                                                                                  .retryPolicy(retry -> true)
                                                                                                  .nodeSelector(lts -> {
                                                                                                      while (true)
                                                                                                      {
                                                                                                          int node = rng.nextInt(0, cluster.size()) + 1;
                                                                                                          if (cluster.get(node).isShutdown())
                                                                                                              continue;
                                                                                                          return node;
                                                                                                      }
                                                                                                  })
                                                                                                  .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                                                  .build(schema, hb, cluster));

                Generator<Integer> pkIdxGen = Generators.int32(0, Math.min(10_000, schema.valueGenerators.ckPopulation()));

                executor.set(executorFactory().infiniteLoop("R/W Worload",
                                                            () -> {
                                                                history.insert(pkIdxGen.generate(rng));
                                                                history.selectPartition(pkIdxGen.generate(rng));
                                                            }, UNSAFE));

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

                cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
                cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
                cluster.schemaChange(withKeyspace("create table %s.xyz (id int primary key)"));
                executor.get().shutdownNow();
                executor.get().awaitTermination(1, TimeUnit.MINUTES);
            }).run();
        });
    }


    private static class Listener implements UpgradeListener
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
    }
}
