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

package org.apache.cassandra.fuzz.topology;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.Generators.TrackingGenerator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.util.ThrowingRunnable;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class AccordRebootstrapTest extends FuzzTestBase
{
    private static final int WRITES = 10;
    private static final int POPULATION = 1000;

    @Test
    public void rebootstrapFuzzTest() throws Throwable
    {
        CassandraRelevantProperties.SYSTEM_TRACES_DEFAULT_RF.setInt(3);
        Cluster.Builder builder = builder();
        try (Cluster cluster = builder.withNodes(3)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(100))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(100, "dc0", "rack0"))
                                      .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP).set("accord.catchup_on_start_fail_latency", "60s"))
                                      .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            waitForCMSToQuiesce(cluster, cmsInstance);

            HashSet<Integer> downInstances = new HashSet<>();
            AtomicInteger nextId = new AtomicInteger();
            withRandom(rng -> {
                Generator<SchemaSpec> schemaGen = SchemaGenerators.trivialSchema(KEYSPACE, () -> "bsfuzz" + (nextId.incrementAndGet()), POPULATION,
                                                                                 SchemaSpec.optionsBuilder()
                                                                                           .addWriteTimestamps(false)
                                                                                           .withTransactionalMode(TransactionalMode.full)
                );

                History history1 = createNewSchemaWithWriteAndValidate(schemaGen, rng, cluster, downInstances::contains);
                history1.writeAndValidate();

                history1.run(() -> {
                    downInstances.add(2);
                    ClusterUtils.stopUnchecked(cluster.get(2));
                    cluster.get(1).logs().watchFor("/127.0.0.2:.* is now DOWN");
                }, "Shut down node 2");

                history1.writeAndValidate();
                History history2 = createNewSchemaWithWriteAndValidate(schemaGen, rng, cluster, downInstances::contains);
                history2.writeAndValidate();
                History history3 = createNewSchemaWithWriteAndValidate(schemaGen, rng, cluster, downInstances::contains);
                history3.writeAndValidate();

                history1.run(() -> {
                    cluster.get(2).config().set("accord.journal.stop_marker_failure_policy", "REBOOTSTRAP");
                    Path journalDir = Path.of(cluster.get(2).config().get("accord.journal_directory").toString());
                    Path stopMarker = journalDir.resolve("stopped");
                    PathUtils.delete(stopMarker);
                    cluster.get(2).startup();
                    cluster.get(2).logs().watchFor(".*Rebootstrapping.*");
                    cluster.get(1).logs().watchFor("/127.0.0.2:.* is now UP");
                    downInstances.remove(2);
                }, "Start down node 2");

                history1.writeAndValidate();
                history2.writeAndValidate();
                history3.writeAndValidate();
            });
        }
    }

    interface History
    {
        void writeAndValidate();
        void run(ThrowingRunnable run, String tag);
    }

    private History createNewSchemaWithWriteAndValidate(Generator<SchemaSpec> schemaGen, EntropySource rng, Cluster cluster, IntPredicate downInstances) throws InterruptedException
    {
        IInvokableInstance cmsInstance = cluster.get(1);
        SchemaSpec schema = schemaGen.generate(rng);
        TrackingGenerator<Integer> pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), POPULATION)));
        Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), POPULATION));
        HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                             hb -> InJvmDTestVisitExecutor.builder()
                                                                                          .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                                          .wrapQueries(QueryBuildingVisitExecutor.WrapQueries.TRANSACTION)
                                                                                          .pageSizeSelector(p -> InJvmDTestVisitExecutor.PageSizeSelector.NO_PAGING)
                                                                                          .nodeSelector(lts -> {
                                                                                              while (true)
                                                                                              {
                                                                                                  int pick = rng.nextInt(1, cluster.size() + 1);
                                                                                                  if (!downInstances.test(pick))
                                                                                                      return pick;
                                                                                              }
                                                                                          })
                                                                                          .build(schema, hb, cluster));

        history.customThrowing(() -> {
            cluster.schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};", KEYSPACE), true);
            cluster.schemaChange(schema.compile(), true);
            waitForCMSToQuiesce(cluster, cmsInstance);
        }, "Setup");
        Thread.sleep(1000);

        return new History()
        {
            @Override
            public void writeAndValidate()
            {
                for (int i = 0; i < WRITES; i++)
                    HistoryBuilderHelper.insertRandomData(schema, pkGen, ckGen, rng, history);

                for (int pk : pkGen.generated())
                    history.selectPartition(pk);
            }

            @Override
            public void run(ThrowingRunnable run, String tag)
            {
                history.customThrowing(run, tag);
            }
        };
    }
}
