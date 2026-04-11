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

import java.util.HashSet;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
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
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.Generators.TrackingGenerator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class AccordBootstrapTest extends FuzzTestBase
{
    private static final int WRITES = 10;
    private static final int POPULATION = 1000;

    @Test
    public void bootstrapFuzzTest() throws Throwable
    {
        CassandraRelevantProperties.SYSTEM_TRACES_DEFAULT_RF.setInt(3);
        IInvokableInstance forShutdown = null;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(100))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(100, "dc0", "rack0"))
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                      .set("write_request_timeout", "2s")
                                                                      .set("request_timeout", "5s")
                                                                      .set("concurrent_accord_operations", 2)
                                                                      .set("progress_barrier_min_consistency_level", "QUORUM")
                                                                      .set("progress_barrier_default_consistency_level", "QUORUM")
                                                                      .set("metadata_snapshot_frequency", 5))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            forShutdown = cmsInstance;
            waitForCMSToQuiesce(cluster, cmsInstance);

            HashSet<Integer> downInstances = new HashSet<>();
            withRandom(rng -> {
                Generator<SchemaSpec> schemaGen = SchemaGenerators.trivialSchema(KEYSPACE, () -> "bootstrap_fuzz", POPULATION,
                                                                                 SchemaSpec.optionsBuilder()
                                                                                           .addWriteTimestamps(false)
                                                                                           .withTransactionalMode(TransactionalMode.full)
                );

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
                                                                                                          if (!downInstances.contains(pick))
                                                                                                              return pick;

                                                                                                      }
                                                                                                  })
                                                                                                  .build(schema, hb, cluster));

                Runnable writeAndValidate = () -> {
                    for (int i = 0; i < WRITES; i++)
                        HistoryBuilderHelper.insertRandomData(schema, pkGen, ckGen, rng, history);

                    for (int pk : pkGen.generated())
                        history.selectPartition(pk);
                };

                history.customThrowing(() -> {
                    cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};", KEYSPACE));
                    cluster.schemaChange(schema.compile());
                    waitForCMSToQuiesce(cluster, cmsInstance);
                }, "Setup");
                Thread.sleep(1000);
                writeAndValidate.run();

                history.customThrowing(() -> {
                    IInstanceConfig config = cluster.newInstanceConfig()
                                                    .set("auto_bootstrap", true)
                                                    .set(Constants.KEY_DTEST_FULL_STARTUP, true);
                    cluster.bootstrap(config).startup();
                    waitForCMSToQuiesce(cluster, cmsInstance);
                }, "Start boostrap");

                writeAndValidate.run();

                history.customThrowing(() -> {
                    downInstances.add(2);
                    ClusterUtils.stopUnchecked(cluster.get(2));
                    cluster.get(1).logs().watchFor("/127.0.0.2:.* is now DOWN");
                }, "Shut down node 2");

                history.customThrowing(() -> {
                    IInstanceConfig config = cluster.newInstanceConfig()
                                                    .set("auto_bootstrap", true)
                                                    .set(Constants.KEY_DTEST_FULL_STARTUP, true);
                    cluster.bootstrap(config).startup();
                    waitForCMSToQuiesce(cluster, cmsInstance);
                }, "Bootstrap one more");

                writeAndValidate.run();

                history.customThrowing(() -> {
                    cluster.get(2).startup();
                    cluster.get(1).logs().watchFor("/127.0.0.2:.* is now UP");
                    downInstances.remove(2);
                }, "Start up node 2");

            });
        }
        catch (Throwable t)
        {
            if (forShutdown != null)
                unpauseCommits(forShutdown);
            throw t;
        }
    }
}
