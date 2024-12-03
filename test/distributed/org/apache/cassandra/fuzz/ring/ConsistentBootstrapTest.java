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

package org.apache.cassandra.fuzz.ring;

import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.execution.RingAwareInJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.PrepareJoin;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getSequenceAfterCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class ConsistentBootstrapTest extends FuzzTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(ConsistentBootstrapTest.class);
    private static int WRITES = 500;

    @Test
    public void bootstrapFuzzTest() throws Throwable
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "bootstrap_fuzz", 1000);
        IInvokableInstance forShutdown = null;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                      .set("write_request_timeout", "10s")
                                                                      .set("metadata_snapshot_frequency", 5))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            forShutdown = cmsInstance;
            waitForCMSToQuiesce(cluster, cmsInstance);

            withRandom(rng -> {
                SchemaSpec schema = schemaGen.generate(rng);
                Generators.TrackingGenerator<Integer> pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), 1000)));
                Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), 1000));

                HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
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

                writeAndValidate.run();

                history.customThrowing(() -> {
                    IInstanceConfig config = cluster.newInstanceConfig()
                                                    .set("auto_bootstrap", true)
                                                    .set(Constants.KEY_DTEST_FULL_STARTUP, true);
                    IInvokableInstance newInstance = cluster.bootstrap(config);

                    // Prime the CMS node to pause before the finish join event is committed
                    Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareJoin.FinishJoin);
                    new Thread(() -> newInstance.startup()).start();
                    pending.call();
                }, "Start boostrap");

                writeAndValidate.run();

                history.customThrowing(() -> {
                    // Make sure there can be only one FinishJoin in flight
                    waitForCMSToQuiesce(cluster, cmsInstance);
                    // set expectation of finish join & retrieve the sequence when it gets committed
                    Callable<Epoch> bootstrapVisible = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareJoin.FinishJoin && r.isSuccess());

                    // wait for the cluster to all witness the finish join event
                    unpauseCommits(cmsInstance);
                    waitForCMSToQuiesce(cluster, bootstrapVisible.call());
                }, "Finish bootstrap");
                writeAndValidate.run();

                RingAwareInJvmDTestVisitExecutor.replay(RingAwareInJvmDTestVisitExecutor.builder()
                                                                                        .replicationFactor(new TokenPlacementModel.SimpleReplicationFactor(2))
                                                                                        .consistencyLevel(ConsistencyLevel.ALL)
                                                                                        .build(schema, history, cluster),
                                                        history);
            });
        }
        catch (Throwable t)
        {
            if (forShutdown != null)
                unpauseCommits(forShutdown);
            throw t;
        }
    }

    @Test
    public void coordinatorIsBehindTest() throws Throwable
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "coordinator_is_behind", 1000);

        IInvokableInstance forShutdown = null;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                      .set("write_request_timeout", "10s")
                                                                      .set("metadata_snapshot_frequency", 5))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            forShutdown = cmsInstance;
            waitForCMSToQuiesce(cluster, cmsInstance);

            withRandom(rng -> {
                SchemaSpec schema = schemaGen.generate(rng);
                Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), 1000));

                HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

                cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};", KEYSPACE));
                cluster.schemaChange(schema.compile());
                waitForCMSToQuiesce(cluster, cluster.get(1));

                cluster.filters().verbs(Verb.TCM_REPLICATION.id,
                                        Verb.TCM_FETCH_CMS_LOG_RSP.id,
                                        Verb.TCM_FETCH_PEER_LOG_RSP.id,
                                        Verb.TCM_CURRENT_EPOCH_REQ.id)
                       .to(2)
                       .drop()
                       .on();

                IInstanceConfig config = cluster.newInstanceConfig()
                                                .set("auto_bootstrap", true)
                                                .set(Constants.KEY_DTEST_FULL_STARTUP, true)
                                                .set("progress_barrier_default_consistency_level", "NODE_LOCAL");
                IInvokableInstance newInstance = cluster.bootstrap(config);
                // Prime the CMS node to pause before the finish join event is committed
                long[] metricCounts = new long[4];
                for (int i = 1; i <= 4; i++)
                    metricCounts[i - 1] = cluster.get(i).callOnInstance(() -> TCMMetrics.instance.coordinatorBehindPlacements.getCount());

                DataTracker tracker = new DataTracker.SequentialDataTracker();
                RingAwareInJvmDTestVisitExecutor executor = RingAwareInJvmDTestVisitExecutor.builder()
                                                                                            .replicationFactor(new TokenPlacementModel.SimpleReplicationFactor(2))
                                                                                            .nodeSelector(i -> 2)
                                                                                            .consistencyLevel(ConsistencyLevel.ALL)
                                                                                            .build(schema,
                                                                                                   tracker,
                                                                                                   new QuiescentChecker(schema.valueGenerators, tracker, history),
                                                                                                   cluster);

                Thread startup = new Thread(() -> newInstance.startup());

                history.customThrowing(() -> {
                    Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareJoin.MidJoin);
                    startup.start();
                    pending.call();
                }, "Startup");

                long[] markers = new long[4];
                history.custom(() -> {
                    for (int n = 0; n < 4; n++)
                        markers[n] = cluster.get(n + 1).logs().mark();
                }, "Start grep");

                outer:
                for (int i = 0; i < history.valueGenerators().pkPopulation(); i++)
                {
                    long pd = history.valueGenerators().pkGen().descriptorAt(i);
                    for (TokenPlacementModel.Replica replica : executor.getReplicasFor(pd))
                    {
                        if (cluster.get(1).config().broadcastAddress().toString().contains(replica.node().id()))
                        {
                            HistoryBuilderHelper.insertRandomData(schema, i, ckGen.generate(rng), rng, history);
                            break outer;
                        }
                    }
                }


                history.customThrowing(() -> {
                    boolean triggered = false;
                    for (int n = 0; n < markers.length; n++)
                    {
                        if ((n + 1) == 2) // skip 2nd node
                            continue;

                        if (!cluster.get(n + 1)
                                    .logs()
                                    .grep(markers[n], "Routing is correct, but coordinator needs to catch-up")
                                    .getResult()
                                    .isEmpty())
                        {
                            triggered = true;
                            break;
                        }
                    }

                    Assert.assertTrue("Should have triggered routing exception on the replica", triggered);
                    boolean metricTriggered = false;
                    for (int i = 1; i <= 4; i++)
                    {
                        long prevMetric = metricCounts[i - 1];
                        long newMetric = cluster.get(i).callOnInstance(() -> TCMMetrics.instance.coordinatorBehindPlacements.getCount());
                        if (newMetric - prevMetric > 0)
                        {
                            metricTriggered = true;
                            break;
                        }
                    }
                    Assert.assertTrue("Metric CoordinatorBehindRing should have been bumped by at least one replica", metricTriggered);

                    cluster.filters().reset();
                    unpauseCommits(cmsInstance);
                    startup.join();
                }, "Validate triggered");

                for (Visit visit : history)
                    executor.execute(visit);
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