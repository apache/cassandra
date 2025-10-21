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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.execution.RingAwareInJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.PrepareLeave;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getClusterMetadataVersion;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getSequenceAfterCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.junit.Assert.assertFalse;

public class ConsistentLeaveTest extends FuzzTestBase
{
    private static int WRITES = 500;

    @Test
    public void decommissionTest() throws Throwable
    {
        Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "decomission", 1000);
        IInvokableInstance forShutdown = null;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .appendConfig(c -> c.with(Feature.NETWORK))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            forShutdown = cmsInstance;

            IInvokableInstance leavingInstance = cluster.get(2);
            waitForCMSToQuiesce(cluster, cmsInstance);

            withRandom(rng -> {
                SchemaSpec schema = schemaGen.generate(rng);
                Generators.TrackingGenerator<Integer> pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), 1000)));
                Generator<Integer> ckGen = Generators.int32(0, Math.min(schema.valueGenerators.ckPopulation(), 1000));

                HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                     (hb) -> RingAwareInJvmDTestVisitExecutor.builder()
                                                                                                             .replicationFactor(new TokenPlacementModel.SimpleReplicationFactor(2))
                                                                                                             .consistencyLevel(ConsistencyLevel.ALL)
                                                                                                             .retryPolicy(InJvmDTestVisitExecutor.RetryPolicy.RETRY_ON_TIMEOUT)
                                                                                                             .nodeSelector(lts -> 1)
                                                                                                             .build(schema, hb, cluster));
                Runnable writeAndValidate = () -> {
                    for (int i = 0; i < WRITES; i++)
                        history.insert(pkGen.generate(rng), ckGen.generate(rng));

                    for (int pk : pkGen.generated())
                        history.selectPartition(pk);
                };

                cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};", KEYSPACE));
                cluster.schemaChange(schema.compile());
                waitForCMSToQuiesce(cluster, cmsInstance);

                writeAndValidate.run();

                history.customThrowing(() -> {
                    // Prime the CMS node to pause before the finish leave event is committed
                    Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareLeave.FinishLeave);
                    new Thread(() -> leavingInstance.runOnInstance(() -> StorageService.instance.decommission(true))).start();
                    pending.call();

                    waitForCMSToQuiesce(cluster, cmsInstance);
                    assertGossipStatus(cluster, leavingInstance.config().num(), "LEAVING");
                }, "Start Leave");

                writeAndValidate.run();

                history.customThrowing(() -> {
                    // Make sure there can be only one FinishLeave in flight
                    waitForCMSToQuiesce(cluster, cmsInstance);
                    // set expectation of finish leave & retrieve the sequence when it gets committed
                    Epoch currentEpoch = getClusterMetadataVersion(cmsInstance);
                    Callable<Epoch> finishedLeaving = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareLeave.FinishLeave && r.isSuccess());
                    unpauseCommits(cmsInstance);
                    Epoch nextEpoch = finishedLeaving.call();
                    Assert.assertEquals(String.format("Epoch %s should have immediately superseded epoch %s.", nextEpoch, currentEpoch),
                                        nextEpoch.getEpoch(), currentEpoch.getEpoch() + 1);

                    // wait for the cluster to all witness the finish join event
                    waitForCMSToQuiesce(cluster, nextEpoch);

                    assertGossipStatus(cluster, leavingInstance.config().num(), "LEFT");
                }, "Finish leave");

                writeAndValidate.run();
            });
        }
        catch (Throwable t)
        {
            if (forShutdown != null)
                unpauseCommits(forShutdown);
            throw t;
        }
    }

    private void assertGossipStatus(Cluster cluster, int leavingInstance, String status)
    {
        int size = cluster.size();
        List<InetAddressAndPort> endpoints = cluster.stream().map(i -> InetAddressAndPort.getByAddress(i.config().broadcastAddress())).collect(Collectors.toList());
        cluster.forEach(inst -> inst.runOnInstance(() -> {
            while (true)
            {
                for (int i = 1; i <= size; i++)
                {
                    String gossipStatus = Gossiper.instance.getApplicationState(endpoints.get(i - 1), ApplicationState.STATUS_WITH_PORT);
                    if (i != leavingInstance)
                    {
                        assertFalse(endpoints.get(i - 1) + ": " + gossipStatus,
                                    gossipStatus.contains("LEFT"));
                        assertFalse(endpoints.get(i - 1) + ": " + gossipStatus,
                                    gossipStatus.contains("LEAVING"));
                    }
                    else
                    {

                        if (gossipStatus.contains(status))
                            return;
                        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }));
    }
}
