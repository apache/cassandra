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

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.harry.HarryHelper;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.PrepareJoin;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getSequenceAfterCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;

public class ConsistentBootstrapTest extends FuzzTestBase
{
    private static int WRITES = 500;

    @Test
    public void bootstrapFuzzTest() throws Throwable
    {
        IInvokableInstance cmsInstance = null;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                      .set("write_request_timeout", "10s")
                                                                      .set("metadata_snapshot_frequency", 5))
                                        .start())
        {
            cmsInstance = cluster.get(1);
            waitForCMSToQuiesce(cluster, cmsInstance);

            ReplayingHistoryBuilder harry = HarryHelper.dataGen(new InJvmSut(cluster),
                                                                new TokenPlacementModel.SimpleReplicationFactor(3),
                                                                SystemUnderTest.ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};", HarryHelper.KEYSPACE),
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(harry.schema().compile().cql(), ConsistencyLevel.ALL);
            waitForCMSToQuiesce(cluster, cluster.get(1));
            Runnable writeAndValidate = () -> {
                System.out.println("Starting write phase...");
                for (int i = 0; i < WRITES; i++)
                    harry.insert();

                System.out.println("Starting validate phase...");
                harry.validateAll(harry.quiescentLocalChecker());
            };
            writeAndValidate.run();

            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance newInstance = cluster.bootstrap(config);

            // Prime the CMS node to pause before the finish join event is committed
            Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareJoin.FinishJoin);
            new Thread(() -> newInstance.startup()).start();
            pending.call();

            writeAndValidate.run();

            // Make sure there can be only one FinishJoin in flight
            waitForCMSToQuiesce(cluster, cmsInstance);
            // set expectation of finish join & retrieve the sequence when it gets committed
            Callable<Epoch> bootstrapVisible = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareJoin.FinishJoin && r.isSuccess());

            // wait for the cluster to all witness the finish join event
            unpauseCommits(cmsInstance);
            waitForCMSToQuiesce(cluster, bootstrapVisible.call());

            writeAndValidate.run();
        }
        catch (Throwable t)
        {
            if (cmsInstance != null)
                unpauseCommits(cmsInstance);
            throw t;
        }
    }

    @Test
    public void coordinatorIsBehindTest() throws Throwable
    {
        IInvokableInstance cmsInstance = null;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP)
                                                                      .set("write_request_timeout", "10s")
                                                                      .set("metadata_snapshot_frequency", 5))
                                        .start())
        {
            cmsInstance = cluster.get(1);
            waitForCMSToQuiesce(cluster, cmsInstance);

            ReplayingHistoryBuilder harry = HarryHelper.dataGen(new InJvmSut(cluster, () -> 2, (t) -> false)
                                                                {
                                                                    public Object[][] execute(String statement, ConsistencyLevel cl, int coordinator, int pagesize, Object... bindings)
                                                                    {
                                                                        try
                                                                        {
                                                                            return super.execute(statement, cl, coordinator, pagesize, bindings);
                                                                        }
                                                                        catch (Throwable t)
                                                                        {
                                                                            // Avoid retries
                                                                            return new Object[][]{};
                                                                        }
                                                                    }
                                                                },
                                                                new TokenPlacementModel.SimpleReplicationFactor(3),
                                                                SystemUnderTest.ConsistencyLevel.ALL);

            cluster.coordinator(1).execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};", HarryHelper.KEYSPACE),
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(harry.schema().compile().cql(), ConsistencyLevel.ALL);
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
            Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareJoin.MidJoin);
            long[] metricCounts = new long[4];
            for (int i = 1; i <= 4; i++)
                metricCounts[i - 1] = cluster.get(i).callOnInstance(() -> TCMMetrics.instance.coordinatorBehindPlacements.getCount());
            Thread thread = new Thread(() -> newInstance.startup());
            thread.start();
            pending.call();

            boolean triggered = false;
            long[] markers = new long[4];
            outer:
            for (int i = 0; i < 20; i++)
            {
                for (int n = 0; n < 4; n++)
                    markers[n] = cluster.get(n + 1).logs().mark();

                try
                {
                    harry.insert();
                }
                catch (Throwable t)
                {
                    // ignore
                }
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
                        break outer;
                    }
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
            thread.join();
        }
        catch (Throwable t)
        {
            if (cmsInstance != null)
                unpauseCommits(cmsInstance);
            throw t;
        }
    }
}