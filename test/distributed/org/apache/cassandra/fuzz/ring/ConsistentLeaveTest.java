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
import org.apache.cassandra.harry.HarryHelper;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.harry.sut.injvm.InJvmSutBase;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.PrepareLeave;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getClusterMetadataVersion;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getSequenceAfterCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeCommit;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseCommits;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.junit.Assert.assertFalse;

public class ConsistentLeaveTest extends FuzzTestBase
{
    private static int WRITES = 500;

    @Test
    public void decommissionTest() throws Throwable
    {
        IInvokableInstance cmsInstance = null;
        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .appendConfig(c -> c.with(Feature.NETWORK))
                                        .start())
        {
            cmsInstance = cluster.get(1);
            IInvokableInstance leavingInstance = cluster.get(2);
            waitForCMSToQuiesce(cluster, cmsInstance);


            ReplayingHistoryBuilder harry = HarryHelper.dataGen(new InJvmSut(cluster, () -> 1, InJvmSutBase.retryOnTimeout()),
                                                                new TokenPlacementModel.SimpleReplicationFactor(2),
                                                                SystemUnderTest.ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};", HarryHelper.KEYSPACE),
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(harry.schema().compile().cql(), ConsistencyLevel.ALL);
            waitForCMSToQuiesce(cluster, cmsInstance);

            Runnable writeAndValidate = () -> {
                System.out.println("Starting write phase...");
                for (int i = 0; i < WRITES; i++)
                    harry.insert();

                System.out.println("Starting validate phase...");
                harry.validateAll(harry.quiescentLocalChecker());
            };
            writeAndValidate.run();

            // Prime the CMS node to pause before the finish leave event is committed
            Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareLeave.FinishLeave);
            new Thread(() -> leavingInstance.runOnInstance(() -> StorageService.instance.decommission(true))).start();
            pending.call();

            waitForCMSToQuiesce(cluster, cmsInstance);
            assertGossipStatus(cluster, leavingInstance.config().num(), "LEAVING");

            writeAndValidate.run();

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

            writeAndValidate.run();
        }
        catch (Throwable t)
        {
            if (cmsInstance != null)
                unpauseCommits(cmsInstance);
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
