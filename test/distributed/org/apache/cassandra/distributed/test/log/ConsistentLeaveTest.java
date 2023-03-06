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

package org.apache.cassandra.distributed.test.log;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.sut.SystemUnderTest;
import harry.visitors.*;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.fuzz.HarryHelper;
import org.apache.cassandra.distributed.fuzz.InJVMTokenAwareVisitorExecutor;
import org.apache.cassandra.distributed.fuzz.InJvmSut;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.PrepareLeave;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getClusterMetadataVersion;
import static org.junit.Assert.assertFalse;

public class ConsistentLeaveTest extends FuzzTestBase
{
    private static int WRITES = 2000;

    @Test
    public void decommissionTest() throws Throwable
    {
        Configuration.ConfigurationBuilder configBuilder = HarryHelper.defaultConfiguration()
                                                                      .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(1, 1))
                                                                      .setClusteringDescriptorSelector(HarryHelper.defaultClusteringDescriptorSelectorConfiguration().setMaxPartitionSize(100).build());

        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(3, "dc0", "rack0"))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            IInvokableInstance leavingInstance = cluster.get(2);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);

            configBuilder.setSUT(() -> new InJvmSut(cluster));
            Run run = configBuilder.build().createRun();

            cluster.coordinator(1).execute("CREATE KEYSPACE " + run.schemaSpec.keyspace +
                                           " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};",
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(run.schemaSpec.compile().cql(), ConsistencyLevel.ALL);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);

            QuiescentLocalStateChecker model = new QuiescentLocalStateChecker(run, ReplicationFactor.fullOnly(2));
            Visitor visitor = new LoggingVisitor(run, MutatingRowVisitor::new);
            for (int i = 0; i < WRITES; i++)
                visitor.visit();

            model.validateAll();
            // Prime the CMS node to pause before the finish leave event is committed
            Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareLeave.FinishLeave);
            new Thread(() -> leavingInstance.runOnInstance(() -> StorageService.instance.decommission(true))).start();
            pending.call();

            assertGossipStatus(cluster, leavingInstance.config().num(), "LEAVING");
            // Streaming for unbootstrap has finished, any rows from the first batch should have been transferred
            // from the leaving node to the new replicas. Continue to write at ONE, replication of these rows will
            // happen via the pending range mechanism
            visitor = new GeneratingVisitor(run, new InJVMTokenAwareVisitorExecutor(run, MutatingRowVisitor::new, SystemUnderTest.ConsistencyLevel.ONE));
            for (int i = 0; i < WRITES; i++)
                visitor.visit();

            model.validateAll();

            // Make sure there can be only one FinishLeave in flight
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);
            // set expectation of finish leave & retrieve the sequence when it gets committed
            Epoch currentEpoch = getClusterMetadataVersion(cmsInstance);
            Callable<Epoch> finishedLeaving = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareLeave.FinishLeave && r.isSuccess());
            cmsInstance.runOnInstance(() -> {
                TestProcessor processor = (TestProcessor) ((ClusterMetadataService.SwitchableProcessor) ClusterMetadataService.instance().processor()).delegate();
                processor.unpause();
            });
            Epoch nextEpoch = finishedLeaving.call();
            Assert.assertEquals(String.format("Epoch %s should have immediately superseded epoch %s.", nextEpoch, currentEpoch),
                                nextEpoch.getEpoch(), currentEpoch.getEpoch() + 1);

            // wait for the cluster to all witness the finish join event
            ClusterUtils.waitForCMSToQuiesce(cluster, nextEpoch);

            assertGossipStatus(cluster, leavingInstance.config().num(), "LEFT");

            for (int i = 0; i < WRITES; i++)
                visitor.visit();
            model.validateAll();
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
