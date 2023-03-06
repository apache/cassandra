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

package org.apache.cassandra.distributed.test.ring;

import java.util.concurrent.Callable;

import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.operations.Query;
import harry.visitors.LoggingVisitor;
import harry.visitors.MutatingRowVisitor;
import harry.visitors.Visitor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.fuzz.HarryHelper;
import org.apache.cassandra.distributed.fuzz.InJvmSut;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.distributed.test.log.TestProcessor;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.PrepareJoin;

public class ConsistentBootstrapTest extends FuzzTestBase
{
    private static int WRITES = 2000;

    @Test
    public void bootstrapFuzzTest() throws Throwable
    {
        Configuration.ConfigurationBuilder configBuilder = HarryHelper.defaultConfiguration()
                                                                      .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(1, 1))
                                                                      .setClusteringDescriptorSelector(HarryHelper.defaultClusteringDescriptorSelectorConfiguration().setMaxPartitionSize(100).build());

        try (Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP).set("metadata_snapshot_frequency", 5))
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);
            configBuilder.setSUT(() -> new InJvmSut(cluster));
            Run run = configBuilder.build().createRun();

            cluster.coordinator(1).execute("CREATE KEYSPACE " + run.schemaSpec.keyspace +
                                           " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};",
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(run.schemaSpec.compile().cql(), ConsistencyLevel.ALL);
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
            Visitor visitor = new LoggingVisitor(run, MutatingRowVisitor::new);
            QuiescentLocalStateChecker model = new QuiescentLocalStateChecker(run);
            System.out.println("Starting write phase...");
            for (int i = 0; i < WRITES; i++)
                visitor.visit();
            System.out.println("Starting validate phase...");
            for (int lts = 0; lts < run.clock.peek(); lts++)
                model.validate(Query.selectPartition(run.schemaSpec, run.pdSelector.pd(lts, run.schemaSpec), false));

            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance newInstance = cluster.bootstrap(config);

            // Prime the DPS node to pause before the finish join event is committed
            Callable<?> pending = pauseBeforeCommit(cmsInstance, (e) -> e instanceof PrepareJoin.FinishJoin);
            new Thread(() -> newInstance.startup()).start();
            pending.call();

            for (int i = 0; i < WRITES; i++)
                visitor.visit();

            try
            {
                for (int lts = 0; lts < run.clock.peek(); lts++)
                    model.validate(Query.selectPartition(run.schemaSpec, run.pdSelector.pd(lts, run.schemaSpec), false));
            }
            catch (Throwable t)
            {
                // Unpause, since otherwise validation exception will prevent graceful shutdown
                cmsInstance.runOnInstance(() -> {
                    TestProcessor processor = (TestProcessor) ((ClusterMetadataService.SwitchableProcessor) ClusterMetadataService.instance().processor()).delegate();
                    processor.unpause();
                });
                throw t;
            }

            // Make sure there can be only one FinishJoin in flight
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);
            // set expectation of finish join & retrieve the sequence when it gets committed
            Callable<Epoch> bootstrapVisible = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareJoin.FinishJoin && r.isSuccess());

            cmsInstance.runOnInstance(() -> {
                TestProcessor processor = (TestProcessor) ((ClusterMetadataService.SwitchableProcessor) ClusterMetadataService.instance().processor()).delegate();
                processor.unpause();
            });

            // wait for the cluster to all witness the finish join event
            ClusterUtils.waitForCMSToQuiesce(cluster, bootstrapVisible.call());

            for (int i = 0; i < WRITES; i++)
                visitor.visit();
            model.validateAll();
        }
    }
}
