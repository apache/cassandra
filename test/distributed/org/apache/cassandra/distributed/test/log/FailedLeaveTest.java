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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.sut.SystemUnderTest;
import harry.visitors.GeneratingVisitor;
import harry.visitors.MutatingRowVisitor;
import harry.visitors.Visitor;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.fuzz.HarryHelper;
import org.apache.cassandra.distributed.fuzz.InJVMTokenAwareVisitorExecutor;
import org.apache.cassandra.distributed.fuzz.InJvmSut;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.transformations.CancelInProgressSequence;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.streaming.IncomingStream;
import org.apache.cassandra.streaming.StreamReceiveTask;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.shared.ClusterUtils.cancelInProgressSequences;
import static org.apache.cassandra.distributed.shared.ClusterUtils.decommission;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getClusterMetadataVersion;

public class FailedLeaveTest extends FuzzTestBase
{
    private static int WRITES = 2000;

    @Test
    public void resumeDecommissionWithStreamingFailureTest() throws Throwable
    {
        // After the leave operation fails (and we've re-enabled streaming), retry it
        // and wait for a FINISH_LEAVE event to be successfully committed.
        failedLeaveTest((ex, inst) -> ex.submit(() -> decommission(inst)),
                        (e, r) -> e instanceof PrepareLeave.FinishLeave && r.isSuccess());
    }

    @Test
    public void cancelDecommissionWithStreamingFailureTest() throws Throwable
    {
        // After the leave operation fails, cancel it and wait for a CANCEL_SEQUENCE event
        // to be successfully committed.
        failedLeaveTest((ex, inst) -> ex.submit(() -> cancelInProgressSequences(inst)),
                        (e, r) -> e instanceof CancelInProgressSequence && r.isSuccess());
    }

    private void failedLeaveTest(BiFunction<ExecutorService, IInvokableInstance, Future<Boolean>> runAfterFailure,
                                 SerializableBiPredicate<Transformation, Commit.Result> actionCommitted)
    throws Exception
    {
        ExecutorService es = Executors.newSingleThreadExecutor();
        Configuration.ConfigurationBuilder configBuilder = HarryHelper.defaultConfiguration()
                                                                      .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(1, 1))
                                                                      .setClusteringDescriptorSelector(HarryHelper.defaultClusteringDescriptorSelectorConfiguration().setMaxPartitionSize(100).build());


        try (Cluster cluster = builder().withNodes(3)
                                        .withInstanceInitializer(BB::install)
                                        .start())
        {
            IInvokableInstance cmsInstance = cluster.get(1);
            IInvokableInstance leavingInstance = cluster.get(2);
            configBuilder.setSUT(() -> new InJvmSut(cluster));
            Run run = configBuilder.build().createRun();

            cluster.coordinator(1).execute("CREATE KEYSPACE " + run.schemaSpec.keyspace +
                                           " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 2};",
                                           ConsistencyLevel.ALL);
            cluster.coordinator(1).execute(run.schemaSpec.compile().cql(), ConsistencyLevel.ALL);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);

            QuiescentLocalStateChecker model = new QuiescentLocalStateChecker(run, ReplicationFactor.fullOnly(2));
            Visitor visitor = new GeneratingVisitor(run, new InJVMTokenAwareVisitorExecutor(run, MutatingRowVisitor::new, SystemUnderTest.ConsistencyLevel.ALL));
            for (int i = 0; i < WRITES; i++)
                visitor.visit();

            Epoch startEpoch = getClusterMetadataVersion(cmsInstance);
            // Configure node 3 to fail when receiving streams, then start decommissioning node 2
            cluster.get(3).runOnInstance(() -> BB.failReceivingStream.set(true));
            Future<Boolean> success = es.submit(() -> decommission(leavingInstance));
            Assert.assertFalse(success.get());

            // metadata event log should have advanced by 2 entries, PREPARE_LEAVE & START_LEAVE
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);
            Epoch currentEpoch = getClusterMetadataVersion(cmsInstance);
            Assert.assertEquals(startEpoch.getEpoch() + 2, currentEpoch.getEpoch());

            // Node 2's leaving failed due to the streaming errors. If decommission is called again on the node, it should
            // resume where it left off. Allow streaming to succeed this time and verify that the node is able to
            // finish leaving.
            cluster.get(3).runOnInstance(() -> BB.failReceivingStream.set(false));

            // Run the desired action to mitigate the failure (i.e. retry or cancel)
            success = runAfterFailure.apply(es, leavingInstance);

            // get the Epoch of the event resulting from that action, so we can wait for it
            Epoch nextEpoch = getSequenceAfterCommit(cmsInstance, actionCommitted).call();

            Assert.assertTrue(success.get());

            // wait for the cluster to all witness the event submitted after failure
            // (i.e. the FINISH_JOIN or CANCEL_SEQUENCE).
            ClusterUtils.waitForCMSToQuiesce(cluster, nextEpoch);

            //validate the state of the cluster
            for (int i = 0; i < WRITES; i++)
                visitor.visit();
            model.validateAll();
        }
    }


    public static class BB
    {
        static AtomicBoolean failReceivingStream = new AtomicBoolean(false);
        public static void install(ClassLoader cl, int instance)
        {
            if (instance == 3)
            {
                new ByteBuddy().rebase(StreamReceiveTask.class)
                               .method(named("received"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void received(IncomingStream stream, @SuperCall Callable<Void> zuper) throws Exception
        {
            if (failReceivingStream.get())
                throw new RuntimeException("XXX Stream receiving error");
            zuper.call();
        }
    }
}
