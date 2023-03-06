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
import java.util.function.BiFunction;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.InProgressSequence;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.InProgressSequences.SequenceState;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT;
import static org.apache.cassandra.config.CassandraRelevantProperties.TCM_PROGRESS_BARRIER_BACKOFF_MILLIS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TCM_PROGRESS_BARRIER_TIMEOUT_MILLIS;
import static org.apache.cassandra.distributed.Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN;
import static org.apache.cassandra.distributed.Constants.KEY_DTEST_FULL_STARTUP;
import static org.apache.cassandra.distributed.shared.ClusterUtils.addInstance;
import static org.apache.cassandra.net.Verb.TCM_REPLICATION;
import static org.apache.cassandra.tcm.sequences.InProgressSequences.SequenceState.BLOCKED;
import static org.apache.cassandra.tcm.sequences.InProgressSequences.SequenceState.CONTINUING;

public class InProgressSequenceCoordinationTest extends FuzzTestBase
{
    @Test
    public void bootstrapProgressTest() throws Throwable
    {
        try (WithProperties properties = new WithProperties().with(TCM_PROGRESS_BARRIER_TIMEOUT_MILLIS.getKey(), "1000",
                                                                   TCM_PROGRESS_BARRIER_BACKOFF_MILLIS.getKey(), "100");
             Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .start())
        {

            IInvokableInstance cmsInstance = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);

            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(KEY_DTEST_FULL_STARTUP, false)
                                            .set(KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false);
            IInvokableInstance newInstance = cluster.bootstrap(config);

            // Set expectation of finish join & retrieve the epoch when it eventually gets committed
            Callable<Epoch> finishJoinEpoch = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareJoin.FinishJoin && r.isSuccess());

            // Drop all messages from the CMS and LOG_REPLICATION from the joining node going to nodes 2 & 3. This will
            // ensure that they do not receive the join events for the new instance. They will then not be able to ack
            // them as the joining node attempts to progress its startup sequence, which should consequently fail.
            cluster.filters().allVerbs().from(1).to(2,3).drop();
            cluster.filters().verbs(TCM_REPLICATION.id).from(4).to(2, 3).drop();

            // Have the joining node pause when the mid join event fails due to ack timeout.
            // The StartJoin event has no prerequisite, so we should see a CONTINUING state, but execution of the
            // MidJoin should be BLOCKED as nodes 2 & 3 can't ack the StartJoin.
            Callable<Void> progressBlocked = waitForListener(newInstance, CONTINUING, BLOCKED);
            new Thread(() -> newInstance.startup()).start();
            progressBlocked.call();

            // Remove the partition between nodes 2 & 3 and the CMS and have them catch up.
            cluster.filters().reset();
            cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().replayAndWait());
            cluster.get(3).runOnInstance(() -> ClusterMetadataService.instance().replayAndWait());

            // Unpause the joining node and have it retry the bootstrap sequence from MidJoin, this time the
            // expectation is that it will succeed, so we can just clear out the listener.
            newInstance.runOnInstance(() -> {
                TestExecutionListener listener = (TestExecutionListener) InProgressSequences.listener;
                listener.restorePrevious();
                listener.releaseAndRetry();
            });

            // Wait for the cluster to all witness the finish join event.
            Epoch finalEpoch = finishJoinEpoch.call();
            ClusterUtils.waitForCMSToQuiesce(cluster, finalEpoch);
        }
    }

    @Test
    public void decommissionProgressTest() throws Throwable
    {
        try (WithProperties properties = new WithProperties().with(TCM_PROGRESS_BARRIER_TIMEOUT_MILLIS.getKey(), "1000",
                                                                   TCM_PROGRESS_BARRIER_BACKOFF_MILLIS.getKey(), "100");
             Cluster cluster = builder().withNodes(4)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .start())
        {

            IInvokableInstance cmsInstance = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);

            // Set expectation of finish leave & retrieve the epoch when it eventually gets committed
            Callable<Epoch> finishLeaveEpoch = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareLeave.FinishLeave && r.isSuccess());

            // Drop all messages from the CMS and LOG_REPLICATION from the leaving node going to nodes 2 & 3. This will
            // ensure that they do not receive the leave events for the new instance. They will then not be able to ack
            // them as the leaving node attempts to progress its leave sequence, which should consequently fail.
            cluster.filters().allVerbs().from(1).to(2,3).drop();
            cluster.filters().verbs(TCM_REPLICATION.id).from(4).to(2, 3).drop();

            // Have the joining node pause when the StartLeave event fails due to ack timeout.
            IInvokableInstance leavingInstance = cluster.get(4);
            Callable<Void> progressBlocked = waitForListener(leavingInstance, BLOCKED);
            new Thread(() -> leavingInstance.runOnInstance(() -> StorageService.instance.decommission(true))).start();
            progressBlocked.call();

            // Remove the partition between nodes 2 & 3 and the CMS and have them catch up.
            cluster.filters().reset();
            cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().replayAndWait());
            cluster.get(3).runOnInstance(() -> ClusterMetadataService.instance().replayAndWait());

            // Now re-partition nodes 2 & 3 so that the MidLeave event cannot be submitted by node 1 as 2 & 3 won't
            // receive/ack the StartLeave.
            cluster.filters().allVerbs().from(1).to(2,3).drop();
            cluster.filters().verbs(TCM_REPLICATION.id).from(4).to(2, 3).drop();

            // Unpause the leaving node and have it retry the StartLeave, which should now be able to proceed as 2 & 3
            // will ack the PrepareJoin. Its progress should be blocked as it comes to submit the next event, its
            // MidJoin, so set a new BLOCKED expectation.
            progressBlocked = waitForExistingListener(leavingInstance, BLOCKED);
            leavingInstance.runOnInstance(() -> {
                TestExecutionListener listener = (TestExecutionListener) InProgressSequences.listener;
                listener.releaseAndRetry();
            });
            progressBlocked.call();

            // Heal the partition again and force a catch up.
            cluster.filters().reset();
            cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().replayAndWait());
            cluster.get(3).runOnInstance(() -> ClusterMetadataService.instance().replayAndWait());

            // Unpause the leaving node and have it retry the MidLeave, which will now be able to proceed as 2 & 3 will
            // ack the StartJoin. This time the expectation is that the remaining events will be successfully acked, so
            // we can just clear out the listener.
            leavingInstance.runOnInstance(() -> {
                TestExecutionListener listener = (TestExecutionListener) InProgressSequences.listener;
                listener.restorePrevious();
                listener.releaseAndRetry();
            });

            // Wait for the cluster to all witness the finish join event.
            Epoch finalEpoch = finishLeaveEpoch.call();
            ClusterUtils.waitForCMSToQuiesce(cluster, finalEpoch);
        }
    }

    @Test
    public void replacementProgressTest() throws Throwable
    {
        try (WithProperties properties = new WithProperties().with(TCM_PROGRESS_BARRIER_TIMEOUT_MILLIS.getKey(), "1000",
                                                                   TCM_PROGRESS_BARRIER_BACKOFF_MILLIS.getKey(), "100");
             Cluster cluster = builder().withNodes(3)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                        .start())
        {

            IInvokableInstance cmsInstance = cluster.get(1);
            ClusterUtils.waitForCMSToQuiesce(cluster, cmsInstance);

            IInvokableInstance toReplace = cluster.get(3);
            toReplace.shutdown(false);
            IInvokableInstance replacement = addInstance(cluster,
                                                         toReplace.config(),
                                                         c -> c.set("auto_bootstrap", true)
                                                               .set(KEY_DTEST_FULL_STARTUP, false)
                                                               .set(KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false));

            // Set expectation of FinishReplace & retrieve the epoch when it eventually gets committed
            Callable<Epoch> finishReplaceEpoch = getSequenceAfterCommit(cmsInstance, (e, r) -> e instanceof PrepareReplace.FinishReplace && r.isSuccess());

            // Drop all messages from the CMS and LOG_REPLICATION from the joining node going to node 2. This will
            // ensure that it does not receive the replace/join events for the new instance. It will then not be able to
            // ack them as the new node attempts to progress its startup sequence, which should consequently fail.
            cluster.filters().allVerbs().from(1).to(2).drop();
            cluster.filters().verbs(TCM_REPLICATION.id).from(4).to(2).drop();

            // Have the joining node pause when the StartReplace event fails due to ack timeout.
            Callable<Void> progressBlocked = waitForListener(replacement, BLOCKED);
            new Thread(() -> {
                try (WithProperties replacementProps = new WithProperties())
                {
                    replacementProps.set(REPLACE_ADDRESS_FIRST_BOOT,
                                         toReplace.config().broadcastAddress().getAddress().getHostAddress());
                    replacement.startup();
                }
            }).start();
            progressBlocked.call();

            // Remove the partition between node 2 and the CMS and have it catch up.
            cluster.filters().reset();
            cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().replayAndWait());

            // Unpause the joining node and have it retry the bootstrap sequence from StartReplace, this time the
            // expectation is that it will succeed, so we can just clear out the listener.
            replacement.runOnInstance(() -> {
                TestExecutionListener listener = (TestExecutionListener) InProgressSequences.listener;
                listener.restorePrevious();
                listener.releaseAndRetry();
            });

            // Wait for the cluster to all witness the finish join event.
            Epoch finalEpoch = finishReplaceEpoch.call();
            ClusterUtils.waitForCMSToQuiesce(cluster, finalEpoch);
        }
    }

    private Callable<Void> waitForListener(IInvokableInstance instance, SequenceState...expected)
    {
        Callable<Void> remoteCallable = instance.callOnInstance(() -> {
            TestExecutionListener listener = new TestExecutionListener(expected);
            listener.replaceCurrent();
            return () -> {
                listener.await();
                return null;
            };
        });
        return remoteCallable::call;
    }

    private Callable<Void> waitForExistingListener(IInvokableInstance instance, SequenceState...expected)
    {
        Callable<Void> remoteCallable = instance.callOnInstance(() -> {
            TestExecutionListener listener = (TestExecutionListener) InProgressSequences.listener;
            listener.withNewExpectations(expected);
            return () -> {
                listener.await();
                return null;
            };
        });
        return remoteCallable::call;
    }

    public static class TestExecutionListener implements BiFunction<InProgressSequence<?>, SequenceState, SequenceState>
    {
        volatile boolean retry = true;
        volatile Condition expectationsMet = Condition.newOneTimeCondition();
        volatile Condition barrier;
        volatile BiFunction<InProgressSequence<?>, SequenceState, SequenceState> previous;

        SequenceState[] expectations;
        int index = 0;

        TestExecutionListener(SequenceState... resumptionStates)
        {
            expectations = resumptionStates;
        }

        public void replaceCurrent()
        {
            previous = InProgressSequences.replaceListener(this);
        }

        public void restorePrevious()
        {
            InProgressSequences.replaceListener(previous);
        }

        @Override
        public SequenceState apply(InProgressSequence<?> sequence, SequenceState state)
        {
            if (null == expectations || expectations.length == 0)
                return state;

            if (state != expectations[index])
                throw new IllegalStateException(String.format("Unexpected outcome for %s; Expected: %s, Actual: %s",
                                                              sequence.kind(), expectations[index], state));

            if (++index == expectations.length)
            {
                barrier = Condition.newOneTimeCondition();
                expectationsMet.signal();
                barrier.awaitUninterruptibly();
                if (retry)
                    state = sequence.executeNext()
                            ? SequenceState.CONTINUING
                            : SequenceState.HALTED;
                return state;
            }

            return state;
        }

        public void await()
        {
            expectationsMet.awaitUninterruptibly();
        }

        public void withNewExpectations(SequenceState...newExpectations)
        {
            expectations = newExpectations;
            index = 0;
            expectationsMet = Condition.newOneTimeCondition();
        }

        public void releaseAndRetry()
        {
            retry = true;
            barrier.signal();
        }
    }
}
