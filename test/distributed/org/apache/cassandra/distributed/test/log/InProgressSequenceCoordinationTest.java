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

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.AddToCMS;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.SequenceState;
import org.apache.cassandra.tcm.transformations.PrepareJoin;
import org.apache.cassandra.tcm.transformations.PrepareLeave;
import org.apache.cassandra.tcm.transformations.PrepareReplace;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.config.CassandraRelevantProperties.REPLACE_ADDRESS_FIRST_BOOT;
import static org.apache.cassandra.config.CassandraRelevantProperties.TCM_SKIP_CMS_RECONFIGURATION_AFTER_TOPOLOGY_CHANGE;
import static org.apache.cassandra.distributed.Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN;
import static org.apache.cassandra.distributed.Constants.KEY_DTEST_FULL_STARTUP;
import static org.apache.cassandra.distributed.shared.ClusterUtils.addInstance;
import static org.apache.cassandra.distributed.shared.ClusterUtils.getSequenceAfterCommit;
import static org.apache.cassandra.net.Verb.TCM_CURRENT_EPOCH_REQ;
import static org.apache.cassandra.net.Verb.TCM_FETCH_PEER_LOG_RSP;
import static org.apache.cassandra.tcm.sequences.SequenceState.blocked;
import static org.apache.cassandra.tcm.sequences.SequenceState.continuable;
import static org.apache.cassandra.tcm.sequences.SequenceState.halted;

public class InProgressSequenceCoordinationTest extends FuzzTestBase
{
    private boolean skipReconfiguation;

    @Before
    public void setup()
    {
        // Skip the automatic, speculative CMS reconfiguration after join/replace operations.
        // These can cause tests to run long or hang as peers start to shutdown while they are
        // in flight. If the node initiating the reconfiguration sees the current CMS node as
        // DOWN, a reconfiguration is triggered which cannot be be completed in a timely fashion.
        // Previously, this was hidden because LegacyStateListener would queue up gossip tasks
        // on a separate executor and the backlog of these prevented the failure detector from
        // marking any nodes DOWN. Since making LegacyStateListener run synchronously on the
        // log follower thread, the joining node does see the CMS node as DOWN and so tries to
        // perform a reconfiguration.
        skipReconfiguation = TCM_SKIP_CMS_RECONFIGURATION_AFTER_TOPOLOGY_CHANGE.getBoolean();
        TCM_SKIP_CMS_RECONFIGURATION_AFTER_TOPOLOGY_CHANGE.setBoolean(true);
    }

    @After
    public void tearDown()
    {
        TCM_SKIP_CMS_RECONFIGURATION_AFTER_TOPOLOGY_CHANGE.setBoolean(skipReconfiguation);
    }

    @Test
    public void bootstrapProgressTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .appendConfig(cfg -> cfg.set("progress_barrier_timeout", "5000ms")
                                                                .set("request_timeout", "5000ms")
                                                                .set("progress_barrier_backoff", "100ms")
                                                                .with(Feature.NETWORK, Feature.GOSSIP))
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
            cluster.filters().verbs(TCM_FETCH_PEER_LOG_RSP.id).from(4).to(2, 3).drop();

            // Have the joining node pause when the mid join event fails due to ack timeout.
            // The StartJoin event has no prerequisite, so we should see a CONTINUING state, but execution of the
            // MidJoin should be BLOCKED as nodes 2 & 3 can't ack the StartJoin.
            Callable<Void> progressBlocked = waitForListener(newInstance, continuable(), blocked());
            Thread startupThread = new Thread(newInstance::startup);
            startupThread.start();
            try
            {
                progressBlocked.call();

                // Remove the partition between nodes 2 & 3 and the CMS and have them catch up.
                cluster.filters().reset();
                cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());
                cluster.get(3).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());

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
            finally
            {
                releaseAndJoin(newInstance, startupThread);
            }
        }
    }

    @Test
    public void decommissionProgressTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                        .appendConfig(cfg -> cfg.set("progress_barrier_timeout", "5000ms")
                                                                .set("request_timeout", "5000ms")
                                                                .set("progress_barrier_backoff", "100ms")
                                                                .with(Feature.NETWORK, Feature.GOSSIP))
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
            cluster.filters().verbs(TCM_FETCH_PEER_LOG_RSP.id).from(4).to(2, 3).drop();

            // Have the joining node pause when the StartLeave event fails due to ack timeout.
            IInvokableInstance leavingInstance = cluster.get(4);
            Callable<Void> progressBlocked = waitForListener(leavingInstance, blocked());
            Thread t = new Thread(() -> leavingInstance.runOnInstance(() -> StorageService.instance.decommission(true)));
            t.start();
            try
            {
                progressBlocked.call();
                // Remove the partition between nodes 2 & 3 and the CMS and have them catch up.
                cluster.filters().reset();
                cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());
                cluster.get(3).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());

                // Now re-partition nodes 2 & 3 so that the MidLeave event cannot be submitted by node 1 as 2 & 3 won't
                // receive/ack the StartLeave.
                cluster.filters().allVerbs().from(1).to(2,3).drop();
                cluster.filters().verbs(TCM_FETCH_PEER_LOG_RSP.id).from(4).to(2, 3).drop();

                // Unpause the leaving node and have it retry the StartLeave, which should now be able to proceed as 2 & 3
                // will ack the PrepareJoin. Its progress should be blocked as it comes to submit the next event, its
                // MidJoin, so set a new BLOCKED expectation.
                progressBlocked = waitForExistingListener(leavingInstance, blocked());
                leavingInstance.runOnInstance(() -> {
                    TestExecutionListener listener = (TestExecutionListener) InProgressSequences.listener;
                    listener.releaseAndRetry();
                });
                progressBlocked.call();

                // Heal the partition again and force a catch up.
                cluster.filters().reset();
                cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());
                cluster.get(3).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());

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
            finally
            {
                releaseAndJoin(leavingInstance, t);
            }
        }
    }

    @Test
    public void replacementProgressTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .appendConfig(cfg -> cfg.set("progress_barrier_timeout", "5000ms")
                                                                .set("request_timeout", "5000ms")
                                                                .set("progress_barrier_backoff", "100ms")
                                                                .with(Feature.NETWORK, Feature.GOSSIP))
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

            // Drop all messages from the progress barrier discovery from the joining node going. This will
            // ensure that it does not receive the replace/join events for the new instance. It will then not be able to
            // ack them as the new node attempts to progress its startup sequence, which should consequently fail.
            cluster.filters().verbs(TCM_CURRENT_EPOCH_REQ.id).from(4).drop();

            // Have the joining node pause when the StartReplace event fails due to ack timeout.
            Callable<Void> progressBlocked = waitForListener(replacement, continuable(), blocked());
            Thread startupThread = new Thread(() -> {
                try (WithProperties replacementProps = new WithProperties())
                {
                    replacementProps.set(REPLACE_ADDRESS_FIRST_BOOT,
                                         toReplace.config().broadcastAddress().getAddress().getHostAddress());
                    replacement.startup();
                }
            });
            startupThread.start();
            try
            {
                progressBlocked.call();

                // Remove the partition between node 2 and the CMS and have it catch up.
                cluster.filters().reset();
                cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().processor().fetchLogAndWait());

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
            finally
            {
                releaseAndJoin(replacement, startupThread);
            }
        }
    }

    @Test
    public void rejectSubsequentInProgressSequence() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .start())
        {
            cluster.get(2).runOnInstance(() -> {
                NodeId self = ClusterMetadata.current().myNodeId();
                ClusterMetadataService.instance().commit(new PrepareLeave(self,
                                                                          true,
                                                                          ClusterMetadataService.instance().placementProvider(),
                                                                          LeaveStreams.Kind.UNBOOTSTRAP));
                try
                {
                    AddToCMS.initiate();
                    Assert.fail("Should have failed");
                }
                catch (Throwable t)
                {
                    Assert.assertTrue(t.getMessage().contains("since it already has an active in-progress sequence"));
                }
            });
        }
    }

    @Test
    public void inProgressSequenceRetryTest() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                                        .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                        .withConfig((config) -> config.with(Feature.NETWORK, Feature.GOSSIP).set("request_timeout_in_ms", "1000"))
                                        .start())
        {
            cluster.filters()
                   .inbound()
                   .messagesMatching(new IMessageFilters.Matcher()
            {
                final Random rng = new Random(1);
                public boolean matches(int i, int i1, IMessage msg)
                {
                    if (msg.verb() != Verb.TCM_COMMIT_REQ.id)
                        return false;
                    return rng.nextBoolean();
                }
            }).drop().on();
            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set(Constants.KEY_DTEST_FULL_STARTUP, true);
            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup();
        }
    }

    /**
     * Ensure that sequential execution results into given SequenceStates. Provide a state for each
     * expected operation.
     */
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

    // Unblock the thread if it's still waiting on the listener barrier, then join to ensure clean shutdown.
    private void releaseAndJoin(IInvokableInstance instance, Thread thread) throws InterruptedException
    {
        instance.runOnInstance(() -> {
            if (InProgressSequences.listener instanceof TestExecutionListener)
                ((TestExecutionListener) InProgressSequences.listener).releaseWithoutRetry();
        });
        thread.join();
    }

    public static class TestExecutionListener implements BiFunction<MultiStepOperation<?>, SequenceState, SequenceState>
    {
        volatile boolean retry = true;
        volatile Condition expectationsMet = Condition.newOneTimeCondition();
        volatile Condition barrier;
        volatile BiFunction<MultiStepOperation<?>, SequenceState, SequenceState> previous;

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
        public SequenceState apply(MultiStepOperation<?> sequence, SequenceState state)
        {
            if (null == expectations || expectations.length == 0)
                return state;

            if (!state.equals(expectations[index]))
                throw new IllegalStateException(String.format("Unexpected outcome for %s step %s; Expected: %s, Actual: %s",
                                                              sequence.kind(), sequence.idx, expectations[index], state));

            if (++index == expectations.length)
            {
                barrier = Condition.newOneTimeCondition();
                expectationsMet.signal();
                barrier.awaitUninterruptibly();
                if (retry)
                    state = sequence.executeNext().isContinuable()
                            ? continuable()
                            : halted();
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

        public void releaseWithoutRetry()
        {
            retry = false;
            if (barrier != null)
                barrier.signal();
        }
    }
}
