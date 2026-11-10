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

import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.distributed.shared.ClusterUtils.getClusterMetadataVersion;
import static org.apache.cassandra.distributed.shared.ClusterUtils.pauseBeforeEnacting;
import static org.apache.cassandra.distributed.shared.ClusterUtils.unpauseEnactment;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Guards against a deadlock between the metadata snapshot trigger and the sstable deletion barrier.
 *
 * {@link org.apache.cassandra.tcm.log.LocalLog#snapshotListener()} submits
 * {@link org.apache.cassandra.tcm.ClusterMetadataService#triggerSnapshot()} to the
 * {@code CMSLogTasks} executor. Suppose it submitted to
 * {@link org.apache.cassandra.concurrent.ScheduledExecutors#nonPeriodicTasks} instead. It would
 * then share that executor with the barrier in
 * {@link org.apache.cassandra.db.lifecycle.LogTransaction#waitForDeletions()}. That executor has a
 * single thread, so the two uses would be mutually exclusive. The deadlock sequence is below.
 *
 * <pre>
 * GlobalLogFollower                        NonPeriodicTasks:1
 *   enacts the snapshot-triggering epoch     triggerSnapshot
 *   (listener queues triggerSnapshot)        ClusterMetadataService.commit
 *   continues the same batch                 log.waitForHighestConsecutive()   <- pre-commit barrier
 *   enacts DROP TABLE                        LocalLog.runOnce(null)
 *   ColumnFamilyStore.invalidate             parks until the follower completes a loop
 *   LifecycleTransaction.waitForDeletions
 *   queues a no-op behind triggerSnapshot
 *   and blocks on it
 * </pre>
 *
 * Both waits are unbounded, so the node's log would never move again. The test builds the same
 * interleaving by holding a node's follower before a replay batch that contains, in order, a
 * snapshot-triggering epoch and a dropped table. It passes while the snapshot trigger does not run
 * on nonPeriodicTasks.
 */
public class SnapshotTriggerDeletionBarrierDeadlockTest extends TestBaseImpl
{
    private static final String KS = "snapshot_barrier_test";
    private static final long CATCHUP_TIMEOUT_SECONDS = 15;

    @Test
    public void snapshotTriggerMustNotBlockDeletionBarrier() throws Exception
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP))
                                        .start())
        {
            // Put all three nodes in the CMS. Only the lowest NodeId member runs the snapshot listener,
            // and that is the node we stall. The other two must be CMS members as well. While the
            // stalled node's follower is held, every ClusterMetadataService.commit that reaches it parks
            // in the pre-commit barrier. The epochs this test needs must therefore be committed
            // elsewhere, by a paxos quorum that does not include it.
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3").asserts().success();
            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

            int stalledNode = lowestCMSMember(cluster);
            int committerNode = stalledNode == 1 ? 2 : 1;
            IInvokableInstance stalled = cluster.get(stalledNode);
            IInvokableInstance committer = cluster.get(committerNode);

            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KS + ".tbl (id int PRIMARY KEY)");
            cluster.coordinator(committerNode).execute("INSERT INTO " + KS + ".tbl (id) VALUES (1)", ConsistencyLevel.ALL);
            for (int i = 1; i <= cluster.size(); i++)
                cluster.get(i).flush(KS);

            ClusterUtils.waitForCMSToQuiesce(cluster, committer);
            long tail = getClusterMetadataVersion(stalled).getEpoch();
            assertEquals("nodes should agree on the epoch before the stall is set up",
                         tail, getClusterMetadataVersion(committer).getEpoch());

            // The first epoch the stalled node replays when it is resumed. It has to trigger the snapshot
            // listener, so point metadata_snapshot_frequency straight at it. A frequency of 1 would not
            // do. Every TriggerSnapshot commit is itself a multiple of 1, so the listener would re-arm on
            // its own output and the cluster would commit snapshots in a loop for as long as it is
            // healthy. Using the epoch itself keeps the trigger to a single, known point.
            final long pauseAt = tail + 1;
            assertTrue("epoch " + pauseAt + " does not fit in metadata_snapshot_frequency", pauseAt <= Integer.MAX_VALUE);
            stalled.runOnInstance(() -> DatabaseDescriptor.setMetadataSnapshotFrequency((int) pauseAt));

            Callable<?> pausedBeforeReplay = pauseBeforeEnacting(stalled, Epoch.create(pauseAt));

            try
            {
                // Epoch pauseAt. Any schema change would do. This one is the entry whose enactment
                // queues triggerSnapshot. The follower must enact it in the same batch as, and before,
                // the drop.
                cluster.coordinator(committerNode).execute("CREATE TABLE " + KS + ".tbl2 (id int PRIMARY KEY)", ConsistencyLevel.ONE);
                assertEquals("expected the CREATE TABLE to be committed at the epoch the pause was armed for",
                             pauseAt, getClusterMetadataVersion(committer).getEpoch());

                // Wait until the follower is actually parked before enacting that epoch.
                pausedBeforeReplay.call();

                // These commit as epochs pauseAt + 1 and pauseAt + 2 while the follower is parked. The
                // dropped table reaches the deletion barrier via SchemaListener -> Keyspace.dropCf ->
                // ColumnFamilyStore.invalidate.
                cluster.coordinator(committerNode).execute("DROP TABLE " + KS + ".tbl", ConsistencyLevel.ONE);
                cluster.coordinator(committerNode).execute("DROP KEYSPACE " + KS, ConsistencyLevel.ONE);

                long ahead = getClusterMetadataVersion(committer).getEpoch();
                long behind = getClusterMetadataVersion(stalled).getEpoch();
                assertEquals("the stalled node should not have enacted anything since the pause", tail, behind);
                assertTrue("expected the committer to be at least at epoch " + (pauseAt + 2) + ", was " + ahead,
                           ahead >= pauseAt + 2);
                assertTrue("the stalled node must be at least two epochs behind before resuming, was " + (ahead - behind),
                           ahead - behind >= 2);

                unpauseEnactment(stalled);

                // The follower now replays pauseAt and queues triggerSnapshot on CMSLogTasks. It then
                // replays the dropped table, which queues the deletion barrier's no-op on
                // nonPeriodicTasks. The two do not share an executor, so the follower finishes the batch.
                // If triggerSnapshot ran on nonPeriodicTasks, the no-op would queue behind it and this
                // would never complete.
                await("node" + stalledNode + " to replay the batch containing the dropped table")
                .atMost(CATCHUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(() -> getClusterMetadataVersion(stalled).getEpoch(), greaterThanOrEqualTo(ahead));
            }
            finally
            {
                // This matters only if the snapshot trigger shares nonPeriodicTasks. In that case both
                // wedged threads wait without a bound and would keep cluster.close() from returning,
                // which turns a failure into a hung build. LocalLog.runOnce waits interruptibly, so
                // interrupting the nonPeriodicTasks worker aborts the parked snapshot commit. That
                // lets the barrier's no-op run and the follower finish.
                releaseNonPeriodicTasks(stalled);
            }
        }
    }

    /**
     * The node with the lowest NodeId among the full CMS members, which is the only node whose
     * snapshot listener fires. NodeIds follow registration order, which is not the instance order
     * when a cluster starts its nodes in parallel, so this is resolved rather than assumed.
     */
    private static int lowestCMSMember(Cluster cluster)
    {
        String endpoint = cluster.get(1).callOnInstance(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            NodeId lowest = metadata.fullCMSMemberIds()
                                    .stream()
                                    .min(Comparator.comparingInt(NodeId::id))
                                    .orElseThrow(() -> new IllegalStateException("No full CMS members"));
            return metadata.directory.endpoint(lowest).getHostAddress(false);
        });

        for (int i = 1; i <= cluster.size(); i++)
        {
            if (cluster.get(i).broadcastAddress().getAddress().getHostAddress().equals(endpoint))
                return i;
        }
        throw new AssertionError("No instance for lowest CMS member " + endpoint);
    }

    private static void releaseNonPeriodicTasks(IInvokableInstance instance)
    {
        if (instance.isShutdown())
            return;

        instance.runOnInstance(() -> {
            ClassLoader mine = Thread.currentThread().getContextClassLoader();
            for (Thread thread : Thread.getAllStackTraces().keySet())
            {
                if (thread.getName().contains("NonPeriodicTasks") && thread.getContextClassLoader() == mine)
                    thread.interrupt();
            }
        });
    }
}
