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

package org.apache.cassandra.service.accord;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import accord.impl.AbstractConfigurationService;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.accord.AccordKeyspace.EpochDiskState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.utils.Simulate.With.MONITORS;

// TODO: listen to FailureDetector and rearrange fast path accordingly
@Simulate(with=MONITORS)
public class AccordConfigurationService extends AbstractConfigurationService<AccordConfigurationService.EpochState, AccordConfigurationService.EpochHistory> implements AccordEndpointMapper, AccordSyncPropagator.Listener, Shutdownable
{
    private final AccordSyncPropagator syncPropagator;
    private final DiskStateManager diskStateManager;

    @GuardedBy("this")
    private EpochDiskState diskState = EpochDiskState.EMPTY;

    private enum State { INITIALIZED, LOADING, STARTED, SHUTDOWN }

    @GuardedBy("this")
    private State state = State.INITIALIZED;
    private volatile EndpointMapping mapping = EndpointMapping.EMPTY;

    public enum SyncStatus { NOT_STARTED, NOTIFYING, COMPLETED }

    static class EpochState extends AbstractConfigurationService.AbstractEpochState
    {
        private volatile SyncStatus syncStatus = SyncStatus.NOT_STARTED;
        protected final AsyncResult.Settable<Void> localSyncNotified = AsyncResults.settable();

        public EpochState(long epoch)
        {
            super(epoch);
        }

        void setSyncStatus(SyncStatus status)
        {
            this.syncStatus = status;
            if (status == SyncStatus.COMPLETED)
                localSyncNotified.trySuccess(null);
        }

        AsyncResult<Topology> received()
        {
            return received;
        }

        AsyncResult<Void> acknowledged()
        {
            return acknowledged;
        }

        @Nullable AsyncResult<Void> reads()
        {
            return reads;
        }

        AsyncResult.Settable<Void> localSyncNotified()
        {
            return localSyncNotified;
        }
    }

    static class EpochHistory extends AbstractConfigurationService.AbstractEpochHistory<EpochState>
    {
        @Override
        protected EpochState createEpochState(long epoch)
        {
            return new EpochState(epoch);
        }
    }

    @VisibleForTesting
    interface DiskStateManager
    {
        /**
         * Loads local states known to the _current_ node.
         */
        EpochDiskState loadLocalTopologyState(AccordKeyspace.TopologyLoadConsumer consumer);

        EpochDiskState setNotifyingLocalSync(long epoch, Set<Node.Id> pending, EpochDiskState diskState);

        EpochDiskState setCompletedLocalSync(long epoch, EpochDiskState diskState);

        EpochDiskState markLocalSyncAck(Node.Id id, long epoch, EpochDiskState diskState);

        EpochDiskState saveTopology(Topology topology, EpochDiskState diskState);

        EpochDiskState markRemoteTopologySync(Node.Id node, long epoch, EpochDiskState diskState);

        EpochDiskState markClosed(Ranges ranges, long epoch, EpochDiskState diskState);

        EpochDiskState markRetired(Ranges ranges, long epoch, EpochDiskState diskState);

        EpochDiskState truncateTopologyUntil(long epoch, EpochDiskState diskState);
    }

    enum SystemTableDiskStateManager implements DiskStateManager
    {
        instance;

        @Override
        public EpochDiskState loadLocalTopologyState(AccordKeyspace.TopologyLoadConsumer consumer)
        {
            return AccordKeyspace.loadTopologies(consumer);
        }

        @Override
        public EpochDiskState setNotifyingLocalSync(long epoch, Set<Node.Id> notify, EpochDiskState diskState)
        {
            return AccordKeyspace.setNotifyingLocalSync(epoch, notify, diskState);
        }

        @Override
        public EpochDiskState setCompletedLocalSync(long epoch, EpochDiskState diskState)
        {
            return AccordKeyspace.setCompletedLocalSync(epoch, diskState);
        }

        @Override
        public EpochDiskState markLocalSyncAck(Node.Id id, long epoch, EpochDiskState diskState)
        {
            return AccordKeyspace.markLocalSyncAck(id, epoch, diskState);
        }

        @Override
        public EpochDiskState saveTopology(Topology topology, EpochDiskState diskState)
        {
            return AccordKeyspace.saveTopology(topology, diskState);
        }

        @Override
        public EpochDiskState markRemoteTopologySync(Node.Id node, long epoch, EpochDiskState diskState)
        {
            return AccordKeyspace.markRemoteTopologySync(node, epoch, diskState);
        }

        @Override
        public EpochDiskState markClosed(Ranges ranges, long epoch, EpochDiskState diskState)
        {
            return AccordKeyspace.markClosed(ranges, epoch, diskState);
        }

        @Override
        public EpochDiskState markRetired(Ranges ranges, long epoch, EpochDiskState diskState)
        {
            return AccordKeyspace.markRetired(ranges, epoch, diskState);
        }

        @Override
        public EpochDiskState truncateTopologyUntil(long epoch, EpochDiskState diskState)
        {
            return AccordKeyspace.truncateTopologyUntil(epoch, diskState);
        }
    }

    public final ChangeListener listener = new MetadataChangeListener();
    private class MetadataChangeListener implements ChangeListener
    {
        @Override
        public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
        {
            maybeReportMetadata(next);
        }
    }

    public AccordConfigurationService(Node.Id node, MessageDelivery messagingService, IFailureDetector failureDetector, DiskStateManager diskStateManager, ScheduledExecutorPlus scheduledTasks)
    {
        super(node);
        this.syncPropagator = new AccordSyncPropagator(localId, this, messagingService, failureDetector, scheduledTasks, this);
        this.diskStateManager = diskStateManager;
    }

    public AccordConfigurationService(Node.Id node)
    {
        this(node, MessagingService.instance(), FailureDetector.instance, SystemTableDiskStateManager.instance, ScheduledExecutors.scheduledTasks);
    }

    @Override
    protected EpochHistory createEpochHistory()
    {
        return new EpochHistory();
    }

    /**
     * On restart, loads topologies. On bootstrap, discovers existing topologies and initializes the node.
     */
    public synchronized void start()
    {
        Invariants.require(state == State.INITIALIZED, "Expected state to be INITIALIZED but was %s", state);
        state = State.LOADING;

        EndpointMapping snapshot = mapping;
        diskStateManager.loadLocalTopologyState((epoch, syncStatus, pendingSyncNotify, remoteSyncComplete, closed, redundant) -> {
            getOrCreateEpochState(epoch).setSyncStatus(syncStatus);
            // TODO (expected, correctness): since this is loading old topologies, might see nodes no longer present (host replacement, decom, shrink, etc.); attempt to remove unknown nodes
            if (Objects.requireNonNull(syncStatus) == SyncStatus.NOTIFYING)
                syncPropagator.reportSyncComplete(epoch, Sets.filter(pendingSyncNotify, snapshot::containsId), localId);

            remoteSyncComplete.forEach(id -> receiveRemoteSyncComplete(id, epoch));
            // TODO (required): disk doesn't get updated until we see our own notification, so there is an edge case where this instance notified others and fails in the middle, but Apply was already sent!  This could leave partial closed/redudant accross the cluster
            receiveClosed(closed, epoch);
            receiveRetired(redundant, epoch);
        });
        state = State.STARTED;

        // for all nodes removed, or pending removal, mark them as removed, so we don't wait on their replies
        Map<Node.Id, Long> removedNodes = mapping.removedNodes();
        for (Map.Entry<Node.Id, Long> e : removedNodes.entrySet())
            onNodeRemoved(e.getValue(), currentTopology(), e.getKey());

        ClusterMetadataService.instance().log().addListener(listener);
    }

    @Override
    public synchronized boolean isTerminated()
    {
        return state == State.SHUTDOWN;
    }

    @Override
    public synchronized void shutdown()
    {
        if (isTerminated())
            return;
        ClusterMetadataService.instance().log().removeListener(listener);
        state = State.SHUTDOWN;
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return isTerminated();
    }

    @Override
    public Node.Id mappedIdOrNull(InetAddressAndPort endpoint)
    {
        return mapping.mappedIdOrNull(endpoint);
    }

    @Override
    public InetAddressAndPort mappedEndpointOrNull(Node.Id id)
    {
        return mapping.mappedEndpointOrNull(id);
    }

    @VisibleForTesting
    synchronized EpochDiskState diskState()
    {
        return diskState;
    }

    @VisibleForTesting
    synchronized void updateMapping(EndpointMapping mapping)
    {
        if (mapping.epoch() > this.mapping.epoch())
            this.mapping = mapping;
    }

    public synchronized void updateMapping(ClusterMetadata metadata)
    {
        updateMapping(AccordTopology.directoryToMapping(metadata.epoch.getEpoch(), metadata.directory));
    }

    private void reportMetadata(ClusterMetadata metadata)
    {
        Stage.MISC.submit(() -> reportMetadataInternal(metadata));
    }

    void reportMetadataInternal(ClusterMetadata metadata)
    {
        updateMapping(metadata);
        Topology topology = AccordTopology.createAccordTopology(metadata);
        if (Invariants.isParanoid())
        {
            for (Node.Id node : topology.nodes())
            {
                if (mapping.mappedEndpointOrNull(node) == null)
                    throw new IllegalStateException(String.format("Epoch %d has node %s but mapping does not!", topology.epoch(), node));
            }
        }
        reportTopology(topology);
        if (epochs.lastAcknowledged() >= topology.epoch()) checkIfNodesRemoved(topology);
        else epochs.acknowledgeFuture(topology.epoch()).addCallback(() -> checkIfNodesRemoved(topology));
    }

    private void checkIfNodesRemoved(Topology topology)
    {
        if (epochs.minEpoch() == topology.epoch()) return;
        Topology previous = getTopologyForEpoch(topology.epoch() - 1);
        // for all nodes removed, or pending removal, mark them as removed so we don't wait on their replies
        Sets.SetView<Node.Id> removedNodes = Sets.difference(previous.nodes(), topology.nodes());
        // TODO (desired, efficiency): there should be no need to notify every epoch for every removed node
        for (Node.Id removedNode : removedNodes)
        {
            if (topology.epoch() >= epochs.minEpoch())
                onNodeRemoved(topology.epoch(), previous, removedNode);
        }
    }

    private static boolean shareShard(Topology current, Node.Id target, Node.Id self)
    {
        for (Shard shard : current.shards())
        {
            if (!shard.contains(target)) continue;
            if (shard.contains(self)) return true;
        }
        return false;
    }

    public void onNodeRemoved(long epoch, Topology current, Node.Id removed)
    {
        syncPropagator.onNodesRemoved(removed);
        // TODO (now): it seems to be incorrect to mark remote syncs complete if/when node got removed.
        for (long oldEpoch : nonCompletedEpochsBefore(epoch))
            receiveRemoteSyncCompletePreListenerNotify(removed, oldEpoch);

        listeners.forEach(l -> l.onRemoveNode(epoch, removed));

        if (shareShard(current, removed, localId))
            AccordService.instance().tryMarkRemoved(current, removed);
    }

    private long[] nonCompletedEpochsBefore(long max)
    {
        LongArrayList notComplete = new LongArrayList();
        synchronized (epochs)
        {
            for (long epoch = epochs.minEpoch(), maxKnown = epochs.maxEpoch(); epoch <= max && epoch <= maxKnown; epoch++)
            {
                EpochSnapshot snapshot = getEpochSnapshot(epoch);
                if (snapshot.syncStatus != SyncStatus.COMPLETED)
                    notComplete.add(epoch);
            }
        }
        return notComplete.toLongArray();
    }

    @VisibleForTesting
    void maybeReportMetadata(ClusterMetadata metadata)
    {
        // don't report metadata until the previous one has been acknowledged
        long epoch = metadata.epoch.getEpoch();
        synchronized (epochs)
        {
            if (epochs.maxEpoch() == 0)
            {
                getOrCreateEpochState(epoch);  // touch epoch state so subsequent calls see it
                reportMetadata(metadata);
                return;
            }
        }
        getOrCreateEpochState(epoch - 1).acknowledged().addCallback(() -> reportMetadata(metadata));
    }

    @Override
    protected void fetchTopologyInternal(long epoch)
    {
        try
        {
            Set<InetAddressAndPort> peers = new HashSet<>(ClusterMetadata.current().directory.allJoinedEndpoints());
            peers.remove(FBUtilities.getBroadcastAddressAndPort());
            if (peers.isEmpty())
                return;
            Topology topology;
            while ((topology =FetchTopology.fetch(SharedContext.Global.instance, peers, epoch).get()) == null) {}
            reportTopology(topology);
        }
        catch (InterruptedException e)
        {
            if (currentEpoch() >= epoch)
                return;
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptedException(e);
        }
        catch (Throwable e)
        {
            if (currentEpoch() >= epoch)
                return;
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    protected void localSyncComplete(Topology topology, boolean startSync)
    {
        long epoch = topology.epoch();
        EpochState epochState = getOrCreateEpochState(epoch);
        if (!startSync || epochState.syncStatus != SyncStatus.NOT_STARTED)
            return;

        Set<Node.Id> notify = topology.nodes().stream().filter(i -> !localId.equals(i)).collect(Collectors.toSet());
        synchronized (this)
        {
            if (epochState.syncStatus != SyncStatus.NOT_STARTED)
                return;
            diskState = diskStateManager.setNotifyingLocalSync(epoch, notify, diskState);
            epochState.setSyncStatus(SyncStatus.NOTIFYING);
        }
        syncPropagator.reportSyncComplete(epoch, notify, localId);
    }

    @Override
    public synchronized void onEndpointAck(Node.Id id, long epoch)
    {
        EpochState epochState = getOrCreateEpochState(epoch);
        if (epochState.syncStatus != SyncStatus.NOTIFYING)
            return;
        diskState = diskStateManager.markLocalSyncAck(id, epoch, diskState);
    }

    @Override
    public void onComplete(long epoch)
    {
        EpochState epochState = getOrCreateEpochState(epoch);
        synchronized (this)
        {
            epochState.setSyncStatus(SyncStatus.COMPLETED);
            diskState = diskStateManager.setCompletedLocalSync(epoch, diskState);
        }
    }

    @Override
    protected synchronized void topologyUpdatePreListenerNotify(Topology topology)
    {
        if (state == State.STARTED)
            diskState = diskStateManager.saveTopology(topology, diskState);
    }

    @Override
    protected synchronized void receiveRemoteSyncCompletePreListenerNotify(Node.Id node, long epoch)
    {
        if (state == State.STARTED)
            diskState = diskStateManager.markRemoteTopologySync(node, epoch, diskState);
    }

    @Override
    public void reportEpochClosed(Ranges ranges, long epoch)
    {
        checkStarted();
        Topology topology = getTopologyForEpoch(epoch);
        syncPropagator.reportClosed(epoch, topology.nodes(), ranges);
    }

    @VisibleForTesting
    public AccordSyncPropagator syncPropagator()
    {
        return syncPropagator;
    }

    @Override
    public void reportEpochRedundant(Ranges ranges, long epoch)
    {
        checkStarted();
        // TODO (expected): ensure we aren't fetching a truncated epoch; otherwise this should be non-null
        Topology topology = getTopologyForEpoch(epoch);
        syncPropagator.reportRedundant(epoch, topology.nodes(), ranges);
    }

    @Override
    public void receiveClosed(Ranges ranges, long epoch)
    {
        synchronized (this)
        {
            diskState = diskStateManager.markClosed(ranges, epoch, diskState);
        }
        super.receiveClosed(ranges, epoch);
    }

    @Override
    public void receiveRetired(Ranges ranges, long epoch)
    {
        synchronized (this)
        {
            diskState = diskStateManager.markRetired(ranges, epoch, diskState);
        }
        super.receiveRetired(ranges, epoch);
    }

    @Override
    protected void truncateTopologiesPreListenerNotify(long epoch)
    {
        checkStarted();
    }

    @Override
    protected synchronized void truncateTopologiesPostListenerNotify(long epoch)
    {
        if (state == State.STARTED)
            diskState = diskStateManager.truncateTopologyUntil(epoch, diskState);
    }

    private synchronized void checkStarted()
    {
        State state = this.state;
        Invariants.require(state == State.STARTED, "Expected state to be STARTED but was %s", state);
    }

    @VisibleForTesting
    public static class EpochSnapshot
    {
        public enum ResultStatus
        {
            PENDING, SUCCESS, FAILURE;

            static ResultStatus of(AsyncResult<?> result)
            {
                if (result == null || !result.isDone())
                    return PENDING;

                return result.isSuccess() ? SUCCESS : FAILURE;
            }
        }

        public final long epoch;
        public final SyncStatus syncStatus;
        public final ResultStatus received;
        public final ResultStatus acknowledged;
        public final ResultStatus reads;

        private EpochSnapshot(EpochState state)
        {
            this.epoch = state.epoch();
            this.syncStatus = state.syncStatus;
            this.received = ResultStatus.of(state.received());
            this.acknowledged = ResultStatus.of(state.acknowledged());
            this.reads = ResultStatus.of(state.reads());
        }

        public EpochSnapshot(long epoch, SyncStatus syncStatus, ResultStatus received, ResultStatus acknowledged, ResultStatus reads)
        {
            this.epoch = epoch;
            this.syncStatus = syncStatus;
            this.received = received;
            this.acknowledged = acknowledged;
            this.reads = reads;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EpochSnapshot that = (EpochSnapshot) o;
            return epoch == that.epoch && syncStatus == that.syncStatus && received == that.received && acknowledged == that.acknowledged && reads == that.reads;
        }

        public int hashCode()
        {
            return Objects.hash(epoch, syncStatus, received, acknowledged, reads);
        }

        public String toString()
        {
            return "EpochSnapshot{" +
                   "epoch=" + epoch +
                   ", syncStatus=" + syncStatus +
                   ", received=" + received +
                   ", acknowledged=" + acknowledged +
                   ", reads=" + reads +
                   '}';
        }

        public static EpochSnapshot completed(long epoch)
        {
            return new EpochSnapshot(epoch, SyncStatus.COMPLETED, ResultStatus.SUCCESS, ResultStatus.SUCCESS, ResultStatus.SUCCESS);
        }

        public static EpochSnapshot notStarted(long epoch)
        {
            return new EpochSnapshot(epoch, SyncStatus.NOT_STARTED, ResultStatus.SUCCESS, ResultStatus.SUCCESS, ResultStatus.SUCCESS);
        }
    }

    @VisibleForTesting
    public EpochSnapshot getEpochSnapshot(long epoch)
    {
        EpochState state;
        // If epoch truncate happens then getting the epoch again will recreate an empty one
        synchronized (epochs)
        {
            if (epoch < epochs.minEpoch() || epoch > epochs.maxEpoch())
                return null;

            state = getOrCreateEpochState(epoch);
        }
        return new EpochSnapshot(state);
    }

    @VisibleForTesting
    public long minEpoch()
    {
        return epochs.minEpoch();
    }

    @VisibleForTesting
    public long maxEpoch()
    {
        return epochs.maxEpoch();
    }

    /**
     * The callback is resolved while holding the object lock, which can cause the future chain to resolve while also
     * holding the lock!  This behavior is exposed for tests and is unsafe due to the lock behind held while resolving
     * the callback
     */
    @VisibleForTesting
    public Future<Void> unsafeLocalSyncNotified(long epoch)
    {
        AsyncPromise<Void> promise = new AsyncPromise<>();
        getOrCreateEpochState(epoch).localSyncNotified().addCallback((result, failure) -> {
            if (failure != null) promise.tryFailure(failure);
            else promise.trySuccess(result);
        });
        return promise;
    }
}
