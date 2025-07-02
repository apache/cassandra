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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import accord.api.Agent;
import accord.impl.AbstractConfigurationService;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.SortedListSet;
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
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Simulate;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.service.accord.AccordTopology.tcmIdToAccord;
import static org.apache.cassandra.utils.Simulate.With.MONITORS;

// TODO (desired): listen to FailureDetector and rearrange fast path accordingly
@Simulate(with=MONITORS)
public class AccordConfigurationService extends AbstractConfigurationService<AccordConfigurationService.EpochState, AccordConfigurationService.EpochHistory> implements AccordEndpointMapper, AccordSyncPropagator.Listener, Shutdownable
{
    public static final Logger logger = LoggerFactory.getLogger(AccordConfigurationService.class);
    private final AccordSyncPropagator syncPropagator;
    public final WatermarkCollector watermarkCollector;

    private enum State { INITIALIZED, STARTED, SHUTDOWN }

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

    public AccordConfigurationService(Node.Id node, Agent agent, MessageDelivery messagingService, IFailureDetector failureDetector, ScheduledExecutorPlus scheduledTasks)
    {
        super(node, agent);
        this.syncPropagator = new AccordSyncPropagator(localId, this, messagingService, failureDetector, scheduledTasks, this);
        this.watermarkCollector = new WatermarkCollector();
        listeners.add(watermarkCollector);
    }

    public AccordConfigurationService(Node.Id node, Agent agent)
    {
        this(node, agent, MessagingService.instance(), FailureDetector.instance, ScheduledExecutors.scheduledTasks);
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
        state = State.STARTED;

        // for all nodes removed, or pending removal, mark them as removed, so we don't wait on their replies
        Map<Node.Id, Long> removedNodes = mapping.removedNodes();
        for (Map.Entry<Node.Id, Long> e : removedNodes.entrySet())
            onNodeRemoved(e.getValue(), currentTopology(), e.getKey());
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
        Topology topology = AccordTopology.createAccordTopology(metadata);
        if (topology.isEmpty() && isEmpty())
            return;

        updateMapping(metadata);
        if (Invariants.isParanoid())
        {
            for (Node.Id node : topology.nodes())
            {
                if (mapping.mappedEndpointOrNull(node) == null)
                    throw new IllegalStateException(String.format("Epoch %d has node %s but mapping does not!", topology.epoch(), node));
            }
        }
        reportTopology(topology);
        Set<Node.Id> stillLiveNodes = metadata.directory.states.entrySet()
                                                               .stream()
                                                               .filter(e -> e.getValue() != NodeState.LEFT && e.getValue() != NodeState.LEAVING)
                                                               .map(e -> tcmIdToAccord(e.getKey()))
                                                               .collect(Collectors.toSet());
        if (epochs.lastAcknowledged() >= topology.epoch()) checkIfNodesRemoved(topology, stillLiveNodes);
        else epochs.acknowledgeFuture(topology.epoch()).invokeIfSuccess(() -> checkIfNodesRemoved(topology, stillLiveNodes));
    }

    private void checkIfNodesRemoved(Topology topology, Set<Node.Id> stillLiveNodes)
    {
        long minEpoch = epochs.minEpoch();
        if (minEpoch == 0 || topology.epoch() <= minEpoch) return;
        Topology previous = getTopologyForEpoch(topology.epoch() - 1);
        // for all nodes removed, or pending removal, mark them as removed so we don't wait on their replies
        Set<Node.Id> removedNodes = Sets.difference(previous.nodes(), topology.nodes());
        removedNodes = Sets.filter(removedNodes, id -> !stillLiveNodes.contains(id));
        // TODO (desired, efficiency): there should be no need to notify every epoch for every removed node
        for (Node.Id removedNode : removedNodes)
        {
            if (topology.epoch() >= minEpoch)
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
            // Accord has never been enabled for this cluster.
            if (epochs.isEmpty() && !metadata.schema.hasAccordKeyspaces())
                return;

            // On first boot, we have 2 options:
            //
            //  - we can start listening to TCM _before_ we replay topologies
            //  - we can start listening to TCM _after_ we replay topologies
            //
            // If we start listening to TCM _before_ we replay topologies from other nodes,
            // we may end up in a situation where TCM reports metadata that would create an
            // `epoch - 1` epoch state that is not associated with any topologies, and
            // therefore should not be listened upon.
            //
            // If we start listening to TCM _after_ we replay topologies, we may end up in a
            // situation where TCM reports metadata that is 1 (or more) epochs _ahead_ of the
            // last known epoch. Previous implementations were using TCM peer catch up, which
            // could have resulted in gaps.
            //
            // Current protocol solves both problems by _first_ replaying topologies form peers,
            // then subscribing to TCM _and_, if there are still any gaps, filling them again.
            // However, it still has a slight chance of creating an `epoch - 1` epoch state
            // not associated with any topologies, which under "right" circumstances could
            // have been waited upon with `epochReady`. This check precludes creation of this
            // epoch: by the time this code can be called, remote topology replay is already
            // done, so TCM listener will only report epochs that are _at least_ min epoch.
            if (epochs.maxEpoch() == 0 || epochs.minEpoch() == metadata.epoch.getEpoch())
            {
                getOrCreateEpochState(epoch);  // touch epoch state so subsequent calls see it
                reportMetadata(metadata);
                return;
            }
        }

        getOrCreateEpochState(epoch - 1).acknowledged().invokeIfSuccess(() -> reportMetadata(metadata));
    }

    private final Map<Long, Future<Void>> pendingTopologies = new ConcurrentHashMap<>();

    @Override
    public void fetchTopologyForEpoch(long epoch)
    {
        long minEpoch = currentEpoch() + 1;
        // Find and fetch all epochs in-between
        for (long i = minEpoch; i <= epoch; ++i)
            fetchTopologyInternal(i);
    }

    private static final Object Success = new Object();

    protected void fetchTopologyInternal(long epoch)
    {
        pendingTopologies.computeIfAbsent(epoch, (epoch_) -> {
            AsyncPromise<Void> future = new AsyncPromise<>();
            fetchTopologyAsync(epoch_,
                               (success, throwable) -> {
                                   Future<Void> removed = pendingTopologies.remove(epoch_);
                                   Invariants.require(future == removed, "%s should be equal to %s", future, removed);
                                   if (success != null)
                                       future.setSuccess(null);
                                   else
                                   {
                                       future.setFailure(Invariants.nonNull(throwable));
                                       fetchTopologyForEpoch(epoch_);
                                   }
                                  });
            return future;
        });
    }

    private void fetchTopologyAsync(long epoch, BiConsumer<Object, ? super Throwable> onResult)
    {
        // It's not safe for this to block on CMS so for now pick a thread pool to handle it
        Stage.ACCORD_MIGRATION.execute(() -> {
            try
            {
                if (ClusterMetadata.current().epoch.getEpoch() < epoch)
                    ClusterMetadataService.instance().fetchLogFromCMS(Epoch.create(epoch));
            }
            catch (Throwable t)
            {
                onResult.accept(null, t);
                return;
            }

            // In most cases, after fetching log from CMS, we will be caught up to the required epoch.
            // This TCM will also notify Accord via reportMetadata, so we do not need to fetch topologies.
            // If metadata has reported has skipped one or more epochs, and is _ahead_ of the requested epoch,
            // we need to fetch topologies from peers to fill in the gap.
            ClusterMetadata metadata = ClusterMetadata.current();
            if (metadata.epoch.getEpoch() == epoch)
            {
                onResult.accept(Success, null);
                return;
            }

            Set<InetAddressAndPort> peers = new HashSet<>(metadata.directory.allJoinedEndpoints());
            peers.remove(FBUtilities.getBroadcastAddressAndPort());
            if (peers.isEmpty())
            {
                onResult.accept(Success, null);
                return;
            }

            // Fetching only one epoch here since later epochs might have already been requested concurrently
            FetchTopologies.fetch(SharedContext.Global.instance, peers, epoch, epoch)
                           .addCallback((topologyRange, t) -> {
                               if (t != null)
                               {
                                   if (currentEpoch() >= epoch)
                                       onResult.accept(Success, null);
                                   else
                                       onResult.accept(null, t);
                               }
                               else
                               {
                                   topologyRange.forEach(this::reportTopology, epoch, 1);
                                   onResult.accept(Success, null);
                               }
                           });
        });
    }

    @Override
    protected Executor executor()
    {
        return Stage.ACCORD_MIGRATION::execute;
    }

    @Override
    public void reportTopology(Topology topology, boolean isLoad, boolean startSync)
    {
        long tcmEpoch = ClusterMetadata.current().epoch.getEpoch();
        Invariants.require(topology.epoch() <= tcmEpoch,
                           "Reported topology %s not known to TCM", topology.epoch(), tcmEpoch);
        super.reportTopology(topology, isLoad, startSync);
    }

    @Override
    protected void localSyncComplete(Topology topology, boolean startSync)
    {
        long epoch = topology.epoch();
        EpochState epochState = getOrCreateEpochState(epoch);
        synchronized (this)
        {
            if (!startSync || epochState.syncStatus != SyncStatus.NOT_STARTED)
                return;

            epochState.setSyncStatus(SyncStatus.NOTIFYING);
        }

        Set<Node.Id> notify = SortedListSet.allOf(topology.nodes());
        notify.remove(localId);
        syncPropagator.reportSyncComplete(epoch, notify, localId);
    }

    @Override
    public synchronized void onEndpointAck(Node.Id id, long epoch)
    {
    }

    @Override
    public void onComplete(long epoch)
    {
        if (epochs.wasTruncated(epoch))
            return;

        EpochState epochState = getOrCreateEpochState(epoch);
        synchronized (this)
        {
            epochState.setSyncStatus(SyncStatus.COMPLETED);
        }
    }

    @Override
    protected synchronized void receiveRemoteSyncCompletePreListenerNotify(Node.Id node, long epoch)
    {
    }

    @Override
    public void reportEpochClosed(Ranges ranges, long epoch)
    {
        checkStarted();
        EpochHistory epochs = this.epochs;
        if (epoch < minEpoch() || epochs.wasTruncated(epoch))
            return;

        syncPropagator.reportClosed(epoch, mapping.nodes(), ranges);
    }

    @VisibleForTesting
    public AccordSyncPropagator syncPropagator()
    {
        return syncPropagator;
    }

    @Override
    public void reportEpochRetired(Ranges ranges, long epoch)
    {
        if (epochs.wasTruncated(epoch))
            return;

        checkStarted();
        // TODO (expected): ensure we aren't fetching a truncated epoch; otherwise this should be non-null
        syncPropagator.reportRetired(epoch, mapping.nodes(), ranges);
    }

    @Override
    public void receiveClosed(Ranges ranges, long epoch)
    {
        super.receiveClosed(ranges, epoch);
    }

    @Override
    public void receiveRetired(Ranges ranges, long epoch)
    {
        super.receiveRetired(ranges, epoch);
    }

    @Override
    public void reportEpochRemoved(long epoch)
    {
        logger.info("Epoch removed, truncated epochs until {}", epoch);
        epochs.truncateUntil(epoch);
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
        getOrCreateEpochState(epoch).localSyncNotified().begin((result, failure) -> {
            if (failure != null) promise.tryFailure(failure);
            else promise.trySuccess(result);
        });
        return promise;
    }
}
