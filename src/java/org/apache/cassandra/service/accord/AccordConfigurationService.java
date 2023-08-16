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

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.AbstractConfigurationService;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.accord.AccordKeyspace.EpochDiskState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

// TODO: listen to FailureDetector and rearrange fast path accordingly
public class AccordConfigurationService extends AbstractConfigurationService<AccordConfigurationService.EpochState, AccordConfigurationService.EpochHistory> implements ChangeListener, AccordEndpointMapper, AccordSyncPropagator.Listener
{
    private static final Logger logger = LoggerFactory.getLogger(AccordConfigurationService.class);
    private final AccordSyncPropagator syncPropagator;

    private EpochDiskState diskState = EpochDiskState.EMPTY;

    private enum State { INITIALIZED, LOADING, STARTED }

    private State state = State.INITIALIZED;
    private volatile EndpointMapping mapping = EndpointMapping.EMPTY;

    public enum SyncStatus { NOT_STARTED, NOTIFYING, COMPLETED }

    static class EpochState extends AbstractConfigurationService.AbstractEpochState
    {
        SyncStatus syncStatus = SyncStatus.NOT_STARTED;
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

    public AccordConfigurationService(Node.Id node, MessageDelivery messagingService, IFailureDetector failureDetector)
    {
        super(node);
        this.syncPropagator = new AccordSyncPropagator(localId, this, messagingService, failureDetector, ScheduledExecutors.scheduledTasks, this);
    }

    public AccordConfigurationService(Node.Id node)
    {
        this(node, MessagingService.instance(), FailureDetector.instance);
    }

    @Override
    protected EpochHistory createEpochHistory()
    {
        return new EpochHistory();
    }

    public synchronized void start()
    {
        Invariants.checkState(state == State.INITIALIZED, "Expected state to be INITIALIZED but was %s", state);
        state = State.LOADING;
        updateMapping(ClusterMetadata.current());
        EndpointMapping snapshot = mapping;
        diskState = AccordKeyspace.loadTopologies(((epoch, topology, syncStatus, pendingSyncNotify, remoteSyncComplete, closed, redundant) -> {
            if (topology != null)
                reportTopology(topology, syncStatus == SyncStatus.NOT_STARTED);

            getOrCreateEpochState(epoch).setSyncStatus(syncStatus);
            if (syncStatus == SyncStatus.NOTIFYING)
            {
                // TODO (expected, correctness): since this is loading old topologies, might see nodes no longer present (host replacement, decom, shrink, etc.); attempt to remove unknown nodes
                syncPropagator.reportSyncComplete(epoch, Sets.filter(pendingSyncNotify, snapshot::containsId), localId);
            }

            remoteSyncComplete.forEach(id -> receiveRemoteSyncComplete(id, epoch));
            // TODO (now): disk doesn't get updated until we see our own notification, so there is an edge case where this instance notified others and fails in the middle, but Apply was already sent!  This could leave partial closed/redudant accross the cluster
            receiveClosed(closed, epoch);
            receiveRedundant(redundant, epoch);
        }));
        state = State.STARTED;
    }

    @Override
    public Node.Id mappedId(InetAddressAndPort endpoint)
    {
        return Invariants.nonNull(mapping.mappedId(endpoint), "Unable to map address %s to a Node.Id", endpoint);
    }

    @Override
    public InetAddressAndPort mappedEndpoint(Node.Id id)
    {
        return Invariants.nonNull(mapping.mappedEndpoint(id), "Unable to map node id %s to a InetAddressAndPort", id);
    }

    @VisibleForTesting
    EpochDiskState diskState()
    {
        return diskState;
    }

    @VisibleForTesting
    synchronized void updateMapping(EndpointMapping mapping)
    {
        if (mapping.epoch() > this.mapping.epoch())
            this.mapping = mapping;
    }

    synchronized void updateMapping(ClusterMetadata metadata)
    {
        updateMapping(AccordTopologyUtils.directoryToMapping(mapping, metadata.epoch.getEpoch(), metadata.directory));
    }

    private void reportMetadata(ClusterMetadata metadata)
    {
        Stage.MISC.submit(() -> {
            synchronized (AccordConfigurationService.this)
            {
                updateMapping(metadata);
                reportTopology(AccordTopologyUtils.createAccordTopology(metadata, this::isAccordManagedKeyspace));
            }
        });
    }

    private void maybeReportMetadata(ClusterMetadata metadata)
    {
        // don't report metadata until the previous one has been acknowledged
        synchronized (this)
        {
            long epoch = metadata.epoch.getEpoch();
            if (epochs.maxEpoch() == 0)
            {
                getOrCreateEpochState(epoch);  // touch epoch state so subsequent calls see it
                reportMetadata(metadata);
                return;
            }
            getOrCreateEpochState(epoch - 1).acknowledged().addCallback(() -> reportMetadata(metadata));
        }
    }

    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        maybeReportMetadata(prev);
        maybeReportMetadata(next);
    }

    @Override
    protected void fetchTopologyInternal(long epoch)
    {
        // TODO: need a non-blocking way to inform CMS of an unknown epoch
//        ClusterMetadataService.instance().maybeCatchup(Epoch.create(epoch));
    }

    @Override
    protected synchronized void localSyncComplete(Topology topology, boolean startSync)
    {
        long epoch = topology.epoch();
        EpochState epochState = getOrCreateEpochState(epoch);
        if (!startSync ||epochState.syncStatus != SyncStatus.NOT_STARTED)
            return;

        Set<Node.Id> notify = topology.nodes().stream().filter(i -> !localId.equals(i)).collect(Collectors.toSet());
        diskState = AccordKeyspace.setNotifyingLocalSync(epoch, notify, diskState);
        epochState.setSyncStatus(SyncStatus.NOTIFYING);
        syncPropagator.reportSyncComplete(epoch, notify, localId);
    }

    @Override
    public synchronized void onEndpointAck(Node.Id id, long epoch)
    {
        EpochState epochState = getOrCreateEpochState(epoch);
        if (epochState.syncStatus != SyncStatus.NOTIFYING)
            return;
        diskState = AccordKeyspace.markLocalSyncAck(id, epoch, diskState);
    }

    @Override
    public synchronized void onComplete(long epoch)
    {
        EpochState epochState = getOrCreateEpochState(epoch);
        epochState.setSyncStatus(SyncStatus.COMPLETED);
        diskState = AccordKeyspace.setCompletedLocalSync(epoch, diskState);
    }

    @Override
    protected synchronized void topologyUpdatePreListenerNotify(Topology topology)
    {
        if (state == State.STARTED)
            diskState = AccordKeyspace.saveTopology(topology, diskState);
    }

    @Override
    protected void receiveRemoteSyncCompletePreListenerNotify(Node.Id node, long epoch)
    {
        if (state == State.STARTED)
            diskState = AccordKeyspace.markRemoteTopologySync(node, epoch, diskState);
    }

    @Override
    public synchronized void reportEpochClosed(Ranges ranges, long epoch)
    {
        Invariants.checkState(state == State.STARTED);
        Topology topology = getTopologyForEpoch(epoch);
        syncPropagator.reportClosed(epoch, topology.nodes(), ranges);
    }

    @Override
    public synchronized void reportEpochRedundant(Ranges ranges, long epoch)
    {
        Invariants.checkState(state == State.STARTED);
        // TODO (expected): ensure we aren't fetching a truncated epoch; otherwise this should be non-null
        Topology topology = getTopologyForEpoch(epoch);
        syncPropagator.reportRedundant(epoch, topology.nodes(), ranges);
    }

    @Override
    public synchronized void receiveClosed(Ranges ranges, long epoch)
    {
        diskState = AccordKeyspace.markClosed(ranges, epoch, diskState);
        super.receiveClosed(ranges, epoch);
    }

    @Override
    public synchronized void receiveRedundant(Ranges ranges, long epoch)
    {
        diskState = AccordKeyspace.markClosed(ranges, epoch, diskState);
        super.receiveRedundant(ranges, epoch);
    }

    @Override
    protected void truncateTopologiesPreListenerNotify(long epoch)
    {
        Invariants.checkState(state == State.STARTED);
    }

    @Override
    protected void truncateTopologiesPostListenerNotify(long epoch)
    {
        if (state == State.STARTED)
            diskState = AccordKeyspace.truncateTopologyUntil(epoch, diskState);
    }

    @VisibleForTesting
    public static class EpochSnapshot
    {
        public enum ResultStatus
        {
            PENDING, SUCCESS, FAILURE;

            static ResultStatus of (AsyncResult<?> result)
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
    public synchronized EpochSnapshot getEpochSnapshot(long epoch)
    {
        if (epoch < epochs.minEpoch() || epoch > epochs.maxEpoch())
            return null;

        return new EpochSnapshot(getOrCreateEpochState(epoch));
    }

    @VisibleForTesting
    public synchronized long minEpoch()
    {
        return epochs.minEpoch();
    }

    @VisibleForTesting
    public synchronized long maxEpoch()
    {
        return epochs.maxEpoch();
    }

    @VisibleForTesting
    public synchronized Future<Void> localSyncNotified(long epoch)
    {
        AsyncPromise<Void> promise = new AsyncPromise<>();
        getOrCreateEpochState(epoch).localSyncNotified().addCallback((result, failure) -> {
            if (failure != null) promise.tryFailure(failure);
            else promise.trySuccess(result);
        });
        return promise;
    }

    public boolean isAccordManagedKeyspace(String keyspace)
    {
        // TODO (required, interop) : replace with schema flag or other mechanism for classifying accord keyspaces
        return !SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(keyspace);
    }
}
