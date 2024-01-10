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

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordFastPath.NodeInfo;
import org.apache.cassandra.service.accord.AccordFastPath.Status;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.transformations.ReconfigureAccordFastPath;
import org.apache.cassandra.utils.Clock;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Listens to availability status of peers and updates tcm fast path data accordingly
 */
public abstract class AccordFastPathCoordinator implements ChangeListener, ConfigurationService.Listener
{
    private static final AsyncResult<Void> SUCCESS = AsyncResults.success(null);

    private static class PeerStatus
    {
        final Node.Id peer;
        final Status status;

        public PeerStatus(Node.Id peer, Status status)
        {
            this.peer = peer;
            this.status = status;
        }

        boolean shouldUpdateFastPath(AccordFastPath fastPath, long nowMillis, long delayMillis)
        {
            NodeInfo info = fastPath.info.get(peer);

            if (info == null)
                return status != Status.NORMAL;

            if (info.status == status || info.status == Status.SHUTDOWN)
                return false;

            return nowMillis - info.updated > delayMillis;
        }
    }

    private static class Peers
    {
        static final Peers EMPTY = new Peers(0, ImmutableSet.of(), Collections.emptyMap());
        final long epoch;
        final ImmutableSet<Node.Id> peers;
        final Map<Node.Id, PeerStatus> statusMap;

        public Peers(long epoch, ImmutableSet<Node.Id> peers, Map<Node.Id, PeerStatus> statusMap)
        {
            this.epoch = epoch;
            this.peers = peers;
            this.statusMap = statusMap;
        }

        public boolean contains(Node.Id node)
        {
            return peers.contains(node);
        }

        public static Peers from(Node.Id localId, Topology topology, Peers prev)
        {
            Set<Node.Id> peers = new HashSet<>();
            topology.forEachOn(localId, (shard, index) -> peers.addAll(shard.nodes));
            peers.remove(localId);

            Map<Node.Id, PeerStatus> statusMap = new HashMap<>();
            for (Node.Id peer : peers)
            {
                PeerStatus status = prev.statusMap.get(peer);
                if (status != null)
                    statusMap.put(peer, status);
            }

            return new Peers(topology.epoch(), ImmutableSet.copyOf(peers), statusMap);
        }

        public PeerStatus onUpdate(Node.Id node, Status status)
        {
            Invariants.checkArgument(contains(node));
            PeerStatus peerStatus = new PeerStatus(node, status);
            statusMap.put(node, peerStatus);
            return peerStatus;
        }

        public Iterable<PeerStatus> statusIterable()
        {
            return statusMap.values();
        }
    }

    private boolean receivedShutdownSignal = false;
    private volatile Epoch startupEpoch = null;
    private volatile boolean issuedStartupUpdate = false;
    private boolean hasRegistered = false;
    private Peers peers = Peers.EMPTY;
    private final Node.Id localId;

    public AccordFastPathCoordinator(Node.Id localId)
    {
        this.localId = localId;
    }

    private boolean isShutdown(AccordFastPath fastPath)
    {
        NodeInfo info = fastPath.info.get(localId);
        return info != null && info.status == Status.SHUTDOWN;
    }

    public synchronized void start()
    {
        if (hasRegistered)
            return;

        ClusterMetadata cm = currentMetadata();
        startupEpoch = cm.epoch;
        registerAsListener();

        // TODO: start check routine

        hasRegistered = true;

        AccordFastPath fastPath = cm.accordFastPath;

        long updateDelayMillis = getAccordFastPathUpdateDelayMillis();
        if (isShutdown(fastPath))
        {
            updateFastPath(localId, Status.NORMAL, Clock.Global.currentTimeMillis(), updateDelayMillis);
            issuedStartupUpdate = true;
        }

        scheduleMaintenanceTask(updateDelayMillis);
    }

    abstract ClusterMetadata currentMetadata();
    abstract void registerAsListener();
    abstract void updateFastPath(Node.Id node, Status status, long updateTimeMillis, long updateDelayMillis);
    abstract long getAccordFastPathUpdateDelayMillis();

    private static class Impl extends AccordFastPathCoordinator implements IEndpointStateChangeSubscriber
    {
        private final AccordConfigurationService configService;

        public Impl(Node.Id localId, AccordConfigurationService configService)
        {
            super(localId);
            this.configService = configService;
        }

        @Override
        ClusterMetadata currentMetadata()
        {
            return ClusterMetadata.current();
        }

        @Override
        void registerAsListener()
        {
            Gossiper.instance.register(this);
            StorageService.instance.addPreShutdownHook(this::onShutdown);
            configService.registerListener(this);
        }

        @Override
        void updateFastPath(Node.Id node, Status status, long updateTimeMillis, long updateDelayMillis)
        {
            ClusterMetadataService.instance().commit(new ReconfigureAccordFastPath(node, status, updateTimeMillis, updateDelayMillis));
        }

        @Override
        long getAccordFastPathUpdateDelayMillis()
        {
            return DatabaseDescriptor.getAccordFastPathUpdateDelayMillis();
        }

        @Override
        public void onAlive(InetAddressAndPort endpoint, EndpointState state)
        {
            Node.Id node = configService.mappedIdOrNull(endpoint);
            if (node != null) onAlive(node);
        }

        @Override
        public void onDead(InetAddressAndPort endpoint, EndpointState state)
        {
            Node.Id node = configService.mappedIdOrNull(endpoint);
            if (node != null) onDead(node);
        }
    }

    public static AccordFastPathCoordinator create(Node.Id localId, AccordConfigurationService configService)
    {
        return new Impl(localId, configService);
    }

    synchronized void maybeUpdateFastPath(Node.Id node, Status status)
    {
        long nowMillis = Clock.Global.currentTimeMillis();
        long delayMillis = getAccordFastPathUpdateDelayMillis();

        // don't schedule updates for nodes we don't share shards with
        if (!peers.contains(node))
            return;

        PeerStatus peerStatus = peers.onUpdate(node, status);
        ClusterMetadata metadata = currentMetadata();
        if (peerStatus.shouldUpdateFastPath(metadata.accordFastPath, nowMillis, delayMillis))
            updateFastPath(node, status, nowMillis, delayMillis);
    }

    private void scheduleMaintenanceTask(long delayMillis)
    {
        ScheduledExecutors.scheduledTasks.schedule(this::maintenance, delayMillis, TimeUnit.MILLISECONDS);
    }

    synchronized void maintenance()
    {
        long nowMillis = Clock.Global.currentTimeMillis();
        long delayMillis = getAccordFastPathUpdateDelayMillis();
        try
        {
            ClusterMetadata metadata = currentMetadata();
            for (PeerStatus status : peers.statusIterable())
            {
                if (status.shouldUpdateFastPath(metadata.accordFastPath, nowMillis, delayMillis))
                    updateFastPath(status.peer, status.status, nowMillis, delayMillis);
            }
        }
        finally
        {
            scheduleMaintenanceTask(delayMillis);
        }
    }

    void onAlive(Node.Id node)
    {
        maybeUpdateFastPath(node, Status.NORMAL);
    }

    public void onDead(Node.Id node)
    {
        maybeUpdateFastPath(node, Status.UNAVAILABLE);
    }

    public void onShutdown()
    {
        synchronized (this)
        {
            receivedShutdownSignal = true;
        }

        updateFastPath(localId, Status.SHUTDOWN, Clock.Global.currentTimeMillis(), getAccordFastPathUpdateDelayMillis());
    }

    /**
     * In case we somehow missed that we've marked ourselves shutdown on startup
     */
    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        if (next.epoch.compareTo(startupEpoch) <= 0)
            return;

        if (!isShutdown(next.accordFastPath))
            return;

        synchronized (this)
        {
            if (receivedShutdownSignal || issuedStartupUpdate)
                return;
            issuedStartupUpdate = true;
        }

        updateFastPath(localId, Status.NORMAL, Clock.Global.currentTimeMillis(), getAccordFastPathUpdateDelayMillis());
    }

    synchronized void updatePeers(Topology topology)
    {
        if (topology.epoch() <= peers.epoch)
            return;

        peers = Peers.from(localId, topology, peers);
    }

    @VisibleForTesting
    synchronized boolean isPeer(Node.Id node)
    {
        return peers.contains(node);
    }

    @Override
    public AsyncResult<Void> onTopologyUpdate(Topology topology, boolean startSync)
    {
        updatePeers(topology);
        return SUCCESS;
    }

    @Override public void onRemoteSyncComplete(Node.Id node, long epoch) {}
    @Override public void truncateTopologyUntil(long epoch) {}
    @Override public void onEpochClosed(Ranges ranges, long epoch) {}
    @Override public void onEpochRedundant(Ranges ranges, long epoch) {}
}
