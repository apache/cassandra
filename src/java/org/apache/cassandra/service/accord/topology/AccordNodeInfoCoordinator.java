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

package org.apache.cassandra.service.accord.topology;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Long2LongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.TopologyListener;
import accord.local.Node;
import accord.topology.Topology;
import accord.utils.Invariants;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.AccordNodeInfo;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.AccordNodeInfo.Delta;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.Status;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.transformations.AccordChangeDownStatus;
import org.apache.cassandra.tcm.transformations.AccordChangeNodeInfo;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Listens to availability status of peers and updates tcm fast path data accordingly
 *
 * TODO (expected): reach better consensus decisions around up/down status, rather than use hysteresis and permitting every node to have a POV
 */
public class AccordNodeInfoCoordinator implements TopologyListener, IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(AccordNodeInfoCoordinator.class);

    final Node.Id localId;
    final AccordEndpointMap endpointMapper;

    private boolean started = false;
    boolean shutdown = false;
    private Long2LongHashMap lastUpdated = new Long2LongHashMap(1, 0.65f, -1L, false);
    private long latestEpoch;

    public AccordNodeInfoCoordinator(Node.Id localId, AccordEndpointMap endpointMapper)
    {
        this.localId = localId;
        this.endpointMapper = endpointMapper;
    }

    public void start()
    {
        synchronized (this)
        {
            Invariants.require(!started, "Already started");
            started = true;
        }

        registerListeners();
        // make sure we are marked SHUTDOWN until we are ready to process any requests
        declareSelf(Delta.status(Status.SHUTDOWN), Long.MAX_VALUE);

        long updateDelayMillis = getAccordFastPathUpdateDelayMillis();
        if (updateDelayMillis < 0)
            return;

        scheduleMaintenanceTask(updateDelayMillis);
    }

    void registerListeners()
    {
        Gossiper.instance.register(this);
        StorageService.instance.addPreShutdownHook(this::onShutdown);
        AccordService.unsafeInstance().topology().addListener(this);
    }

    void updateDownStatusAsync(Node.Id node, Status status, long updateTimeMillis, long updateDelayMillis)
    {
        Stage.MISC.execute(() -> updateDownStatus(node, status, updateTimeMillis, updateDelayMillis, Long.MAX_VALUE));
    }

    void updateDownStatus(Node.Id node, Status status, long updateTimeMillis, long updateDelayMillis, long deadlineLimitNanos)
    {
        ClusterMetadataService.instance().commit(new AccordChangeDownStatus(node, status, updateTimeMillis, updateDelayMillis),
                                                 metadata -> {
                                                     if (node.equals(localId))
                                                     {
                                                         AccordNodeInfo updated = metadata.accordNodeInfos.get(localId);
                                                         if (updated == null || updated.status() != status)
                                                             logger.error("Our update {} was lost or incorrectly applied: {}", status, updated);
                                                     }
                                                     return true;
                                                 }, ((code, message) -> {
            if (node.equals(localId))
                logger.error("Could not apply update {}: ({}) {}", status, code, message);
            return false;
        }), deadlineLimitNanos);
    }

    void declareSelf(Delta delta, long updatedTimeMillis, long deadlineLimitNanos)
    {
        ClusterMetadataService.instance().commit(new AccordChangeNodeInfo(localId, delta, updatedTimeMillis),
                                                 metadata -> {
                                                     AccordNodeInfo updated = metadata.accordNodeInfos.get(localId);
                                                     if (updated == null || !delta.hasNoEffectUpon(updated))
                                                         logger.error("Our update {} was lost or incorrectly applied: {}", delta, updated);
                                                     return true;
                                                 }, ((code, message) -> {
            logger.error("Could not apply update {}: ({}) {}", delta, code, message);
            return false;
        }), deadlineLimitNanos);
    }

    @Nullable
    Boolean isAlive(Node.Id node)
    {
        InetAddressAndPort endpoint = endpointMapper.mappedEndpointOrNull(node, this);
        if (endpoint == null)
            return null;
        return FailureDetector.instance.isAlive(endpoint);
    }

    @Override
    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {
        Node.Id node = endpointMapper.mappedIdOrNull(endpoint);
        if (node != null)
            maybeUpdateDownStatus(node, Status.NORMAL);
    }

    @Override
    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        Node.Id node = endpointMapper.mappedIdOrNull(endpoint);
        if (node != null)
            maybeUpdateDownStatus(node, Status.MAYBE_DOWN);
    }

    long getAccordFastPathUpdateDelayMillis()
    {
        return DatabaseDescriptor.getAccordFastPathUpdateDelayMillis();
    }

    boolean isShutdown()
    {
        return shutdown || StorageService.instance.isShutdown();
    }

    ClusterMetadata currentMetadata()
    {
        return ClusterMetadata.current();
    }

    // overridable for tests that override currentMetadata()
    boolean isMember(ClusterMetadata metadata)
    {
        return metadata.directory.peerIds().contains(new NodeId(localId.id));
    }

    public static AccordNodeInfoCoordinator create(Node.Id localId, AccordEndpointMap endpointMapper)
    {
        return new AccordNodeInfoCoordinator(localId, endpointMapper);
    }

    void maybeUpdateDownStatus(Node.Id node, Status newStatus)
    {
        long nowMillis = Clock.Global.currentTimeMillis();
        long delayMillis = getAccordFastPathUpdateDelayMillis();

        synchronized (this)
        {
            if (isShutdown())
                return;

            // don't schedule updates for nodes we don't share shards with
            long lastUpdate = lastUpdated.get(node.id);
            if (lastUpdate < 0)
                return;

            if (lastUpdate + delayMillis >= nowMillis)
                return; // if we've already submitted an update, don't try again

            // update our timestamp regardless, to avoid updating status unnecessarily
            lastUpdated.put(node.id, nowMillis);
        }

        ClusterMetadata metadata = currentMetadata();
        if (shouldUpdatePeerStatus(metadata, node, newStatus, nowMillis, delayMillis))
            updateDownStatusAsync(node, newStatus, nowMillis, delayMillis);
    }

    private static boolean shouldUpdatePeerStatus(ClusterMetadata metadata, Node.Id peer, Status newStatus, long nowMillis, long delayMillis)
    {
        if (metadata.accordNodeInfos.status(peer) == Status.SHUTDOWN)
            return false;

        return metadata.accordNodeInfos.shouldUpdateDownStatus(peer, newStatus, nowMillis, delayMillis);
    }

    private void scheduleMaintenanceTask(long delayMillis)
    {
        ScheduledExecutors.scheduledTasks.scheduleSelfRecurring(this::maintenance, delayMillis, TimeUnit.MILLISECONDS);
    }

    synchronized void maintenance()
    {
        if (isShutdown())
            return;

        long nowMillis = Clock.Global.currentTimeMillis();
        long delayMillis = getAccordFastPathUpdateDelayMillis();
        try
        {
            ClusterMetadata metadata = currentMetadata();
            Long2LongHashMap.EntryIterator iterator = lastUpdated.entrySet().iterator();
            while (iterator.hasNext())
            {
                iterator.next();
                if (iterator.getLongValue() + delayMillis >= nowMillis)
                    continue;

                Node.Id peer = new Node.Id((int)iterator.getLongKey());
                Boolean isAlive = isAlive(peer);
                if (isAlive == null)
                    continue;

                Status inferredStatus = isAlive ? Status.NORMAL : Status.MAYBE_DOWN;
                if (shouldUpdatePeerStatus(metadata, peer, inferredStatus, nowMillis, delayMillis))
                {
                    iterator.setValue(nowMillis);
                    updateDownStatusAsync(peer, inferredStatus, nowMillis, delayMillis);
                }
            }
        }
        finally
        {
            scheduleMaintenanceTask(delayMillis);
        }
    }

    void onAlive(Node.Id node)
    {
        maybeUpdateDownStatus(node, Status.NORMAL);
    }

    public void onDead(Node.Id node)
    {
        maybeUpdateDownStatus(node, Status.MAYBE_DOWN);
    }

    public void onShutdown()
    {
        synchronized (this)
        {
            shutdown = true;
        }

        // Best effort: the CMS may be unreachable or slow, which should not unduly delay or interfere with a clean shutdown
        try { declareSelf(Delta.status(Status.SHUTDOWN), nanoTime() + TimeUnit.SECONDS.toNanos(15L)); }
        catch (Throwable t) { logger.warn("Failed to declareSelf SHUTDOWN", t); }
    }

    /**
     * Our command stores accept at least some requests: we will receive and act on transactions, but must not be read
     * from or waited for until {@link #declareReady()}. Only the node itself may say this about itself.
     */
    public void declareStartedUnreadable()
    {
        declareSelf(Delta.status(Status.NORMAL).combine(Delta.unreadable(true)), Long.MAX_VALUE);
    }

    /**
     * Accord is ready to serve: clear the unreadable bit. Only the node itself may do this.
     */
    public void declareReady()
    {
        declareSelf(Delta.status(Status.NORMAL).combine(Delta.unreadable(false)), Long.MAX_VALUE);
    }

    private void declareSelf(Delta delta, long deadlineLimitNanos)
    {
        long nowMillis = Clock.Global.currentTimeMillis();
        ClusterMetadata metadata = currentMetadata();
        if (!isMember(metadata))
            return;

        AccordNodeInfo current = metadata.accordNodeInfos.get(localId);
        if (AccordNodeInfos.supportsExtendedNodeInfo(metadata))
        {
            if (current == null || !delta.hasNoEffectUpon(current))
                declareSelf(delta, nowMillis, deadlineLimitNanos);
        }
        else
        {
            Status newStatus = delta.status();
            if ((newStatus == Status.NORMAL || newStatus == Status.SHUTDOWN) && (current == null || current.status() != newStatus))
                updateDownStatus(localId, newStatus, nowMillis, 0, deadlineLimitNanos);
        }
    }

    synchronized void updatePeers(Topology topology)
    {
        if (topology.epoch() <= latestEpoch)
            return;

        Long2LongHashMap refresh = new Long2LongHashMap(1, 0.65f, -1L, false);
        topology.forEachOn(localId, (shard, index) -> {
            for (Node.Id id : shard.nodes)
                refresh.put(id.id, lastUpdated.getOrDefault(id.id, 0));
        });
        refresh.remove(localId.id);

        lastUpdated = refresh;
        latestEpoch = topology.epoch();
    }

    @VisibleForTesting
    synchronized boolean isPeer(Node.Id node)
    {
        return lastUpdated.containsKey(node.id);
    }

    @Override
    public void onReceived(Topology topology)
    {
        updatePeers(topology);
    }
}
