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
import java.util.Set;
import java.util.function.BiConsumer;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.TopologyListener;
import accord.api.TopologyService;
import accord.local.Node;
import accord.topology.Topology;
import accord.topology.TopologyRetiredException;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults.SettableByCallback;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Simulate;

import static org.apache.cassandra.utils.Simulate.With.MONITORS;

@Simulate(with=MONITORS)
public class AccordTopologyService implements TopologyService, TopologyListener
{
    public static final Logger logger = LoggerFactory.getLogger(AccordTopologyService.class);

    // TODO (expected): move syncPropagator and watermarkCollector out of this class (and merge them)
    private final AccordSyncPropagator syncPropagator;
    private final WatermarkCollector watermarkCollector;

    private SortedArrayList<Node.Id> previouslyRemovedIds = SortedArrayList.ofSorted();

    private enum State { INITIALIZED, STARTED, SHUTDOWN }

    @GuardedBy("this")
    private State state = State.INITIALIZED;

    public AccordTopologyService(Node.Id node, AccordEndpointMapper endpointMapper, MessageDelivery messagingService, ScheduledExecutorPlus scheduledTasks)
    {
        this.syncPropagator = new AccordSyncPropagator(node, endpointMapper, messagingService, scheduledTasks);
        this.watermarkCollector = new WatermarkCollector();
    }

    public AccordTopologyService(Node.Id node, AccordEndpointMapper endpointMapper)
    {
        this(node, endpointMapper, MessagingService.instance(), ScheduledExecutors.scheduledTasks);
    }

    /**
     * On restart, loads topologies. On bootstrap, discovers existing topologies and initializes the node.
     */
    public void onStartup(Node node)
    {
        SortedArrayList<Node.Id> removed = node.topology().current().removedIds();
        synchronized (this)
        {
            Invariants.require(state == State.INITIALIZED, "Expected state to be INITIALIZED but was %s", state);
            state = State.STARTED;
            previouslyRemovedIds = removed;
        }
        node.topology().addListener(watermarkCollector);
        node.topology().addListener(syncPropagator);
        syncPropagator.onNodesRemoved(removed);
    }

    public synchronized void shutdown()
    {
        state = State.SHUTDOWN;
    }

    @Override
    public void onReceived(Topology topology)
    {
        SortedArrayList<Node.Id> newlyRemoved;
        synchronized (this)
        {
            newlyRemoved = topology.removedIds().without(previouslyRemovedIds);
            previouslyRemovedIds = topology.removedIds().with(previouslyRemovedIds);
        }
        syncPropagator.onNodesRemoved(newlyRemoved);
    }

    @Override
    public AsyncResult<Topology> fetchTopologyForEpoch(long epoch)
    {
        SettableByCallback<Topology> result = new SettableByCallback<>();
        fetchTopologyAsync(epoch, result);
        return result;
    }

    private void fetchTopologyAsync(long epoch, BiConsumer<? super Topology, ? super Throwable> onResult)
    {
        // It's not safe for this to block on CMS so for now pick a thread pool to handle it
        Stage.ACCORD_MIGRATION.execute(() -> {
            ClusterMetadata metadata = ClusterMetadata.current();
            try
            {
                if (metadata.epoch.getEpoch() < epoch)
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
            if (metadata.epoch.getEpoch() == epoch)
            {
                Topology topology = AccordTopology.createAccordTopology(metadata);
                onResult.accept(topology, null);
                return;
            }

            Set<InetAddressAndPort> peers = new HashSet<>(metadata.directory.allJoinedEndpoints());
            peers.remove(FBUtilities.getBroadcastAddressAndPort());
            if (peers.isEmpty())
            {
                onResult.accept(null, new TopologyRetiredException("No joined nodes to query; latest epoch was not the one requested", null));
                return;
            }

            // Fetching only one epoch here since later epochs might have already been requested concurrently
            FetchTopologies.fetch(SharedContext.Global.instance, peers, epoch, epoch)
                           .addCallback((success, fail) -> {
                               if (fail != null) onResult.accept(null, fail);
                               else if (success.hasEpoch(epoch)) onResult.accept(success.get(epoch), null);
                               else if (success.min > epoch) onResult.accept(null, new TopologyRetiredException("Could not fetch epoch " + epoch + " from peers; too far ahead", null));
                               else onResult.accept(null, new RuntimeException("Could not yet retrieve epoch " + epoch));
                           });
        });
    }

    @VisibleForTesting
    public AccordSyncPropagator syncPropagator()
    {
        return syncPropagator;
    }

    public WatermarkCollector watermarkCollector()
    {
        return watermarkCollector;
    }
}
