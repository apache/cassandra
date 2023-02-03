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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.topology.Topology;

/**
 * Currently a stubbed out config service meant to be triggered from a dtest
 */
public class AccordConfigurationService implements ConfigurationService
{
    private final Node.Id localId;
    private final List<Listener> listeners = new ArrayList<>();
    private final List<Topology> epochs = new ArrayList<>();

    public AccordConfigurationService(Node.Id localId)
    {
        this.localId = localId;
        epochs.add(Topology.EMPTY);
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    @Override
    public synchronized Topology currentTopology()
    {
        return epochs.get(epochs.size() - 1);
    }

    @Override
    public Topology getTopologyForEpoch(long epoch)
    {
        return epochs.get((int) epoch);
    }

    @Override
    public synchronized void fetchTopologyForEpoch(long epoch)
    {
        Topology current = currentTopology();
        Preconditions.checkArgument(epoch > current.epoch(), "Requested to fetch epoch %d which is <= %d (current epoch)", epoch, current.epoch());
        while (current.epoch() < epoch)
        {
            current = AccordTopologyUtils.createTopology(epochs.size());
            unsafeAddEpoch(current);
        }
    }

    @Override
    public void acknowledgeEpoch(long epoch)
    {
        Topology acknowledged = getTopologyForEpoch(epoch);
        for (Node.Id node : acknowledged.nodes())
        {
            if (node.equals(localId))
                continue;
            for (Listener listener : listeners)
                listener.onEpochSyncComplete(node, epoch);
        }
    }

    public synchronized void createEpochFromConfig()
    {
        Topology current = currentTopology();
        Topology topology = AccordTopologyUtils.createTopology(epochs.size());
        if (current.equals(topology.withEpoch(current.epoch()))) return;
        unsafeAddEpoch(topology);
    }

    private void unsafeAddEpoch(Topology topology)
    {
        epochs.add(topology);
        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);

        // TODO: This is a hack to enable simplistic cluster reuse for TxnAuthTest, AccordCQLTest, etc.
        // Since we don't have a dist sys that sets this up, we have to just lie...
        EndpointMapping.knownIds().forEach(id -> {
            for (Listener listener : listeners)
                listener.onEpochSyncComplete(id, topology.epoch());
        });
    }
}
