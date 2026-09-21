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

import javax.annotation.Nullable;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.AccordNodeInfo;
import org.apache.cassandra.service.accord.topology.AccordNodeInfos.Status;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.util.concurrent.TimeUnit.MINUTES;

// essential AccordNodeInfos enriched with address, and indexed by both Id and address
public class AccordEndpointInfos implements AccordEndpointMap
{
    public static class AccordEndpointInfo extends AccordNodeInfo
    {
        public final Node.Id id;
        public final InetAddressAndPort endpoint;

        AccordEndpointInfo(Node.Id id, InetAddressAndPort endpoint, AccordNodeInfo nodeInfo)
        {
            super(nodeInfo);
            this.id = id;
            this.endpoint = endpoint;
        }
    }

    public static class Updateable implements AccordEndpointMap
    {
        private volatile AccordEndpointInfos mapping = EMPTY;

        @Nullable
        @Override
        public Node.Id mappedIdOrNull(InetAddressAndPort endpoint, @Nullable Object logIdentityIfUnmapped)
        {
            return mapping.mappedIdOrNull(endpoint, logIdentityIfUnmapped);
        }

        @Nullable
        @Override
        public InetAddressAndPort mappedEndpointOrNull(Node.Id id, @Nullable Object logIdentityIfUnmapped)
        {
            return mapping.mappedEndpointOrNull(id, logIdentityIfUnmapped);
        }

        @Override
        public NodeStatus nodeStatus(Node.Id id)
        {
            return mapping.nodeStatus(id);
        }

        @Override
        public synchronized void updateMapping(ClusterMetadata metadata)
        {
            if (needsUpdate(metadata.epoch) && (needsUpdate(metadata.directory.lastModified()) || needsUpdate(metadata.accordNodeInfos.lastModified())))
                this.mapping = AccordTopology.directoryToMapping(metadata.epoch.getEpoch(), metadata.directory, metadata.accordNodeInfos);
        }

        private boolean needsUpdate(Epoch test)
        {
            return test.getEpoch() > mapping.epoch();
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(AccordEndpointInfos.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, MINUTES);

    public static final AccordEndpointInfos EMPTY = new AccordEndpointInfos(0, new Int2ObjectHashMap<>(1, 0.65f, false), new Object2ObjectHashMap<>(1, 0.65f, false));
    private final long epoch;
    private final Int2ObjectHashMap<AccordEndpointInfo> byId;
    private final Object2ObjectHashMap<InetAddressAndPort, AccordEndpointInfo> byAddress;

    private AccordEndpointInfos(long epoch, Int2ObjectHashMap<AccordEndpointInfo> byId, Object2ObjectHashMap<InetAddressAndPort, AccordEndpointInfo> byAddress)
    {
        this.epoch = epoch;
        this.byId = byId;
        this.byAddress = byAddress;
    }

    long epoch()
    {
        return epoch;
    }

    public @Nullable AccordEndpointInfo info(Node.Id id)
    {
        return byId.get(id.id);
    }

    public @Nullable AccordEndpointInfo info(InetAddressAndPort endpoint)
    {
        return byAddress.get(endpoint);
    }

    @Override
    public Node.Id mappedIdOrNull(InetAddressAndPort endpoint, Object logIdentityIfUnmapped)
    {
        AccordEndpointInfo info = byAddress.get(endpoint);
        if (info != null)
            return info.id;
        if (logIdentityIfUnmapped == null) noSpamLogger.warn("Could not find Node.Id for endpoint {}", endpoint);
        else noSpamLogger.warn("Could not find Node.Id for endpoint {} on behalf of {}", endpoint, logIdentityIfUnmapped);
        return null;
    }

    @Override
    public InetAddressAndPort mappedEndpointOrNull(Node.Id id, Object logIdentityIfUnmapped)
    {
        AccordEndpointInfo info = byId.get(id.id);
        if (info != null)
            return info.endpoint;
        if (logIdentityIfUnmapped == null) noSpamLogger.warn("Could not find InetAddressAndPort for Node.Id {}", id);
        else noSpamLogger.warn("Could not find InetAddressAndPort for Node.Id {} on behalf of {}", id, logIdentityIfUnmapped);
        return null;
    }

    @Override
    public NodeStatus nodeStatus(Node.Id id)
    {
        AccordEndpointInfo info = byId.get(id.id);
        if (info == null)
            return NodeStatus.UNKNOWN;

        switch (info.status())
        {
            default: throw new UnhandledEnum(info.status());
            case REMOVED:
            case HARD_REMOVED:
                return NodeStatus.REMOVED;

            case SHUTDOWN:
            case MAYBE_DOWN:
                return NodeStatus.UNAVAILABLE;

            case UNKNOWN_STATUS:
            case NORMAL:
                break;
        }

        if (info.isUnreadable())
            return NodeStatus.UNREADABLE;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(info.endpoint);
        if (epState == null)
            return NodeStatus.UNKNOWN;

        return epState.isAlive() ? NodeStatus.HEALTHY : NodeStatus.UNAVAILABLE;
    }

    public static class Builder
    {
        private final long epoch;
        private final AccordNodeInfos nodeInfos;
        private final Int2ObjectHashMap<AccordEndpointInfo> byId;
        private final Object2ObjectHashMap<InetAddressAndPort, AccordEndpointInfo> byAddress;

        public Builder(long epoch, int capacity, AccordNodeInfos nodeInfos)
        {
            this.epoch = epoch;
            this.nodeInfos = nodeInfos;
            int adjustedCapacity = 1 + (int)(capacity / 0.65f);
            byId = new Int2ObjectHashMap<>(adjustedCapacity, 0.65f, false);
            byAddress = new Object2ObjectHashMap<>(adjustedCapacity, 0.65f, false);
        }

        public Builder add(InetAddressAndPort endpoint, Node.Id id)
        {
            return add(endpoint, id, nodeInfo(id, false));
        }

        public Builder removed(InetAddressAndPort endpoint, Node.Id id)
        {
            return add(endpoint, id, nodeInfo(id, true));
        }

        private AccordNodeInfo nodeInfo(Node.Id id, boolean isRemoved)
        {
            AccordNodeInfo info = nodeInfos.getOrDefault(id);
            if (isRemoved && !info.is(Status.HARD_REMOVED))
                info = new AccordNodeInfo(Status.REMOVED);
            return info;
        }

        private Builder add(InetAddressAndPort endpoint, Node.Id id, AccordNodeInfo nodeInfo)
        {
            Invariants.requireArgument(!byId.containsKey(id.id), "Mapping already exists for Node.Id %s", id);
            AccordEndpointInfo info = new AccordEndpointInfo(id, endpoint, nodeInfo);
            if (byAddress.containsKey(endpoint))
            {
                AccordEndpointInfo existing = byAddress.get(endpoint);
                if (existing.status().isRemoved())
                {
                    byAddress.remove(endpoint);
                    byAddress.put(endpoint, info);
                }
                else Invariants.require(nodeInfo.status().isRemoved(), "Mapping already exists for %s", endpoint);
            }
            else
            {
                byAddress.put(endpoint, info);
            }
            byId.put(id.id, info);
            return this;
        }

        public AccordEndpointInfos build()
        {
            return new AccordEndpointInfos(epoch, byId, byAddress);
        }
    }

    public static Builder builder(long epoch, int capacity, AccordNodeInfos nodeInfos)
    {
        return new Builder(epoch, capacity, nodeInfos);
    }
}
