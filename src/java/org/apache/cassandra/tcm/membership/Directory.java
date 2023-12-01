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

package org.apache.cassandra.tcm.membership;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.btree.BTreeBiMap;
import org.apache.cassandra.utils.btree.BTreeMap;
import org.apache.cassandra.utils.btree.BTreeMultimap;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.tcm.membership.NodeVersion.CURRENT;

public class Directory implements MetadataValue<Directory>
{
    public static final Serializer serializer = new Serializer();

    public static Directory EMPTY = new Directory();

    private final int nextId;
    private final Epoch lastModified;
    private final BTreeBiMap<NodeId, InetAddressAndPort> peers;
    private final BTreeMap<NodeId, Location> locations;
    public final BTreeMap<NodeId, NodeState> states;
    public final BTreeMap<NodeId, NodeVersion> versions;
    public final BTreeMap<NodeId, NodeAddresses> addresses;
    private final BTreeBiMap<NodeId, UUID> hostIds;
    private final BTreeMultimap<String, InetAddressAndPort> endpointsByDC;
    private final BTreeMap<String, Multimap<String, InetAddressAndPort>> racksByDC;
    public final NodeVersion clusterMinVersion;
    public final NodeVersion clusterMaxVersion;

    public Directory()
    {
        this(1,
             Epoch.EMPTY,
             BTreeBiMap.empty(),
             BTreeMap.empty(),
             BTreeMap.empty(),
             BTreeMap.empty(),
             BTreeBiMap.empty(),
             BTreeMap.empty(),
             BTreeMultimap.empty(),
             BTreeMap.empty());
    }

    private Directory(int nextId,
                      Epoch lastModified,
                      BTreeBiMap<NodeId, InetAddressAndPort> peers,
                      BTreeMap<NodeId, Location> locations,
                      BTreeMap<NodeId, NodeState> states,
                      BTreeMap<NodeId, NodeVersion> versions,
                      BTreeBiMap<NodeId, UUID> hostIds,
                      BTreeMap<NodeId, NodeAddresses> addresses,
                      BTreeMultimap<String, InetAddressAndPort> endpointsByDC,
                      BTreeMap<String, Multimap<String, InetAddressAndPort>> racksByDC)
    {
        this.nextId = nextId;
        this.lastModified = lastModified;
        this.peers = peers;
        this.locations = locations;
        this.states = states;
        this.versions = versions;
        this.hostIds = hostIds;
        this.addresses = addresses;
        this.endpointsByDC = endpointsByDC;
        this.racksByDC = racksByDC;
        Pair<NodeVersion, NodeVersion> minMaxVer = minMaxVersions(states, versions);
        clusterMinVersion = minMaxVer.left;
        clusterMaxVersion = minMaxVer.right;
    }

    @Override
    public String toString()
    {
        return "Directory{" +
               "nextId=" + nextId +
               ", lastModified=" + lastModified +
               ", peers=" + peers +
               ", locations=" + locations +
               ", states=" + states +
               ", versions=" + versions +
               ", addresses=" + addresses +
               ", hostIds=" + hostIds +
               ", endpointsByDC=" + endpointsByDC +
               ", racksByDC=" + racksByDC +
               '}';
    }

    public Set<NodeId> toNodeIds(Collection<InetAddressAndPort> addrs)
    {
        Set<NodeId> nodeIds = new HashSet<>();
        for (InetAddressAndPort addr : addrs)
            nodeIds.add(peerId(addr));
        return nodeIds;
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    @Override
    public Directory withLastModified(Epoch epoch)
    {
        return new Directory(nextId, epoch, peers, locations, states, versions, hostIds, addresses, endpointsByDC, racksByDC);
    }

    public Directory withNonUpgradedNode(NodeAddresses addresses,
                                         Location location,
                                         NodeVersion version,
                                         NodeState state,
                                         UUID hostId)
    {
        NodeId id = new NodeId(nextId);
        return with(addresses, id, hostId, location, version).withNodeState(id, state).withRackAndDC(id);
    }

    @VisibleForTesting
    public Directory with(NodeAddresses addresses, Location location)
    {
        return with(addresses, location, CURRENT);
    }

    public Directory with(NodeAddresses addresses, Location location, NodeVersion nodeVersion)
    {
        NodeId id = new NodeId(nextId);
        if (peers.containsKey(id))
            throw new IllegalStateException("Directory already contains a node with id " + id);
        return with(addresses, id, id.toUUID(), location, nodeVersion);
    }

    private Directory with(NodeAddresses nodeAddresses, NodeId id, UUID hostId, Location location, NodeVersion nodeVersion)
    {
        if (peers.containsKey(id))
            return this;
        if (peers.containsValue(nodeAddresses.broadcastAddress))
            return this;
        if (locations.containsKey(id))
            return this;

        return new Directory(nextId + 1,
                             lastModified,
                             peers.without(id).with(id, nodeAddresses.broadcastAddress),
                             locations.withForce(id, location),
                             states.withForce(id, NodeState.REGISTERED),
                             versions.withForce(id, nodeVersion),
                             hostIds.withForce(id, hostId),
                             addresses.withForce(id, nodeAddresses),
                             endpointsByDC,
                             racksByDC);
    }

    public Directory withNodeState(NodeId id, NodeState state)
    {
        return new Directory(nextId, lastModified, peers, locations, states.withForce(id, state), versions, hostIds, addresses, endpointsByDC, racksByDC);
    }

    public Directory withNodeVersion(NodeId id, NodeVersion version)
    {
        if (Objects.equals(versions.get(id), version))
            return this;
        return new Directory(nextId, lastModified, peers, locations, states, versions.withForce(id, version), hostIds, addresses, endpointsByDC, racksByDC);
    }

    public Directory withNodeAddresses(NodeId id, NodeAddresses nodeAddresses)
    {
        if (Objects.equals(addresses.get(id), nodeAddresses))
            return this;

        InetAddressAndPort oldEp = addresses.get(id).broadcastAddress;
        BTreeMultimap<String, InetAddressAndPort> updatedEndpointsByDC = endpointsByDC.without(location(id).datacenter, oldEp)
                                                                                      .with(location(id).datacenter, nodeAddresses.broadcastAddress);

        Location location = location(id);
        BTreeMultimap<String, InetAddressAndPort> rackEP = (BTreeMultimap<String, InetAddressAndPort>) racksByDC.get(location.datacenter);
        if (rackEP == null)
            rackEP = BTreeMultimap.empty();

        rackEP = rackEP.without(location.rack, oldEp)
                       .with(location.rack, nodeAddresses.broadcastAddress);
        BTreeMap<String, Multimap<String, InetAddressAndPort>> updatedEndpointsByRack = racksByDC.withForce(location(id).datacenter, rackEP);

        return new Directory(nextId, lastModified,
                             peers.withForce(id,nodeAddresses.broadcastAddress), locations, states, versions, hostIds, addresses.withForce(id, nodeAddresses),
                             updatedEndpointsByDC,
                             updatedEndpointsByRack);
    }

    public Directory withRackAndDC(NodeId id)
    {
        InetAddressAndPort endpoint = peers.get(id);
        Location location = locations.get(id);

        BTreeMultimap<String, InetAddressAndPort> rackEP = (BTreeMultimap<String, InetAddressAndPort>) racksByDC.get(location.datacenter);
        if (rackEP == null)
            rackEP = BTreeMultimap.empty();
        rackEP = rackEP.with(location.rack, endpoint);

        return new Directory(nextId, lastModified, peers, locations, states, versions, hostIds, addresses,
                             endpointsByDC.with(location.datacenter, endpoint),
                             racksByDC.withForce(location.datacenter, rackEP));
    }

    public Directory withoutRackAndDC(NodeId id)
    {
        InetAddressAndPort endpoint = peers.get(id);
        Location location = locations.get(id);
        BTreeMultimap<String, InetAddressAndPort> rackEP = (BTreeMultimap<String, InetAddressAndPort>) racksByDC.get(location.datacenter);
        rackEP = rackEP.without(location.rack, endpoint);
        BTreeMap<String, Multimap<String, InetAddressAndPort>> newRacksByDC;
        if (rackEP.isEmpty())
            newRacksByDC = racksByDC.without(location.datacenter);
        else
            newRacksByDC = racksByDC.withForce(location.datacenter, rackEP);
        return new Directory(nextId, lastModified, peers, locations, states, versions, hostIds, addresses,
                             endpointsByDC.without(location.datacenter, endpoint),
                             newRacksByDC);
    }

    public Directory without(NodeId id)
    {
        InetAddressAndPort endpoint = peers.get(id);
        Location location = locations.get(id);
        // Last node in dc
        if (!racksByDC.containsKey(location.datacenter))
        {
            assert !endpointsByDC.containsKey(location.datacenter);

            return new Directory(nextId,
                                 lastModified,
                                 peers.without(id),
                                 locations.without(id),
                                 states.without(id),
                                 versions.without(id),
                                 hostIds.without(id),
                                 addresses.without(id),
                                 endpointsByDC,
                                 racksByDC);

        }

        BTreeMultimap<String, InetAddressAndPort> rackEP = (BTreeMultimap<String, InetAddressAndPort>) racksByDC.get(location.datacenter);
        rackEP = rackEP.without(location.rack, endpoint);

        return new Directory(nextId,
                             lastModified,
                             peers.without(id),
                             locations.without(id),
                             states.without(id),
                             versions.without(id),
                             hostIds.without(id),
                             addresses.without(id),
                             endpointsByDC.without(location.datacenter, endpoint),
                             racksByDC.withForce(location.datacenter, rackEP));
    }

    public NodeId peerId(InetAddressAndPort endpoint)
    {
        return peers.inverse().get(endpoint);
    }

    public boolean isRegistered(InetAddressAndPort endpoint)
    {
        return peers.inverse().containsKey(endpoint);
    }

    public InetAddressAndPort endpoint(NodeId id)
    {
        return peers.get(id);
    }

    public boolean isEmpty()
    {
        return peers.isEmpty();
    }

    /**
     * Includes every registered endpoint, including those which haven't yet joined and those which have
     * left but are yet to be unregistered. Not for use when calculating availablity or placements, in
     * those cases use allJoinedEndpoints.
     * @return
     */
    public ImmutableList<InetAddressAndPort> allAddresses()
    {
        return ImmutableList.copyOf(peers.values());
    }

    public ImmutableSet<NodeId> peerIds()
    {
        return ImmutableSet.copyOf(peers.keySet());
    }

    public NodeAddresses getNodeAddresses(NodeId id)
    {
        return addresses.get(id);
    }

    private Node getNode(NodeId id)
    {
        return new Node(id, addresses.get(id), locations.get(id), states.get(id), versions.get(id), hostIds.get(id));
    }

    public Location location(NodeId id)
    {
        return locations.get(id);
    }

    public Set<InetAddressAndPort> datacenterEndpoints(String datacenter)
    {
        return (Set<InetAddressAndPort>) endpointsByDC.get(datacenter);
    }

    public Multimap<String, InetAddressAndPort> datacenterRacks(String datacenter)
    {
        return racksByDC.get(datacenter);
    }

    public NodeState peerState(NodeId peer)
    {
        return states.get(peer);
    }

    public NodeVersion version(NodeId peer)
    {
        return versions.get(peer);
    }

    public UUID hostId(NodeId peer)
    {
        return hostIds.getOrDefault(peer, peer.toUUID());
    }

    /**
     * Retrieve the NodeId for a peer from its pre-upgrade HostId
     * @param hostId
     * @return NodeId for the peer which prior to upgrade had the supplied Host ID, or null if no mapping is found
     */
    @Nullable
    public NodeId nodeIdFromHostId(UUID hostId)
    {
        return hostIds.inverse().getOrDefault(hostId, null);
    }

    public Map<String, Multimap<String, InetAddressAndPort>> allDatacenterRacks()
    {
        return racksByDC;
    }

    public Set<String> knownDatacenters()
    {
        return locations.values().stream().map(l -> l.datacenter).collect(Collectors.toSet());
    }

    public Multimap<String, InetAddressAndPort> allDatacenterEndpoints()
    {
        return endpointsByDC;
    }

    public Collection<InetAddressAndPort> allJoinedEndpoints()
    {
        return endpointsByDC.values();
    }

    public NodeState peerState(InetAddressAndPort peer)
    {
        return states.get(peers.inverse().get(peer));
    }

    public String toDebugString()
    {
        return peers.keySet()
                    .stream()
                    .sorted()
                    .map(this::getNode)
                    .map(Node::toString)
                    .collect(Collectors.joining("\n"));
    }

    private static class Node
    {
        public static final Serializer serializer = new Serializer();

        public final NodeId id;
        public final NodeAddresses addresses;
        public final Location location;
        public final NodeState state;
        public final NodeVersion version;
        public final UUID hostId;

        public Node(NodeId id, NodeAddresses addresses, Location location, NodeState state, NodeVersion version, UUID hostId)
        {
            this.id = Preconditions.checkNotNull(id, "Node ID must not be null");
            this.addresses = Preconditions.checkNotNull(addresses, "Node addresses must not be null");
            this.location = Preconditions.checkNotNull(location, "Node location must not be null");
            this.state = Preconditions.checkNotNull(state, "Node state must not be null");
            this.version = Preconditions.checkNotNull(version, "Node version must not be null");
            this.hostId = hostId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return id.equals(node.id)
                   && addresses.equals(node.addresses)
                   && location.equals(node.location)
                   && state == node.state
                   && version.equals(node.version);
        }


        @Override
        public int hashCode()
        {
            return Objects.hash(id, addresses, location, state, version);
        }

        @Override
        public String toString()
        {
            return "Node{" +
                   "id=" + id +
                   ", addresses=" + addresses +
                   ", location=" + location +
                   ", state=" + state +
                   ", version=" + version +
                   '}';
        }

        public static class Serializer implements MetadataSerializer<Node>
        {
            public void serialize(Node node, DataOutputPlus out, Version version) throws IOException
            {
                NodeId.serializer.serialize(node.id, out, version);
                NodeAddresses.serializer.serialize(node.addresses, out, version);
                out.writeUTF(node.location.datacenter);
                out.writeUTF(node.location.rack);
                out.writeInt(node.state.ordinal());
                NodeVersion.serializer.serialize(node.version, out, version);
                if (node.hostId == null)
                    out.writeBoolean(false);
                else
                {
                    out.writeBoolean(true);
                    UUIDSerializer.serializer.serialize(node.hostId, out, MessagingService.VERSION_51);
                }

            }

            public Node deserialize(DataInputPlus in, Version version) throws IOException
            {
                NodeId id = NodeId.serializer.deserialize(in, version);
                NodeAddresses addresses = NodeAddresses.serializer.deserialize(in, version);
                Location location = new Location(in.readUTF(), in.readUTF());
                NodeState state = NodeState.values()[in.readInt()];
                NodeVersion nodeVersion = NodeVersion.serializer.deserialize(in, version);
                boolean hasHostId = in.readBoolean();
                UUID hostId = hasHostId ? UUIDSerializer.serializer.deserialize(in, MessagingService.VERSION_51) : null;
                return new Node(id, addresses, location, state, nodeVersion, hostId);
            }

            public long serializedSize(Node node, Version version)
            {
                long size = 0;
                size += NodeId.serializer.serializedSize(node.id, version);
                size += NodeAddresses.serializer.serializedSize(node.addresses, version);
                size += sizeof(node.location.datacenter);
                size += sizeof(node.location.rack);
                size += TypeSizes.INT_SIZE;
                size += NodeVersion.serializer.serializedSize(node.version, version);
                size += TypeSizes.BOOL_SIZE;
                if (node.hostId != null)
                    size += UUIDSerializer.serializer.serializedSize(node.hostId, MessagingService.VERSION_51);
                return size;
            }
        }
    }

    public static class Serializer implements MetadataSerializer<Directory>
    {
        public void serialize(Directory t, DataOutputPlus out, Version version) throws IOException
        {
            if (version.isAtLeast(Version.V1))
                out.writeInt(t.nextId);
            out.writeInt(t.states.size());
            for (NodeId nodeId : t.states.keySet())
                Node.serializer.serialize(t.getNode(nodeId), out, version);

            Set<String> dcs = t.racksByDC.keySet();
            out.writeInt(dcs.size());
            for (String dc : dcs)
            {
                out.writeUTF(dc);
                Map<String, Collection<InetAddressAndPort>> racks = t.racksByDC.get(dc).asMap();
                out.writeInt(racks.size());
                for (String rack : racks.keySet())
                {
                    out.writeUTF(rack);
                    Collection<InetAddressAndPort> endpoints = racks.get(rack);
                    out.writeInt(endpoints.size());
                    for (InetAddressAndPort endpoint : endpoints)
                    {
                        InetAddressAndPort.MetadataSerializer.serializer.serialize(endpoint, out, version);
                    }
                }
            }
            Epoch.serializer.serialize(t.lastModified, out, version);
        }

        public Directory deserialize(DataInputPlus in, Version version) throws IOException
        {
            int nextId = -1;
            if (version.isAtLeast(Version.V1))
                nextId = in.readInt();
            int count = in.readInt();
            Directory newDir = new Directory();

            for (int i = 0; i < count; i++)
            {
                Node n = Node.serializer.deserialize(in, version);
                // todo: bulk operations
                newDir = newDir.with(n.addresses, n.id, n.hostId, n.location, n.version)
                               .withNodeState(n.id, n.state);
            }

            int dcCount = in.readInt();
            BTreeMultimap<String, InetAddressAndPort> dcEndpoints = BTreeMultimap.empty();
            BTreeMap<String, Multimap<String, InetAddressAndPort>> racksByDC = BTreeMap.empty();
            for (int i=0; i<dcCount; i++)
            {
                String dc = in.readUTF();
                int rackCount = in.readInt();
                BTreeMultimap<String, InetAddressAndPort> rackEndpoints = BTreeMultimap.empty();
                for (int j=0; j<rackCount; j++)
                {
                    String rack = in.readUTF();
                    int epCount = in.readInt();
                    for (int k=0; k<epCount; k++)
                    {
                        InetAddressAndPort endpoint = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
                        rackEndpoints = rackEndpoints.with(rack, endpoint);
                        dcEndpoints = dcEndpoints.with(dc, endpoint);
                    }
                    racksByDC = racksByDC.withForce(dc, rackEndpoints);
                }
            }

            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            if (version.isBefore(Version.V1))
            {
                NodeId maxId = null;
                for (NodeId id : newDir.peers.keySet())
                {
                    if (maxId == null || id.compareTo(maxId) > 0)
                        maxId = id;
                }

                if (maxId == null)
                    nextId = 1;
                else
                    nextId = maxId.id() + 1;
            }
            return new Directory(nextId,
                                 lastModified,
                                 newDir.peers,
                                 newDir.locations,
                                 newDir.states,
                                 newDir.versions,
                                 newDir.hostIds,
                                 newDir.addresses,
                                 dcEndpoints,
                                 racksByDC);
        }

        public long serializedSize(Directory t, Version version)
        {
            long size = 0;
            if (version.isAtLeast(Version.V1))
                size += sizeof(t.nextId);

            size += sizeof(t.states.size());
            for (NodeId nodeId : t.states.keySet())
                size += Node.serializer.serializedSize(t.getNode(nodeId), version);

            size += sizeof(t.racksByDC.size());
            for (Map.Entry<String, Multimap<String, InetAddressAndPort>> entry : t.racksByDC.entrySet())
            {
                size += sizeof(entry.getKey());
                Map<String, Collection<InetAddressAndPort>> racks = entry.getValue().asMap();
                size += sizeof(racks.size());
                for (Map.Entry<String, Collection<InetAddressAndPort>> e : racks.entrySet())
                {
                    size += sizeof(e.getKey());
                    Collection<InetAddressAndPort> endpoints = e.getValue();
                    size += sizeof(endpoints.size());
                    for (InetAddressAndPort endpoint : endpoints)
                    {
                        size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(endpoint, version);
                    }
                }
            }
            size += Epoch.serializer.serializedSize(t.lastModified, version);
            return size;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof Directory)) return false;
        Directory directory = (Directory) o;

        return Objects.equals(lastModified, directory.lastModified) &&
               isEquivalent(directory);
    }

    private static Pair<NodeVersion, NodeVersion> minMaxVersions(BTreeMap<NodeId, NodeState> states, BTreeMap<NodeId, NodeVersion> versions)
    {
        NodeVersion minVersion = null;
        NodeVersion maxVersion = null;
        for (Map.Entry<NodeId, NodeState> entry : states.entrySet())
        {
            if (entry.getValue() != NodeState.LEFT)
            {
                NodeVersion ver = versions.get(entry.getKey());
                if (minVersion == null || ver.compareTo(minVersion) < 0)
                    minVersion = ver;
                if (maxVersion == null || ver.compareTo(maxVersion) > 0)
                    maxVersion = ver;
            }
        }
        if (minVersion == null)
            return Pair.create(CURRENT, CURRENT);
        return Pair.create(minVersion, maxVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nextId, lastModified, peers, locations, states, endpointsByDC, racksByDC, versions, addresses);
    }

    /**
     * returns true if this directory is functionally equivalent to the given one
     *
     * does not check equality of lastModified
     */
    @VisibleForTesting
    public boolean isEquivalent(Directory directory)
    {
        return nextId == directory.nextId &&
               Objects.equals(peers, directory.peers) &&
               Objects.equals(locations, directory.locations) &&
               Objects.equals(states, directory.states) &&
               Objects.equals(endpointsByDC, directory.endpointsByDC) &&
               Objects.equals(racksByDC, directory.racksByDC) &&
               Objects.equals(versions, directory.versions) &&
               Objects.equals(addresses, directory.addresses);
    }
    
    private static final Logger logger = LoggerFactory.getLogger(Directory.class);

    public void dumpDiff(Directory other)
    {
        if (nextId != other.nextId)
        {
            logger.warn("nextId differ: {} != {}", nextId, other.nextId);
        }
        if (!Objects.equals(peers, other.peers))
        {
            logger.warn("Peers differ: {} != {}", peers, other.peers);
            dumpDiff(logger, peers, other.peers);
        }
        if (!Objects.equals(states, other.states))
        {
            logger.warn("States differ: {} != {}", states, other.states);
            dumpDiff(logger, states, other.states);
        }
        if (!Objects.equals(endpointsByDC, other.endpointsByDC))
        {
            logger.warn("Endpoints by dc differ: {} != {}", endpointsByDC, other.endpointsByDC);
            dumpDiff(logger, endpointsByDC.asMap(), other.endpointsByDC.asMap());
        }
        if (!Objects.equals(versions, other.versions))
        {
            logger.warn("Versions differ: {} != {}", versions, other.versions);
            dumpDiff(logger, versions, other.versions);
        }
        if (!Objects.equals(addresses, other.addresses))
        {
            logger.warn("Addresses differ: {} != {}", addresses, other.addresses);
            dumpDiff(logger, addresses, other.addresses);
        }
    }

    public static <K, V> void dumpDiff(Logger logger, Map<K, V> l, Map<K, V> r)
    {
        for (K k : Sets.intersection(l.keySet(), r.keySet()))
        {
            V lv = l.get(k);
            V rv = r.get(k);
            if (!Objects.equals(lv, rv))
                logger.warn("Values for key {} differ: {} != {}", k, lv, rv);
        }
        for (K k : Sets.difference(l.keySet(), r.keySet()))
            logger.warn("Value for key {} is only present in the left set: {}", k, l.get(k));
        for (K k : Sets.difference(r.keySet(), l.keySet()))
            logger.warn("Value for key {} is only present in the right set: {}", k, r.get(k));

    }
}
