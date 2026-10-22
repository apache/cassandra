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

package org.apache.cassandra.tcm.ownership;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.CMSLookup;
import org.apache.cassandra.tcm.CMSMembership;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.EndpointLookup;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.PojoToString;

/**
 * Finds the replica peers of nodes. A node's replica peers are the other nodes that share at least one token range
 * with it. They are a subset of the nodes in {@code system.peers}, which lists every other node in the cluster.
 *
 * <p>Operators use this to avoid taking down a node and its peers at the same time, for example during a rolling
 * restart or upgrade.
 *
 * <p>Peers are listed separately for reads and for writes. Both are grouped by datacenter. When several keyspaces are
 * selected, a node's peers from all of them are listed together. The answer is correct even while nodes are joining
 * or leaving the cluster. Nodes that have already left the cluster are not reported.
 */
public final class ReplicaPeers
{
    /**
     * Schema version of the tree returned by {@link #toMap()}. Bumped only for breaking changes; additive changes do
     * not bump it, so consumers must tolerate unknown keys.
     */
    private static final int FORMAT_VERSION = 1;

    private final ClusterMetadata metadata;
    private final SortedSet<String> keyspaces;
    private final SortedMap<NodeId, PeerSets> peersByNodeId;

    private ReplicaPeers(ClusterMetadata metadata,
                         SortedSet<String> keyspaces,
                         SortedMap<NodeId, PeerSets> peersByNodeId)
    {
        this.metadata = metadata;
        this.keyspaces = keyspaces;
        this.peersByNodeId = peersByNodeId;
    }

    /**
     *  Computes the replica peers for the given cluster metadata and options.
     *
     *  @param options The resolved options specifying the nodes, datacenters, and keyspaces to include.
     */
    public static ReplicaPeers compute(ClusterMetadata metadata, Options options)
    {
        return new ReplicaPeers(metadata, options.keyspaces, computePeers(metadata, options));
    }

    /**
     * Filtering and scope options for replica peer computation.
     * Use {@link #resolve} to create a validated instance against cluster metadata.
     */
    public static final class Options
    {
        private static final Pattern NODE_ID_PATTERN = Pattern.compile("\\d+");

        @Nullable
        private final NodeId subject;
        private final Set<String> datacenters;
        private final SortedSet<String> keyspaces;

        private Options(@Nullable NodeId subject, Set<String> datacenters, SortedSet<String> keyspaces)
        {
            this.subject = subject;
            this.datacenters = Collections.unmodifiableSet(datacenters);
            this.keyspaces = Collections.unmodifiableSortedSet(keyspaces);
        }

        /**
         * Checks the caller's arguments against {@code metadata} and turns them into options.
         *
         * @param node pass null to report on every node in the cluster, else a node id ({@code "3"}), a node or host
         *             UUID, or an address with an optional port. An address can be the one reported for the node, even
         *             while a CMS member's new address is not yet committed. Cannot be combined with
         *             {@code datacenters}.
         * @param datacenters pass null or empty to report on nodes in every datacenter, else only on nodes in these
         *                    datacenters. Their peers are still listed from every datacenter. Cannot be combined
         *                    with {@code node}.
         * @param keyspaces pass null or empty to use every keyspace except {@code excludedKeyspaces}, else only these
         *                  keyspaces
         * @param excludedKeyspaces pass null or empty to leave out no keyspace, else the keyspaces to leave out.
         *                          System keyspaces that are replicated, such as {@code system_auth}, are used unless
         *                          left out here. Cannot be combined with {@code keyspaces}.
         * @throws IllegalArgumentException in any of these cases:
         *                                  <ul>
         *                                      <li>a node, datacenter or keyspace is not known</li>
         *                                      <li>a keyspace in {@code keyspaces} uses {@code LocalStrategy}</li>
         *                                      <li>{@code excludedKeyspaces} leaves out every replicated keyspace</li>
         *                                      <li>two arguments that cannot be used together are both passed</li>
         *                                  </ul>
         */
        public static Options resolve(ClusterMetadata metadata,
                                      @Nullable String node,
                                      @Nullable Set<String> datacenters,
                                      @Nullable Set<String> keyspaces,
                                      @Nullable Set<String> excludedKeyspaces)
        {
            boolean keyspacesSupplied = keyspaces != null && !keyspaces.isEmpty();
            boolean keyspaceExclusionsSupplied = excludedKeyspaces != null && !excludedKeyspaces.isEmpty();
            if (keyspacesSupplied && keyspaceExclusionsSupplied)
                throw new IllegalArgumentException("Cannot combine an explicit keyspace selection with a keyspace exclusion list");

            Directory directory = metadata.directory;
            Set<String> dcs = datacenters == null ? Collections.emptySet() : new TreeSet<>(datacenters);
            if (node != null && !dcs.isEmpty())
                throw new IllegalArgumentException("Cannot combine a node with a datacenter restriction");

            if (!dcs.isEmpty())
            {
                // Not using directory.knownDatacenters(), which still includes datacenters whose nodes have all left
                Set<String> known = new HashSet<>();
                for (NodeId nodeId : directory.peerIds())
                {
                    if (!hasLeft(directory, nodeId))
                        known.add(datacenterOf(directory, nodeId));
                }
                for (String dc : dcs)
                {
                    if (!known.contains(dc))
                        throw new IllegalArgumentException("Unknown datacenter: " + dc + ". Known datacenters: " + new TreeSet<>(known));
                }
            }

            NodeId subject = node == null ? null : resolveNode(metadata, node);
            return new Options(subject, dcs, selectKeyspaces(metadata, keyspaces, excludedKeyspaces));
        }

        private static SortedSet<String> selectKeyspaces(ClusterMetadata metadata,
                                                         @Nullable Set<String> keyspaces,
                                                         @Nullable Set<String> excludedKeyspaces)
        {
            Keyspaces all = metadata.schema.getKeyspaces();
            SortedSet<String> selected = new TreeSet<>();

            if (keyspaces != null && !keyspaces.isEmpty())
            {
                for (String keyspace : keyspaces)
                {
                    KeyspaceMetadata ksm = all.getNullable(keyspace);
                    if (ksm == null)
                        throw new IllegalArgumentException("Unknown keyspace: " + keyspace);
                    // naming one explicitly and silently getting nothing back would be worse than failing
                    if (ksm.params.replication.isLocal())
                        throw new IllegalArgumentException("Keyspace " + keyspace + " uses LocalStrategy and so has no replica peers");
                    selected.add(keyspace);
                }
                return selected;
            }

            Set<String> excluded = excludedKeyspaces == null ? Collections.emptySet() : excludedKeyspaces;
            for (String keyspace : excluded)
            {
                if (all.getNullable(keyspace) == null)
                    throw new IllegalArgumentException("Unknown keyspace: " + keyspace);
            }

            for (KeyspaceMetadata ksm : all)
            {
                // LocalStrategy keyspaces are replicated to the local node only, so they can never establish a peer
                if (excluded.contains(ksm.name) || ksm.params.replication.isLocal())
                    continue;
                selected.add(ksm.name);
            }
            // As above: reporting no peers for every node would read as safe to take them all down at once
            if (selected.isEmpty())
                throw new IllegalArgumentException("No replicated keyspace is left to report on after excluding " +
                                                   new TreeSet<>(excluded));
            return selected;
        }

        /**
         * Resolve a node identifier supplied by an operator. Accepted forms, in the order they are tried:
         * <ol>
         *     <li>a decimal {@link NodeId}, e.g. {@code 3}</li>
         *     <li>a UUID, either a node id in UUID form or a pre-upgrade host id</li>
         *     <li>an address, with or without a port. This is the node's committed address or, while a CMS member's
         *     address change is not yet committed, also the new address which is reported for it</li>
         * </ol>
         * Digits are tried first deliberately: {@code InetAddressAndPort.getByName("1")} resolves to {@code 0.0.0.1},
         * so trying addresses first would silently mis-resolve node ids.
         *
         * @throws IllegalArgumentException if the string is not one of the accepted forms, or names a node which is
         *                                  not registered in the supplied metadata
         */
        @VisibleForTesting
        static NodeId resolveNode(ClusterMetadata metadata, String node)
        {
            if (node == null || node.trim().isEmpty())
                throw new IllegalArgumentException("Node identifier must not be empty");

            String id = node.trim();
            Directory directory = metadata.directory;
            NodeId nodeId = parseNode(metadata, id);

            if (directory.endpoint(nodeId) == null)
                throw new IllegalArgumentException("Unknown node: " + id);
            if (hasLeft(directory, nodeId))
                throw new IllegalArgumentException("Node " + id + " has left the cluster and so has no replica peers");

            return nodeId;
        }

        private static NodeId parseNode(ClusterMetadata metadata, String id)
        {
            if (NODE_ID_PATTERN.matcher(id).matches())
                return nodeIdFromDigits(id);

            UUID uuid = tryParseUuid(id);
            if (uuid != null)
                return nodeIdFromUuid(metadata.directory, uuid);

            return nodeIdFromEndpoint(metadata, id);
        }

        private static NodeId nodeIdFromDigits(String id)
        {
            try
            {
                return new NodeId(Integer.parseInt(id));
            }
            catch (NumberFormatException e)
            {
                throw new IllegalArgumentException("Unable to parse node id " + id, e);
            }
        }

        private static NodeId nodeIdFromUuid(Directory directory, UUID uuid)
        {
            if (NodeId.isValidNodeId(uuid))
                return NodeId.fromUUID(uuid);

            // Not a node id in UUID form, so it may be a host id assigned before the cluster was upgraded
            NodeId nodeId = directory.nodeIdFromHostId(uuid);
            if (nodeId == null)
                throw new IllegalArgumentException("Unknown host id: " + uuid);
            return nodeId;
        }

        private static NodeId nodeIdFromEndpoint(ClusterMetadata metadata, String id)
        {
            InetAddressAndPort endpoint;
            try
            {
                endpoint = InetAddressAndPort.getByName(id);
            }
            catch (UnknownHostException e)
            {
                throw new IllegalArgumentException("Unable to look up endpoint " + id, e);
            }

            // A node is known by its committed address and, while an address change of a CMS member is not yet
            // committed, also by the new address which the output reports for it
            Set<NodeId> matches = new TreeSet<>();
            NodeId committed = metadata.directory.peerId(endpoint);
            if (committed != null)
                matches.add(committed);
            EndpointLookup lookup = metadata.endpointLookup();
            for (NodeId nodeId : metadata.directory.peerIds())
            {
                if (endpoint.equals(lookup.endpoint(nodeId)))
                    matches.add(nodeId);
            }

            if (matches.isEmpty())
                throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
            if (matches.size() > 1)
                throw new IllegalArgumentException("Address " + endpoint + " belongs to nodes " + matches +
                                                   " while an address change is not yet committed; name the node by its id");
            return matches.iterator().next();
        }

        @Nullable
        private static UUID tryParseUuid(String value)
        {
            try
            {
                return UUID.fromString(value);
            }
            catch (IllegalArgumentException e)
            {
                return null;
            }
        }
    }

    /**
     * Compute replicas for the supplied metadata into a node-centric view.
     *
     * @return A map of each subject node to its respective read and write peers.
     */
    private static SortedMap<NodeId, PeerSets> computePeers(ClusterMetadata metadata, Options options)
    {
        Directory directory = metadata.directory;
        Map<NodeId, String> dcByNodeId = datacentersById(directory);
        Map<InetAddressAndPort, NodeId> nodeIdByEndpoint = nodeIdByEndpoint(directory);
        SortedSet<NodeId> subjects = subjects(directory, options, dcByNodeId);

        // Seed empty peer sets for every subject, so that a node which is registered but does not yet appear in
        // any placement is reported with no peers rather than omitted. Callers need to distinguish
        // "this node has no peers" from "I have never heard of this node".
        Map<NodeId, Set<NodeId>> readPeers = new HashMap<>();
        Map<NodeId, Set<NodeId>> writePeers = new HashMap<>();
        for (NodeId subject : subjects)
        {
            readPeers.put(subject, new HashSet<>());
            writePeers.put(subject, new HashSet<>());
        }

        for (ReplicationParams params : replicationOf(metadata, options.keyspaces))
        {
            if (params.isMeta())
            {
                addCmsPeers(metadata.cmsMembership, readPeers, writePeers);
                continue;
            }

            DataPlacement placement = metadata.placement(params);
            if (placement == null || placement.isEmpty())
                continue;

            // DataPlacement shares one ReplicaGroups instance between both sides when they are equal, which lets us
            // walk once instead of twice. This is an opportunistic shortcut only: the two sides are stamped with an
            // epoch per range, so identity can be lost after a topology change even when the replica sets match.
            if (placement.writes == placement.reads)
            {
                walk(placement.reads, nodeIdByEndpoint, readPeers, writePeers);
            }
            else
            {
                walk(placement.reads, nodeIdByEndpoint, readPeers);
                walk(placement.writes, nodeIdByEndpoint, writePeers);
            }
        }

        SortedMap<NodeId, PeerSets> nodes = new TreeMap<>();
        for (NodeId subject : subjects)
            nodes.put(subject, new PeerSets(groupByDatacenter(readPeers.get(subject), dcByNodeId),
                                            groupByDatacenter(writePeers.get(subject), dcByNodeId)));

        return Collections.unmodifiableSortedMap(nodes);
    }

    /**
     * The nodes for which replica information should be returned.
     */
    private static SortedSet<NodeId> subjects(Directory directory, Options options, Map<NodeId, String> dcByNodeId)
    {
        SortedSet<NodeId> subjects = new TreeSet<>();
        if (options.subject != null)
        {
            subjects.add(options.subject);
            return subjects;
        }

        for (NodeId nodeId : directory.peerIds())
        {
            if (hasLeft(directory, nodeId))
                continue;
            if (options.datacenters.isEmpty() || options.datacenters.contains(dcByNodeId.get(nodeId)))
                subjects.add(nodeId);
        }
        return subjects;
    }

    /**
     * Add an edge for every ordered pair of replicas in every range of {@code groups} to each of {@code sides}, keeping
     * only pairs whose subject side is one we are reporting on, i.e. is a key of the side's map. The peer side is not
     * filtered, so peers in every datacenter are recorded.
     */
    @SafeVarargs
    private static void walk(ReplicaGroups groups,
                             Map<InetAddressAndPort, NodeId> nodeIdByEndpoint,
                             Map<NodeId, Set<NodeId>>... sides)
    {
        for (VersionedEndpoints.ForRange forRange : groups.endpoints)
        {
            EndpointsForRange replicas = forRange.get();
            if (replicas.size() < 2)
            {
                continue;
            }

            List<NodeId> ids = new ArrayList<>(replicas.size());
            for (Replica replica : replicas)
            {
                NodeId nodeId = nodeIdByEndpoint.get(replica.endpoint());
                // TCM updates placements and the directory together, so a replica the directory does not know
                // means the metadata is inconsistent, and any peer list computed from it would be incomplete
                if (nodeId == null)
                    throw new IllegalStateException("Placement for range " + replica.range() + " names replica " +
                                                    replica.endpoint() + ", which is not in the cluster metadata " +
                                                    "directory");
                ids.add(nodeId);
            }

            addPeers(ids, sides);
        }
    }

    /**
     * Add the peers of the metadata keyspace, mirroring {@link CMSMembership#toPlacement}: its single range is read
     * and written by the full CMS members, and also written by the members which are still joining.
     *
     * <p>Taken from the membership rather than from {@link ClusterMetadata#placement}: that placement names members by
     * address, and while a member's address change is not yet committed it names the member by an address which the
     * directory does not know. See {@link CMSLookup}.
     */
    private static void addCmsPeers(CMSMembership cms,
                                    Map<NodeId, Set<NodeId>> readPeers,
                                    Map<NodeId, Set<NodeId>> writePeers)
    {
        List<NodeId> readers = new ArrayList<>(cms.fullMembers());
        List<NodeId> writers = new ArrayList<>(readers);
        writers.addAll(cms.joiningMembers());
        addPeers(readers, readPeers);
        addPeers(writers, writePeers);
    }

    /**
     * Add an edge for every ordered pair of {@code replicas} to each of {@code sides}, keeping only pairs whose subject
     * side is a key of the side's map.
     */
    @SafeVarargs
    private static void addPeers(List<NodeId> replicas, Map<NodeId, Set<NodeId>>... sides)
    {
        for (Map<NodeId, Set<NodeId>> side : sides)
        {
            for (int i = 0; i < replicas.size(); i++)
            {
                Set<NodeId> peers = side.get(replicas.get(i));
                if (peers == null)
                    continue;

                for (int j = 0; j < replicas.size(); j++)
                {
                    if (i != j)
                        peers.add(replicas.get(j));
                }
            }
        }
    }

    /** Distinct replication configurations of the selected keyspaces. Several keyspaces commonly share one. */
    private static Set<ReplicationParams> replicationOf(ClusterMetadata metadata, Set<String> keyspaces)
    {
        Set<ReplicationParams> params = new HashSet<>();
        Keyspaces all = metadata.schema.getKeyspaces();
        for (String keyspace : keyspaces)
        {
            KeyspaceMetadata ksm = all.getNullable(keyspace);
            // the schema is from the same snapshot as the selection, so this is belt and braces
            if (ksm == null || ksm.params.replication.isLocal())
                continue;
            params.add(ksm.params.replication);
        }
        return params;
    }

    private static Map<NodeId, String> datacentersById(Directory directory)
    {
        Map<NodeId, String> dcById = new HashMap<>();
        for (NodeId nodeId : directory.peerIds())
            dcById.put(nodeId, datacenterOf(directory, nodeId));
        return dcById;
    }

    /**
     * The same answers as {@link Directory#peerId}, as a hash map: placements are walked once per replica of every
     * range, and the directory's own lookup is a tree search.
     */
    private static Map<InetAddressAndPort, NodeId> nodeIdByEndpoint(Directory directory)
    {
        Map<InetAddressAndPort, NodeId> nodeIdByEndpoint = new HashMap<>();
        for (NodeId nodeId : directory.peerIds())
            nodeIdByEndpoint.put(directory.endpoint(nodeId), nodeId);
        return nodeIdByEndpoint;
    }

    @VisibleForTesting
    SortedSet<String> keyspaces()
    {
        return keyspaces;
    }

    @VisibleForTesting
    SortedMap<NodeId, PeerSets> peersByNodeId()
    {
        return peersByNodeId;
    }

    public PeerSets peersOf(NodeId node)
    {
        return peersByNodeId.get(node);
    }

    /**
     * Render as a tree of maps, lists and scalars suitable for {@link PojoToString}. Note that formatter's constraints:
     * no booleans, no nulls, and map keys must be strings or integers.
     */
    public Map<String, Object> toMap()
    {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("version", PojoToString.CURRENT_VERSION);
        result.put("format_version", FORMAT_VERSION);
        result.put("epoch", metadata.epoch.getEpoch());
        result.put("keyspaces", new ArrayList<>(keyspaces));

        Map<Integer, Object> peers = new LinkedHashMap<>();
        Set<NodeId> mentioned = new TreeSet<>(peersByNodeId.keySet());
        for (Map.Entry<NodeId, PeerSets> entry : peersByNodeId.entrySet())
        {
            PeerSets sets = entry.getValue();
            Map<String, Object> renderedSets = new LinkedHashMap<>();
            put(renderedSets, "read", sets.read, mentioned);
            put(renderedSets, "write", sets.write, mentioned);
            peers.put(entry.getKey().id(), renderedSets);
        }

        // The current address of each node, which for a CMS member may be a new address not yet committed
        EndpointLookup lookup = metadata.endpointLookup();
        Map<Integer, String> endpoints = new LinkedHashMap<>();
        for (NodeId nodeId : mentioned)
        {
            InetAddressAndPort endpoint = lookup.endpoint(nodeId);
            if (endpoint != null)
                endpoints.put(nodeId.id(), endpoint.getHostAddress(true));
        }

        result.put("endpoints", endpoints);
        result.put("peers", peers);
        return result;
    }

    /**
     * Emit one peer grouping as node ids by datacenter, recording each id so that the endpoints section can resolve it.
     * Always emitted, even when empty, so that consumers can rely on it.
     */
    private void put(Map<String, Object> renderedSets,
                     String name,
                     SortedMap<String, SortedSet<NodeId>> byDatacenter,
                     Set<NodeId> mentioned)
    {
        Map<String, Object> rendered = new LinkedHashMap<>();
        for (Map.Entry<String, SortedSet<NodeId>> entry : byDatacenter.entrySet())
        {
            SortedSet<NodeId> peers = entry.getValue();
            mentioned.addAll(peers);

            List<Integer> ids = new ArrayList<>(peers.size());
            for (NodeId peer : peers)
                ids.add(peer.id());
            rendered.put(entry.getKey(), ids);
        }
        renderedSets.put(name, rendered);
    }

    private static SortedMap<String, SortedSet<NodeId>> groupByDatacenter(Set<NodeId> peers,
                                                                          Map<NodeId, String> dcByNodeId)
    {
        SortedMap<String, SortedSet<NodeId>> peersByDatacenter = new TreeMap<>();
        for (NodeId peer : peers)
            peersByDatacenter.computeIfAbsent(dcByNodeId.getOrDefault(peer, Location.UNKNOWN.datacenter),
                                         dc -> new TreeSet<>()).add(peer);
        return peersByDatacenter;
    }

    private static boolean hasLeft(Directory directory, NodeId nodeId)
    {
        return directory.peerState(nodeId) == NodeState.LEFT;
    }

    private static String datacenterOf(Directory directory, NodeId nodeId)
    {
        Location location = directory.location(nodeId);
        return location == null ? Location.UNKNOWN.datacenter : location.datacenter;
    }

    /**
     * The peers of a single node, bucketed by the peer's datacenter. Peers which are read replicas and peers which are
     * write replicas are reported separately, since a node may be one without being the other while a topology change
     * is in flight.
     */
    public static final class PeerSets
    {
        private final SortedMap<String, SortedSet<NodeId>> read;
        private final SortedMap<String, SortedSet<NodeId>> write;

        private PeerSets(SortedMap<String, SortedSet<NodeId>> read,
                         SortedMap<String, SortedSet<NodeId>> write)
        {
            this.read = unmodifiable(read);
            this.write = unmodifiable(write);
        }

        private static SortedMap<String, SortedSet<NodeId>> unmodifiable(SortedMap<String, SortedSet<NodeId>> byDatacenter)
        {
            SortedMap<String, SortedSet<NodeId>> copy = new TreeMap<>();
            byDatacenter.forEach((dc, peers) -> copy.put(dc, Collections.unmodifiableSortedSet(peers)));
            return Collections.unmodifiableSortedMap(copy);
        }

        /** Nodes which share at least one range with this node in the read placement, by datacenter. */
        public SortedMap<String, SortedSet<NodeId>> read()
        {
            return read;
        }

        /** Nodes which share at least one range with this node in the write placement, by datacenter. */
        public SortedMap<String, SortedSet<NodeId>> write()
        {
            return write;
        }

        /** All peers on both sides, flattened across datacenters. */
        @VisibleForTesting
        SortedSet<NodeId> all()
        {
            SortedSet<NodeId> all = new TreeSet<>();
            read.values().forEach(all::addAll);
            write.values().forEach(all::addAll);
            return all;
        }

        @Override
        public String toString()
        {
            return "PeerSets{read=" + read + ", write=" + write + '}';
        }
    }
}
