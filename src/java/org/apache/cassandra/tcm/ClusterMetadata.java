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

package org.apache.cassandra.tcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.CMSIdentifierMismatchException;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeAddresses;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.DataPlacement;
import org.apache.cassandra.tcm.ownership.DataPlacements;
import org.apache.cassandra.tcm.ownership.PrimaryRangeComparator;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.tcm.sequences.InProgressSequences;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.apache.cassandra.config.CassandraRelevantProperties.LINE_SEPARATOR;
import static org.apache.cassandra.db.TypeSizes.sizeof;

public class ClusterMetadata
{
    public static final int EMPTY_METADATA_IDENTIFIER = 0;
    public static final Serializer serializer = new Serializer();

    public final int metadataIdentifier;

    public final Epoch epoch;
    public final IPartitioner partitioner;       // Set during (initial) construction and not modifiable via Transformer

    public final DistributedSchema schema;
    public final Directory directory;
    public final TokenMap tokenMap;
    public final DataPlacements placements;
    public final LockedRanges lockedRanges;
    public final InProgressSequences inProgressSequences;
    public final ImmutableMap<ExtensionKey<?,?>, ExtensionValue<?>> extensions;

    // These fields are lazy but only for the test purposes, since their computation requires initialization of the log ks
    private EndpointsForRange fullCMSReplicas;
    private Set<InetAddressAndPort> fullCMSEndpoints;
    private Set<NodeId> fullCMSIds;

    public ClusterMetadata(IPartitioner partitioner)
    {
        this(partitioner, Directory.EMPTY);
    }

    @VisibleForTesting
    public ClusterMetadata(IPartitioner partitioner, Directory directory)
    {
        this(partitioner, directory, DistributedSchema.first(directory.knownDatacenters()));
    }

    @VisibleForTesting
    public ClusterMetadata(IPartitioner partitioner, Directory directory, DistributedSchema schema)
    {
        this(EMPTY_METADATA_IDENTIFIER,
             Epoch.EMPTY,
             partitioner,
             schema,
             directory,
             new TokenMap(partitioner),
             DataPlacements.EMPTY,
             LockedRanges.EMPTY,
             InProgressSequences.EMPTY,
             ImmutableMap.of());
    }

    public ClusterMetadata(Epoch epoch,
                           IPartitioner partitioner,
                           DistributedSchema schema,
                           Directory directory,
                           TokenMap tokenMap,
                           DataPlacements placements,
                           LockedRanges lockedRanges,
                           InProgressSequences inProgressSequences,
                           Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions)
    {
        this(EMPTY_METADATA_IDENTIFIER,
             epoch,
             partitioner,
             schema,
             directory,
             tokenMap,
             placements,
             lockedRanges,
             inProgressSequences,
             extensions);
    }

    private ClusterMetadata(int metadataIdentifier,
                           Epoch epoch,
                           IPartitioner partitioner,
                           DistributedSchema schema,
                           Directory directory,
                           TokenMap tokenMap,
                           DataPlacements placements,
                           LockedRanges lockedRanges,
                           InProgressSequences inProgressSequences,
                           Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions)
    {
        // TODO: token map is a feature of the specific placement strategy, and so may not be a relevant component of
        //  ClusterMetadata in the long term. We need to consider how the actual components of metadata can be evolved
        //  over time.
        assert tokenMap == null || tokenMap.partitioner().getClass().equals(partitioner.getClass()) : "Partitioner for TokenMap doesn't match base partitioner";
        this.metadataIdentifier = metadataIdentifier;
        this.epoch = epoch;
        this.partitioner = partitioner;
        this.schema = schema;
        this.directory = directory;
        this.tokenMap = tokenMap;
        this.placements = placements;
        this.lockedRanges = lockedRanges;
        this.inProgressSequences = inProgressSequences;
        this.extensions = ImmutableMap.copyOf(extensions);
    }

    public Set<InetAddressAndPort> fullCMSMembers()
    {
        if (fullCMSEndpoints == null)
            this.fullCMSEndpoints = ImmutableSet.copyOf(placements.get(ReplicationParams.meta(this)).reads.byEndpoint().keySet());
        return fullCMSEndpoints;
    }

    public Set<NodeId> fullCMSMemberIds()
    {
        if (fullCMSIds == null)
            this.fullCMSIds = placements.get(ReplicationParams.meta(this)).reads.byEndpoint().keySet().stream().map(directory::peerId).collect(toImmutableSet());
        return fullCMSIds;
    }

    public EndpointsForRange fullCMSMembersAsReplicas()
    {
        if (fullCMSReplicas == null)
            fullCMSReplicas = placements.get(ReplicationParams.meta(this)).reads.forRange(MetaStrategy.entireRange).get();
        return fullCMSReplicas;
    }

    public boolean isCMSMember(InetAddressAndPort endpoint)
    {
        return fullCMSMembers().contains(endpoint);
    }

    public Transformer transformer()
    {
        return new Transformer(this, this.nextEpoch());
    }

    public ClusterMetadata forceEpoch(Epoch epoch)
    {
        // In certain circumstances, the last modified epoch of the individual
        // components may have been updated beyond the epoch we're specifying here.
        // An example is the execution of an UnsafeJoin transformation, where the
        // sub-steps (Start/Mid/Finish) are executed in series, each updating a
        // single ClusterMetadata and its individual components. At the end of that
        // sequence, the CM epoch is then set forcibly to ensure the UnsafeJoin only
        // increments the published epoch by one. As each component has its own last
        // modified epoch, we may also need to coerce those, but only if they are
        // greater than the epoch we're forcing here.
        return new ClusterMetadata(metadataIdentifier,
                                   epoch,
                                   partitioner,
                                   capLastModified(schema, epoch),
                                   capLastModified(directory, epoch),
                                   capLastModified(tokenMap, epoch),
                                   capLastModified(placements, epoch),
                                   capLastModified(lockedRanges, epoch),
                                   capLastModified(inProgressSequences, epoch),
                                   capLastModified(extensions, epoch));
    }

    public ClusterMetadata initializeClusterIdentifier(int clusterIdentifier)
    {
        if (this.metadataIdentifier != EMPTY_METADATA_IDENTIFIER)
            throw new IllegalStateException(String.format("Can only initialize cluster identifier once, but it was already set to %d", this.metadataIdentifier));

        if (clusterIdentifier == EMPTY_METADATA_IDENTIFIER)
            throw new IllegalArgumentException("Can not initialize cluster with empty cluster identifier");

        return new ClusterMetadata(clusterIdentifier,
                                   epoch,
                                   partitioner,
                                   schema,
                                   directory,
                                   tokenMap,
                                   placements,
                                   lockedRanges,
                                   inProgressSequences,
                                   extensions);
    }

    private static Map<ExtensionKey<?,?>, ExtensionValue<?>> capLastModified(Map<ExtensionKey<?,?>, ExtensionValue<?>> original, Epoch maxEpoch)
    {
        Map<ExtensionKey<?, ?>, ExtensionValue<?>> updated = new HashMap<>();
        original.forEach((key, value) -> {
            ExtensionValue<?> newValue = value == null || value.lastModified().isEqualOrBefore(maxEpoch)
                                         ? value
                                         : (ExtensionValue<?>)value.withLastModified(maxEpoch);
            updated.put(key, newValue);
        });
        return updated;
    }

    @SuppressWarnings("unchecked")
    private static <V> V capLastModified(MetadataValue<V> value, Epoch maxEpoch)
    {
        return value == null || value.lastModified().isEqualOrBefore(maxEpoch)
               ? (V)value
               : value.withLastModified(maxEpoch);
    }

    public Epoch nextEpoch()
    {
        return epoch.nextEpoch();
    }

    public DataPlacement writePlacementAllSettled(KeyspaceMetadata ksm)
    {
        ClusterMetadata metadata = this;
        Iterator<MultiStepOperation<?>> iter = metadata.inProgressSequences.iterator();
        while (iter.hasNext())
        {
            Transformation.Result result = iter.next().applyTo(metadata);
            assert result.isSuccess();
            metadata = result.success().metadata;
        }
        return metadata.placements.get(ksm.params.replication);
    }

    // TODO Remove this as it isn't really an equivalent to the previous concept of pending ranges
    public boolean hasPendingRangesFor(KeyspaceMetadata ksm, Token token)
    {
        ReplicaGroups writes = placements.get(ksm.params.replication).writes;
        ReplicaGroups reads = placements.get(ksm.params.replication).reads;
        if (ksm.params.replication.isMeta())
            return !reads.equals(writes);
        return !reads.forToken(token).equals(writes.forToken(token));
    }

    // TODO Remove this as it isn't really an equivalent to the previous concept of pending ranges
    public boolean hasPendingRangesFor(KeyspaceMetadata ksm, InetAddressAndPort endpoint)
    {
        ReplicaGroups writes = placements.get(ksm.params.replication).writes;
        ReplicaGroups reads = placements.get(ksm.params.replication).reads;
        return !writes.byEndpoint().get(endpoint).equals(reads.byEndpoint().get(endpoint));
    }

    public Collection<Range<Token>> localWriteRanges(KeyspaceMetadata metadata)
    {
        return writeRanges(metadata, FBUtilities.getBroadcastAddressAndPort());
    }

    public Collection<Range<Token>> writeRanges(KeyspaceMetadata metadata, InetAddressAndPort peer)
    {
        return placements.get(metadata.params.replication).writes.byEndpoint().get(peer).ranges();
    }

    // TODO Remove this as it isn't really an equivalent to the previous concept of pending ranges
    public Map<Range<Token>, VersionedEndpoints.ForRange> pendingRanges(KeyspaceMetadata metadata)
    {
        Map<Range<Token>, VersionedEndpoints.ForRange> map = new HashMap<>();
        ReplicaGroups writes = placements.get(metadata.params.replication).writes;
        ReplicaGroups reads = placements.get(metadata.params.replication).reads;

        // first, pending ranges as the result of range splitting or merging
        // i.e. new ranges being created through join/leave
        List<Range<Token>> pending = new ArrayList<>(writes.ranges());
        pending.removeAll(reads.ranges());
        for (Range<Token> p : pending)
            map.put(p, placements.get(metadata.params.replication).writes.forRange(p));

        // next, ranges where the ranges themselves are not changing, but the replicas are
        // i.e. replacement or RF increase
        writes.forEach((range, endpoints) -> {
            VersionedEndpoints.ForRange readGroup = reads.forRange(range);
            if (!readGroup.equals(endpoints))
                map.put(range, VersionedEndpoints.forRange(endpoints.lastModified(),
                                                           endpoints.get().filter(r -> !readGroup.get().contains(r))));
        });

        return map;
    }

    // TODO Remove this as it isn't really an equivalent to the previous concept of pending endpoints
    public VersionedEndpoints.ForToken pendingEndpointsFor(KeyspaceMetadata metadata, Token t)
    {
        VersionedEndpoints.ForToken writeEndpoints = placements.get(metadata.params.replication).writes.forToken(t);
        VersionedEndpoints.ForToken readEndpoints = placements.get(metadata.params.replication).reads.forToken(t);
        EndpointsForToken.Builder endpointsForToken = writeEndpoints.get().newBuilder(writeEndpoints.size() - readEndpoints.size());

        for (Replica writeReplica : writeEndpoints.get())
        {
            if (!readEndpoints.get().contains(writeReplica))
                endpointsForToken.add(writeReplica);
        }
        return VersionedEndpoints.forToken(writeEndpoints.lastModified(), endpointsForToken.build());
    }

    public static class Transformer
    {
        private final ClusterMetadata base;
        private final Epoch epoch;
        private final IPartitioner partitioner;
        private DistributedSchema schema;
        private Directory directory;
        private TokenMap tokenMap;
        private DataPlacements placements;
        private LockedRanges lockedRanges;
        private InProgressSequences inProgressSequences;
        private final Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions;
        private final Set<MetadataKey> modifiedKeys;

        private Transformer(ClusterMetadata metadata, Epoch epoch)
        {
            this.base = metadata;
            this.epoch = epoch;
            this.partitioner = metadata.partitioner;
            this.schema = metadata.schema;
            this.directory = metadata.directory;
            this.tokenMap = metadata.tokenMap;
            this.placements = metadata.placements;
            this.lockedRanges = metadata.lockedRanges;
            this.inProgressSequences = metadata.inProgressSequences;
            extensions = new HashMap<>(metadata.extensions);
            modifiedKeys = new HashSet<>();
        }

        public Transformer with(DistributedSchema schema)
        {
            this.schema = schema;
            return this;
        }

        public Transformer with(Directory directory)
        {
            this.directory = directory;
            return this;
        }

        public Transformer register(NodeAddresses addresses, Location location, NodeVersion version)
        {
            directory = directory.with(addresses, location, version);
            return this;
        }

        public Transformer unregister(NodeId nodeId)
        {
            directory = directory.without(nodeId);
            return this;
        }

        public Transformer withNewAddresses(NodeId nodeId, NodeAddresses addresses)
        {
            directory = directory.withNodeAddresses(nodeId, addresses);
            return this;
        }

        public Transformer withVersion(NodeId nodeId, NodeVersion version)
        {
            directory = directory.withNodeVersion(nodeId, version);
            return this;
        }

        public Transformer withNodeState(NodeId id, NodeState state)
        {
            directory = directory.withNodeState(id, state);
            return this;
        }

        public Transformer proposeToken(NodeId nodeId, Collection<Token> tokens)
        {
            tokenMap = tokenMap.assignTokens(nodeId, tokens);
            return this;
        }

        public Transformer addToRackAndDC(NodeId nodeId)
        {
            directory = directory.withRackAndDC(nodeId);
            return this;
        }

        public Transformer unproposeTokens(NodeId nodeId)
        {
            tokenMap = tokenMap.unassignTokens(nodeId);
            directory = directory.withoutRackAndDC(nodeId);
            return this;
        }

        public Transformer moveTokens(NodeId nodeId, Collection<Token> tokens)
        {
            tokenMap = tokenMap.unassignTokens(nodeId)
                               .assignTokens(nodeId, tokens);
            return this;
        }

        public Transformer join(NodeId nodeId)
        {
            directory = directory.withNodeState(nodeId, NodeState.JOINED);
            return this;
        }

        public Transformer replaced(NodeId replaced, NodeId replacement)
        {
            Collection<Token> transferringTokens = tokenMap.tokens(replaced);
            tokenMap = tokenMap.unassignTokens(replaced)
                               .assignTokens(replacement, transferringTokens);
            directory = directory.without(replaced)
                                 .withRackAndDC(replacement)
                                 .withNodeState(replacement, NodeState.JOINED);
            return this;
        }

        public Transformer proposeRemoveNode(NodeId id)
        {
            tokenMap = tokenMap.unassignTokens(id);
            return this;
        }

        public Transformer left(NodeId id)
        {
            tokenMap = tokenMap.unassignTokens(id);
            directory = directory.withNodeState(id, NodeState.LEFT)
                                 .withoutRackAndDC(id);
            return this;
        }

        public Transformer with(DataPlacements placements)
        {
            this.placements = placements;
            return this;
        }

        public Transformer with(LockedRanges lockedRanges)
        {
            this.lockedRanges = lockedRanges;
            return this;
        }

        public Transformer with(InProgressSequences sequences)
        {
            this.inProgressSequences = sequences;
            return this;
        }

        public Transformer with(ExtensionKey<?, ?> key, ExtensionValue<?> obj)
        {
            if (MetadataKeys.CORE_METADATA.contains(key))
                throw new IllegalArgumentException("Core cluster metadata objects should be addressed directly, " +
                                                   "not using the associated MetadataKey");

            if (!key.valueType.isInstance(obj))
                throw new IllegalArgumentException("Value of type " + obj.getClass() +
                                                   " is incompatible with type for key " + key +
                                                   " (" + key.valueType + ")");

            extensions.put(key, obj);
            modifiedKeys.add(key);
            return this;
        }

        public Transformer withIfAbsent(ExtensionKey<?, ?> key, ExtensionValue<?> obj)
        {
            if (extensions.containsKey(key))
                return this;
            return with(key, obj);
        }

        public Transformer without(ExtensionKey<?, ?> key)
        {
            if (MetadataKeys.CORE_METADATA.contains(key))
                throw new IllegalArgumentException("Core cluster metadata objects should be addressed directly, " +
                                                   "not using the associated MetadataKey");
            if (extensions.remove(key) != null)
                modifiedKeys.add(key);
            return this;
        }

        public Transformed build()
        {
            // Process extension first as a) these are actually mutable and b) they are added to the set of
            // modified keys when added/updated/removed
            for (MetadataKey key : modifiedKeys)
            {
                ExtensionValue<?> mutable = extensions.get(key);
                if (null != mutable)
                    mutable.withLastModified(epoch);
            }

            if (schema != base.schema)
            {
                modifiedKeys.add(MetadataKeys.SCHEMA);
                schema = schema.withLastModified(epoch);
            }

            if (directory != base.directory)
            {
                modifiedKeys.add(MetadataKeys.NODE_DIRECTORY);
                directory = directory.withLastModified(epoch);
            }

            if (tokenMap != base.tokenMap)
            {
                modifiedKeys.add(MetadataKeys.TOKEN_MAP);
                tokenMap = tokenMap.withLastModified(epoch);
            }

            if (placements != base.placements)
            {
                modifiedKeys.add(MetadataKeys.DATA_PLACEMENTS);
                // sort all endpoint lists to preserve primary replica
                if (CassandraRelevantProperties.TCM_SORT_REPLICA_GROUPS.getBoolean())
                {
                    PrimaryRangeComparator comparator = new PrimaryRangeComparator(tokenMap, directory);
                    placements = DataPlacements.sortReplicaGroups(placements, comparator);
                }
                placements = placements.withLastModified(epoch);
            }

            if (lockedRanges != base.lockedRanges)
            {
                modifiedKeys.add(MetadataKeys.LOCKED_RANGES);
                lockedRanges = lockedRanges.withLastModified(epoch);
            }

            if (inProgressSequences != base.inProgressSequences)
            {
                modifiedKeys.add(MetadataKeys.IN_PROGRESS_SEQUENCES);
                inProgressSequences = inProgressSequences.withLastModified(epoch);
            }

            return new Transformed(new ClusterMetadata(base.metadataIdentifier,
                                                       epoch,
                                                       partitioner,
                                                       schema,
                                                       directory,
                                                       tokenMap,
                                                       placements,
                                                       lockedRanges,
                                                       inProgressSequences,
                                                       extensions),
                                   ImmutableSet.copyOf(modifiedKeys));
        }

        public ClusterMetadata buildForGossipMode()
        {
            return new ClusterMetadata(base.metadataIdentifier,
                                       Epoch.UPGRADE_GOSSIP,
                                       partitioner,
                                       schema,
                                       directory,
                                       tokenMap,
                                       placements,
                                       lockedRanges,
                                       inProgressSequences,
                                       extensions);
        }

        @Override
        public String toString()
        {
            return "Transformer{" +
                   "baseEpoch=" + base.epoch +
                   ", epoch=" + epoch +
                   ", partitioner=" + partitioner +
                   ", schema=" + schema +
                   ", directory=" + schema +
                   ", tokenMap=" + tokenMap +
                   ", placement=" + placements +
                   ", lockedRanges=" + lockedRanges +
                   ", inProgressSequences=" + inProgressSequences +
                   ", extensions=" + extensions +
                   ", modifiedKeys=" + modifiedKeys +
                   '}';
        }

        public static class Transformed
        {
            public final ClusterMetadata metadata;
            public final ImmutableSet<MetadataKey> modifiedKeys;

            public Transformed(ClusterMetadata metadata, ImmutableSet<MetadataKey> modifiedKeys)
            {
                this.metadata = metadata;
                this.modifiedKeys = modifiedKeys;
            }
        }
    }

    public String legacyToString()
    {
        StringBuilder sb = new StringBuilder();
        Set<Pair<Token, InetAddressAndPort>> normal = new HashSet<>();
        Set<Pair<Token, InetAddressAndPort>> bootstrapping = new HashSet<>();
        Set<InetAddressAndPort> leaving = new HashSet<>();

        for (Map.Entry<NodeId, NodeState> entry : directory.states.entrySet())
        {
            InetAddressAndPort endpoint = directory.endpoint(entry.getKey());
            switch (entry.getValue())
            {
                case BOOTSTRAPPING:
                    for (Token t : tokenMap.tokens(entry.getKey()))
                        bootstrapping.add(Pair.create(t, endpoint));
                    break;
                case LEAVING:
                    leaving.add(endpoint);
                    break;
                case JOINED:
                    for (Token t : tokenMap.tokens(entry.getKey()))
                        normal.add(Pair.create(t, endpoint));
                    break;
                case MOVING:
                    // todo when adding MOVE
                    break;
            }
        }

        if (!normal.isEmpty())
        {
            sb.append("Normal Tokens:");
            sb.append(LINE_SEPARATOR.getString());
            for (Pair<Token, InetAddressAndPort> ep : normal)
            {
                sb.append(ep.right);
                sb.append(':');
                sb.append(ep.left);
                sb.append(LINE_SEPARATOR.getString());
            }
        }

        if (!bootstrapping.isEmpty())
        {
            sb.append("Bootstrapping Tokens:" );
            sb.append(LINE_SEPARATOR.getString());
            for (Pair<Token, InetAddressAndPort> entry : bootstrapping)
            {
                sb.append(entry.right).append(':').append(entry.left);
                sb.append(LINE_SEPARATOR.getString());
            }
        }

        if (!leaving.isEmpty())
        {
            sb.append("Leaving Endpoints:");
            sb.append(LINE_SEPARATOR.getString());
            for (InetAddressAndPort ep : leaving)
            {
                sb.append(ep);
                sb.append(LINE_SEPARATOR.getString());
            }
        }
        return sb.toString();
    }

    @Override
    public String toString()
    {
        return "ClusterMetadata{" +
               "epoch=" + epoch +
               ", schema=" + schema +
               ", directory=" + directory +
               ", tokenMap=" + tokenMap +
               ", placements=" + placements +
               ", lockedRanges=" + lockedRanges +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof ClusterMetadata)) return false;
        ClusterMetadata that = (ClusterMetadata) o;
        return epoch.equals(that.epoch) &&
               schema.equals(that.schema) &&
               directory.equals(that.directory) &&
               tokenMap.equals(that.tokenMap) &&
               placements.equals(that.placements) &&
               lockedRanges.equals(that.lockedRanges) &&
               inProgressSequences.equals(that.inProgressSequences) &&
               extensions.equals(that.extensions);
    }

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadata.class);

    public void dumpDiff(ClusterMetadata other)
    {
        if (!epoch.equals(other.epoch))
        {
            logger.warn("Epoch {} != {}", epoch, other.epoch);
        }
        if (!schema.equals(other.schema))
        {
            Keyspaces.KeyspacesDiff diff = Keyspaces.diff(schema.getKeyspaces(), other.schema.getKeyspaces());
            logger.warn("Schemas differ {}", diff);
        }
        if (!directory.equals(other.directory))
        {
            logger.warn("Directories differ:");
            directory.dumpDiff(other.directory);
        }
        if (!tokenMap.equals(other.tokenMap))
        {
            logger.warn("Token maps differ:");
            tokenMap.dumpDiff(other.tokenMap);
        }
        if (!placements.equals(other.placements))
        {
            logger.warn("Placements differ:");
            placements.dumpDiff(other.placements);
        }
        if (!lockedRanges.equals(other.lockedRanges))
        {
            logger.warn("Locked ranges differ: {} != {}", lockedRanges, other.lockedRanges);
        }
        if (!inProgressSequences.equals(other.inProgressSequences))
        {
            logger.warn("In progress sequences differ: {} != {}", inProgressSequences, other.inProgressSequences);
        }
        if (!extensions.equals(other.extensions))
        {
            logger.warn("Extensions differ: {} != {}", extensions, other.extensions);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(epoch, schema, directory, tokenMap, placements, lockedRanges, inProgressSequences, extensions);
    }

    public static ClusterMetadata current()
    {
        return ClusterMetadataService.instance().metadata();
    }

    public static void checkIdentifier(int remoteIdentifier)
    {
        ClusterMetadata metadata = currentNullable();
        if (metadata != null)
        {
            int currentIdentifier = metadata.metadataIdentifier;
            // We haven't yet joined CMS fully
            if (currentIdentifier == EMPTY_METADATA_IDENTIFIER)
                return;

            // Peer hasn't yet joined CMS fully
            if (remoteIdentifier == EMPTY_METADATA_IDENTIFIER)
                return;

            if (currentIdentifier != remoteIdentifier)
                throw new CMSIdentifierMismatchException(String.format("Cluster Metadata Identifier mismatch. Node is attempting to communicate with a node from a different cluster. Current identifier %d. Remote identifier: %d", currentIdentifier, remoteIdentifier));
        }
    }

    /**
     * Startup of some services may race with cluster metadata initialization. We allow those services to
     * gracefully handle scenarios when it is not yet initialized.
     */
    public static ClusterMetadata currentNullable()
    {
        ClusterMetadataService service = ClusterMetadataService.instance();
        if (service == null)
            return null;
        return service.metadata();
    }

    public NodeId myNodeId()
    {
        return directory.peerId(FBUtilities.getBroadcastAddressAndPort());
    }

    public NodeState myNodeState()
    {
        NodeId nodeId = myNodeId();
        if (myNodeId() != null)
            return directory.peerState(nodeId);
        return null;
    }

    public boolean metadataSerializationUpgradeInProgress()
    {
        return !directory.clusterMaxVersion.serializationVersion().equals(directory.clusterMinVersion.serializationVersion());
    }

    public static class Serializer implements MetadataSerializer<ClusterMetadata>
    {
        @Override
        public void serialize(ClusterMetadata metadata, DataOutputPlus out, Version version) throws IOException
        {
            if (version.isAtLeast(Version.V1))
                out.writeUTF(metadata.partitioner.getClass().getCanonicalName());

            if (version.isAtLeast(Version.V2))
                out.writeUnsignedVInt32(metadata.metadataIdentifier);

            Epoch.serializer.serialize(metadata.epoch, out);

            if (version.isBefore(Version.V1))
                out.writeUTF(metadata.partitioner.getClass().getCanonicalName());

            DistributedSchema.serializer.serialize(metadata.schema, out, version);
            Directory.serializer.serialize(metadata.directory, out, version);
            TokenMap.serializer.serialize(metadata.tokenMap, out, version);
            DataPlacements.serializer.serialize(metadata.placements, out, version);
            LockedRanges.serializer.serialize(metadata.lockedRanges, out, version);
            InProgressSequences.serializer.serialize(metadata.inProgressSequences, out, version);
            out.writeInt(metadata.extensions.size());
            for (Map.Entry<ExtensionKey<?, ?>, ExtensionValue<?>> entry : metadata.extensions.entrySet())
            {
                ExtensionKey<?, ?> key = entry.getKey();
                ExtensionValue<?> value = entry.getValue();
                ExtensionKey.serializer.serialize(key, out, version);
                assert key.valueType.isInstance(value);
                value.serialize(out, version);
            }
        }

        @Override
        public ClusterMetadata deserialize(DataInputPlus in, Version version) throws IOException
        {
            IPartitioner partitioner = null;
            if (version.isAtLeast(Version.V1))
                partitioner = FBUtilities.newPartitioner(in.readUTF());

            int clusterIdentifier = EMPTY_METADATA_IDENTIFIER;
            if (version.isAtLeast(Version.V2))
            {
                clusterIdentifier = in.readUnsignedVInt32();
                checkIdentifier(clusterIdentifier);
            }

            Epoch epoch = Epoch.serializer.deserialize(in);

            if (version.isBefore(Version.V1))
                partitioner = FBUtilities.newPartitioner(in.readUTF());

            DistributedSchema schema = DistributedSchema.serializer.deserialize(in, version);
            Directory dir = Directory.serializer.deserialize(in, version);
            TokenMap tokenMap = TokenMap.serializer.deserialize(in, version);
            DataPlacements placements = DataPlacements.serializer.deserialize(in, version);
            LockedRanges lockedRanges = LockedRanges.serializer.deserialize(in, version);
            InProgressSequences ips = InProgressSequences.serializer.deserialize(in, version);
            int items = in.readInt();
            Map<ExtensionKey<?, ?>, ExtensionValue<?>> extensions = new HashMap<>(items);
            for (int i = 0; i < items; i++)
            {
                ExtensionKey<?, ?> key = ExtensionKey.serializer.deserialize(in, version);
                ExtensionValue<?> value = key.newValue();
                value.deserialize(in, version);
                extensions.put(key, value);
            }
            return new ClusterMetadata(clusterIdentifier,
                                       epoch,
                                       partitioner,
                                       schema,
                                       dir,
                                       tokenMap,
                                       placements,
                                       lockedRanges,
                                       ips,
                                       extensions);
        }

        @Override
        public long serializedSize(ClusterMetadata metadata, Version version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Map.Entry<ExtensionKey<?, ?>, ExtensionValue<?>> entry : metadata.extensions.entrySet())
                size += ExtensionKey.serializer.serializedSize(entry.getKey(), version) +
                        entry.getValue().serializedSize(version);

            if (version.isAtLeast(Version.V2))
                size += TypeSizes.sizeofUnsignedVInt(metadata.metadataIdentifier);

            size += Epoch.serializer.serializedSize(metadata.epoch) +
                    sizeof(metadata.partitioner.getClass().getCanonicalName()) +
                    DistributedSchema.serializer.serializedSize(metadata.schema, version) +
                    Directory.serializer.serializedSize(metadata.directory, version) +
                    TokenMap.serializer.serializedSize(metadata.tokenMap, version) +
                    DataPlacements.serializer.serializedSize(metadata.placements, version) +
                    LockedRanges.serializer.serializedSize(metadata.lockedRanges, version) +
                    InProgressSequences.serializer.serializedSize(metadata.inProgressSequences, version);

            return size;
        }

        public static IPartitioner getPartitioner(DataInputPlus in, Version version) throws IOException
        {
            if (version.isAtLeast(Version.V1))
                return FBUtilities.newPartitioner(in.readUTF());

            Epoch.serializer.deserialize(in);
            in.readUnsignedVInt();
            in.readBoolean();
            return FBUtilities.newPartitioner(in.readUTF());
        }
    }
}
