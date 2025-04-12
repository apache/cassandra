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

package org.apache.cassandra.tcm.transformations.cms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.CMSPlacementStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicationFactor;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.locator.MetaStrategy.entireRange;
import static org.apache.cassandra.tcm.CMSOperations.REPLICATION_FACTOR;

public abstract class PrepareCMSReconfiguration implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCMSReconfiguration.class);
    final Set<NodeId> downNodes;

    public PrepareCMSReconfiguration(Set<NodeId> downNodes)
    {
        this.downNodes = downNodes;
    }

    protected abstract Predicate<NodeId> additionalFilteringPredicate(Set<NodeId> downNodes);
    protected abstract ReplicationParams newReplicationParams(ClusterMetadata prev);

    protected Transformation.Result executeInternal(ClusterMetadata prev, Function<ClusterMetadata.Transformer, ClusterMetadata.Transformer> transform)
    {
        Diff diff = getDiff(prev);
        if (!diff.hasChanges())
        {
            logger.info("Proposed CMS reconfiguration resulted in no required modifications at epoch {}", prev.epoch.getEpoch());
            return Transformation.success(prev.transformer(), LockedRanges.AffectedRanges.EMPTY);
        }

        LockedRanges.Key lockKey = LockedRanges.keyFor(prev.nextEpoch());
        Set<NodeId> cms = prev.fullCMSMembers().stream().map(prev.directory::peerId).collect(Collectors.toSet());
        Set<NodeId> tmp = new HashSet<>(cms);
        tmp.addAll(diff.additions);
        tmp.removeAll(diff.removals);
        if (tmp.isEmpty())
            return new Transformation.Rejected(INVALID, String.format("Applying diff %s to %s would leave CMS empty", cms, diff));

        ClusterMetadata.Transformer transformer = prev.transformer()
                                                      .with(prev.inProgressSequences.with(ReconfigureCMS.SequenceKey.instance,
                                                                                          ReconfigureCMS.newSequence(lockKey, diff)))
                                                      .with(prev.lockedRanges.lock(lockKey, LockedRanges.AffectedRanges.singleton(ReplicationParams.meta(prev), entireRange)));
        return Transformation.success(transform.apply(transformer), LockedRanges.AffectedRanges.EMPTY);
    }

    private Diff getDiff(ClusterMetadata prev)
    {
        Set<NodeId> currentCms = prev.fullCMSMemberIds();
        Map<String, Integer> dcRF = extractRf(newReplicationParams(prev));
        Set<NodeId> newCms = prepareNewCMS(dcRF, prev);
        if (newCms.equals(currentCms))
            return Diff.NOCHANGE;
        return diff(currentCms, newCms);
    }

    private Set<NodeId> prepareNewCMS(Map<String, Integer> dcRf, ClusterMetadata prev)
    {
        CMSPlacementStrategy placementStrategy = new CMSPlacementStrategy(dcRf, additionalFilteringPredicate(downNodes));
        return placementStrategy.reconfigure(prev);
    }

    public void verify(ClusterMetadata prev)
    {
        Map<String, Integer> dcRf = extractRf(newReplicationParams(prev));
        int expectedSize = dcRf.values().stream().mapToInt(Integer::intValue).sum();
        Set<NodeId> newCms = prepareNewCMS(dcRf, prev);
        if (newCms.size() < (expectedSize / 2) + 1)
            throw new IllegalStateException("Too many nodes are currently DOWN to safely perform the reconfiguration");
    }

    private static void serializeDownNodes(PrepareCMSReconfiguration transformation, DataOutputPlus out, Version version) throws IOException
    {
        out.writeUnsignedVInt32(transformation.downNodes.size());
        for (NodeId nodeId : transformation.downNodes)
            NodeId.serializer.serialize(nodeId, out, version);
    }

    private static Set<NodeId> deserializeDownNodes(DataInputPlus in, Version version) throws IOException
    {
        Set<NodeId> downNodes = new HashSet<>();
        int count = in.readUnsignedVInt32();
        for (int i = 0; i < count; i++)
            downNodes.add(NodeId.serializer.deserialize(in, version));
        return downNodes;
    }

    private static long serializedDownNodesSize(PrepareCMSReconfiguration transformation, Version version)
    {
        long size = TypeSizes.sizeofUnsignedVInt(transformation.downNodes.size());
        for (NodeId nodeId : transformation.downNodes)
            size += NodeId.serializer.serializedSize(nodeId, version);
        return size;
    }

    public static class Simple extends PrepareCMSReconfiguration
    {
        public static final Simple.Serializer serializer = new Serializer();

        private final NodeId toReplace;

        public Simple(NodeId toReplace, Set<NodeId> downNodes)
        {
            super(downNodes);
            this.toReplace = toReplace;
        }

        @Override
        protected Predicate<NodeId> additionalFilteringPredicate(Set<NodeId> downNodes)
        {
            // exclude the node being replaced from the new CMS, and avoid any down nodes
            return nodeId -> !nodeId.equals(toReplace) && !downNodes.contains(nodeId);
        }

        @Override
        protected ReplicationParams newReplicationParams(ClusterMetadata prev)
        {
            // a simple reconfiguration retains the existing replication params
            return ReplicationParams.meta(prev);
        }

        @Override
        public Kind kind()
        {
            return Kind.PREPARE_SIMPLE_CMS_RECONFIGURATION;
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            if (!prev.fullCMSMembers().contains(prev.directory.getNodeAddresses(toReplace).broadcastAddress))
                return new Rejected(INVALID, String.format("%s is not a member of CMS. Members: %s", toReplace, prev.fullCMSMembers()));

            // A simple reconfiguration only kicks off the sequence of membership changes, no additional metadata
            // transformation is required.
            return executeInternal(prev, t -> t);
        }

        public String toString()
        {
            return "PrepareCMSReconfiguration#Simple{" +
                   "toReplace=" + toReplace +
                   ", downNodes=" + downNodes +
                   '}';
        }

        public static class Serializer implements AsymmetricMetadataSerializer<Transformation, Simple>
        {
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                Simple transformation = (Simple) t;
                NodeId.serializer.serialize(transformation.toReplace, out, version);
                if (version.isAtLeast(Version.V3))
                    PrepareCMSReconfiguration.serializeDownNodes(transformation, out, version);
            }

            public Simple deserialize(DataInputPlus in, Version version) throws IOException
            {
                NodeId replaceNode = NodeId.serializer.deserialize(in, version);
                Set<NodeId> downNodes = version.isAtLeast(Version.V3)
                                        ? PrepareCMSReconfiguration.deserializeDownNodes(in, version)
                                        : Collections.emptySet();
                return new Simple(replaceNode, downNodes);
            }

            public long serializedSize(Transformation t, Version version)
            {
                Simple transformation = (Simple) t;
                long size = NodeId.serializer.serializedSize(transformation.toReplace, version);
                if (version.isAtLeast(Version.V3))
                    size += PrepareCMSReconfiguration.serializedDownNodesSize(transformation, version);
                return size;
            }
        }
    }

    public static class Complex extends PrepareCMSReconfiguration
    {
        public static final Complex.Serializer serializer = new Complex.Serializer();

        private final ReplicationParams replicationParams;

        public Complex(ReplicationParams replicationParams, Set<NodeId> downNodes)
        {
            super(downNodes);
            this.replicationParams = replicationParams;
        }

        @Override
        protected Predicate<NodeId> additionalFilteringPredicate(Set<NodeId> downNodes)
        {
            // exclude any down nodes
            return nodeId -> !downNodes.contains(nodeId);
        }

        @Override
        protected ReplicationParams newReplicationParams(ClusterMetadata ignored)
        {
            // desired replication params are supplied, so just return them
            return replicationParams;
        }

        @Override
        public Kind kind()
        {
            return Kind.PREPARE_COMPLEX_CMS_RECONFIGURATION;
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            // In a complex reconfiguration, in addition to initiating the sequence of membership changes,
            // we're modifying the replication params of the metadata keyspace so we supply a function to do that
            KeyspaceMetadata keyspace = prev.schema.getKeyspaceMetadata(SchemaConstants.METADATA_KEYSPACE_NAME);
            KeyspaceMetadata newKeyspace = keyspace.withSwapped(new KeyspaceParams(keyspace.params.durableWrites, replicationParams));

            return executeInternal(prev,
                                   transformer -> transformer.with(prev.placements.replaceParams(prev.nextEpoch(), ReplicationParams.meta(prev), replicationParams))
                                                             .with(new DistributedSchema(prev.schema.getKeyspaces().withAddedOrUpdated(newKeyspace))));
        }

        public String toString()
        {
            return "PrepareCMSReconfiguration#Complex{" +
                   "replicationParams=" + replicationParams +
                   ", downNodes=" + downNodes +
                   '}';
        }

        public static class Serializer implements AsymmetricMetadataSerializer<Transformation, Complex>
        {
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                Complex transformation = (Complex) t;
                ReplicationParams.serializer.serialize(transformation.replicationParams, out, version);
                if (version.isAtLeast(Version.V3))
                    PrepareCMSReconfiguration.serializeDownNodes(transformation, out, version);
            }

            public Complex deserialize(DataInputPlus in, Version version) throws IOException
            {
                ReplicationParams params = ReplicationParams.serializer.deserialize(in, version);
                Set<NodeId> downNodes = version.isAtLeast(Version.V3)
                                        ? PrepareCMSReconfiguration.deserializeDownNodes(in, version)
                                        : Collections.emptySet();
                return new Complex(params, downNodes);
            }

            public long serializedSize(Transformation t, Version version)
            {
                Complex transformation = (Complex) t;
                long size = ReplicationParams.serializer.serializedSize(transformation.replicationParams, version);
                if (version.isAtLeast(Version.V3))
                    size += PrepareCMSReconfiguration.serializedDownNodesSize(transformation, version);
                return size;
            }
        }
    }

    public static Diff diff(Set<NodeId> currentCms, Set<NodeId> newCms)
    {
        assert !currentCms.contains(null) : "Current CMS contains a null value " + currentCms;
        assert !newCms.contains(null) : "New CMS contains a null value " + newCms;

        List<NodeId> additions = new ArrayList<>();
        for (NodeId node : newCms)
        {
            if (!currentCms.contains(node))
                additions.add(node);
        }

        List<NodeId> removals = new ArrayList<>();
        for (NodeId nodeId : currentCms)
        {
            if (!newCms.contains(nodeId))
                removals.add(nodeId);
        }

        return new Diff(additions, removals);
    }

    private static Map<String, Integer> extractRf(ReplicationParams params)
    {
        if (params.isMeta())
        {
            assert !params.options.containsKey(REPLICATION_FACTOR);
            Map<String, Integer> dcRf = new HashMap<>();
            for (Map.Entry<String, String> entry : params.options.entrySet())
            {
                String dc = entry.getKey();
                ReplicationFactor rf = ReplicationFactor.fromString(entry.getValue());
                dcRf.put(dc, rf.fullReplicas);
            }
            return dcRf;
        }
        else
        {
            throw new IllegalStateException("Can't parse the params: " + params);
        }
    }

    public static boolean needsReconfiguration(ClusterMetadata metadata)
    {
        Map<String, Integer> dcRf = extractRf(ReplicationParams.meta(metadata));
        Set<NodeId> currentCms = metadata.fullCMSMembers()
                                         .stream()
                                         .map(metadata.directory::peerId)
                                         .collect(Collectors.toSet());
        int expectedSize = dcRf.values().stream().mapToInt(Integer::intValue).sum();
        if (currentCms.size() != expectedSize)
            return true;
        for (Map.Entry<String, Integer> dcRfEntry : dcRf.entrySet())
        {
            Collection<InetAddressAndPort> nodesInDc = metadata.directory.allDatacenterEndpoints().get(dcRfEntry.getKey());
            if (nodesInDc.size() < dcRfEntry.getValue())
                return true;
        }

        CMSPlacementStrategy placementStrategy = new CMSPlacementStrategy(dcRf, nodeId -> true);
        Set<NodeId> newCms = placementStrategy.reconfigure(metadata);
        return !currentCms.equals(newCms);
    }

    public static class Diff
    {
        public static final Diff NOCHANGE = new Diff(Collections.emptyList(), Collections.emptyList());
        public static final Serializer serializer = new Serializer();

        public final List<NodeId> additions;
        public final List<NodeId> removals;

        public Diff(List<NodeId> additions, List<NodeId> removals)
        {
            this.additions = additions;
            this.removals = removals;
        }

        public boolean hasChanges()
        {
            return this != NOCHANGE;
        }

        public String toString()
        {
            return "Diff{" +
                   "additions=" + additions +
                   ", removals=" + removals +
                   '}';
        }

        public static class Serializer implements MetadataSerializer<Diff>
        {
            public void serialize(Diff diff, DataOutputPlus out, Version version) throws IOException
            {
                out.writeInt(diff.additions.size());
                for (NodeId addition : diff.additions)
                    NodeId.serializer.serialize(addition, out, version);

                out.writeInt(diff.removals.size());
                for (NodeId removal : diff.removals)
                    NodeId.serializer.serialize(removal, out, version);
            }

            public Diff deserialize(DataInputPlus in, Version version) throws IOException
            {
                int additionsCount = in.readInt();
                List<NodeId> additions = new ArrayList<>();
                for (int i = 0; i < additionsCount; i++)
                {
                    NodeId addition = NodeId.serializer.deserialize(in, version);
                    additions.add(addition);
                }

                int removalsCount = in.readInt();
                List<NodeId> removals = new ArrayList<>();
                for (int i = 0; i < removalsCount; i++)
                {
                    NodeId removal = NodeId.serializer.deserialize(in, version);
                    removals.add(removal);
                }

                return new Diff(additions, removals);
            }

            public long serializedSize(Diff diff, Version version)
            {
                long size = 0;
                size += TypeSizes.INT_SIZE;
                for (NodeId addition : diff.additions)
                    size += NodeId.serializer.serializedSize(addition, version);
                size += TypeSizes.INT_SIZE;
                for (NodeId removal : diff.removals)
                    size += NodeId.serializer.serializedSize(removal, version);

                return size;
            }
        }
    }
}
