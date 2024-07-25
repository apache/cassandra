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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.CMSPlacementStrategy;
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

public class PrepareCMSReconfiguration
{
    private static final Logger logger = LoggerFactory.getLogger(PrepareCMSReconfiguration.class);

    private static Transformation.Result executeInternal(ClusterMetadata prev, Function<ClusterMetadata.Transformer, ClusterMetadata.Transformer> transform, Diff diff)
    {
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

    public static class Simple implements Transformation
    {
        public static final Simple.Serializer serializer = new Serializer();

        private final NodeId toReplace;

        public Simple(NodeId toReplace)
        {
            this.toReplace = toReplace;
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

            ReplicationParams metaParams = ReplicationParams.meta(prev);
            CMSPlacementStrategy placementStrategy = CMSPlacementStrategy.fromReplicationParams(metaParams, nodeId -> !nodeId.equals(toReplace));
            Set<NodeId> currentCms = prev.fullCMSMembers()
                                         .stream()
                                         .map(prev.directory::peerId)
                                         .collect(Collectors.toSet());

            Set<NodeId> withoutReplaced = new HashSet<>(currentCms);
            withoutReplaced.remove(toReplace);
            Set<NodeId> newCms = placementStrategy.reconfigure(withoutReplaced, prev);
            if (newCms.equals(currentCms))
            {
                logger.info("Proposed CMS reconfiguration resulted in no required modifications at epoch {}", prev.epoch.getEpoch());
                return Transformation.success(prev.transformer(), LockedRanges.AffectedRanges.EMPTY);
            }
            Diff diff = diff(currentCms, newCms);
            return executeInternal(prev, t -> t, diff);
        }

        public String toString()
        {
            return "PrepareCMSReconfiguration#Simple{" +
                   "toReplace=" + toReplace +
                   '}';
        }

        public static class Serializer implements AsymmetricMetadataSerializer<Transformation, Simple>
        {
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                Simple tranformation = (Simple) t;
                NodeId.serializer.serialize(tranformation.toReplace, out, version);
            }

            public Simple deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new Simple(NodeId.serializer.deserialize(in, version));
            }

            public long serializedSize(Transformation t, Version version)
            {
                Simple tranformation = (Simple) t;
                return NodeId.serializer.serializedSize(tranformation.toReplace, version);
            }
        }
    }

    public static class Complex implements Transformation
    {
        public static final Complex.Serializer serializer = new Complex.Serializer();

        private final ReplicationParams replicationParams;

        public Complex(ReplicationParams replicationParams)
        {
            this.replicationParams = replicationParams;
        }

        @Override
        public Kind kind()
        {
            return Kind.PREPARE_COMPLEX_CMS_RECONFIGURATION;
        }

        @Override
        public Result execute(ClusterMetadata prev)
        {
            KeyspaceMetadata keyspace = prev.schema.getKeyspaceMetadata(SchemaConstants.METADATA_KEYSPACE_NAME);
            KeyspaceMetadata newKeyspace = keyspace.withSwapped(new KeyspaceParams(keyspace.params.durableWrites, replicationParams));

            CMSPlacementStrategy placementStrategy = CMSPlacementStrategy.fromReplicationParams(replicationParams, nodeId -> true);

            Set<NodeId> currentCms = prev.fullCMSMembers()
                                         .stream()
                                         .map(prev.directory::peerId)
                                         .collect(Collectors.toSet());

            Set<NodeId> newCms = placementStrategy.reconfigure(currentCms, prev);
            if (newCms.equals(currentCms))
            {
                logger.info("Proposed CMS reconfiguration resulted in no required modifications at epoch {}", prev.epoch.getEpoch());
                return Transformation.success(prev.transformer(), LockedRanges.AffectedRanges.EMPTY);
            }
            Diff diff = diff(currentCms, newCms);

            return executeInternal(prev,
                                   transformer -> transformer.with(prev.placements.replaceParams(prev.nextEpoch(),ReplicationParams.meta(prev), replicationParams))
                                                             .with(new DistributedSchema(prev.schema.getKeyspaces().withAddedOrUpdated(newKeyspace))),
                                   diff);
        }

        public String toString()
        {
            return "PrepareCMSReconfiguration#Complex{" +
                   "replicationParams=" + replicationParams +
                   '}';
        }

        public static class Serializer implements AsymmetricMetadataSerializer<Transformation, Complex>
        {
            public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
            {
                Complex tranformation = (Complex) t;
                ReplicationParams.serializer.serialize(tranformation.replicationParams, out, version);
            }

            public Complex deserialize(DataInputPlus in, Version version) throws IOException
            {
                return new Complex(ReplicationParams.serializer.deserialize(in, version));
            }

            public long serializedSize(Transformation t, Version version)
            {
                Complex tranformation = (Complex) t;
                return ReplicationParams.serializer.serializedSize(tranformation.replicationParams, version);
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

    public static boolean needsReconfiguration(ClusterMetadata metadata)
    {
        CMSPlacementStrategy placementStrategy = CMSPlacementStrategy.fromReplicationParams(ReplicationParams.meta(metadata), nodeId -> true);
        Set<NodeId> currentCms = metadata.fullCMSMembers()
                                         .stream()
                                         .map(metadata.directory::peerId)
                                         .collect(Collectors.toSet());

        Set<NodeId> newCms = placementStrategy.reconfigure(currentCms, metadata);
        return !currentCms.equals(newCms);
    }

    public static class Diff
    {
        public static final Serializer serializer = new Serializer();

        public final List<NodeId> additions;
        public final List<NodeId> removals;

        public Diff(List<NodeId> additions, List<NodeId> removals)
        {
            this.additions = additions;
            this.removals = removals;
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
