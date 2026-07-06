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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.topology.Shard;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.accord.topology.AccordTopology;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CollectionSerializers;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class AccordMarkStale implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(AccordMarkStale.class);
    
    private final Set<NodeId> ids;

    public AccordMarkStale(Set<NodeId> ids)
    {
        this.ids = ids;
    }

    @Override
    public Kind kind()
    {
        return Kind.ACCORD_MARK_STALE;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        for (NodeId id : ids)
            if (!prev.directory.peerIds().contains(id))
                return new Rejected(INVALID, String.format("Can not mark node %s stale as it is not present in the directory.", id));

        SortedArrayList<Node.Id> staleIds = SortedArrayList.ofUnsorted(ids.stream().map(AccordTopology::tcmIdToAccord).toArray(Node.Id[]::new));
        SortedArrayList<Node.Id> allStaleIds = prev.accordStaleReplicas.stale().with(staleIds);

        for (Node.Id id : staleIds)
            if (prev.accordStaleReplicas.stale().contains(id))
                return new Rejected(INVALID, String.format("Can not mark node %s stale as it already is.", id));

        for (KeyspaceMetadata keyspace : prev.schema.getKeyspaces().without(SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES))
        {
            List<AccordTopology.KeyspaceShard> shards = AccordTopology.KeyspaceShard.forKeyspace(keyspace, prev.placements(), prev.directory);
            
            for (AccordTopology.KeyspaceShard shard : shards)
            {
                SortedArrayList<Node.Id> intersecting = allStaleIds.intersecting(shard.nodes());
                if (intersecting.size() > Shard.maxToleratedFailures(shard.nodes().size()))
                {
                    return new Rejected(INVALID, String.format("Can not mark nodes %s stale as that would leave fewer than a quorum of nodes active for ranges %s in keyspace '%s'.",
                                                               allStaleIds, shard.ranges(), keyspace.name));
                }
            }
        }

        logger.info("Marking " + ids + " stale. They will no longer participate in durability status coordination...");
        ClusterMetadata.Transformer next = prev.transformer().markStaleReplicas(staleIds);
        return Transformation.success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public String toString()
    {
        return "AccordMarkStale{ids=" + ids + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordMarkStale that = (AccordMarkStale) o;
        return Objects.equals(ids, that.ids);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ids);
    }

    public static final AsymmetricMetadataSerializer<Transformation, AccordMarkStale> serializer = new AsymmetricMetadataSerializer<>()
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof AccordMarkStale;
            AccordMarkStale mark = (AccordMarkStale) t;
            CollectionSerializers.serializeCollection(mark.ids, out, version, NodeId.serializer);
        }

        @Override
        public AccordMarkStale deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AccordMarkStale(CollectionSerializers.deserializeSet(in, version, NodeId.serializer));
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof AccordMarkStale;
            AccordMarkStale mark = (AccordMarkStale) t;
            return CollectionSerializers.serializedCollectionSize(mark.ids, version, NodeId.serializer);
        }
    };
}
