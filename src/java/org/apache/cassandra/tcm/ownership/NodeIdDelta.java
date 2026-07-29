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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class NodeIdDelta implements Delta
{
    public static final Serializer serializer = new Serializer();

    private static final Delta EMPTY = new NodeIdDelta(ImmutableMultimap.of(), ImmutableMultimap.of());

    public final ImmutableMultimap<NodeId, ReplicaNode> removals;
    public final ImmutableMultimap<NodeId, ReplicaNode> additions;

    public NodeIdDelta(ImmutableMultimap<NodeId, ReplicaNode> removals,
                       ImmutableMultimap<NodeId, ReplicaNode> additions)
    {
        this.removals = removals;
        this.additions = additions;
    }

    /**
     * Merges this delta with `other`
     *
     * Note that if opposite operations (add a range in this, remove it in other for example) exist in
     * `this` and `other` the operations cancel eachother out and neither will be in the resulting delta.
     * @param other
     * @return
     */
    public Delta merge(Delta other)
    {
        if (!(other instanceof NodeIdDelta))
            throw new IllegalStateException("Can't merge deltas of different kinds - " + this + " and " + other);
        NodeIdDelta nodeIdOther = (NodeIdDelta) other;
        Multimap<NodeId, ReplicaNode> removalsBuilder = ArrayListMultimap.create();
        Multimap<NodeId, ReplicaNode> additionsBuilder = ArrayListMultimap.create();
        addChange(removals, nodeIdOther.additions, removalsBuilder);
        addChange(nodeIdOther.removals, additions, removalsBuilder);
        addChange(additions, nodeIdOther.removals, additionsBuilder);
        addChange(nodeIdOther.additions, removals, additionsBuilder);
        return new NodeIdDelta(ImmutableMultimap.copyOf(removalsBuilder),
                               ImmutableMultimap.copyOf(additionsBuilder));
    }

    private static void addChange(Multimap<NodeId, ReplicaNode> change, Multimap<NodeId, ReplicaNode> opposite, Multimap<NodeId, ReplicaNode> builder)
    {
        change.asMap().forEach((node, replicas) -> {
            replicas.forEach(replica -> {
                if (!opposite.get(node).contains(replica) && !builder.get(node).contains(replica))
                    builder.put(node, replica);
            });
        });
    }

    public Delta invert()
    {
        return new NodeIdDelta(additions, removals);
    }

    @Override
    public boolean isEmpty()
    {
        return additions.isEmpty() && removals.isEmpty();
    }

    @Override
    public EndpointDelta asEndpointDelta(Function<NodeId, InetAddressAndPort> endpointLookup)
    {
        return new EndpointDelta(removals(endpointLookup),
                                 additions(endpointLookup));
    }

    @Override
    public RangesByEndpoint removals(Function<NodeId, InetAddressAndPort> endpointLookup)
    {
        return RangesByEndpoint.fromNodeIds(removals, endpointLookup);
    }

    @Override
    public RangesByEndpoint additions(Function<NodeId, InetAddressAndPort> endpointLookup)
    {
        return RangesByEndpoint.fromNodeIds(additions, endpointLookup);
    }

    @Override
    public Collection<Range<Token>> addedRanges()
    {
        return additions.values().stream().map(rn -> rn.range).collect(Collectors.toSet());
    }

    @Override
    public Collection<Range<Token>> removedRanges()
    {
        return removals.values().stream().map(rn -> rn.range).collect(Collectors.toSet());
    }

    public Set<NodeId> allPeers(Function<InetAddressAndPort, NodeId> nodeIdLookup)
    {
        Set<NodeId> peers = new HashSet<>(removals.keySet());
        peers.addAll(additions.keySet());
        return peers;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeIdDelta delta = (NodeIdDelta) o;

        return Objects.equals(removals, delta.removals) && Objects.equals(additions, delta.additions);
    }

    public int hashCode()
    {
        return Objects.hash(removals, additions);
    }

    @Override
    public String toString()
    {
        return "NodeIdDelta{" +
               "removals=" + removals +
               ", additions=" + additions +
               '}';
    }

    public static Delta empty()
    {
        return EMPTY;
    }

    public static final class Serializer implements MetadataSerializer<NodeIdDelta>
    {
        public void serialize(NodeIdDelta t, DataOutputPlus out, Version version) throws IOException
        {
            serialize(t.removals, out, version);
            serialize(t.additions, out, version);
        }

        public NodeIdDelta deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new NodeIdDelta(deserializeDelta(in, version),
                                   deserializeDelta(in, version));
        }

        public long serializedSize(NodeIdDelta t, Version version)
        {
            return serializedSizeDelta(t.removals, version) +
                   serializedSizeDelta(t.additions, version);
        }

        private static void serialize(ImmutableMultimap<NodeId, ReplicaNode> delta, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt32(delta.keySet().size());
            for (NodeId nodeId : delta.keySet())
            {
                NodeId.serializer.serialize(nodeId, out, version);
                Collection<ReplicaNode> replicas = delta.get(nodeId);
                out.writeUnsignedVInt32(replicas.size());
                for (ReplicaNode replica : replicas)
                    ReplicaNode.serializer.serialize(replica, out, version);
            }
        }

        public static ImmutableMultimap<NodeId, ReplicaNode> deserializeDelta(DataInputPlus in, Version version) throws IOException
        {
            ImmutableMultimap.Builder<NodeId, ReplicaNode> builder = ImmutableMultimap.builder();
            int size = in.readUnsignedVInt32();
            for (int i = 0; i < size; i++)
            {
                NodeId nodeId = NodeId.serializer.deserialize(in, version);
                int replicasSize = in.readUnsignedVInt32();
                for (int j = 0; j < replicasSize; j++)
                    builder.put(nodeId, ReplicaNode.serializer.deserialize(in, version));
            }
            return builder.build();
        }

        private static long serializedSizeDelta(ImmutableMultimap<NodeId, ReplicaNode> delta, Version version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(delta.keySet().size());
            for (NodeId nodeId : delta.keySet())
            {
                size+= NodeId.serializer.serializedSize(nodeId, version);
                Collection<ReplicaNode> replicas = delta.get(nodeId);
                size += TypeSizes.sizeofUnsignedVInt(replicas.size());
                for (ReplicaNode replica : replicas)
                    size += ReplicaNode.serializer.serializedSize(replica, version);
            }
            return size;
        }

    }
}
