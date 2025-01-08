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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;

import accord.local.Node;
import accord.primitives.Range;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.CollectionSerializers;

public class TopologySerializers
{
    private TopologySerializers() {}

    public static final NodeIdSerializer nodeId = new NodeIdSerializer();
    public static class NodeIdSerializer implements IVersionedSerializer<Node.Id>, MetadataSerializer<Node.Id>
    {
        private NodeIdSerializer() {}

        public static void serialize(Node.Id id, DataOutputPlus out) throws IOException
        {
            out.writeInt(id.id);
        }

        @Override
        public void serialize(Node.Id id, DataOutputPlus out, int version) throws IOException
        {
            serialize(id, out);
        }

        @Override
        public void serialize(Node.Id id, DataOutputPlus out, Version version) throws IOException
        {
            serialize(id, out);
        }

        public <V> int serialize(Node.Id id, V dst, ValueAccessor<V> accessor, int offset)
        {
            return accessor.putInt(dst, offset, id.id);
        }

        public void serialize(Node.Id id, ByteBuffer out)
        {
            out.putInt(id.id);
        }

        public static Node.Id deserialize(DataInputPlus in) throws IOException
        {
            return new Node.Id(in.readInt());
        }

        @Override
        public Node.Id deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in);
        }

        @Override
        public Node.Id deserialize(DataInputPlus in, Version version) throws IOException
        {
            return deserialize(in);
        }

        public <V> Node.Id deserialize(V src, ValueAccessor<V> accessor, int offset)
        {
            return new Node.Id(accessor.getInt(src, offset));
        }

        public <V> Node.Id deserialize(ByteBuffer src, int position)
        {
            return new Node.Id(src.getInt(position));
        }

        public int serializedSize()
        {
            return TypeSizes.INT_SIZE;  // id.id
        }

        @Override
        public long serializedSize(Node.Id id, int version)
        {
            return serializedSize();
        }

        @Override
        public long serializedSize(Node.Id t, Version version)
        {
            return serializedSize();
        }
    };

    public static final IVersionedSerializer<Shard> shard = new ShardSerializer((IVersionedSerializer<Range>)
                                                                                (IVersionedSerializer<?>)
                                                                                TokenRange.serializer);

    public static class ShardSerializer implements IVersionedSerializer<Shard>
    {
        protected IVersionedSerializer<Range> range;

        public ShardSerializer(IVersionedSerializer<Range> range)
        {
            this.range = range;
        }

        @Override
        public void serialize(Shard shard, DataOutputPlus out, int version) throws IOException
        {
            range.serialize(shard.range, out, version);
            CollectionSerializers.serializeList(shard.nodes, out, version, nodeId);
            CollectionSerializers.serializeList(shard.notInFastPath, out, version, nodeId);
            CollectionSerializers.serializeList(shard.joining, out, version, nodeId);
            out.writeBoolean(shard.pendingRemoval);
        }

        @Override
        public Shard deserialize(DataInputPlus in, int version) throws IOException
        {
            Range range = ShardSerializer.this.range.deserialize(in, version);
            SortedArrayList<Node.Id> nodes = CollectionSerializers.deserializeSortedArrayList(in, version, nodeId, Node.Id[]::new);
            SortedArrayList<Node.Id> notInFastPath = CollectionSerializers.deserializeSortedArrayList(in, version, nodeId, Node.Id[]::new);
            SortedArrayList<Node.Id> joining = CollectionSerializers.deserializeSortedArrayList(in, version, nodeId, Node.Id[]::new);
            boolean pendingRemoval = in.readBoolean();
            return Shard.SerializerSupport.create(range, nodes, notInFastPath, joining, pendingRemoval);
        }

        @Override
        public long serializedSize(Shard shard, int version)
        {
            long size = range.serializedSize(shard.range, version);
            size += CollectionSerializers.serializedListSize(shard.nodes, version, nodeId);
            size += CollectionSerializers.serializedListSize(shard.notInFastPath, version, nodeId);
            size += CollectionSerializers.serializedListSize(shard.joining, version, nodeId);
            size += TypeSizes.BOOL_SIZE;
            return size;
        }
    };

    public static final IVersionedSerializer<Topology> topology = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Topology topology, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(topology.epoch());
            ArraySerializers.serializeArray(topology.unsafeGetShards(), out, version, shard);
            CollectionSerializers.serializeCollection(topology.staleIds(), out, version, TopologySerializers.nodeId);
        }

        @Override
        public Topology deserialize(DataInputPlus in, int version) throws IOException
        {
            long epoch = in.readLong();
            Shard[] shards = ArraySerializers.deserializeArray(in, version, shard, Shard[]::new);
            SortedArrayList<Node.Id> staleIds = CollectionSerializers.deserializeSortedArrayList(in, version, TopologySerializers.nodeId, Node.Id[]::new);
            return new Topology(epoch, staleIds, shards);
        }

        @Override
        public long serializedSize(Topology topology, int version)
        {
            long size = 0;
            size += TypeSizes.LONG_SIZE; // epoch
            size += ArraySerializers.serializedArraySize(topology.unsafeGetShards(), version, shard);
            size += CollectionSerializers.serializedCollectionSize(topology.staleIds(), version, TopologySerializers.nodeId);
            return size;
        }
    };
}
