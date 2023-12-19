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
import java.util.List;
import java.util.Set;

import accord.local.Node;
import accord.primitives.Range;
import accord.topology.Shard;
import accord.topology.Topology;
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

        private static void serialize(Node.Id id, DataOutputPlus out) throws IOException
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

        private static Node.Id deserialize(DataInputPlus in) throws IOException
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

    public static final IVersionedSerializer<Shard> shard = new IVersionedSerializer<Shard>()
    {
        @Override
        public void serialize(Shard shard, DataOutputPlus out, int version) throws IOException
        {
            TokenRange.serializer.serialize((TokenRange) shard.range, out, version);
            CollectionSerializers.serializeList(shard.nodes, out, version, nodeId);
            CollectionSerializers.serializeCollection(shard.fastPathElectorate, out, version, nodeId);
            CollectionSerializers.serializeCollection(shard.joining, out, version, nodeId);

        }

        @Override
        public Shard deserialize(DataInputPlus in, int version) throws IOException
        {
            Range range = TokenRange.serializer.deserialize(in, version);
            List<Node.Id> nodes = CollectionSerializers.deserializeList(in, version, nodeId);
            Set<Node.Id> fastPathElectorate = CollectionSerializers.deserializeSet(in, version, nodeId);
            Set<Node.Id> joining = CollectionSerializers.deserializeSet(in, version, nodeId);
            return new Shard(range, nodes, fastPathElectorate, joining);
        }

        @Override
        public long serializedSize(Shard shard, int version)
        {
            long size = TokenRange.serializer.serializedSize((TokenRange) shard.range, version);
            size += CollectionSerializers.serializedListSize(shard.nodes, version, nodeId);
            size += CollectionSerializers.serializedCollectionSize(shard.fastPathElectorate, version, nodeId);
            size += CollectionSerializers.serializedCollectionSize(shard.joining, version, nodeId);
            return size;
        }
    };

    public static final IVersionedSerializer<Topology> topology = new IVersionedSerializer<Topology>()
    {
        @Override
        public void serialize(Topology topology, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(topology.epoch());
            ArraySerializers.serializeArray(topology.unsafeGetShards(), out, version, shard);
        }

        @Override
        public Topology deserialize(DataInputPlus in, int version) throws IOException
        {
            long epoch = in.readLong();
            Shard[] shards = ArraySerializers.deserializeArray(in, version, shard, Shard[]::new);
            return new Topology(epoch, shards);
        }

        @Override
        public long serializedSize(Topology topology, int version)
        {
            long size = 0;
            size += TypeSizes.LONG_SIZE; // epoch
            size += ArraySerializers.serializedArraySize(topology.unsafeGetShards(), version, shard);
            return size;
        }
    };
}
