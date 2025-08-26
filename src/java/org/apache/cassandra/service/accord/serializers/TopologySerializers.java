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
import java.util.List;
import java.util.Map;

import accord.local.Node;
import accord.primitives.Range;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SimpleBitSet;
import accord.utils.SortedArrays;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.TinyEnumSet;
import org.agrona.collections.Object2IntHashMap;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.SimpleBitSetSerializer;

import static accord.utils.SortedArrays.fromSimpleBitSet;

public class TopologySerializers
{
    private TopologySerializers() {}

    public static final NodeIdSerializer nodeId = new NodeIdSerializer();
    public static class NodeIdSerializer implements UnversionedSerializer<Node.Id>
    {
        private NodeIdSerializer() {}

        @Override
        public void serialize(Node.Id id, DataOutputPlus out) throws IOException
        {
            out.writeInt(id.id);
        }

        public <V> int serialize(Node.Id id, V dst, ValueAccessor<V> accessor, int offset)
        {
            return accessor.putInt(dst, offset, id.id);
        }

        public void serialize(Node.Id id, ByteBuffer out)
        {
            out.putInt(id.id);
        }

        @Override
        public Node.Id deserialize(DataInputPlus in) throws IOException
        {
            return new Node.Id(in.readInt());
        }

        public <V> Node.Id deserialize(V src, ValueAccessor<V> accessor, int offset)
        {
            return new Node.Id(accessor.getInt(src, offset));
        }

        public <V> Node.Id deserialize(ByteBuffer src, int position)
        {
            return new Node.Id(src.getInt(position));
        }

        @Override
        public long serializedSize(Node.Id id)
        {
            return TypeSizes.INT_SIZE;  // id.id
        }
    }

    public static final UnversionedSerializer<Shard> shard = new ShardSerializer((UnversionedSerializer<Range>)
                                                                      (UnversionedSerializer<?>)
                                                                      TokenRange.serializer);

    public static class ShardSerializer implements UnversionedSerializer<Shard>
    {
        protected UnversionedSerializer<Range> range;

        public ShardSerializer(UnversionedSerializer<Range> range)
        {
            this.range = range;
        }

        @Override
        public void serialize(Shard shard, DataOutputPlus out) throws IOException
        {
            range.serialize(shard.range, out);
            CollectionSerializers.serializeList(shard.nodes, out, nodeId);
            CollectionSerializers.serializeList(shard.notInFastPath, out, nodeId);
            CollectionSerializers.serializeList(shard.joining, out, nodeId);
            out.writeUnsignedVInt32(shard.flags().bitset());
        }

        @Override
        public Shard deserialize(DataInputPlus in) throws IOException
        {
            Range range = ShardSerializer.this.range.deserialize(in);
            SortedArrayList<Node.Id> nodes = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
            SortedArrayList<Node.Id> notInFastPath = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
            SortedArrayList<Node.Id> joining = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
            int flags = in.readUnsignedVInt32();
            return Shard.SerializerSupport.create(range, nodes, notInFastPath, joining, new TinyEnumSet<>(flags));
        }

        @Override
        public long serializedSize(Shard shard)
        {
            long size = range.serializedSize(shard.range);
            size += CollectionSerializers.serializedListSize(shard.nodes, nodeId);
            size += CollectionSerializers.serializedListSize(shard.notInFastPath, nodeId);
            size += CollectionSerializers.serializedListSize(shard.joining, nodeId);
            size += TypeSizes.sizeofUnsignedVInt(shard.flags().bitset());
            return size;
        }
    }

    public static final UnversionedSerializer<Topology> topology = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Topology topology, DataOutputPlus out) throws IOException
        {
            out.writeLong(topology.epoch());
            CollectionSerializers.serializeList(topology.shards(), out, shard);
            CollectionSerializers.serializeCollection(topology.removedIds(), out, TopologySerializers.nodeId);
            CollectionSerializers.serializeCollection(topology.staleIds(), out, TopologySerializers.nodeId);
        }

        @Override
        public Topology deserialize(DataInputPlus in) throws IOException
        {
            long epoch = in.readLong();
            Shard[] shards = ArraySerializers.deserializeArray(in, shard, Shard[]::new);
            SortedArrayList<Node.Id> removedIds = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            SortedArrayList<Node.Id> staleIds = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            return new Topology(epoch, removedIds, staleIds, shards);
        }

        @Override
        public long serializedSize(Topology topology)
        {
            long size = 0;
            size += TypeSizes.LONG_SIZE; // epoch
            size += CollectionSerializers.serializedListSize(topology.shards(), shard);
            size += CollectionSerializers.serializedCollectionSize(topology.removedIds(), TopologySerializers.nodeId);
            size += CollectionSerializers.serializedCollectionSize(topology.staleIds(), TopologySerializers.nodeId);
            return size;
        }
    };

    public static final UnversionedSerializer<Topology> compactTopology = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Topology topology, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(topology.epoch());
            CollectionSerializers.serializeList(topology.removedIds(), out, TopologySerializers.nodeId);
            CollectionSerializers.serializeList(topology.staleIds(), out, TopologySerializers.nodeId);

            List<Shard> shards = topology.shards();

            // need to loop twice; once to collect ranges, and another to save shards
            Object2IntHashMap<TokenRange> ranges;
            {
                Object2IntHashMap<TokenRange> rangesBuilder = new Object2IntHashMap<>(-2);
                for (Shard shard : shards)
                {
                    TokenRange range = (TokenRange) shard.range;
                    rangesBuilder.putIfAbsent(range.withTable(TableId.UNDEFINED), -1);
                }
                int count = 0;
                for (Map.Entry<TokenRange, Integer> e : rangesBuilder.entrySet())
                    e.setValue(count++);

                ranges = rangesBuilder;
            }

            CollectionSerializers.serializeCollection(ranges.keySet(), out, TokenRange.noTableSerializer);

            out.writeUnsignedVInt32(shards.size());
            TableId activeTableId = null;
            for (Shard shard : shards)
            {
                TokenRange range = (TokenRange) shard.range;
                if (activeTableId == null || !activeTableId.equals(range.table()))
                {
                    activeTableId = range.table();
                    out.writeBoolean(false);
                    TableId.compactComparableSerializer.serialize(activeTableId, out);
                }
                else
                {
                    out.writeBoolean(true);
                }
                int rangeIdx = ranges.getValue(range.withTable(TableId.UNDEFINED));
                out.writeUnsignedVInt32(rangeIdx);

                CollectionSerializers.serializeList(shard.nodes, out, TopologySerializers.nodeId);
                SimpleBitSet notInFastPath = SortedArrays.toSimpleBitSet(shard.nodes, shard.notInFastPath);
                SimpleBitSetSerializer.instance.serialize(notInFastPath, out);
                SimpleBitSet joining = SortedArrays.toSimpleBitSet(shard.nodes, shard.joining);
                SimpleBitSetSerializer.instance.serialize(joining, out);
                out.writeUnsignedVInt32(shard.flags().bitset());
            }
        }

        @Override
        public long serializedSize(Topology topology)
        {
            long size = TypeSizes.sizeofUnsignedVInt(topology.epoch());
            size += CollectionSerializers.serializedListSize(topology.removedIds(), TopologySerializers.nodeId);
            size += CollectionSerializers.serializedListSize(topology.staleIds(), TopologySerializers.nodeId);

            List<Shard> shards = topology.shards();

            // need to loop twice; once to collect ranges, and another to save shards
            Object2IntHashMap<TokenRange> ranges;
            {
                Object2IntHashMap<TokenRange> rangesBuilder = new Object2IntHashMap<>(-2);
                for (Shard shard : shards)
                {
                    TokenRange range = (TokenRange) shard.range;
                    rangesBuilder.putIfAbsent(range.withTable(TableId.UNDEFINED), -1);
                }
                int count = 0;
                for (Map.Entry<TokenRange, Integer> e : rangesBuilder.entrySet())
                    e.setValue(count++);

                ranges = rangesBuilder;
            }

            size += CollectionSerializers.serializedCollectionSize(ranges.keySet(), TokenRange.noTableSerializer);

            size += TypeSizes.sizeofUnsignedVInt(shards.size());
            TableId activeTableId = null;
            for (Shard shard : shards)
            {
                TokenRange range = (TokenRange) shard.range;
                size += TypeSizes.sizeof(true);
                if (activeTableId == null || !activeTableId.equals(range.table()))
                {
                    activeTableId = range.table();
                    size += TableId.compactComparableSerializer.serializedSize(activeTableId);
                }
                int rangeIdx = ranges.getValue(range.withTable(TableId.UNDEFINED));

                size += TypeSizes.sizeofUnsignedVInt(rangeIdx);

                size += CollectionSerializers.serializedListSize(shard.nodes, TopologySerializers.nodeId);
                SimpleBitSet notInFastPath = SortedArrays.toSimpleBitSet(shard.nodes, shard.notInFastPath);
                size += SimpleBitSetSerializer.instance.serializedSize(notInFastPath);
                SimpleBitSet joining = SortedArrays.toSimpleBitSet(shard.nodes, shard.joining);
                size += SimpleBitSetSerializer.instance.serializedSize(joining);
                size += TypeSizes.sizeofUnsignedVInt(shard.flags().bitset());
            }
            return size;
        }

        @Override
        public Topology deserialize(DataInputPlus in) throws IOException
        {
            long epoch = in.readUnsignedVInt();
            SortedArrays.SortedArrayList<Node.Id> removedIds = SortedArrays.SortedArrayList.copySorted(CollectionSerializers.deserializeList(in, TopologySerializers.nodeId), Node.Id[]::new);
            SortedArrays.SortedArrayList<Node.Id> staleIds = SortedArrays.SortedArrayList.copySorted(CollectionSerializers.deserializeList(in, TopologySerializers.nodeId), Node.Id[]::new);

            List<TokenRange> ranges = CollectionSerializers.deserializeList(in, TokenRange.noTableSerializer);

            int size = in.readUnsignedVInt32();
            Shard[] shards = new Shard[size];
            TableId activeTableId = null;
            for (int i = 0; i < size; i++)
            {
                if (!in.readBoolean())
                    activeTableId = TableId.compactComparableSerializer.deserialize(in);
                int rangeIndex = in.readUnsignedVInt32();

                TokenRange range = ranges.get(rangeIndex).withTable(activeTableId);

                SortedArrays.SortedArrayList<Node.Id> nodes = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
                SimpleBitSet notInFastPath = SimpleBitSetSerializer.instance.deserialize(in);
                SimpleBitSet joining = SimpleBitSetSerializer.instance.deserialize(in);
                int flags = in.readUnsignedVInt32();
                shards[i] = Shard.SerializerSupport.create(range, nodes, fromSimpleBitSet(nodes, notInFastPath, Node.Id[]::new), fromSimpleBitSet(nodes, joining, Node.Id[]::new), new TinyEnumSet<>(flags));
            }
            return new Topology(epoch, removedIds, staleIds, shards);
        }
    };
}
