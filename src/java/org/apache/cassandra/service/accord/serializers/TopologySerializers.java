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

import org.agrona.collections.Object2IntHashMap;

import accord.local.Node;
import accord.primitives.Range;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.LargeBitSet;
import accord.utils.SortedArrays;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.TinyEnumSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.SimpleBitSetSerializers;

import static accord.topology.Topology.NO_IDS;
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
            out.writeUnsignedVInt32(0); // was joining collection, can now be encoding flag bits
            out.writeUnsignedVInt32(shard.flags().bitset());
        }

        @Override
        public Shard deserialize(DataInputPlus in) throws IOException
        {
            Range range = ShardSerializer.this.range.deserialize(in);
            SortedArrayList<Node.Id> nodes = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
            SortedArrayList<Node.Id> notInFastPath = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
            in.readUnsignedVInt32();
            int flags = in.readUnsignedVInt32();
            return Shard.SerializerSupport.create(range, nodes, notInFastPath, NO_IDS, new TinyEnumSet<>(flags));
        }

        @Override
        public long serializedSize(Shard shard)
        {
            long size = range.serializedSize(shard.range);
            size += CollectionSerializers.serializedListSize(shard.nodes, nodeId);
            size += CollectionSerializers.serializedListSize(shard.notInFastPath, nodeId);
            size += TypeSizes.sizeofUnsignedVInt(0);
            size += TypeSizes.sizeofUnsignedVInt(shard.flags().bitset());
            return size;
        }
    }

    private static final int HAS_STALE_IDS = 0x1;
    private static final int HAS_HARD_REMOVED_IDS = 0x2;

    private static void serializeRemovedAndStale(Topology topology, DataOutputPlus out) throws IOException
    {
        CollectionSerializers.serializeList(topology.removedIds(), out, TopologySerializers.nodeId);
        int flags = 0;
        if (!topology.staleIds().isEmpty())
            flags |= HAS_STALE_IDS;
        if (!topology.hardRemovedIds().isEmpty())
            flags |= HAS_HARD_REMOVED_IDS;
        out.writeUnsignedVInt32(flags);
        if (!topology.staleIds().isEmpty())
            CollectionSerializers.serializeList(topology.staleIds(), out, TopologySerializers.nodeId);
        if (!topology.hardRemovedIds().isEmpty())
            CollectionSerializers.serializeList(topology.hardRemovedIds(), out, TopologySerializers.nodeId);
    }

    private static long serializedSizeOfRemovedAndStale(Topology topology)
    {
        long size = CollectionSerializers.serializedListSize(topology.removedIds(), TopologySerializers.nodeId);
        int flags = 0;
        if (!topology.staleIds().isEmpty())
            flags |= HAS_STALE_IDS;
        if (!topology.hardRemovedIds().isEmpty())
            flags |= HAS_HARD_REMOVED_IDS;
        size += TypeSizes.sizeofUnsignedVInt(flags);
        if (!topology.staleIds().isEmpty())
            size += CollectionSerializers.serializedListSize(topology.staleIds(), TopologySerializers.nodeId);
        if (!topology.hardRemovedIds().isEmpty())
            size += CollectionSerializers.serializedListSize(topology.hardRemovedIds(), TopologySerializers.nodeId);
        return size;
    }

    public static final UnversionedSerializer<Topology> topology = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Topology topology, DataOutputPlus out) throws IOException
        {
            out.writeLong(topology.epoch());
            CollectionSerializers.serializeList(topology.shards(), out, shard);
            serializeRemovedAndStale(topology, out);
        }

        @Override
        public Topology deserialize(DataInputPlus in) throws IOException
        {
            long epoch = in.readLong();
            Shard[] shards = ArraySerializers.deserializeArray(in, shard, Shard[]::new);
            SortedArrayList<Node.Id> removedIds = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            SortedArrayList<Node.Id> staleIds = NO_IDS, hardRemovedIds = NO_IDS;
            int flags = in.readUnsignedVInt32();
            if ((flags & HAS_STALE_IDS) != 0)
                staleIds = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            if ((flags & HAS_HARD_REMOVED_IDS) != 0)
                hardRemovedIds = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            // we don't currently serialize hardRemoved at the shard level, so we must re-apply here
            return new Topology(epoch, removedIds, NO_IDS, staleIds, shards).withHardRemoved(hardRemovedIds);
        }

        @Override
        public long serializedSize(Topology topology)
        {
            long size = 0;
            size += TypeSizes.LONG_SIZE; // epoch
            size += CollectionSerializers.serializedListSize(topology.shards(), shard);
            size += serializedSizeOfRemovedAndStale(topology);
            return size;
        }
    };

    public static final UnversionedSerializer<Topology> compactTopology = new UnversionedSerializer<>()
    {
        private final LargeBitSet NO_BITS = new LargeBitSet(0);

        private Object2IntHashMap<TokenRange> ranges(Topology topology)
        {
            // need to loop twice; once to collect ranges, and another to save shards
            Object2IntHashMap<TokenRange> result = new Object2IntHashMap<>(-2);
            for (Shard shard : topology.shards())
            {
                TokenRange range = (TokenRange) shard.range;
                result.putIfAbsent(range.withTable(TableId.UNDEFINED), -1);
            }
            int count = 0;
            for (Map.Entry<TokenRange, Integer> e : result.entrySet())
                e.setValue(count++);
            return result;
        }

        @Override
        public void serialize(Topology topology, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(topology.epoch());
            serializeRemovedAndStale(topology, out);

            Object2IntHashMap<TokenRange> ranges = ranges(topology);
            CollectionSerializers.serializeCollection(ranges.keySet(), out, TokenRange.noTableSerializer);

            out.writeUnsignedVInt32(topology.shards().size());
            TableId activeTableId = null;
            for (Shard shard : topology.shards())
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
                LargeBitSet notInFastPath = SortedArrays.toLargeBitSet(shard.nodes, shard.notInFastPath);
                SimpleBitSetSerializers.large.serialize(notInFastPath, out);
                SimpleBitSetSerializers.large.serialize(NO_BITS, out);
                out.writeUnsignedVInt32(shard.flags().bitset());
            }
        }

        @Override
        public long serializedSize(Topology topology)
        {
            long size = TypeSizes.sizeofUnsignedVInt(topology.epoch());
            size += serializedSizeOfRemovedAndStale(topology);

            // need to loop twice; once to collect ranges, and another to save shards
            Object2IntHashMap<TokenRange> ranges = ranges(topology);

            size += CollectionSerializers.serializedCollectionSize(ranges.keySet(), TokenRange.noTableSerializer);

            size += TypeSizes.sizeofUnsignedVInt(topology.shards().size());
            TableId activeTableId = null;
            for (Shard shard : topology.shards())
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
                LargeBitSet notInFastPath = SortedArrays.toLargeBitSet(shard.nodes, shard.notInFastPath);
                size += SimpleBitSetSerializers.large.serializedSize(notInFastPath);
                size += SimpleBitSetSerializers.large.serializedSize(NO_BITS);
                size += TypeSizes.sizeofUnsignedVInt(shard.flags().bitset());
            }
            return size;
        }

        @Override
        public Topology deserialize(DataInputPlus in) throws IOException
        {
            long epoch = in.readUnsignedVInt();
            SortedArrays.SortedArrayList<Node.Id> removedIds = SortedArrays.SortedArrayList.copySorted(CollectionSerializers.deserializeList(in, TopologySerializers.nodeId), Node.Id[]::new);
            SortedArrayList<Node.Id> staleIds = NO_IDS, hardRemovedIds = NO_IDS;
            {
                int flags = in.readUnsignedVInt32();
                if ((flags & HAS_STALE_IDS) != 0)
                    staleIds = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
                if ((flags & HAS_HARD_REMOVED_IDS) != 0)
                    hardRemovedIds = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            }

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
                LargeBitSet notInFastPath = SimpleBitSetSerializers.large.deserialize(in);
                SimpleBitSetSerializers.large.deserialize(in);
                int flags = in.readUnsignedVInt32();
                shards[i] = Shard.SerializerSupport.create(range, nodes, fromSimpleBitSet(nodes, notInFastPath, Node.Id[]::new), NO_IDS, new TinyEnumSet<>(flags));
            }
            return new Topology(epoch, removedIds, NO_IDS, staleIds, shards).withHardRemoved(hardRemovedIds);
        }
    };
}
