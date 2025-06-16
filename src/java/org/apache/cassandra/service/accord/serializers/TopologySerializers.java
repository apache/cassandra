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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import accord.local.Node;
import accord.primitives.Range;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.TinyEnumSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.ImmutableUniqueList;

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

    public static class CompactTopology
    {
        private static class Range
        {
            final TokenKey start;
            final TokenKey end;

            private Range(TokenKey start, TokenKey end)
            {
                this.start = start;
                this.end = end;
            }

            private Range(TokenRange other)
            {
                this(other.start(), other.end());
            }

            @Override
            public boolean equals(Object o)
            {
                if (o == null || getClass() != o.getClass()) return false;
                Range range = (Range) o;
                return Objects.equals(start, range.start) && Objects.equals(end, range.end);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(start, end);
            }
        }

        private static final class RangeSerializer implements UnversionedSerializer<CompactTopology.Range>
        {
            private final ImmutableUniqueList<TableId> tables;

            private RangeSerializer(ImmutableUniqueList<TableId> tables)
            {
                this.tables = tables;
            }

            @Override
            public void serialize(CompactTopology.Range t, DataOutputPlus out) throws IOException
            {
                out.writeUnsignedVInt32(tables.indexOf(t.start.table()));
                TokenKey.noTableSerializer.serialize(t.start, out);
                TokenKey.noTableSerializer.serialize(t.end, out);
            }

            @Override
            public CompactTopology.Range deserialize(DataInputPlus in) throws IOException
            {
                int idx = in.readUnsignedVInt32();
                TableId tableId = tables.get(idx);
                return new CompactTopology.Range(TokenKey.noTableSerializer.deserialize(tableId, in),
                                                 TokenKey.noTableSerializer.deserialize(tableId, in));
            }

            @Override
            public long serializedSize(CompactTopology.Range t)
            {
                return TypeSizes.sizeofUnsignedVInt(tables.indexOf(t.start.table()))
                       + TokenKey.noTableSerializer.serializedSize(t.start)
                       + TokenKey.noTableSerializer.serializedSize(t.end);
            }
        }

        private final long epoch;
        private final SortedArrayList<Node.Id> nodeIds;
        @Nullable
        private final BitSet staleNodes;
        private final ImmutableUniqueList<TableId> tables;
        private final ImmutableUniqueList<Range> ranges;
        private final List<Shard> shards;

        public CompactTopology(long epoch,
                               SortedArrayList<Node.Id> nodeIds,
                               @Nullable BitSet staleNodes,
                               ImmutableUniqueList<TableId> tables,
                               ImmutableUniqueList<Range> ranges,
                               List<Shard> shards)
        {
            this.epoch = epoch;
            this.nodeIds = nodeIds;
            this.staleNodes = staleNodes;
            this.tables = tables;
            this.ranges = ranges;
            this.shards = shards;
        }

        public CompactTopology(Topology topology)
        {
            epoch = topology.epoch();
            nodeIds = topology.nodes();
            staleNodes = getStaleNodes(topology);
            ImmutableUniqueList.Builder<TableId> tablesBuilder = ImmutableUniqueList.builder();
            ImmutableUniqueList.Builder<Range> rangesBuilder = ImmutableUniqueList.builder();
            for (Shard shard : topology.shards())
            {
                TokenRange range = (TokenRange) shard.range;
                tablesBuilder.add(range.table());
                rangesBuilder.add(new Range(range));
            }
            this.tables = tablesBuilder.buildAndClear();
            this.ranges = rangesBuilder.buildAndClear();
            this.shards = topology.shards();
        }

        public Topology topology()
        {
            return null;
        }

        private static BitSet getStaleNodes(Topology topology)
        {
            if (topology.staleIds().isEmpty())
                return null;
            BitSet set = new BitSet();
            SortedArrayList<Node.Id> nodes = topology.nodes();
            Set<Node.Id> staleIds = topology.staleIds();
            for (int i = 0; i < nodes.size(); i++)
            {
                if (staleIds.contains(nodes.get(i)))
                    set.set(i);
            }
            return set.isEmpty() ? null : set;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            CompactTopology that = (CompactTopology) o;
            return epoch == that.epoch && Objects.equals(nodeIds, that.nodeIds) && Objects.equals(staleNodes, that.staleNodes) && Objects.equals(tables, that.tables) && Objects.equals(ranges, that.ranges) && Objects.equals(shards, that.shards);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(epoch, nodeIds, staleNodes, tables, ranges, shards);
        }
    }

    public static final UnversionedSerializer<CompactTopology> dictionaryTopology = new UnversionedSerializer<CompactTopology>()
    {
        @Override
        public void serialize(CompactTopology topology, DataOutputPlus out) throws IOException
        {
            out.writeLong(topology.epoch);
            CollectionSerializers.serializeList(topology.nodeIds, out, nodeId);

            byte[] staleNodes = topology.staleNodes == null ? new byte[0] : topology.staleNodes.toByteArray();
            out.writeUnsignedVInt32(staleNodes.length);
            out.write(staleNodes);

            CollectionSerializers.serializeList(topology.tables, out, TableId.compactComparableSerializer);
            CollectionSerializers.serializeList(topology.ranges, out, new CompactTopology.RangeSerializer(topology.tables));

            ImmutableUniqueList<TableId> tables = topology.tables;
            ImmutableUniqueList<CompactTopology.Range> ranges = topology.ranges;

            out.writeUnsignedVInt32(topology.shards.size());
            for (Shard shard : topology.shards)
            {
                TokenRange tokenRange = (TokenRange) shard.range;
                out.writeUnsignedVInt32(ranges.indexOf(new CompactTopology.Range(tokenRange)));

                //TODO (perf): can this be compressed?
                CollectionSerializers.serializeList(shard.nodes, out, nodeId);
                CollectionSerializers.serializeList(shard.notInFastPath, out, nodeId);
                CollectionSerializers.serializeList(shard.joining, out, nodeId);
                out.writeUnsignedVInt32(shard.flags().bitset());
            }
        }

        @Override
        public long serializedSize(CompactTopology topology)
        {
            long size = Long.BYTES;
            size += CollectionSerializers.serializedListSize(topology.nodeIds, nodeId);
            byte[] staleNodes = topology.staleNodes == null ? new byte[0] : topology.staleNodes.toByteArray();
            size += TypeSizes.sizeofUnsignedVInt(staleNodes.length);
            size += staleNodes.length;
            size += CollectionSerializers.serializedListSize(topology.tables, TableId.compactComparableSerializer);
            size += CollectionSerializers.serializedListSize(topology.ranges, new CompactTopology.RangeSerializer(topology.tables));

            ImmutableUniqueList<TableId> tables = topology.tables;
            ImmutableUniqueList<CompactTopology.Range> ranges = topology.ranges;

            size += TypeSizes.sizeofUnsignedVInt(topology.shards.size());
            for (Shard shard : topology.shards)
            {
                TokenRange tokenRange = (TokenRange) shard.range;
                size += TypeSizes.sizeofUnsignedVInt(ranges.indexOf(new CompactTopology.Range(tokenRange)));

                size += CollectionSerializers.serializedListSize(shard.nodes, nodeId);
                size += CollectionSerializers.serializedListSize(shard.notInFastPath, nodeId);
                size += CollectionSerializers.serializedListSize(shard.joining, nodeId);
                size += TypeSizes.sizeofUnsignedVInt(shard.flags().bitset());
            }
            return size;
        }

        @Override
        public CompactTopology deserialize(DataInputPlus in) throws IOException
        {
            long epoch = in.readLong();
            SortedArrayList<Node.Id> nodeIds = SortedArrayList.copySorted(CollectionSerializers.deserializeList(in, nodeId), Node.Id[]::new);
            int size = in.readUnsignedVInt32();
            byte[] staleNodesBytes = new byte[size];
            in.readFully(staleNodesBytes);
            BitSet staleNodes = staleNodesBytes.length == 0 ? null : BitSet.valueOf(staleNodesBytes);

            ImmutableUniqueList<TableId> tables = ImmutableUniqueList.copyOf(CollectionSerializers.deserializeList(in, TableId.compactComparableSerializer));
            ImmutableUniqueList<CompactTopology.Range> ranges = ImmutableUniqueList.copyOf(CollectionSerializers.deserializeList(in, new CompactTopology.RangeSerializer(tables)));

            size = in.readUnsignedVInt32();
            List<Shard> shards = new ArrayList<>();
            for (int i = 0; i < size; i++)
            {
                CompactTopology.Range range = ranges.get(in.readUnsignedVInt32());
                TokenRange tokenRange = TokenRange.createUnsafe(range.start, range.end);

                SortedArrayList<Node.Id> nodes = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
                SortedArrayList<Node.Id> notInFastPath = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
                SortedArrayList<Node.Id> joining = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
                int flags = in.readUnsignedVInt32();
                shards.add(Shard.SerializerSupport.create(tokenRange, nodes, notInFastPath, joining, new TinyEnumSet<>(flags)));
            }
            return new CompactTopology(epoch, nodeIds, staleNodes, tables, ranges, shards);
        }
    };

    public static final UnversionedSerializer<Topology> topology = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Topology topology, DataOutputPlus out) throws IOException
        {
            out.writeLong(topology.epoch());
            CollectionSerializers.serializeList(topology.shards(), out, shard);
            CollectionSerializers.serializeCollection(topology.staleIds(), out, TopologySerializers.nodeId);
        }

        @Override
        public Topology deserialize(DataInputPlus in) throws IOException
        {
            long epoch = in.readLong();
            Shard[] shards = ArraySerializers.deserializeArray(in, shard, Shard[]::new);
            SortedArrayList<Node.Id> staleIds = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            return new Topology(epoch, staleIds, shards);
        }

        @Override
        public long serializedSize(Topology topology)
        {
            long size = 0;
            size += TypeSizes.LONG_SIZE; // epoch
            size += CollectionSerializers.serializedListSize(topology.shards(), shard);
            size += CollectionSerializers.serializedCollectionSize(topology.staleIds(), TopologySerializers.nodeId);
            return size;
        }
    };
}
