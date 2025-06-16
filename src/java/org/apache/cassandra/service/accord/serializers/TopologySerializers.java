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
import org.apache.cassandra.utils.BitSetSerializer;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.ImmutableUniqueList;

public class TopologySerializers
{
    public static final TableId EMPTY = TableId.fromRaw(Long.MIN_VALUE, Long.MIN_VALUE);

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
                this.start = start.withTable(EMPTY);
                this.end = end.withTable(EMPTY);
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
            private static final RangeSerializer instance = new RangeSerializer();

            @Override
            public void serialize(CompactTopology.Range t, DataOutputPlus out) throws IOException
            {
                TokenKey.noTableSerializer.serialize(t.start, out);
                TokenKey.noTableSerializer.serialize(t.end, out);
            }

            @Override
            public CompactTopology.Range deserialize(DataInputPlus in) throws IOException
            {
                return new CompactTopology.Range(TokenKey.noTableSerializer.deserialize(EMPTY, in),
                                                 TokenKey.noTableSerializer.deserialize(EMPTY, in));
            }

            @Override
            public long serializedSize(CompactTopology.Range t)
            {
                return TokenKey.noTableSerializer.serializedSize(t.start)
                       + TokenKey.noTableSerializer.serializedSize(t.end);
            }
        }

        private static BitSet bitSet(SortedArrayList<Node.Id> src, SortedArrayList<Node.Id> subset)
        {
            BitSet bitSet = new BitSet(src.size());
            for (int i = 0; i < src.size(); i++)
            {
                if (subset.contains(src.get(i)))
                    bitSet.set(i);
            }
            return bitSet;
        }

        private static SortedArrayList<Node.Id> fromBitSet(SortedArrayList<Node.Id> src, BitSet bitSet)
        {
            SortedArrayList.Builder<Node.Id> builder = new SortedArrayList.Builder<>(new Node.Id[bitSet.cardinality()]);
            for (int i = 0; i < src.size(); i++)
            {
                if (bitSet.get(i))
                    builder.add(src.get(i));
            }
            return builder.build();
        }

        private static class ShardRef
        {
            final int tableIdx;
            final int rangeIdx;
            final SortedArrayList<Node.Id> nodes;
            final BitSet notInFastPath;
            final BitSet joining;
            final TinyEnumSet<Shard.Flag> flags;

            private ShardRef(int tableIdx, int rangeIdx,
                             SortedArrayList<Node.Id> nodes,
                             SortedArrayList<Node.Id> notInFastPath,
                             SortedArrayList<Node.Id> joining,
                             TinyEnumSet<Shard.Flag> flags)
            {
                this.tableIdx = tableIdx;
                this.rangeIdx = rangeIdx;
                this.nodes = nodes;
                this.notInFastPath = bitSet(nodes, notInFastPath);
                this.joining = bitSet(nodes, joining);
                this.flags = flags;
            }

            private ShardRef(int tableIdx, int rangeIdx, SortedArrayList<Node.Id> nodes, BitSet notInFastPath, BitSet joining, TinyEnumSet<Shard.Flag> flags)
            {
                this.tableIdx = tableIdx;
                this.rangeIdx = rangeIdx;
                this.nodes = nodes;
                this.notInFastPath = notInFastPath;
                this.joining = joining;
                this.flags = flags;
            }

            public Shard shard(TokenRange range)
            {
                return Shard.SerializerSupport.create(range, nodes, fromBitSet(nodes, notInFastPath), fromBitSet(nodes, joining), flags);
            }

            @Override
            public boolean equals(Object o)
            {
                if (o == null || getClass() != o.getClass()) return false;
                ShardRef shardRef = (ShardRef) o;
                return tableIdx == shardRef.tableIdx && rangeIdx == shardRef.rangeIdx && Objects.equals(nodes, shardRef.nodes) && Objects.equals(notInFastPath, shardRef.notInFastPath) && Objects.equals(joining, shardRef.joining) && Objects.equals(flags, shardRef.flags);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(tableIdx, rangeIdx, nodes, notInFastPath, joining, flags);
            }
        }

        private final long epoch;
        private final SortedArrayList<Node.Id> staleNodes;
        private final ImmutableUniqueList<TableId> tables;
        private final ImmutableUniqueList<Range> ranges;
        private final List<ShardRef> shards;

        CompactTopology(long epoch,
                        SortedArrayList<Node.Id> staleNodes,
                        ImmutableUniqueList<TableId> tables,
                        ImmutableUniqueList<Range> ranges,
                        List<ShardRef> shards)
        {
            this.epoch = epoch;
            this.staleNodes = staleNodes;
            this.tables = tables;
            this.ranges = ranges;
            this.shards = shards;
        }

        public CompactTopology(Topology topology)
        {
            epoch = topology.epoch();
            staleNodes = topology.staleIds();
            ImmutableUniqueList.Builder<TableId> tablesBuilder = ImmutableUniqueList.builder();
            ImmutableUniqueList.Builder<Range> rangesBuilder = ImmutableUniqueList.builder();
            List<ShardRef> shards = new ArrayList<>(topology.size());
            for (Shard shard : topology.shards())
            {
                TokenRange range = (TokenRange) shard.range;
                tablesBuilder.add(range.table());
                Range r = new Range(range);
                rangesBuilder.add(r);

                int tableIdx = tablesBuilder.indexOf(range.table());
                int rangeIdx = rangesBuilder.indexOf(r);
                shards.add(new ShardRef(tableIdx, rangeIdx, shard.nodes, shard.notInFastPath, shard.joining, shard.flags()));
            }
            this.tables = tablesBuilder.buildAndClear();
            this.ranges = rangesBuilder.buildAndClear();
            this.shards = shards;
        }

        public Topology topology()
        {
            Shard[] shards = shards();
            return new Topology(epoch, staleNodes, shards);
        }

        private Shard[] shards()
        {
            Shard[] result = new Shard[this.shards.size()];
            for (int i = 0; i < result.length; i++)
            {
                ShardRef ref = this.shards.get(i);
                TokenRange range = tokenRange(ref.tableIdx, ref.rangeIdx);

                result[i] = ref.shard(range);
            }
            return result;
        }

        private TokenRange tokenRange(int tableIdx, int rangeIdx)
        {
            TableId tableId = tableId(tableIdx);
            Range range = ranges.get(rangeIdx);
            return TokenRange.createUnsafe(range.start.withTable(tableId), range.end.withTable(tableId));
        }

        private TableId tableId(int tableIdx)
        {
            return tables.get(tableIdx);
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            CompactTopology that = (CompactTopology) o;
            return epoch == that.epoch && Objects.equals(staleNodes, that.staleNodes) && Objects.equals(tables, that.tables) && Objects.equals(ranges, that.ranges) && Objects.equals(shards, that.shards);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(epoch, staleNodes, tables, ranges, shards);
        }
    }

    public static final UnversionedSerializer<CompactTopology> dictionaryTopology = new UnversionedSerializer<CompactTopology>()
    {
        @Override
        public void serialize(CompactTopology topology, DataOutputPlus out) throws IOException
        {
            out.writeLong(topology.epoch);
            CollectionSerializers.serializeList(topology.staleNodes, out, nodeId);

            CollectionSerializers.serializeList(topology.tables, out, TableId.compactComparableSerializer);
            CollectionSerializers.serializeList(topology.ranges, out, CompactTopology.RangeSerializer.instance);

            out.writeUnsignedVInt32(topology.shards.size());
            for (CompactTopology.ShardRef shard : topology.shards)
            {
                out.writeUnsignedVInt32(shard.tableIdx);
                out.writeUnsignedVInt32(shard.rangeIdx);

                //TODO (perf): can this be compressed?
                CollectionSerializers.serializeList(shard.nodes, out, nodeId);
                BitSetSerializer.instance.serialize(shard.notInFastPath, out);
                BitSetSerializer.instance.serialize(shard.joining, out);
                out.writeUnsignedVInt32(shard.flags.bitset());
            }
        }

        @Override
        public long serializedSize(CompactTopology topology)
        {
            long size = Long.BYTES;
            size += CollectionSerializers.serializedListSize(topology.staleNodes, nodeId);

            size += CollectionSerializers.serializedListSize(topology.tables, TableId.compactComparableSerializer);
            size += CollectionSerializers.serializedListSize(topology.ranges, CompactTopology.RangeSerializer.instance);

            ImmutableUniqueList<TableId> tables = topology.tables;
            ImmutableUniqueList<CompactTopology.Range> ranges = topology.ranges;

            size += TypeSizes.sizeofUnsignedVInt(topology.shards.size());
            for (CompactTopology.ShardRef shard : topology.shards)
            {
                size += TypeSizes.sizeofUnsignedVInt(shard.tableIdx);
                size += TypeSizes.sizeofUnsignedVInt(shard.rangeIdx);

                size += CollectionSerializers.serializedListSize(shard.nodes, nodeId);
                size += BitSetSerializer.instance.serializedSize(shard.notInFastPath);
                size += BitSetSerializer.instance.serializedSize(shard.joining);
                size += TypeSizes.sizeofUnsignedVInt(shard.flags.bitset());
            }
            return size;
        }

        @Override
        public CompactTopology deserialize(DataInputPlus in) throws IOException
        {
            long epoch = in.readLong();
            SortedArrayList<Node.Id> staleNodes = SortedArrayList.copySorted(CollectionSerializers.deserializeList(in, nodeId), Node.Id[]::new);

            ImmutableUniqueList<TableId> tables = ImmutableUniqueList.copyOf(CollectionSerializers.deserializeList(in, TableId.compactComparableSerializer));
            ImmutableUniqueList<CompactTopology.Range> ranges = ImmutableUniqueList.copyOf(CollectionSerializers.deserializeList(in, CompactTopology.RangeSerializer.instance));

            int size = in.readUnsignedVInt32();
            List<CompactTopology.ShardRef> shards = new ArrayList<>();
            for (int i = 0; i < size; i++)
            {
                int tableIndex = in.readUnsignedVInt32();
                int rangeIndex = in.readUnsignedVInt32();
                // confirm indexes are valid
                //noinspection ResultOfMethodCallIgnored
                tables.get(tableIndex); // will index-out-of-bounds if not valid
                //noinspection ResultOfMethodCallIgnored
                ranges.get(rangeIndex); // will index-out-of-bounds if not valid

                SortedArrayList<Node.Id> nodes = CollectionSerializers.deserializeSortedArrayList(in, nodeId, Node.Id[]::new);
                BitSet notInFastPath = BitSetSerializer.instance.deserialize(in);
                BitSet joining = BitSetSerializer.instance.deserialize(in);
                int flags = in.readUnsignedVInt32();
                shards.add(new CompactTopology.ShardRef(tableIndex, rangeIndex, nodes, notInFastPath, joining, new TinyEnumSet<>(flags)));
            }
            return new CompactTopology(epoch, staleNodes, tables, ranges, shards);
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
