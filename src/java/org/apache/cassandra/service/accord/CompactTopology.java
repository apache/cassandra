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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.SortedArrays;
import accord.utils.TinyEnumSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.utils.BitSetSerializer;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.ImmutableUniqueList;

public class CompactTopology
{
    public static class Range
    {
        final TokenKey start;
        final TokenKey end;

        private Range(TokenKey start, TokenKey end)
        {
            this.start = start.withTable(TopologySerializers.EMPTY);
            this.end = end.withTable(TopologySerializers.EMPTY);
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

    public static class ShardRef
    {
        final int tableIdx;
        final int rangeIdx;
        final SortedArrays.SortedArrayList<Node.Id> nodes;
        final BitSet notInFastPath;
        final BitSet joining;
        final TinyEnumSet<Shard.Flag> flags;

        private ShardRef(int tableIdx, int rangeIdx,
                         SortedArrays.SortedArrayList<Node.Id> nodes,
                         SortedArrays.SortedArrayList<Node.Id> notInFastPath,
                         SortedArrays.SortedArrayList<Node.Id> joining,
                         TinyEnumSet<Shard.Flag> flags)
        {
            this.tableIdx = tableIdx;
            this.rangeIdx = rangeIdx;
            this.nodes = nodes;
            this.notInFastPath = bitSet(nodes, notInFastPath);
            this.joining = bitSet(nodes, joining);
            this.flags = flags;
        }

        private ShardRef(int tableIdx, int rangeIdx, SortedArrays.SortedArrayList<Node.Id> nodes, BitSet notInFastPath, BitSet joining, TinyEnumSet<Shard.Flag> flags)
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

    public static final UnversionedSerializer<Range> rangeSerializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Range t, DataOutputPlus out) throws IOException
        {
            TokenKey.noTableSerializer.serialize(t.start, out);
            TokenKey.noTableSerializer.serialize(t.end, out);
        }

        @Override
        public Range deserialize(DataInputPlus in) throws IOException
        {
            return new Range(TokenKey.noTableSerializer.deserialize(TopologySerializers.EMPTY, in),
                             TokenKey.noTableSerializer.deserialize(TopologySerializers.EMPTY, in));
        }

        @Override
        public long serializedSize(Range t)
        {
            return TokenKey.noTableSerializer.serializedSize(t.start)
                   + TokenKey.noTableSerializer.serializedSize(t.end);
        }
    };

    public static final UnversionedSerializer<CompactTopology> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(CompactTopology topology, DataOutputPlus out) throws IOException
        {
            out.writeLong(topology.epoch);
            CollectionSerializers.serializeList(topology.staleNodes, out, TopologySerializers.nodeId);

            CollectionSerializers.serializeList(topology.tables, out, TableId.compactComparableSerializer);
            CollectionSerializers.serializeList(topology.ranges, out, rangeSerializer);

            out.writeUnsignedVInt32(topology.shards.size());
            for (CompactTopology.ShardRef shard : topology.shards)
            {
                out.writeUnsignedVInt32(shard.tableIdx);
                out.writeUnsignedVInt32(shard.rangeIdx);

                CollectionSerializers.serializeList(shard.nodes, out, TopologySerializers.nodeId);
                BitSetSerializer.instance.serialize(shard.notInFastPath, out);
                BitSetSerializer.instance.serialize(shard.joining, out);
                out.writeUnsignedVInt32(shard.flags.bitset());
            }
        }

        @Override
        public long serializedSize(CompactTopology topology)
        {
            long size = Long.BYTES;
            size += CollectionSerializers.serializedListSize(topology.staleNodes, TopologySerializers.nodeId);

            size += CollectionSerializers.serializedListSize(topology.tables, TableId.compactComparableSerializer);
            size += CollectionSerializers.serializedListSize(topology.ranges, rangeSerializer);

            size += TypeSizes.sizeofUnsignedVInt(topology.shards.size());
            for (CompactTopology.ShardRef shard : topology.shards)
            {
                size += TypeSizes.sizeofUnsignedVInt(shard.tableIdx);
                size += TypeSizes.sizeofUnsignedVInt(shard.rangeIdx);

                size += CollectionSerializers.serializedListSize(shard.nodes, TopologySerializers.nodeId);
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
            SortedArrays.SortedArrayList<Node.Id> staleNodes = SortedArrays.SortedArrayList.copySorted(CollectionSerializers.deserializeList(in, TopologySerializers.nodeId), Node.Id[]::new);

            ImmutableUniqueList<TableId> tables = ImmutableUniqueList.copyOf(CollectionSerializers.deserializeList(in, TableId.compactComparableSerializer));
            ImmutableUniqueList<CompactTopology.Range> ranges = ImmutableUniqueList.copyOf(CollectionSerializers.deserializeList(in, rangeSerializer));

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

                SortedArrays.SortedArrayList<Node.Id> nodes = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
                BitSet notInFastPath = BitSetSerializer.instance.deserialize(in);
                BitSet joining = BitSetSerializer.instance.deserialize(in);
                int flags = in.readUnsignedVInt32();
                shards.add(new CompactTopology.ShardRef(tableIndex, rangeIndex, nodes, notInFastPath, joining, new TinyEnumSet<>(flags)));
            }
            return new CompactTopology(epoch, staleNodes, tables, ranges, shards);
        }
    };

    private final long epoch;
    private final SortedArrays.SortedArrayList<Node.Id> staleNodes;
    private final ImmutableUniqueList<TableId> tables;
    private final ImmutableUniqueList<Range> ranges;
    private final List<ShardRef> shards;

    public CompactTopology(long epoch,
                           SortedArrays.SortedArrayList<Node.Id> staleNodes,
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

    private static BitSet bitSet(SortedArrays.SortedArrayList<Node.Id> src, SortedArrays.SortedArrayList<Node.Id> subset)
    {
        BitSet bitSet = new BitSet(src.size());
        for (int i = 0; i < src.size(); i++)
        {
            if (subset.contains(src.get(i)))
                bitSet.set(i);
        }
        return bitSet;
    }

    private static SortedArrays.SortedArrayList<Node.Id> fromBitSet(SortedArrays.SortedArrayList<Node.Id> src, BitSet bitSet)
    {
        SortedArrays.SortedArrayList.Builder<Node.Id> builder = new SortedArrays.SortedArrayList.Builder<>(new Node.Id[bitSet.cardinality()]);
        for (int i = 0; i < src.size(); i++)
        {
            if (bitSet.get(i))
                builder.add(src.get(i));
        }
        return builder.build();
    }
}
