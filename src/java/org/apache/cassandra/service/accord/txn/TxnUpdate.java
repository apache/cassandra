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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Data;
import accord.api.Key;
import accord.api.Update;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import accord.utils.SimpleBitSets;
import accord.utils.SortedArrays;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.accord.AccordObjectSizes;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.SerializePacked;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.txn.TxnCondition.SerializedTxnCondition;
import org.apache.cassandra.service.accord.txn.TxnWrite.Fragment;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.ArrayBuffers.cachedInts;
import static accord.utils.Invariants.requireArgument;
import static accord.utils.SortedArrays.Search.CEIL;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.FALSE;
import static org.apache.cassandra.service.accord.serializers.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.service.accord.txn.TxnUpdate.BlockFragment.NO_BLOCK_FRAGMENTS;
import static org.apache.cassandra.service.accord.txn.TxnUpdate.ConditionalBlock.NO_CONDITIONAL_BLOCKS;
import static org.apache.cassandra.utils.ArraySerializers.skipArray;
import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.skipWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public final class TxnUpdate extends AccordUpdate
{
    static class ConditionalBlock
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new ConditionalBlock(0, null, null));
        static final ConditionalBlock[] NO_CONDITIONAL_BLOCKS = new ConditionalBlock[0];
        public static final UnversionedSerializer<ConditionalBlock> serializer = new UnversionedSerializer<>()
        {
            @Override
            public void serialize(ConditionalBlock t, DataOutputPlus out) throws IOException
            {
                out.writeUnsignedVInt32(t.id);
                writeWithVIntLength(t.condition.bytes(), out);
                SerializePacked.serializePackedSortedIntsAndLength(t.fragmentIds, out);
            }

            @Override
            public ConditionalBlock deserialize(DataInputPlus in) throws IOException
            {
                int id = in.readUnsignedVInt32();
                SerializedTxnCondition condition = new SerializedTxnCondition(readWithVIntLength(in));

                // Deserialize mutations
                int[] mutations = SerializePacked.deserializePackedSortedIntsAndLength(in);
                return new ConditionalBlock(id, condition, mutations);
            }

            @Override
            public void skip(DataInputPlus in) throws IOException
            {
                in.readUnsignedVInt32();
                skipWithVIntLength(in);
                SerializePacked.skipPackedSortedIntsAndLength(in);
            }

            @Override
            public long serializedSize(ConditionalBlock t)
            {
                long size = TypeSizes.sizeofUnsignedVInt(t.id);
                size += serializedSizeWithVIntLength(t.condition.bytes());
                size += SerializePacked.serializedSizeOfPackedSortedIntsAndLength(t.fragmentIds);
                return size;
            }
        };

        final int id;
        final SerializedTxnCondition condition;
        final int[] fragmentIds;

        ConditionalBlock(int id, SerializedTxnCondition condition, int[] fragmentIds)
        {
            this.id = id;
            this.condition = condition;
            this.fragmentIds = fragmentIds;
        }

        public long estimatedSizeOnHeap()
        {
            long size = EMPTY_SIZE;
            size += condition.estimatedSizeOnHeap();
            size += ObjectSizes.sizeOfArray(fragmentIds);
            return size;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            ConditionalBlock that = (ConditionalBlock) o;
            return id == that.id && condition.equals(that.condition) && Arrays.equals(fragmentIds, that.fragmentIds);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, condition, Arrays.hashCode(fragmentIds));
        }

        public void toString(StringBuilder sb, TableMetadatas tables, Block block)
        {
            sb.append("{condition=")
              .append(condition.deserialize(tables))
              .append(", fragments=")
              .append(deserialize(tables, block, fragmentIds))
              .append('}');
        }

        public ConditionalBlock merge(ConditionalBlock that)
        {
            requireArgument(this.id == that.id, "Tried to merge different blocks; expected %d but given %d", this.id, that.id);
            return new ConditionalBlock(id, condition, SortedArrays.linearUnion(this.fragmentIds, 0, this.fragmentIds.length, that.fragmentIds, 0, that.fragmentIds.length, cachedInts()));
        }
    }

    static class BlockFragment
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new BlockFragment(0, null, null));
        static final BlockFragment[] NO_BLOCK_FRAGMENTS = new BlockFragment[0];
        public static final ParameterisedUnversionedSerializer<BlockFragment, TableMetadatasAndKeys> serializer = new ParameterisedUnversionedSerializer<>()
        {
            @Override
            public void serialize(BlockFragment t, TableMetadatasAndKeys p, DataOutputPlus out) throws IOException
            {
                out.writeUnsignedVInt32(t.id);
                p.serializeKey(t.key, out);
                writeWithVIntLength(t.bytes, out);
            }

            @Override
            public BlockFragment deserialize(TableMetadatasAndKeys p, DataInputPlus in) throws IOException
            {
                int id = in.readUnsignedVInt32();
                PartitionKey key = p.deserializeKey(in);
                ByteBuffer bytes = readWithVIntLength(in);
                return new BlockFragment(id, key, bytes);
            }

            @Override
            public void skip(TableMetadatasAndKeys p, DataInputPlus in) throws IOException
            {
                in.readUnsignedVInt32();
                p.skipKeys(in);
                skipWithVIntLength(in);
            }

            @Override
            public long serializedSize(BlockFragment t, TableMetadatasAndKeys p)
            {
                long size = TypeSizes.sizeofUnsignedVInt(t.id);
                size += p.serializedKeySize(t.key);
                size += serializedSizeWithVIntLength(t.bytes);
                return size;
            }
        };

        final int id;
        final PartitionKey key;
        final ByteBuffer bytes;

        BlockFragment(int id, PartitionKey key, ByteBuffer bytes)
        {
            this.id = id;
            this.key = key;
            this.bytes = bytes;
        }

        public boolean equals(Object that)
        {
            return that instanceof BlockFragment && equals((BlockFragment) that);
        }

        public boolean equals(BlockFragment that)
        {
            return this.id == that.id && this.key.equals(that.key) && this.bytes.equals(that.bytes);
        }

        public long estimatedSizeOnHeap()
        {
            long size = EMPTY_SIZE;
            size += ObjectSizes.sizeOnHeapOf(bytes);
            // don't count key as reference to key in parent
            return size;
        }
    }

    public static class Block
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Block(null, null));
        public static final ParameterisedUnversionedSerializer<Block, TableMetadatasAndKeys> serializer = new ParameterisedUnversionedSerializer<>()
        {
            @Override
            public void serialize(Block t, TableMetadatasAndKeys p, DataOutputPlus out) throws IOException
            {
                ArraySerializers.serializeArray(t.fragments, p, out, BlockFragment.serializer);
                ArraySerializers.serializeArray(t.conditionalBlocks, out, ConditionalBlock.serializer);
            }

            @Override
            public Block deserialize(TableMetadatasAndKeys p, DataInputPlus in) throws IOException
            {
                BlockFragment[] fragments = ArraySerializers.deserializeArray(p, in, BlockFragment.serializer, BlockFragment[]::new);
                ConditionalBlock[] conditionalBlocks = ArraySerializers.deserializeArray(in, ConditionalBlock.serializer, ConditionalBlock[]::new);
                return new Block(fragments, conditionalBlocks);
            }

            @Override
            public void skip(TableMetadatasAndKeys p, DataInputPlus in) throws IOException
            {
                ArraySerializers.skipArray(p, in, BlockFragment.serializer);
                ArraySerializers.skipArray(in, ConditionalBlock.serializer);
            }

            @Override
            public long serializedSize(Block t, TableMetadatasAndKeys p)
            {
                long size = 0;
                size += ArraySerializers.serializedArraySize(t.fragments, p, BlockFragment.serializer);
                size += ArraySerializers.serializedArraySize(t.conditionalBlocks, ConditionalBlock.serializer);
                return size;
            }
        };

        final BlockFragment[] fragments;
        final ConditionalBlock[] conditionalBlocks;

        // Must ensure the following invariants when constructing a Block
        // (1) Every fragment in fragments is sorted by key order
        // (2) conditionalBlocks occurs in the same order as the IF/ELSE IF/ELSE statements because the order of the array
        // determines the order in which the conditions get evaluated
        // (3) Within each ConditionalBlock, fragmentIds is in sorted order
        Block(BlockFragment[] fragments, ConditionalBlock[] conditionalBlocks)
        {
            this.fragments = fragments;
            this.conditionalBlocks = conditionalBlocks;
        }

        public long estimatedSizeOnHeap()
        {
            long size = EMPTY_SIZE;
            size += ObjectSizes.sizeOfArray(fragments);
            for (BlockFragment bf : fragments)
                size += bf.estimatedSizeOnHeap();
            for (ConditionalBlock conditionalBlock : conditionalBlocks)
                size += conditionalBlock.estimatedSizeOnHeap();
            return size;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            Block block = (Block) o;
            return Arrays.equals(fragments, block.fragments) && Arrays.equals(conditionalBlocks, block.conditionalBlocks);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(fragments), Arrays.hashCode(conditionalBlocks));
        }

        public void toString(StringBuilder sb, TableMetadatas tables)
        {
            sb.append("{conditionalBlocks=[");
            for (int j = 0; j < conditionalBlocks.length; j++)
            {
                if (j > 0) sb.append(", ");
                conditionalBlocks[j].toString(sb, tables, this);
            }
            sb.append("]}");
        }

        public Block select(Keys keys)
        {
            int[] outFragmentIds = cachedInts().getInts(fragments.length);
            BlockFragment[] outFragments;
            int count = 0;
            {
                {
                    int i = 0, j = 0;
                    while (i < keys.size() && j < fragments.length)
                    {
                        Key key = keys.get(i++);
                        j = SortedArrays.exponentialSearch(fragments, j, fragments.length, key, (k, b) -> k.compareTo(b.key), CEIL);
                        if (j < 0) j = -1 - j;
                        else
                        {
                            do outFragmentIds[count++] = j;
                            while (++j < fragments.length && fragments[j].key.equals(key));
                        }
                    }
                }

                if (count == fragments.length)
                    return this;

                if (count == 0)
                    return new Block(NO_BLOCK_FRAGMENTS, NO_CONDITIONAL_BLOCKS);

                outFragments = new BlockFragment[count];
                for (int i = 0 ; i < count ; ++i)
                {
                    outFragments[i] = fragments[outFragmentIds[i]];
                    outFragmentIds[i] = outFragments[i].id;
                }
            }

            ConditionalBlock[] outConditions;
            {
                List<ConditionalBlock> collect = new ArrayList<>(conditionalBlocks.length);
                for (int i = 0 ; i < conditionalBlocks.length ; ++i)
                {
                    ConditionalBlock cb = conditionalBlocks[i];
                    int[] cbOutFragmentIds = SortedArrays.linearIntersection(cb.fragmentIds, 0, cb.fragmentIds.length, outFragmentIds, 0, count, cachedInts());

                    if (cbOutFragmentIds.length > 0)
                        collect.add(new ConditionalBlock(cb.id, cb.condition, cbOutFragmentIds));
                }
                if (collect.isEmpty()) outConditions = NO_CONDITIONAL_BLOCKS;
                else outConditions = collect.toArray(ConditionalBlock[]::new);
            }

            cachedInts().forceDiscard(outFragmentIds);
            return new Block(outFragments, outConditions);
        }

        public Block merge(Block that)
        {
            BlockFragment[] outFragments;
            if (this.fragments.length == 0) outFragments = that.fragments;
            else if (that.fragments.length == 0) outFragments = this.fragments;
            else
            {
                int minId = Math.min(this.fragments[0].id, that.fragments[0].id);
                int maxId = Math.max(this.fragments[this.fragments.length - 1].id, that.fragments[that.fragments.length - 1].id);
                outFragments = new BlockFragment[Math.min(this.fragments.length + that.fragments.length, 1 + (maxId - minId))];

                int i = 0, j = 0, count = 0;
                while (i < this.fragments.length || j < that.fragments.length)
                {
                    int cmp;
                    if (i == this.fragments.length) cmp = 1;
                    else if (j == that.fragments.length) cmp = -1;
                    else cmp = this.fragments[i].id - that.fragments[j].id;

                    if (cmp <= 0)
                    {
                        outFragments[count] = this.fragments[i];
                        ++i;
                        j += cmp == 0 ? 1 : 0;
                    }
                    else
                    {
                        outFragments[count] = that.fragments[j];
                        ++j;
                    }
                    ++count;
                }

                if (count != outFragments.length)
                    outFragments = Arrays.copyOf(outFragments, count);
            }

            ConditionalBlock[] outConditions;
            if (this.conditionalBlocks.length == 0) outConditions = that.conditionalBlocks;
            else if (that.conditionalBlocks.length == 0) outConditions = this.conditionalBlocks;
            else
            {
                int minId = Math.min(this.conditionalBlocks[0].id, that.conditionalBlocks[0].id);
                int maxId = Math.max(this.conditionalBlocks[this.conditionalBlocks.length - 1].id, that.conditionalBlocks[that.conditionalBlocks.length - 1].id);
                outConditions = new ConditionalBlock[Math.min(this.conditionalBlocks.length + that.conditionalBlocks.length, 1 + maxId - minId)];
                int i = 0, j = 0, count = 0;
                while (i < this.conditionalBlocks.length || j < that.conditionalBlocks.length)
                {
                    int cmp;
                    if (i == this.conditionalBlocks.length) cmp = 1;
                    else if (j == that.conditionalBlocks.length) cmp = -1;
                    else cmp = this.conditionalBlocks[i].id - that.conditionalBlocks[j].id;

                    if (cmp == 0)
                        outConditions[count] = this.conditionalBlocks[i++].merge(that.conditionalBlocks[j++]);
                    else if (cmp < 0)
                        outConditions[count] = this.conditionalBlocks[i++];
                    else
                        outConditions[count] = that.conditionalBlocks[j++];
                    ++count;
                }
                if (count < outConditions.length)
                    outConditions = Arrays.copyOf(outConditions, count);
            }
            return new Block(outFragments, outConditions);
        }
    }

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnUpdate(TableMetadatas.none(), Keys.EMPTY, Collections.emptyList(), null, PreserveTimestamp.no));
    private static final int FLAG_PRESERVE_TIMESTAMPS = 0x1;

    final TableMetadatas tables;
    final Keys keys;
    /**
     * CASSANDRA-20883 added this logic in, but didn't update the CQL layer to leverage it; left for follow-up work.
     * <p>
     * The reason for this setup is to allow the following in CQL (any any combination of them):
     * <p>
     * <code>
     *     IF cond1 THEN
     *       mutation1
     *     ELSE
     *       mutation2
     *     END IF
     * </code>
     * <p>
     * <code>
     *     IF cond1 THEN
     *       mutation1
     *       IF cond2 THEN
     *         mutation2
     *       ELSE
     *         mutation3
     *       END IF
     *     ELSE IF cond3 THEN
     *       mutation4
     *     END IF
     * </code>
     * <p>
     * and lastly
     * <p>
     * <code>
     *     IF cond THEN
     *       mutation1
     *     END IF
     *     mutation2
     * </code>
     * <p>
     * Each {@link Block} represents a single <code>IF / END IF</code> block.
     * Each {@link ConditionalBlock} represents a single condition with its mutations
     * <p>
     * Given the flat structure, you must rewrite the <code>IF / END IF</code> into this structure, so for cases like nested IF they should uplift the conditions as so
     * <p>
     * Before
     * <code>
     *     IF cond1 THEN
     *       mutation1
     *       IF cond2 THEN
     *         mutation2
     *       ELSE
     *         mutation3
     *       END IF
     *     END IF
     * </code>
     * <p>
     * After
     * <code>
     *     IF cond1 AND cond2 THEN
     *       mutation1
     *       mutation2
     *     ELSE IF cond1
     *       mutation1
     *     END IF
     * </code>
     * <p>
     * When a non-conditional set of mutations exists with conditional ones, then the non-conditional mutations should
     * be in their own block with a empty condition.
     */
    final List<Block> blocks;

    @Nullable
    private final ConsistencyLevel cassandraCommitCL;

    // Hints and batchlog want to write with the lower timestamp they generated when applying their writes via Accord
    // so they don't resurrect data if they are applied at a later time. Accord should be fine with this because
    // the writes are still deterministic from the perspective of coordinators/recovery coordinators.
    private final PreserveTimestamp preserveTimestamps;

    // Memoize computation of condition
    private Boolean anyConditionResult;

    @Nullable
    private transient volatile Object validationException = this;

    public TxnUpdate(TableMetadatas tables, List<Fragment> fragments, TxnCondition condition, @Nullable ConsistencyLevel cassandraCommitCL, PreserveTimestamp preserveTimestamps)
    {
        requireArgument(cassandraCommitCL == null || IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.contains(cassandraCommitCL));
        fragments.sort(Fragment::compareKeys);
        this.tables = tables;
        this.keys = Keys.of(fragments, fragment -> fragment.key);

        BlockFragment[] blockFragments = new BlockFragment[fragments.size()];
        // TODO (required): this node could be on version N while the peers are on N-1, which would have issues as the peers wouldn't know about N yet.
        //  Can not eagerly serialize until we know the "correct" version, else we need a way to fallback on mismatch.
        int[] fragmentIds = new int[fragments.size()];
        for (int i = 0 ; i < fragments.size() ; ++i)
        {
            blockFragments[i] = new BlockFragment(i, fragments.get(i).key, Fragment.FragmentSerializer.serialize(fragments.get(i), tables, Version.LATEST));
            fragmentIds[i] = i;
        }

        SerializedTxnCondition serializedCondition = new SerializedTxnCondition(condition, tables);
        this.blocks = Collections.singletonList(new Block(blockFragments, new ConditionalBlock[] { new ConditionalBlock(0, serializedCondition, fragmentIds) }));
        this.cassandraCommitCL = cassandraCommitCL;
        this.preserveTimestamps = preserveTimestamps;
    }

    public TxnUpdate(TableMetadatas tables, List<PreTransformedBlock> preTransformedBlocks, @Nullable ConsistencyLevel cassandraCommitCL, PreserveTimestamp preserveTimestamps)
    {
        requireArgument(cassandraCommitCL == null || IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.contains(cassandraCommitCL));
        this.tables = tables;
        this.keys = sortedFragmentKeys(preTransformedBlocks);

        List<Block> blocks = new ArrayList<>();
        int nextConditionIndex = 0;
        for (PreTransformedBlock preTransformedBlock : preTransformedBlocks)
        {
            Pair<Integer, Block> pair = preTransformedBlock.generateBlock(nextConditionIndex, tables);
            nextConditionIndex = pair.left();
            blocks.add(pair.right());
        }

        this.blocks = blocks;
        this.cassandraCommitCL = cassandraCommitCL;
        this.preserveTimestamps = preserveTimestamps;
    }

    private static Keys sortedFragmentKeys(List<PreTransformedBlock> preTransformedBlocks)
    {
        List<Key> keys = new ArrayList<>();

        for (PreTransformedBlock preTransformedBlock : preTransformedBlocks)
            preTransformedBlock.accumulateKeys(keys);

        // The PreTransformedBlock constructor maintains the invariant that
        // all TxnWrite.Fragments are sorted by key, so we do not need to sort them again
        if (preTransformedBlocks.size() == 1)
            return Keys.of(keys);

        keys.sort(RoutableKey::compareTo);
        return Keys.of(keys);
    }

    public static class PreTransformedBlock
    {
        private final TxnCondition[] conditions;
        @Nullable
        private final List<Pair<TxnWrite.Fragment, Integer>> fragmentConditionIndexPair;
        @Nullable
        private final List<TxnWrite.Fragment> noneConditions;

        public PreTransformedBlock(TxnCondition[] conditions, List<Pair<TxnWrite.Fragment, Integer>> fragmentConditionIndexPair, List<TxnWrite.Fragment> noneConditions)
        {
            Invariants.require((fragmentConditionIndexPair == null && noneConditions != null) || (fragmentConditionIndexPair != null && noneConditions == null));
            this.conditions = conditions;
            if (fragmentConditionIndexPair != null)
                fragmentConditionIndexPair.sort((l, r) -> TxnWrite.Fragment.compareKeys(l.left(), r.left()));
            this.fragmentConditionIndexPair = fragmentConditionIndexPair;
            if (noneConditions != null)
                noneConditions.sort(Fragment::compareKeys);
            this.noneConditions = noneConditions;
        }

        private boolean isTrailingUpdate()
        {
            return noneConditions != null;
        }

        public void accumulateKeys(List<Key> keys)
        {
            if (isTrailingUpdate())
                for (int i = 0; i < noneConditions.size(); i++)
                    keys.add(noneConditions.get(i).key);
            else
                for (int i = 0; i < fragmentConditionIndexPair.size(); i++)
                    keys.add(fragmentConditionIndexPair.get(i).left().key);
        }

        public Pair<Integer, Block> generateBlock(int conditionalBlockIndex, TableMetadatas tables)
        {
            if (isTrailingUpdate())
            {
                BlockFragment[] blockFragments = new BlockFragment[noneConditions.size()];
                int[] fragmentIds = new int[noneConditions.size()];
                for (int i = 0 ; i < noneConditions.size() ; ++i)
                {
                    Fragment fragment = noneConditions.get(i);
                    blockFragments[i] = new BlockFragment(i, fragment.key, Fragment.FragmentSerializer.serialize(fragment, tables, Version.LATEST));
                    fragmentIds[i] = i;
                }

                SerializedTxnCondition serializedCondition = new SerializedTxnCondition(TxnCondition.none(), tables);
                ConditionalBlock[] conditionalBlock = new ConditionalBlock[] { new ConditionalBlock(conditionalBlockIndex, serializedCondition, fragmentIds) };
                conditionalBlockIndex++;
                return Pair.create(conditionalBlockIndex, new Block(blockFragments, conditionalBlock));
            }
            else
            {
                List<List<Integer>> conditionsIndexToFragmentId = new ArrayList<>(conditions.length);
                for (int i = 0; i < conditions.length; i++)
                    conditionsIndexToFragmentId.add(new ArrayList<>());

                BlockFragment[] blockFragments = new BlockFragment[fragmentConditionIndexPair.size()];
                ConditionalBlock[] conditionalBlocks = new ConditionalBlock[conditions.length];

                int blockFragmentIdx = 0;
                for (Pair<Fragment, Integer> pair : fragmentConditionIndexPair)
                {
                    Fragment fragment = pair.left;
                    int conditionsIndex = pair.right;
                    blockFragments[blockFragmentIdx] = new BlockFragment(blockFragmentIdx, fragment.key, Fragment.FragmentSerializer.serialize(fragment, tables, Version.LATEST));
                    conditionsIndexToFragmentId.get(conditionsIndex).add(blockFragmentIdx);
                    blockFragmentIdx++;
                }

                for (int i = 0; i < conditions.length; i++)
                {
                    int[] fragmentIds = conditionsIndexToFragmentId.get(i).stream().mapToInt(x -> x).toArray();
                    SerializedTxnCondition serializedCondition = new SerializedTxnCondition(conditions[i], tables);
                    conditionalBlocks[i] = new ConditionalBlock(conditionalBlockIndex, serializedCondition, fragmentIds);
                    conditionalBlockIndex++;
                }

                return Pair.create(conditionalBlockIndex, new Block(blockFragments, conditionalBlocks));
            }
        }
    }

    private TxnUpdate(TableMetadatas tables, Keys keys, List<Block> blocks, ConsistencyLevel cassandraCommitCL, PreserveTimestamp preserveTimestamps)
    {
        this.tables = tables;
        this.keys = keys;
        this.blocks = blocks;
        this.cassandraCommitCL = cassandraCommitCL;
        this.preserveTimestamps = preserveTimestamps;
    }

    public static TxnUpdate empty()
    {
        return new TxnUpdate(TableMetadatas.none(), Keys.EMPTY, Collections.emptyList(), null, PreserveTimestamp.no);
    }

    public static TxnValidationRejection validationRejection(@Nullable Update update)
    {
        if (update != null && update.getClass() == TxnUpdate.class)
        {
            RequestValidationException e = ((TxnUpdate) update).validationRejection();
            if (e != null) return new TxnValidationRejection(e);
        }
        return null;
    }

    @Nullable
    private RequestValidationException validationRejection()
    {
        Object snapshot = validationException;
        if (snapshot == this)
            throw Invariants.illegalState("Attempted to check for validation exception before .apply was called");
        return snapshot == null ? null : (RequestValidationException) snapshot;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (Block block : blocks)
            size += block.estimatedSizeOnHeap();
        size += AccordObjectSizes.keys(keys);
        return size;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("TxnUpdate{blocks=[");
        for (int i = 0; i < blocks.size(); i++)
        {
            if (i > 0) sb.append(", ");
            blocks.get(i).toString(sb, tables);
        }
        sb.append("]}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnUpdate txnUpdate = (TxnUpdate) o;
        return Objects.equals(blocks, txnUpdate.blocks);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(blocks);
    }

    @Override
    public Keys keys()
    {
        return keys;
    }

    // Batch log and hints want to keep their lower timestamp for the applied writes to avoid resurrecting old data
    // when they are applied later, possibly after further updates have already been acknowledged.
    public PreserveTimestamp preserveTimestamps()
    {
        return preserveTimestamps;
    }

    @Override
    public TxnUpdate slice(Ranges ranges)
    {
        return getTxnUpdate(keys -> keys.slice(ranges, Minimal));
    }

    @Override
    public TxnUpdate intersecting(Participants<?> participants)
    {
        return getTxnUpdate(keys -> keys.intersecting(participants, Minimal));
    }

    @VisibleForTesting
    TxnUpdate getTxnUpdate(Function<Keys, Keys> fn)
    {
        Keys newKeys = fn.apply(keys);
        List<Block> blocks = new ArrayList<>(this.blocks.size());
        for (Block block : this.blocks)
            blocks.add(block.select(newKeys));
        return new TxnUpdate(tables, newKeys, blocks, cassandraCommitCL, preserveTimestamps);
    }

    @Override
    public TxnUpdate merge(Update update)
    {
        TxnUpdate that = (TxnUpdate) update;
        requireArgument(that.blocks.size() == this.blocks.size(), "Blocks dont have the same sizes; expected %d but was %d", this.blocks.size(), that.blocks.size());
        Keys keys = this.keys.with(that.keys);

        List<Block> mergedBlocks = new ArrayList<>(this.blocks.size());
        for (int i = 0; i < this.blocks.size(); i++)
            mergedBlocks.add(this.blocks.get(i).merge(that.blocks.get(i)));
        
        return new TxnUpdate(tables, keys, mergedBlocks, cassandraCommitCL, preserveTimestamps);
    }

    @Override
    public TxnWrite apply(Timestamp executeAt, Data data)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        checkState(cm.epoch.getEpoch() >= executeAt.epoch(), "TCM epoch %d is < executeAt epoch %d", cm.epoch.getEpoch(), executeAt.epoch());

        Pair<List<TxnWrite.Update>, SimpleBitSet> pair = null;
        try
        {
            validationException = null;
            pair = processCondition(executeAt, data);
        }
        catch (RequestValidationException e)
        {
            // the update isn't allowed
            validationException = e;
        }

        if (pair == null)
            return new TxnWrite(TableMetadatas.none(), Collections.emptyList(), SimpleBitSets.allUnset(numConditionalBlocks()));

        List<TxnWrite.Update> allUpdates = pair.left;
        SimpleBitSet conditionalBlockBitSet = pair.right;
        if (keys.isEmpty())
            return new TxnWrite(TableMetadatas.none(), Collections.emptyList(), SimpleBitSets.allSet(numConditionalBlocks()));

        return new TxnWrite(tables, allUpdates, conditionalBlockBitSet);
    }

    
    private boolean checkCondition(Data data, SerializedTxnCondition condition)
    {
        TxnCondition deserializedCondition = condition.deserialize(tables);
        if (deserializedCondition == TxnCondition.none())
            return true;
        return deserializedCondition.applies((TxnData) data);
    }

    public List<TxnWrite.Update> completeUpdatesForKey(SimpleBitSet conditionalBlockBitSet, RoutableKey key)
    {
        List<TxnWrite.Update> updates = new ArrayList<>();
        
        for (Block block : blocks)
        {
            for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
            {
                if (!conditionalBlockBitSet.get(conditionalBlock.id)) continue;
                List<Fragment> fragments = deserialize(tables, block, conditionalBlock.fragmentIds);
                for (Fragment fragment : fragments)
                    if (fragment.isComplete() && fragment.key.equals(key))
                        updates.add(fragment.toUpdate(tables));
            }
        }

        return updates;
    }

    public static final AccordUpdateSerializer<TxnUpdate> serializer = new AccordUpdateSerializer<>()
    {
        @Override
        public void serialize(TxnUpdate update, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out, Version version) throws IOException
        {
            // Serializing it with the condition result set shouldn't be needed
//            checkState(update.anyConditionResult == null, "Can't serialize if conditionResult is set without adding it to serialization");
            // Once in accord "mixedTimeSource" and "yes" are the same, so only care about the side effect: that the timestamp is preserved or not
            out.writeByte(update.preserveTimestamps.preserve ? FLAG_PRESERVE_TIMESTAMPS : 0);
            tablesAndKeys.serializeKeys(update.keys, out);
            serializeNullable(update.cassandraCommitCL, out, consistencyLevelSerializer);
            CollectionSerializers.serializeList(update.blocks, tablesAndKeys, out, Block.serializer);
        }

        @Override
        public TxnUpdate deserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            int flags = in.readByte();
            boolean preserveTimestamps = (FLAG_PRESERVE_TIMESTAMPS & flags) == 1;
            Keys keys = tablesAndKeys.deserializeKeys(in);
            ConsistencyLevel consistencyLevel = deserializeNullable(in, consistencyLevelSerializer);
            List<Block> blocks = CollectionSerializers.deserializeList(tablesAndKeys, in, Block.serializer);

            return new TxnUpdate(tablesAndKeys.tables, keys, blocks, consistencyLevel, preserveTimestamps ? PreserveTimestamp.yes : PreserveTimestamp.no);
        }

        @Override
        public void skip(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            in.readByte(); // flags
            deserializeNullable(in, consistencyLevelSerializer); // consistency level
            skipArray(tablesAndKeys, in, Block.serializer);
        }

        @Override
        public long serializedSize(TxnUpdate update, TableMetadatasAndKeys tablesAndKeys, Version version)
        {
            long size = 1; // flags
            size += tablesAndKeys.serializedKeysSize(update.keys);
            size += serializedNullableSize(update.cassandraCommitCL, consistencyLevelSerializer);
            size += CollectionSerializers.serializedListSize(update.blocks, tablesAndKeys, Block.serializer);
            return size;
        }
    };

    private static List<Fragment> deserialize(TableMetadatas tables, Block block, int[] includeFragmentIds)
    {
        List<Fragment> result = new ArrayList<>(includeFragmentIds.length);
        int i = 0;
        for (int fragmentId : includeFragmentIds)
        {
            while (block.fragments[i].id < fragmentId)
                ++i;

            Invariants.require(block.fragments[i].id == fragmentId);
            BlockFragment fragment = block.fragments[i];
            try (DataInputBuffer in = new DataInputBuffer(fragment.bytes, true))
            {
                Version version = Version.fromVersion(in.readUnsignedVInt32());
                result.add(Fragment.serializer.deserialize(fragment.key, tables, in, version));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        return result;
    }

    @Override
    public void failCondition()
    {
        anyConditionResult = FALSE;
    }

    @Override
    public boolean checkAnyConditionMatch(Data data)
    {
        // Assert data that was memoized is same as data that is provided?
        if (anyConditionResult != null)
            return anyConditionResult;
            
        // Check if any block has a matching condition
        for (Block block : blocks)
        {
            for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
            {
                if (checkCondition(data, conditionalBlock.condition))
                    return anyConditionResult = true;
            }
        }
        return anyConditionResult = false;
    }

    @Nullable
    private Pair<List<TxnWrite.Update>, SimpleBitSet> processCondition(Timestamp executeAt, Data data)
    {
        int numConditionalBlocks = numConditionalBlocks();
        SimpleBitSet conditionalBlocksMatched = SimpleBitSet.allocate(numConditionalBlocks);
        List<Fragment> fragments = null;
        // Each block is executed indepdendently so a match in one block has no effect on another block,
        // this is done this way to support conditional with unconditional writes, and multiple IF/END IF blocks
        for (Block block : blocks)
        {
            // This loop needs to support the expected semantics of IF/ELSE IF/ELSE blocks;
            // first condition that is true is the only one that applies.
            for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
            {
                if (checkCondition(data, conditionalBlock.condition))
                {
                    conditionalBlocksMatched.set(conditionalBlock.id);
                    if (fragments == null) fragments = new ArrayList<>();
                    fragments.addAll(deserialize(tables, block, conditionalBlock.fragmentIds));
                    break;
                }
            }
        }
        if (fragments == null) return null;

        List<TxnWrite.Update> allUpdates = new ArrayList<>(fragments.size());
        QueryOptions options = QueryOptions.forProtocolVersion(ProtocolVersion.CURRENT);
        AccordUpdateParameters parameters = new AccordUpdateParameters((TxnData) data, options, executeAt.uniqueHlc());

        for (Fragment fragment : fragments)
            if (!fragment.isComplete())
                allUpdates.add(fragment.complete(parameters, tables));
        return Pair.create(allUpdates, conditionalBlocksMatched);
    }

    private int numConditionalBlocks()
    {
        int numConditionalBlocks = 0;
        for (Block block : blocks)
            numConditionalBlocks += block.conditionalBlocks.length;
        return numConditionalBlocks;
    }

    @Override
    public Kind kind()
    {
        return Kind.TXN;
    }

    @Override
    public ConsistencyLevel cassandraCommitCL()
    {
        return cassandraCommitCL;
    }

    @VisibleForTesting
    public void unsafeResetCondition()
    {
        anyConditionResult = null;
    }
}
