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
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import accord.api.Key;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.SimpleBitSet;
import accord.utils.SortedArrays;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.AccordSerializers;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.txn.TxnCondition.SerializedTxnCondition;
import org.apache.cassandra.service.accord.txn.TxnUpdate.Block;
import org.apache.cassandra.service.accord.txn.TxnUpdate.BlockFragment;
import org.apache.cassandra.service.accord.txn.TxnUpdate.ConditionalBlock;
import org.apache.cassandra.service.accord.txn.TxnWrite.Fragment;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.NullableSerializer;

import static accord.utils.Property.qt;
import static accord.utils.SortedArrays.Search.FAST;
import static org.assertj.core.api.Assertions.assertThat;

public class TxnUpdateTest
{
    private static final LongToken T0 = new LongToken(0);
    private static final LongToken T42 = new LongToken(42);

    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final Gen<ByteBuffer> bytesGen = Generators.toGen(Generators.bytes(0, 20));
    private static final Gen<List<TableId>> uniqueIds = Gens.lists(Generators.toGen(CassandraGenerators.TABLE_ID_GEN)).unique().ofSizeBetween(1, 3);
    private static final Gen<List<TableMetadata>> tablesGen = uniqueIds.map(ids -> {
        List<TableMetadata> tables = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++)
        {
            tables.add(TableMetadata.builder("ks", "tbl" + i, ids.get(i))
                                    .addPartitionKeyColumn("key", BytesType.instance)
                                    .partitioner(Murmur3Partitioner.instance)
                                    .build());
        }
        return tables;
    });

    @Test
    public void conditionalBlockSerde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(conditionalBlock()).check(expected -> Serializers.testSerde(output, ConditionalBlock.serializer, expected));
    }

    @Test
    public void blockSerde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(block()).check(expected -> {
            TableMetadatasAndKeys.KeyCollector collector = new TableMetadatasAndKeys.KeyCollector(TableMetadatas.none());
            for (TxnUpdate.BlockFragment fragment : expected.fragments)
                collector.add(fragment.key);
            Serializers.testSerde(output, Block.serializer, expected, collector.buildTablesAndKeys());
        });
    }

    /**
     * The shape of a CQL transaction body: conditional branches in declaration order (IF / ELSE IF / ELSE),
     * each possibly producing no fragments (e.g. DELETE ... WHERE c < 0 AND c > 0), followed by an optional
     * unconditional (trailing) set of fragments. Keys are drawn from a small pool, so that the same key is
     * typically written by multiple branches, and fragments are not generated in key order.
     */
    private static class BuilderInput
    {
        final TableMetadatas tables;
        final List<List<Fragment>> branches;
        final @Nullable List<Fragment> trailing;

        BuilderInput(TableMetadatas tables, List<List<Fragment>> branches, @Nullable List<Fragment> trailing)
        {
            this.tables = tables;
            this.branches = branches;
            this.trailing = trailing;
        }

        List<Fragment> all()
        {
            List<Fragment> all = new ArrayList<>();
            branches.forEach(all::addAll);
            if (trailing != null)
                all.addAll(trailing);
            return all;
        }

        boolean hasFragments()
        {
            return !all().isEmpty();
        }

        List<Block> build()
        {
            int fragmentCount = 0;
            for (List<Fragment> branch : branches)
                fragmentCount += branch.size();

            TxnUpdate.BlocksBuilder builder = new TxnUpdate.BlocksBuilder(branches.size(), fragmentCount);
            for (int i = 0 ; i < branches.size() ; ++i)
            {
                // the else branch is always last; the condition kind is otherwise irrelevant to block construction
                TxnCondition condition = i == branches.size() - 1 ? TxnCondition.Else.instance : TxnCondition.none();
                builder.addConditional(condition, branches.get(i), tables);
            }
            if (trailing != null)
                builder.addUnconditional(trailing, tables);

            assertThat(builder.isEmpty()).isEqualTo(!hasFragments());
            return builder.isEmpty() ? null : builder.build();
        }

        TxnUpdate buildUpdate()
        {
            List<Block> blocks = build();
            if (blocks == null) return null;
            return TxnUpdate.create(tables, Keys.of(all(), f -> f.key), blocks, null);
        }
    }

    private static Gen<BuilderInput> builderInput()
    {
        return rs -> {
            List<TableMetadata> tables = tablesGen.next(rs);
            List<PartitionKey> keyPool = new ArrayList<>();
            for (int i = 0, mi = rs.nextInt(1, 6) ; i < mi ; ++i)
            {
                TableMetadata metadata = rs.pick(tables);
                keyPool.add(new PartitionKey(metadata.id, metadata.partitioner.decorateKey(bytesGen.next(rs))));
            }
            // Fragment.index identifies the statement, as in TransactionStatement.createWriteFragments: indexes are
            // assigned in declaration order across all branches and the trailing block, and a statement may produce
            // zero fragments (e.g. DELETE ... WHERE c < 0 AND c > 0) or many (e.g. ... WHERE k IN (...)), so indexes
            // within a block are neither unique nor dense
            int[] nextStatementIndex = new int[1];
            Gen<List<Fragment>> branchGen = rs0 -> statements(rs0, tables, keyPool, nextStatementIndex, rs0.nextInt(0, 4));
            List<List<Fragment>> branches = new ArrayList<>();
            for (int i = 0, mi = rs.nextInt(0, 5) ; i < mi ; ++i)
                branches.add(branchGen.next(rs));
            // TransactionStatement never adds an empty unconditional block, and a transaction must have at least one block
            List<Fragment> trailing = null;
            if (branches.isEmpty() || rs.nextBoolean())
            {
                trailing = branchGen.next(rs);
                if (trailing.isEmpty())
                    trailing = branches.isEmpty() ? statements(rs, tables, keyPool, nextStatementIndex, 1, 1) : null;
            }
            return new BuilderInput(TableMetadatas.of(tables), branches, trailing);
        };
    }

    @Test
    public void blocksBuilder()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().forAll(builderInput()).check(input -> {
            List<Block> blocks = input.build();
            if (blocks == null)
                return; // no fragments in any branch: TransactionStatement handles this as an empty write

            int expectedBlocks = (input.branches.isEmpty() ? 0 : 1) + (input.trailing == null ? 0 : 1);
            assertThat(blocks).hasSize(expectedBlocks);

            // conditional block ids are global, and assigned in declaration order with the unconditional block last
            List<List<Fragment>> expectedPerConditionalBlock = new ArrayList<>(input.branches);
            if (input.trailing != null)
                expectedPerConditionalBlock.add(input.trailing);
            int nextId = 0;
            for (Block block : blocks)
            {
                for (ConditionalBlock cb : block.conditionalBlocks)
                {
                    assertThat(cb.id).isEqualTo(nextId);
                    assertThat(fragmentBytes(block, cb)).isEqualTo(serialized(expectedPerConditionalBlock.get(nextId), input.tables));
                    ++nextId;
                }
            }
            assertThat(nextId).isEqualTo(expectedPerConditionalBlock.size());
            assertThat(ensureThatConditionalBlockIndexesAreDisjointAcrossBlocks(blocks)).isTrue();

            for (Block block : blocks)
                assertBlockInvariants(block);

            TxnUpdate update = TxnUpdate.create(input.tables, Keys.of(input.all(), f -> f.key), blocks, null);
            TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(input.tables, update.keys());
            output.clear();
            Serializers.testSerde(output, TxnUpdate.serializer, update, tablesAndKeys, Version.LATEST);
        });
    }

    @Test
    public void blocksBuilderSliceMergeAndComplete()
    {
        Gen<BuilderInput> inputGen = builderInput();
        qt().check(rs -> {
            BuilderInput input = inputGen.next(rs);
            TxnUpdate update = input.buildUpdate();
            if (update == null)
                return;

            // pick the branch that "matched" for the conditional block, if any; the unconditional block always matches
            int numConditionalBlocks = input.branches.size() + (input.trailing == null ? 0 : 1);
            int matchedBranch = input.branches.isEmpty() ? -1 : rs.nextInt(-1, input.branches.size());
            SimpleBitSet matched = SimpleBitSet.allocate(numConditionalBlocks);
            if (matchedBranch >= 0) matched.set(matchedBranch);
            if (input.trailing != null) matched.set(input.branches.size());

            Keys keys = update.keys();
            List<TxnUpdate> perKey = new ArrayList<>(keys.size());
            for (Key key : keys)
            {
                int expected = count(matchedBranch >= 0 ? input.branches.get(matchedBranch) : null, key)
                               + count(input.trailing, key);

                List<PartitionUpdate> updates = new ArrayList<>();
                update.completeUpdatesForKey(updates, matched, key, input.tables);
                assertThat(updates).hasSize(expected);

                // a slice containing only this key must produce the same updates, and keep all the
                // conditional blocks that could write to it (plus any no-op branches) in declaration order
                TxnUpdate single = update.getTxnUpdate(k -> k.overlapping(Keys.of(key)));
                updates.clear();
                single.completeUpdatesForKey(updates, matched, key, input.tables);
                assertThat(updates).hasSize(expected);
                for (int i = 0 ; i < update.blocks.size() ; ++i)
                {
                    Block original = update.blocks.get(i), sliced = single.blocks.get(i);
                    assertBlockInvariants(sliced);
                    assertThat(ensureConditionalBlockPreserveOrder(sliced, original)).isTrue();
                    for (BlockFragment fragment : sliced.fragments)
                        assertThat(fragment.key).isEqualTo(key);
                    for (ConditionalBlock cb : original.conditionalBlocks)
                    {
                        boolean writesKey = false;
                        for (int id : cb.fragmentIds)
                            writesKey |= original.fragments[id].key.equals(key);
                        boolean retained = false;
                        for (ConditionalBlock scb : sliced.conditionalBlocks)
                            retained |= scb.id == cb.id;
                        // no-op branches are retained by any slice that retains any of the block's fragments, so that
                        // merging slices covering all keys reconstructs them (see AccordEmptyBranchRecoveryTest)
                        boolean retainsNoOps = sliced.fragments.length > 0 || original.fragments.length == 0;
                        assertThat(retained).isEqualTo(writesKey || (cb.fragmentIds.length == 0 && retainsNoOps));
                    }
                }
                perKey.add(single);
            }

            // a random multi-key slice must produce the same updates for each key it contains
            List<Key> shuffled = new ArrayList<>();
            keys.forEach(shuffled::add);
            Collections.shuffle(shuffled, rs.asJdkRandom());
            Keys subset = Keys.of(shuffled.subList(0, rs.nextInt(1, shuffled.size() + 1)));
            TxnUpdate multi = update.getTxnUpdate(k -> k.overlapping(subset));
            for (Key key : subset)
            {
                List<PartitionUpdate> multiUpdates = new ArrayList<>();
                List<PartitionUpdate> updateUpdates = new ArrayList<>();
                multi.completeUpdatesForKey(multiUpdates, matched, key, input.tables);
                update.completeUpdatesForKey(updateUpdates, matched, key, input.tables);
                assertThat(multiUpdates).hasSize(updateUpdates.size());
            }
            for (Block block : multi.blocks)
                assertBlockInvariants(block);

            // merging the per-key slices (in any order) reconstructs the original, as recovery requires
            Collections.shuffle(perKey, rs.asJdkRandom());
            TxnUpdate accum = perKey.get(0);
            for (int i = 1 ; i < perKey.size() ; ++i)
                accum = accum.merge(perKey.get(i));
            assertThat(accum).isEqualTo(update);
        });
    }

    private static int count(@Nullable List<Fragment> fragments, Key key)
    {
        int count = 0;
        if (fragments != null)
            for (Fragment fragment : fragments)
                if (fragment.key.equals(key))
                    ++count;
        return count;
    }

    private static List<ByteBuffer> serialized(List<Fragment> fragments, TableMetadatas tables)
    {
        List<ByteBuffer> result = new ArrayList<>();
        for (Fragment fragment : fragments)
            result.add(Fragment.FragmentSerializer.serialize(fragment, tables, Version.LATEST));
        Collections.sort(result);
        return result;
    }

    private static List<ByteBuffer> fragmentBytes(Block block, ConditionalBlock cb)
    {
        List<ByteBuffer> result = new ArrayList<>();
        for (int id : cb.fragmentIds)
            result.add(block.fragments[id].bytes);
        Collections.sort(result);
        return result;
    }

    private void assertBlockInvariants(Block block)
    {
        assertThat(ensureBlockFragmentsAreSortedByKey(block)).isTrue();
        assertThat(ensureFragmentIdsAreOrdered(block)).isTrue();
        assertThat(ensureInjectivityOfFragmentIdsToFragments(block)).isTrue();
        // ids must ascend with keys, as merge and deserialize walk fragments by id
        for (int i = 1 ; i < block.fragments.length ; ++i)
            assertThat(block.fragments[i - 1].id).isLessThan(block.fragments[i].id);
    }

    @Test
    public void slice()
    {
        qt().check(rs -> {
            List<TableMetadata> tables = tablesGen.next(rs);
            TableMetadatas metadatas = TableMetadatas.of(tables);
            List<Fragment> fragments = Gens.lists(fragment(tables)).ofSizeBetween(1, 10).next(rs);
            TxnUpdate update = new TxnUpdate(metadatas, fragments, TxnCondition.none(), null);

            // ask for ranges outside the update; should be empty
            for (var block : update.slice(Ranges.single(TokenRange.create(TableId.UNDEFINED, T0, T42))).blocks)
            {
                assertThat(block.fragments).isEmpty();
                for (var cb : block.conditionalBlocks)
                    assertThat(cb.fragmentIds).isEmpty();
            }

            // slice the same key should return the same block
            TxnUpdate noUpdate = update.getTxnUpdate(k -> k.overlapping(update.keys()));
            for (int i = 0; i < update.blocks.size(); i++)
                assertThat(noUpdate.blocks.get(i)).isSameAs(update.blocks.get(i));

            // slicing a single key yields a single key
            if (update.keys().size() == 1) return;
            int keyIndex = rs.nextInt(0, update.keys().size());
            Key key = update.keys().get(keyIndex);
            Keys singleKey = Keys.of(key);
            TxnUpdate singleKeyUpdate = update.getTxnUpdate(k -> k.overlapping(singleKey));
            for (int i = 0; i < update.blocks.size(); i++)
            {
                var block = singleKeyUpdate.blocks.get(i);
                assertThat(block.fragments).hasSize((int)fragments.stream().filter(f -> f.key.equals(key)).count());
                for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
                {
                    for (int fragmentId : conditionalBlock.fragmentIds)
                    {
                        int fragmentIndex = SortedArrays.binarySearch(block.fragments, 0, block.fragments.length, fragmentId, (id, bf) -> Integer.compare(id, bf.id), FAST);
                        assertThat(fragmentIndex >= 0).isTrue();
                        assertThat(block.fragments[fragmentIndex].key).isEqualTo(key);
                    }
                }
            }
        });
    }

    @Test
    public void merge()
    {
        qt().check(rs -> {
            List<TableMetadata> tables = tablesGen.next(rs);
            TableMetadatas metadatas = TableMetadatas.of(tables);
            List<Fragment> fragments = Gens.lists(fragment(tables)).ofSizeBetween(1, 10).next(rs);
            TxnUpdate update = new TxnUpdate(metadatas, fragments, TxnCondition.none(), null);
            TxnUpdate emptyUpdate = update.slice(Ranges.single(TokenRange.create(TableId.UNDEFINED, T0, T42)));
            List<TxnUpdate> perKeyUpdate = new ArrayList<>(update.keys().size());
            for (int i = 0; i < update.keys().size(); i++)
            {
                int finalI = i;
                perKeyUpdate.add(update.getTxnUpdate(k -> k.overlapping(Keys.of(update.keys().get(finalI)))));
            }

            assertThat(update.merge(update)).isEqualTo(update); // merge with self produces self
            assertThat(emptyUpdate.merge(emptyUpdate)).isEqualTo(emptyUpdate); // merge

            // empty with full is commutative
            assertThat(update.merge(emptyUpdate)).isEqualTo(update);
            assertThat(emptyUpdate.merge(update)).isEqualTo(update);

            // merge per key is commutative
            TxnUpdate accum = emptyUpdate;
            for (TxnUpdate other : perKeyUpdate)
                accum = accum.merge(other);
            assertThat(accum).isEqualTo(update);

            accum = emptyUpdate;
            Collections.reverse(perKeyUpdate);
            for (TxnUpdate other : perKeyUpdate)
                accum = accum.merge(other);
            assertThat(accum).isEqualTo(update);
        });
    }

    @Test
    public void select()
    {
        qt().check(rs -> {
            Gen<Block> blockGen = block();
            Block block = blockGen.next(rs);
            List<PartitionKey> allKeys = new ArrayList<>();
            for (TxnUpdate.BlockFragment fragment : block.fragments)
                allKeys.add(fragment.key);

            if (!allKeys.isEmpty())
            {
                // Get a random subset of the keys
                Collections.shuffle(allKeys, rs.asJdkRandom());
                NavigableSet<Key> subListKey = new TreeSet<>(allKeys.subList(0, rs.nextInt(0, allKeys.size())));

                Block selectedBlock = block.select(new Keys(subListKey));
                assertThat(ensureFragmentIdsAreOrdered(selectedBlock)).isTrue();
                assertThat(ensureConditionalBlockPreserveOrder(selectedBlock, block)).isTrue();
                assertThat(ensureBlockFragmentsAreSortedByKey(selectedBlock)).isTrue();
                assertThat(ensureInjectivityOfFragmentIdsToFragments(selectedBlock)).isTrue();
            }
        });
    }

    @Test
    public void skip()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().check(rs -> {
            List<TableMetadata> tables = tablesGen.next(rs);
            TableMetadatas metadatas = TableMetadatas.of(tables);
            List<Fragment> fragments = Gens.lists(fragment(tables)).ofSizeBetween(1, 100).next(rs);
            TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(metadatas, Keys.of(fragments, f -> f.key));
            TxnUpdate update = new TxnUpdate(metadatas, fragments, TxnCondition.none(), null);
            output.clear();
            TxnUpdate.serializer.serialize(update, tablesAndKeys, output, Version.LATEST);
            ByteBuffer buffer = output.unsafeGetBufferAndFlip();
            TxnUpdate.serializer.skip(tablesAndKeys, new DataInputBuffer(buffer, false), Version.LATEST);
            assertThat(buffer.remaining()).isEqualTo(0);
        });
    }

    /**
     * The serializer prior to the introduction of general purpose flags, which only knew FLAG_PRESERVE_TIMESTAMPS
     */
    private static void legacySerialize(TxnUpdate update, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out) throws IOException
    {
        out.writeByte(update.preserveTimestamps() ? 0x1 : 0);
        tablesAndKeys.serializeKeys(update.keys, out);
        NullableSerializer.serializeNullable(update.cassandraCommitCL(), out, AccordSerializers.consistencyLevelSerializer);
        CollectionSerializers.serializeList(update.blocks, tablesAndKeys, out, Block.serializer);
    }

    /**
     * The deserializer prior to the introduction of general purpose flags; returns {preserveTimestamps, cassandraCommitCL}
     */
    private static Object[] legacyDeserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in) throws IOException
    {
        int flags = in.readByte();
        boolean preserveTimestamps = (0x1 & flags) == 1;
        tablesAndKeys.deserializeKeys(in);
        ConsistencyLevel consistencyLevel = NullableSerializer.deserializeNullable(in, AccordSerializers.consistencyLevelSerializer);
        CollectionSerializers.deserializeList(tablesAndKeys, in, Block.serializer);
        return new Object[] { preserveTimestamps, consistencyLevel };
    }

    private static final ConsistencyLevel[] COMMIT_CLS = IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.toArray(new ConsistencyLevel[0]);

    @Test
    public void flagsSerde()
    {
        @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();
        qt().check(rs -> {
            List<TableMetadata> tables = tablesGen.next(rs);
            TableMetadatas metadatas = TableMetadatas.of(tables);
            List<Fragment> fragments = Gens.lists(fragment(tables)).ofSizeBetween(1, 10).next(rs);
            TableMetadatasAndKeys tablesAndKeys = new TableMetadatasAndKeys(metadatas, Keys.of(fragments, f -> f.key));
            ConsistencyLevel commitCL = rs.nextBoolean() ? null : COMMIT_CLS[rs.nextInt(COMMIT_CLS.length)];
            PreserveTimestamp preserveTimestamp = PreserveTimestamp.values()[rs.nextInt(PreserveTimestamp.values().length)];
            boolean requestReplicaExecution = rs.nextBoolean();
            boolean fromFragments = rs.nextBoolean();
            TxnUpdate update = fromFragments ? new TxnUpdate(metadatas, fragments, TxnCondition.none(), commitCL, preserveTimestamp, requestReplicaExecution)
                                             : TxnUpdate.create(metadatas, Keys.of(fragments, f -> f.key), new TxnUpdate(metadatas, fragments, TxnCondition.none(), null).blocks, commitCL, preserveTimestamp, requestReplicaExecution);
            // replica execution cannot honour a commit CL, and a TxnUpdate constructed from fragments must also perform no read
            boolean replicaExecutable = requestReplicaExecution && (commitCL == null || commitCL == ConsistencyLevel.ANY) && (!fromFragments || fragments.stream().allMatch(Fragment::isComplete));
            assertThat(update.preserveTimestamps()).isEqualTo(preserveTimestamp.preserve);
            assertThat(update.permitsReplicaExecution()).isEqualTo(replicaExecutable);
            assertThat(update.cassandraCommitCL()).isEqualTo(commitCL);

            output.clear();
            TxnUpdate.serializer.serialize(update, tablesAndKeys, output, Version.LATEST);
            assertThat((long) output.getLength()).isEqualTo(TxnUpdate.serializer.serializedSize(update, tablesAndKeys, Version.LATEST));
            byte[] bytes = output.toByteArray();

            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                TxnUpdate roundTrip = TxnUpdate.serializer.deserialize(tablesAndKeys, in, Version.LATEST);
                assertThat(in.available()).isZero();
                assertThat(roundTrip).isEqualTo(update);
                assertThat(roundTrip.preserveTimestamps()).isEqualTo(preserveTimestamp.preserve);
                assertThat(roundTrip.permitsReplicaExecution()).isEqualTo(replicaExecutable);
                assertThat(roundTrip.cassandraCommitCL()).isEqualTo(commitCL);
                assertThat(roundTrip.keys()).isEqualTo(update.keys());
            }
            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                TxnUpdate.serializer.skip(tablesAndKeys, in, Version.LATEST);
                assertThat(in.available()).isZero();
            }

            // the prior version's encoding is unchanged (except for flags it ignores), and remains readable in both directions
            output.clear();
            legacySerialize(update, tablesAndKeys, output);
            byte[] legacyBytes = output.toByteArray();
            if (replicaExecutable) legacyBytes[0] |= TxnUpdate.FLAG_PERMIT_REPLICA_EXECUTION;
            assertThat(bytes).isEqualTo(legacyBytes);
            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                Object[] legacy = legacyDeserialize(tablesAndKeys, in);
                assertThat(in.available()).isZero();
                assertThat(legacy[0]).isEqualTo(update.preserveTimestamps());
                assertThat(legacy[1]).isEqualTo(commitCL);
            }

            // flags in the first byte that we do not know (e.g. introduced by a later version) are ignored
            byte[] unknownFlags = bytes.clone();
            unknownFlags[0] |= (byte) (rs.nextInt(0x40) << 2);
            try (DataInputBuffer in = new DataInputBuffer(unknownFlags))
            {
                TxnUpdate withUnknownFlags = TxnUpdate.serializer.deserialize(tablesAndKeys, in, Version.LATEST);
                assertThat(in.available()).isZero();
                assertThat(withUnknownFlags.preserveTimestamps()).isEqualTo(preserveTimestamp.preserve);
                assertThat(withUnknownFlags.permitsReplicaExecution()).isEqualTo(replicaExecutable);
                assertThat(withUnknownFlags.cassandraCommitCL()).isEqualTo(commitCL);
            }
        });
    }

    @Test
    public void flags()
    {
        assertThat(TxnUpdate.flags(null, PreserveTimestamp.no)).isZero();
        assertThat(TxnUpdate.flags(null, PreserveTimestamp.yes)).isEqualTo(TxnUpdate.FLAG_PRESERVE_TIMESTAMPS);
        assertThat(TxnUpdate.flags(null, PreserveTimestamp.mixedTimeSource)).isEqualTo(TxnUpdate.FLAG_PRESERVE_TIMESTAMPS);
        for (ConsistencyLevel commitCL : COMMIT_CLS)
        {
            // the commit CL is encoded by its protocol code, not its ordinal
            assertThat(TxnUpdate.flags(commitCL, PreserveTimestamp.no) >>> 16).isEqualTo(commitCL.code);
            TxnUpdate update = new TxnUpdate(TableMetadatas.none(), new ArrayList<>(), TxnCondition.none(), commitCL, PreserveTimestamp.yes);
            assertThat(update.cassandraCommitCL()).isEqualTo(commitCL);
            assertThat(update.preserveTimestamps()).isTrue();
        }
    }

    @Test
    public void replicaExecutable()
    {
        qt().check(rs -> {
            List<TableMetadata> tables = tablesGen.next(rs);
            TableMetadatas metadatas = TableMetadatas.of(tables);
            List<Fragment> fragments = Gens.lists(fragment(tables)).ofSizeBetween(1, 10).next(rs);
            Keys keys = Keys.of(fragments, f -> f.key);
            assertThat(fragments).allMatch(Fragment::isComplete);
            TxnUpdate update = new TxnUpdate(metadatas, fragments, TxnCondition.none(), null, PreserveTimestamp.no, true);
            assertThat(update.permitsReplicaExecution()).isTrue();
            assertThat(TxnUpdate.permitsReplicaExecution(new Txn.InMemory(keys, TxnRead.empty(Domain.Key), TxnQuery.NONE, update))).isTrue();
            // slicing preserves the flag (the predicate is evaluated against both Txn and PartialTxn)
            assertThat(update.slice(Ranges.single(TokenRange.create(TableId.UNDEFINED, T0, T42))).permitsReplicaExecution()).isTrue();
            assertThat(update.intersecting(keys.toParticipants()).permitsReplicaExecution()).isTrue();
            // we must not produce a result, nor require a commit CL
            assertThat(TxnUpdate.permitsReplicaExecution(new Txn.InMemory(keys, TxnRead.empty(Domain.Key), TxnQuery.ALL, update))).isFalse();
            TxnUpdate withCommitCL = new TxnUpdate(metadatas, fragments, TxnCondition.none(), ConsistencyLevel.QUORUM, PreserveTimestamp.no, true);
            assertThat(withCommitCL.permitsReplicaExecution()).isFalse();
            assertThat(TxnUpdate.permitsReplicaExecution(new Txn.InMemory(keys, TxnRead.empty(Domain.Key), TxnQuery.NONE, withCommitCL))).isFalse();
            TxnUpdate createdWithCommitCL = TxnUpdate.create(metadatas, keys, update.blocks, ConsistencyLevel.QUORUM, PreserveTimestamp.no, true);
            assertThat(createdWithCommitCL.permitsReplicaExecution()).isFalse();
            // a transaction that returns no result may be executed by replicas (reading locally, if ReplicaExecution.ALL)
            TxnUpdate created = TxnUpdate.create(metadatas, keys, update.blocks, null, PreserveTimestamp.no, true);
            assertThat(created.permitsReplicaExecution()).isTrue();
            assertThat(TxnUpdate.permitsReplicaExecution(new Txn.InMemory(keys, TxnRead.empty(Domain.Key), TxnQuery.NONE, created))).isTrue();
            // and must be explicitly flagged
            TxnUpdate notFlagged = new TxnUpdate(metadatas, fragments, TxnCondition.none(), null);
            assertThat(notFlagged.permitsReplicaExecution()).isFalse();
            assertThat(TxnUpdate.permitsReplicaExecution(new Txn.InMemory(keys, TxnRead.empty(Domain.Key), TxnQuery.NONE, notFlagged))).isFalse();
        });
    }

    @Test
    public void unsupportedCommitCL()
    {
        TableMetadatas metadatas = TableMetadatas.none();
        for (ConsistencyLevel commitCL : ConsistencyLevel.values())
        {
            if (IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.contains(commitCL))
                continue;

            Assertions.assertThatThrownBy(() -> new TxnUpdate(metadatas, new ArrayList<>(), TxnCondition.none(), commitCL))
                      .isInstanceOf(IllegalArgumentException.class);
            Assertions.assertThatThrownBy(() -> TxnUpdate.create(metadatas, Keys.EMPTY, Collections.emptyList(), commitCL))
                      .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private boolean ensureThatConditionalBlockIndexesAreDisjointAcrossBlocks(List<Block> blocks)
    {
        Set<Integer> seenConditionalBlockIndexes = new HashSet<>();
        for (Block block : blocks)
        {
            for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
                if (!seenConditionalBlockIndexes.add(conditionalBlock.id))
                    return false;
        }

        return true;
    }

    private boolean ensureFragmentIdsAreOrdered(Block block)
    {
        for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
        {
            for (int i = 1; i < conditionalBlock.fragmentIds.length; i++)
                if (conditionalBlock.fragmentIds[i-1] > conditionalBlock.fragmentIds[i])
                    return false;
        }

        return true;
    }

    private boolean ensureConditionalBlockPreserveOrder(Block selectedBlock, Block originalBlock)
    {
        ConditionalBlock[] selectedConditionalBlocks = selectedBlock.conditionalBlocks;
        ConditionalBlock[] originalConditionalBlocks = originalBlock.conditionalBlocks;

        int selectedIndex = 0;
        int originalIndex = 0;
        while (selectedIndex < selectedConditionalBlocks.length && originalIndex < originalConditionalBlocks.length)
        {
            ConditionalBlock selected = selectedConditionalBlocks[selectedIndex];
            ConditionalBlock original = originalConditionalBlocks[originalIndex];
            if (selected.id == original.id)
                selectedIndex++;
            originalIndex++;
        }

        return selectedIndex == selectedConditionalBlocks.length;
    }

    private boolean ensureBlockFragmentsAreSortedByKey(Block block)
    {
        for (int i = 1; i < block.fragments.length; i++)
            if (block.fragments[i-1].key.compareTo(block.fragments[i].key) > 0)
                return false;
        return true;
    }

    private boolean ensureInjectivityOfFragmentIdsToFragments(Block block)
    {
        Set<Integer> seenFragmentIds = new HashSet<>();
        for (ConditionalBlock conditionalBlock : block.conditionalBlocks)
        {
            for (int fragmentId : conditionalBlock.fragmentIds)
                if (!seenFragmentIds.add(fragmentId))
                    return false;
        }

        for (TxnUpdate.BlockFragment fragment : block.fragments)
            if (!seenFragmentIds.remove(fragment.id))
                return false;

        return seenFragmentIds.isEmpty();
    }

    // fragments over a fixed pool of keys, so that keys repeat across (and within) branches
    private static List<Fragment> statements(RandomSource rs, List<TableMetadata> tables, List<PartitionKey> keyPool, int[] nextStatementIndex, int statementCount)
    {
        return statements(rs, tables, keyPool, nextStatementIndex, statementCount, 0);
    }

    // generate the fragments for statementCount statements, each producing between minFragments and 3 fragments
    // sharing the statement's index, for distinct keys (as a statement produces at most one fragment per partition)
    private static List<Fragment> statements(RandomSource rs, List<TableMetadata> tables, List<PartitionKey> keyPool, int[] nextStatementIndex, int statementCount, int minFragments)
    {
        List<Fragment> fragments = new ArrayList<>();
        for (int s = 0 ; s < statementCount ; ++s)
        {
            int index = nextStatementIndex[0]++;
            List<PartitionKey> keys = new ArrayList<>(keyPool);
            Collections.shuffle(keys, rs.asJdkRandom());
            for (int f = 0, mf = rs.nextInt(minFragments, Math.min(3, keys.size()) + 1) ; f < mf ; ++f)
            {
                PartitionKey key = keys.get(f);
                TableMetadata metadata = tables.stream().filter(t -> t.id.equals(key.table())).findFirst().get();
                PartitionUpdate update = PartitionUpdate.emptyUpdate(metadata, key.partitionKey());
                fragments.add(new Fragment(key, index, update, TxnReferenceOperations.empty(), rs.nextLong(1, Long.MAX_VALUE)));
            }
        }
        return fragments;
    }

    private static Gen<Fragment> fragment(List<TableMetadata> tables)
    {
        return rs -> {
            var metadata = rs.pick(tables);
            var pk = bytesGen.next(rs);
            DecoratedKey key = metadata.partitioner.decorateKey(pk);

            PartitionUpdate update = PartitionUpdate.emptyUpdate(metadata, key);

            return new Fragment(new PartitionKey(metadata.id, key), rs.nextInt(0, Integer.MAX_VALUE), update, TxnReferenceOperations.empty(), rs.nextLong(1, Long.MAX_VALUE));
        };
    }

    private static Gen<ConditionalBlock> conditionalBlock()
    {
        Gen<SerializedTxnCondition> serializedTxnConditionGen = serializedTxnCondition();
        Gen<int[]> fragmentsGen = Gens.arrays(Gens.ints().between(0, Integer.MAX_VALUE)).ofSizeBetween(0, 10).map(vs -> { Arrays.sort(vs); return vs; });
        return rs -> {
            int id = rs.nextInt(-1, Integer.MAX_VALUE) + 1;
            SerializedTxnCondition condition = serializedTxnConditionGen.next(rs);
            int[] fragments = fragmentsGen.next(rs);
            return new ConditionalBlock(id, condition, fragments);
        };
    }

    private static Gen<SerializedTxnCondition> serializedTxnCondition()
    {
        Gen<ByteBuffer> bytesGen = TxnUpdateTest.bytesGen.filter(ByteBuffer::hasRemaining);
        return rs -> new SerializedTxnCondition(bytesGen.next(rs));
    }

    private static Gen<Block> block()
    {
        // can't have a empty block
        Gen<ByteBuffer[]> bytesGen = Gens.arrays(ByteBuffer.class, TxnUpdateTest.bytesGen.filter(ByteBuffer::hasRemaining))
                                                          .ofSizeBetween(0, 10);
        Gen<Key> keyGen = (Gen<Key>) (Gen<?>) AccordGenerators.keys(Murmur3Partitioner.instance);
        Gen<SerializedTxnCondition> serializedTxnConditionGen = serializedTxnCondition();
        return rs -> {
            ByteBuffer[] bbs = bytesGen.next(rs);
            Key[] keys = IntStream.range(0, bbs.length).mapToObj(i -> keyGen.next(rs)).toArray(Key[]::new);
            int[] ids = IntStream.range(0, bbs.length).toArray();
            Arrays.sort(ids);
            Arrays.sort(keys);
            TxnUpdate.BlockFragment[] fragments = new TxnUpdate.BlockFragment[bbs.length];
            for (int i = 0 ; i < fragments.length ; ++i)
                fragments[i] = new TxnUpdate.BlockFragment(ids[i], (PartitionKey) keys[i], bbs[i]);

            List<ConditionalBlock> conditionalBlocks = new ArrayList<>();
            List<Integer> fragmentIds = Arrays.stream(ids).boxed().collect(Collectors.toList());
            List<Integer> sublist = new ArrayList<>();

            boolean createNewSublist = false;
            int conditionalBlockIdx = 0;
            while (!fragmentIds.isEmpty())
            {
                if (createNewSublist)
                {
                    conditionalBlocks.add(new ConditionalBlock(conditionalBlockIdx++, serializedTxnConditionGen.next(rs), sublist.stream().sorted().mapToInt(i->i).toArray()));
                    sublist = new ArrayList<>();
                }

                int index = rs.nextInt(0, fragmentIds.size());
                sublist.add(fragmentIds.get(index));
                fragmentIds.remove(index);
                createNewSublist = rs.nextBoolean();
            }

            if (!sublist.isEmpty())
                conditionalBlocks.add(new ConditionalBlock(conditionalBlockIdx, serializedTxnConditionGen.next(rs), sublist.stream().sorted().mapToInt(i->i).toArray()));

            return new Block(fragments, conditionalBlocks.toArray(new ConditionalBlock[0]));
        };
    }
}