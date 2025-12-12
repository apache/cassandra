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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Test;

import accord.api.Key;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.SortedArrays;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.io.Serializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.TxnCondition.SerializedTxnCondition;
import org.apache.cassandra.service.accord.txn.TxnUpdate.Block;
import org.apache.cassandra.service.accord.txn.TxnUpdate.ConditionalBlock;
import org.apache.cassandra.service.accord.txn.TxnWrite.Fragment;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.Generators;

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

    @Test
    public void slice()
    {
        qt().check(rs -> {
            List<TableMetadata> tables = tablesGen.next(rs);
            TableMetadatas metadatas = TableMetadatas.of(tables);
            List<Fragment> fragments = Gens.lists(fragment(tables)).ofSizeBetween(1, 10).next(rs);
            TxnUpdate update = new TxnUpdate(metadatas, fragments, TxnCondition.none(), null, PreserveTimestamp.no);

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
            TxnUpdate update = new TxnUpdate(metadatas, fragments, TxnCondition.none(), null, PreserveTimestamp.no);
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
        var conditionGen = Gens.arrays(ConditionalBlock.class, conditionalBlock()).ofSizeBetween(1, 10);
        Gen<Key> keyGen = (Gen<Key>) (Gen<?>) AccordGenerators.keys(Murmur3Partitioner.instance);
        return rs -> {
            ByteBuffer[] bbs = bytesGen.next(rs);
            Key[] keys = IntStream.range(0, bbs.length).mapToObj(i -> keyGen.next(rs)).toArray(Key[]::new);
            int[] ids = IntStream.range(0, bbs.length).toArray();
            Arrays.sort(ids);
            Arrays.sort(keys);
            TxnUpdate.BlockFragment[] fragments = new TxnUpdate.BlockFragment[bbs.length];
            for (int i = 0 ; i < fragments.length ; ++i)
                fragments[i] = new TxnUpdate.BlockFragment(ids[i], (PartitionKey) keys[i], bbs[i]);
            return new Block(fragments, conditionGen.next(rs));
        };
    }
}