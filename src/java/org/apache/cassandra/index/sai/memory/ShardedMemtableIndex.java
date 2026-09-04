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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeConcatIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;

public class ShardedMemtableIndex implements MemtableIndex
{
    private final ShardBoundaries boundaries;
    private final MemoryIndex[] shards;
    private final StorageAttachedIndex index;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedMemoryUsed = new LongAdder();
    private final Memtable memtable;

    private static final int DEFAULT_SHARD_COUNT = MEMTABLE_SHARD_COUNT.getInt(FBUtilities.getAvailableProcessors());
    public static final String SHARDS_OPTION = "shards";

    public ShardedMemtableIndex(StorageAttachedIndex index,
                                Memtable.Owner owner,
                                Integer shardCountOption,
                                Memtable memtable)
    {
        this.index = index;
        int shardCount = (null == shardCountOption) ? DEFAULT_SHARD_COUNT : shardCountOption;
        this.boundaries = owner.localRangeSplits(shardCount);
        this.shards = generateShards(boundaries.shardCount(), index);
        this.memtable = memtable;
    }

    private MemoryIndex[] generateShards(int splits, StorageAttachedIndex index)
    {
        MemoryIndex[] generatedShards = new MemoryIndex[splits];

        for (int shard = 0; shard < boundaries.shardCount(); shard++)
        {
            generatedShards[shard] = new TrieMemoryIndex(index);
        }

        return generatedShards;
    }

    @VisibleForTesting
    public int shardCount()
    {
        return shards.length;
    }

    public long writeCount()
    {
        return writeCount.sum();
    }

    public boolean isEmpty()
    {
        return getMinTerm() == null;
    }

    public Memtable getMemtable()
    {
        return memtable;
    }

    public long estimatedMemoryUsed()
    {
        return estimatedMemoryUsed.sum();
    }

    /**
     * Returns the minimum indexed term in the combined memory indexes.
     * This can be {@code null} if the indexed memtable was empty. Users of the
     * {@code MemtableIndex} requiring a non-null minimum term should
     * use the {@link MemtableIndex#isEmpty} method.
     *
     * <p>
     * <b>Note:</b> Individual index shards can return {@code null} here if the index
     *      didn't receive any terms within the token range of the shard
     * </p>
     *
     * @return the minimum indexed term across all shards, or {@code null} if the index is empty.
     */
    @Nullable
    public ByteBuffer getMinTerm()
    {
        ByteBuffer result = null;
        for (MemoryIndex shard : shards)
            result = index.termType().min(shard.getMinTerm(), result);
        return result;
    }

    /**
     *  Returns the maximum indexed term in the combined memory indexes.
     *  This can be {@code null} if the indexed memtable was empty. Users of the
     *  {@code MemtableIndex} requiring a non-null maximum term should
     *  use the {@link MemtableIndex#isEmpty} method.
     *
     *  <p>
     *  <b>Note:</b> Individual index shards can return {@code null} here if the index
     *      didn't receive any terms within the token range of the shard
     *  </p>
     *
     * @return the maximum indexed term across all shards, or {@code null} if the index is empty
     */
    @Nullable
    public ByteBuffer getMaxTerm()
    {
        ByteBuffer result = null;
        for (MemoryIndex shard : shards)
            result = index.termType().max(shard.getMaxTerm(), result);
        return result;
    }

    public long index(DecoratedKey key, Clustering<?> clustering, ByteBuffer value)
    {
        if (value == null || (value.remaining() == 0 && index.termType().skipsEmptyValue()))
            return 0;

        long ram = shards[boundaries.getShardForKey(key)].add(key, clustering, value);
        writeCount.increment();
        estimatedMemoryUsed.add(ram);
        return ram;
    }

    public KeyRangeIterator search(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        List<Integer> shardsForRange = boundaries.getShardsForRange(keyRange);
        KeyRangeConcatIterator.Builder builder = KeyRangeConcatIterator.builder(shardsForRange.size());

        for (int shard: shardsForRange)
        {
            assert shards[shard] != null;
            builder.add(shards[shard].search(queryContext, expression, keyRange));
        }

        return builder.build();
    }

    @Override
    public Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        int minSubrange = min == null ? 0 : boundaries.getShardForKey(min);
        int maxSubrange = max == null ? shards.length - 1 : boundaries.getShardForKey(max);

        List<Iterator<Pair<ByteComparable, PrimaryKeys>>> rangeIterators = new ArrayList<>(maxSubrange - minSubrange + 1);

        for (int i = minSubrange; i <= maxSubrange; i++)
            rangeIterators.add(shards[i].iterator());

        return MergeIterator.get(rangeIterators,
                                 (o1, o2) -> ByteComparable.compare(o1.left, o2.left,
                                                                    ByteComparable.Version.OSS50),
                                 new PrimaryKeysMergeReducer(rangeIterators.size()));
    }

    /**
     *  The PrimaryKeysMergeReducer receives the range iterators from each of the shards selected based on the
     *  min and max keys passed to the iterator method. It doesn't strictly do any reduction because the terms in each
     *  shard are unique. It will receive at most one shard entry per selected shard before {@link #getReduced}
     *  is called.
     */
    private static class PrimaryKeysMergeReducer extends MergeIterator.Reducer<Pair<ByteComparable, PrimaryKeys>, Pair<ByteComparable, Iterator<PrimaryKey>>>
    {
        private final Pair<ByteComparable, PrimaryKeys>[] shardEntriesToMerge;
        private final Comparator<PrimaryKey> comparator;

        private ByteComparable term;

        @SuppressWarnings("unchecked")
            // The size represents the number of shards that have been selected for the merger
        PrimaryKeysMergeReducer(int size)
        {
            this.shardEntriesToMerge = new Pair[size];
            this.comparator = PrimaryKey::compareTo;
        }

        /**
         * Receive the term entry for a shard. This should only be called once for each
         * shard before reduction.
         *
         * @param idx the index of the shard contributing this entry
         * @param current the term and its associated primary keys from the shard
         */
        @Override
        public void reduce(int idx, Pair<ByteComparable, PrimaryKeys> current)
        {
            Preconditions.checkArgument(shardEntriesToMerge[idx] == null, "Terms should be unique in the memory index");

            shardEntriesToMerge[idx] = current;
            if (current != null && term == null)
                term = current.left;
        }

        @Override
        protected Pair<ByteComparable, Iterator<PrimaryKey>> getReduced()
        {
            Preconditions.checkArgument(term != null, "The term must exist in memory index");

            List<Iterator<PrimaryKey>> keyIterators = new ArrayList<>(shardEntriesToMerge.length);
            for (Pair<ByteComparable, PrimaryKeys> p : shardEntriesToMerge)
                if (p != null && p.right != null && !p.right.isEmpty())
                    keyIterators.add(p.right.iterator());

            Iterator<PrimaryKey> primaryKeys = MergeIterator.get(keyIterators, comparator, new MergeIterator.Reducer.Trivial<>());
            return Pair.create(term, primaryKeys);
        }

        @Override
        protected void onKeyChange()
        {
            Arrays.fill(shardEntriesToMerge, null);
            term = null;
        }
    }
}
