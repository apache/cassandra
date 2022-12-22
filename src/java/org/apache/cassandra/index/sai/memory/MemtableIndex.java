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
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.ShardBoundaries;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;
import org.apache.cassandra.index.sai.utils.RangeConcatIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Reducer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static java.util.function.Function.identity;

public class MemtableIndex
{
    private final ShardBoundaries boundaries;
    private final MemoryIndex[] rangeIndexes;
    private final AbstractType<?> validator;
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder estimatedOnHeapMemoryUsed = new LongAdder();
    private final LongAdder estimatedOffHeapMemoryUsed = new LongAdder();

    public MemtableIndex(IndexContext indexContext)
    {
        this.boundaries = indexContext.owner().localRangeSplits(TrieMemtable.SHARD_COUNT);
        this.rangeIndexes = new MemoryIndex[boundaries.shardCount()];
        this.validator = indexContext.getValidator();
        for (int shard = 0; shard < boundaries.shardCount(); shard++)
        {
            this.rangeIndexes[shard] = new TrieMemoryIndex(indexContext);
        }
    }

    @VisibleForTesting
    public int shardCount()
    {
        return rangeIndexes.length;
    }

    public long writeCount()
    {
        return writeCount.sum();
    }

    public long estimatedOnHeapMemoryUsed()
    {
        return estimatedOnHeapMemoryUsed.sum();
    }

    public long estimatedOffHeapMemoryUsed()
    {
        return estimatedOffHeapMemoryUsed.sum();
    }

    public boolean isEmpty()
    {
        return getMinTerm() == null;
    }

    // Returns the minimum indexed term in the combined memory indexes.
    // This can be null if the indexed memtable was empty. Users of the
    // {@code MemtableIndex} requiring a non-null minimum term should
    // use the {@link MemtableIndex#isEmpty} method.
    // Note: Individual index shards can return null here if the index
    // didn't receive any terms within the token range of the shard
    @Nullable
    public ByteBuffer getMinTerm()
    {
        return Arrays.stream(rangeIndexes)
                     .map(MemoryIndex::getMinTerm)
                     .filter(Objects::nonNull)
                     .min(Comparator.comparing(identity(), validator))
                     .orElse(null);
    }

    // Returns the maximum indexed term in the combined memory indexes.
    // This can be null if the indexed memtable was empty. Users of the
    // {@code MemtableIndex} requiring a non-null maximum term should
    // use the {@link MemtableIndex#isEmpty} method.
    // Note: Individual index shards can return null here if the index
    // didn't receive any terms within the token range of the shard
    @Nullable
    public ByteBuffer getMaxTerm()
    {
        return Arrays.stream(rangeIndexes)
                     .map(MemoryIndex::getMaxTerm)
                     .filter(Objects::nonNull)
                     .max(Comparator.comparing(identity(), validator))
                     .orElse(null);
    }

    public void index(DecoratedKey key, Clustering clustering, ByteBuffer value, Memtable memtable, OpOrder.Group opGroup)
    {
        if (value == null || value.remaining() == 0)
            return;

        rangeIndexes[boundaries.getShardForKey(key)].add(key,
                                                         clustering,
                                                         value,
                                                         allocatedBytes -> {
                                                             memtable.markExtraOnHeapUsed(allocatedBytes, opGroup);
                                                             estimatedOnHeapMemoryUsed.add(allocatedBytes);
                                                         },
                                                         allocatedBytes -> {
                                                             memtable.markExtraOffHeapUsed(allocatedBytes, opGroup);
                                                             estimatedOffHeapMemoryUsed.add(allocatedBytes);
                                                         });
        writeCount.increment();
    }

    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        RangeConcatIterator.Builder builder = RangeConcatIterator.builder();

        for (int shard : boundaries.getShardsForRange(keyRange))
        {
            assert rangeIndexes[shard] != null;
            builder.add(rangeIndexes[shard].search(expression, keyRange));
        }

        return builder.build();
    }

    /**
     * NOTE: returned data may contain partition key not within the provided min and max which are only used to find
     * corresponding subranges. We don't do filtering here to avoid unnecessary token comparison. In case of JBOD,
     * min/max should align exactly at token boundaries. In case of tiered-storage, keys within min/max may not
     * belong to the given sstable.
     *
     * @param min minimum partition key used to find min subrange
     * @param max maximum partition key used to find max subrange
     *
     * @return iterator of indexed term to primary keys mapping in sorted by indexed term and primary key.
     */
    public Iterator<Pair<ByteComparable, Iterator<PrimaryKey>>> iterator(DecoratedKey min, DecoratedKey max)
    {
        int minSubrange = min == null ? 0 : boundaries.getShardForKey(min);
        int maxSubrange = max == null ? rangeIndexes.length - 1 : boundaries.getShardForKey(max);

        List<Iterator<Pair<ByteComparable, PrimaryKeys>>> rangeIterators = new ArrayList<>(maxSubrange - minSubrange + 1);
        for (int i = minSubrange; i <= maxSubrange; i++)
            rangeIterators.add(rangeIndexes[i].iterator());

        return MergeIterator.get(rangeIterators, (o1, o2) -> ByteComparable.compare(o1.left, o2.left, ByteComparable.Version.OSS41),
                                 new PrimaryKeysMergeReducer(rangeIterators.size()));
    }

    // The PrimaryKeysMergeReducer receives the range iterators from each of the range indexes selected based on the
    // min and max keys passed to the iterator method. It doesn't strictly do any reduction because the terms in each
    // range index are unique. It will receive at most one range index entry per selected range index before getReduced
    // is called.
    private static class PrimaryKeysMergeReducer extends Reducer<Pair<ByteComparable, PrimaryKeys>, Pair<ByteComparable, Iterator<PrimaryKey>>>
    {
        private final Pair<ByteComparable, PrimaryKeys>[] rangeIndexEntriesToMerge;
        private final Comparator<PrimaryKey> comparator;

        private ByteComparable term;

        @SuppressWarnings("unchecked")
        // The size represents the number of range indexes that have been selected for the merger
        PrimaryKeysMergeReducer(int size)
        {
            this.rangeIndexEntriesToMerge = new Pair[size];
            this.comparator = PrimaryKey::compareTo;
        }

        @Override
        // Receive the term entry for a range index. This should only be called once for each
        // range index before reduction.
        public void reduce(int index, Pair<ByteComparable, PrimaryKeys> termPair)
        {
            Preconditions.checkArgument(rangeIndexEntriesToMerge[index] == null, "Terms should be unique in the memory index");

            rangeIndexEntriesToMerge[index] = termPair;
            if (termPair != null && term == null)
                term = termPair.left;
        }

        @Override
        // Return a merger of the term keys for the term.
        public Pair<ByteComparable, Iterator<PrimaryKey>> getReduced()
        {
            Preconditions.checkArgument(term != null, "The term must exist in the memory index");

            List<Iterator<PrimaryKey>> keyIterators = new ArrayList<>(rangeIndexEntriesToMerge.length);
            for (Pair<ByteComparable, PrimaryKeys> p : rangeIndexEntriesToMerge)
                if (p != null && p.right != null && !p.right.isEmpty())
                    keyIterators.add(p.right.iterator());

            Iterator<PrimaryKey> primaryKeys = MergeIterator.get(keyIterators, comparator, Reducer.getIdentity());
            return Pair.create(term, primaryKeys);
        }

        @Override
        public void onKeyChange()
        {
            Arrays.fill(rangeIndexEntriesToMerge, null);
            term = null;
        }
    }
}
