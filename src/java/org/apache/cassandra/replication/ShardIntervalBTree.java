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
package org.apache.cassandra.replication;

import java.util.Collection;
import java.util.Comparator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import accord.utils.AsymmetricComparator;
import accord.utils.btree.BTree;
import accord.utils.btree.IntervalBTree;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.endWithStart;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.keyEndWithStart;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.keyStartWithEnd;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.keyStartWithStart;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.startWithEnd;
import static accord.utils.btree.IntervalBTree.InclusiveEndHelper.startWithStart;

/**
 * Immutable interval tree of {@link Shard}s, backed by an augmented
 * interval BTree (see {@link IntervalBTree}).
 * <p>
 * Shard ranges can overlap, so multiple shards may cover the same token
 * (at different {@link Shard#sinceEpoch epochs}).
 */
final class ShardIntervalBTree
{
    private final Object[] tree;

    ShardIntervalBTree()
    {
        this(IntervalBTree.empty());
    }

    private ShardIntervalBTree(Object[] tree)
    {
        this.tree = tree;
    }

    /*
     * TODO: manipulation method list:
     *
     *
     */

    /**
     * Return a copy of this map with the provided shard added.
     * @throws IllegalStateException if it already exists in the map.
     */
    ShardIntervalBTree with(Shard shard)
    {
        if (shard.range.isTrulyWrapAround())
            throw new IllegalArgumentException("Shard's range truly wraps around: " + shard);

        if (BTree.find(tree, Shard.COMPARATOR, shard) != null)
            throw new IllegalStateException("Shard is already present: " + shard);

        return new ShardIntervalBTree(IntervalBTree.update(tree, IntervalBTree.singleton(shard), BuildComparators.INSTANCE));
    }

    /**
     * Return a copy of this map without the provided shard.
     * @throws IllegalStateException if it is not present in the map.
     */
    ShardIntervalBTree without(Shard shard)
    {
        if (shard.range.isTrulyWrapAround())
            throw new IllegalArgumentException("Shard's range truly wraps around: " + shard);

        if (BTree.find(tree, Shard.COMPARATOR, shard) == null)
            throw new IllegalStateException("Shard is not present: " + shard);

        return new ShardIntervalBTree(IntervalBTree.subtract(tree, IntervalBTree.singleton(shard), BuildComparators.INSTANCE));
    }

    /**
     * Builds {@code ShardIntervalBTree} from an ordered collection of Shards.
     *
     * @param orderedShards An ordered collection of shards (ordered by {@code Shard.COMPARATOR}).
     */
    static ShardIntervalBTree fromSorted(Collection<Shard> orderedShards)
    {
       return new ShardIntervalBTree(IntervalBTree.build(orderedShards, BuildComparators.INSTANCE));
    }

    /*
     * TODO: lookup method list:
     * x find the matching active shard by token (for mutation id generation)
     * x forEach() over shards that contain the token (for mutation summaries)
     * x forEach() over shards that overlap with the provided token range
     * - get (by range and sinceEpoch, exact)
     * - forEach() over shards that match the partition position bounds
     */

    /**
     * Return the latest {@link Shard} (with the highest {@link Shard#sinceEpoch})
     * responsible for the provided token, or {@code null} if no shard covers it.
     */
    @Nullable
    Shard latestShardCovering(Token token)
    {
        return IntervalBTree.accumulate(
            tree, PointQueryComparators.INSTANCE, token,
            (ignore1, ignore2, shard, acc) -> (acc == null || shard.sinceEpoch > acc.sinceEpoch) ? shard : acc,
            null, null, null);
    }

    /**
     * @return the Shard matching the provided {@code range} and {@code sinceEpoch} exactly (or null if none do).
     */
    @Nullable
    Shard get(Range<Token> range, long sinceEpoch)
    {
        return IntervalBTree.accumulate(
            tree, RangeQueryComparators.INSTANCE, range,
            (epoch, p2, shard, acc) -> shard.range.equals(range) && shard.sinceEpoch == epoch ? shard : acc,
            sinceEpoch, null, null);
    }

    /**
     * Apply {@code consumer} to every shard whose range covers {@code token}.
     */
    void forEachCovering(Token token, Consumer<Shard> consumer)
    {
        IntervalBTree.accumulate(
            tree, PointQueryComparators.INSTANCE, token,
            (sink, ignore, shard, acc) -> { sink.accept(shard); return null; },
            consumer, null, null);
    }

    /**
     * Apply {@code consumer} to every shard that intersects with {@code range}.
     */
    void forEachIntersecting(Range<Token> range, Consumer<Shard> consumer)
    {
        // TODO (expected): valitate if the range cannot be truly- or regular wrap-around
        if (range.isTrulyWrapAround())
            throw new IllegalArgumentException("Query range truly wraps around: " + range);

        IntervalBTree.accumulate(
            tree, RangeQueryComparators.INSTANCE, range,
            (sink, ignore, shard, acc) -> { sink.accept(shard); return null; },
            consumer, null, null);
    }

    /**
     * Fold {@code folder} over every shard that intersects {@code range}.
     */
    <A> A foldIntersecting(Range<Token> range, BiFunction<Shard, A, A> folder, A accumulator)
    {
        if (range.isTrulyWrapAround())
            throw new IllegalArgumentException("Query range truly wraps around: " + range);

        return IntervalBTree.accumulate(
            tree, RangeQueryComparators.INSTANCE, range,
            (BiFunction<Shard, A, A> f, Object ignore, Shard shard, A acc) -> f.apply(shard, acc),
            folder, null, accumulator);
    }

    void forEachIntersecting(Collection<Range<Token>> ranges, Consumer<Shard> consumer)
    {
        for (Shard shard : BTree.<Shard>iterable(tree))
            if (shard.range.intersects(ranges))
                consumer.accept(shard);
    }

    void forEachIntersecting(AbstractBounds<PartitionPosition> bounds, Consumer<Shard> consumer)
    {
        // TODO (expected): partial workaround - is there a better way to do this?
        //  SELECT * statements create Bounds[min,min], (PartitionKeyRestrictions.java:L174) not Range(min,min],
        //  which Ranges generally won't intersect with (Range.java:L148), so contains is used here to make it work
        for (Shard shard : BTree.<Shard>iterable(tree))
        {
            Range<PartitionPosition> rowRange = Range.makeRowRange(shard.range);
            if (bounds.contains(rowRange.right) || rowRange.intersects(bounds))
                consumer.accept(shard);
        }
    }

    /**
     * Invoke {@code consumer} for every Shard in the tree (exactly once for each shard).
     */
    void forEach(Consumer<Shard> consumer)
    {
        for (Shard shard : BTree.<Shard>iterable(tree))
            consumer.accept(shard);
    }

    /**
     * Invoke {@code consumer} for every Shard in the tree (exactly once for each shard).
     * Allows one pass-through arg to allow not allocating some capturing lambdas.
     */
    <P> void forEach(BiConsumer<Shard, P> consumer, P param)
    {
        for (Shard shard : BTree.<Shard>iterable(tree))
            consumer.accept(shard, param);
    }

    boolean isEmpty()
    {
        return BTree.isEmpty(tree);
    }

    /*
     * Comparators for shard-vs-shard overlap (used by build/update/subtract).
     */
    private static final class BuildComparators implements IntervalBTree.IntervalComparators<Shard>
    {
        private static final BuildComparators INSTANCE = new BuildComparators();

        @Override
        public Comparator<Shard> totalOrder()
        {
            return Shard.COMPARATOR;
        }

        @Override
        public Comparator<Shard> endWithEndSorter()
        {
            return (a, b) -> Range.compareRightToken(a.range.right, b.range.right);
        }

        @Override
        public AsymmetricComparator<Shard, Shard> startWithStartSeeker()
        {
            // required by IntervalComparators; never invoked
            return (a, b) -> startWithStart(a.range.left.compareTo(b.range.left));
        }

        @Override
        public AsymmetricComparator<Shard, Shard> startWithEndSeeker()
        {
            // required by IntervalComparators; never invoked
            return (a, b) -> startWithEnd(compareTokenToEnd(a.range.left, b.range.right));
        }

        @Override
        public AsymmetricComparator<Shard, Shard> endWithStartSeeker()
        {
            // required by IntervalComparators; never invoked
            return (a, b) -> endWithStart(compareEndToToken(a.range.right, b.range.left));
        }
    }

    /*
     * Comparators for point queries.
     */
    private static final class PointQueryComparators implements IntervalBTree.WithIntervalComparators<Token, Shard>
    {
        private static final PointQueryComparators INSTANCE = new PointQueryComparators();

        @Override
        public AsymmetricComparator<Token, Shard> startWithStartSeeker()
        {
            return (token, shard) -> keyStartWithStart(token.compareTo(shard.range.left));
        }

        @Override
        public AsymmetricComparator<Token, Shard> startWithEndSeeker()
        {
            return (token, shard) -> keyStartWithEnd(compareTokenToEnd(token, shard.range.right));
        }

        @Override
        public AsymmetricComparator<Token, Shard> endWithStartSeeker()
        {
            return (token, shard) -> keyEndWithStart(token.compareTo(shard.range.left));
        }
    }

    /*
     * Comparators for range queries.
     */
    private static final class RangeQueryComparators implements IntervalBTree.WithIntervalComparators<Range<Token>, Shard>
    {
        private static final RangeQueryComparators INSTANCE = new RangeQueryComparators();

        @Override
        public AsymmetricComparator<Range<Token>, Shard> startWithStartSeeker()
        {
            // two exclusive starts;
            // tie means range starts 'after' for sort-positioning
            return (range, shard) -> startWithStart(range.left.compareTo(shard.range.left));
        }

        @Override
        public AsymmetricComparator<Range<Token>, Shard> startWithEndSeeker()
        {
            // range.left exclusive, shard.right inclusive (min = +∞):
            // tie (range.left == shard.right) means query starts AFTER shard ends, no overlap
            return (range, shard) -> startWithEnd(compareTokenToEnd(range.left, shard.range.right));
        }

        @Override
        public AsymmetricComparator<Range<Token>, Shard> endWithStartSeeker()
        {
            // range.right inclusive (min = +∞), shard.left exclusive:
            // tie (range.right == shard.left) means query ends BEFORE shard starts, no overlap
            return (range, shard) -> endWithStart(compareEndToToken(range.right, shard.range.left));
        }
    }

    private static int compareTokenToEnd(Token t, Token end)
    {
        return end.isMinimum() ? -1 : t.compareTo(end);
    }

    private static int compareEndToToken(Token end, Token t)
    {
        return end.isMinimum() ? 1 : end.compareTo(t);
    }
}
