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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.jctools.maps.NonBlockingHashMapLong;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShardIntervalBTreeTest
{
    private static final Participants PARTICIPANTS = new Participants(List.of(1));
    private static final Token MIN = Murmur3Partitioner.MINIMUM;

    private static Token tk(long token)
    {
        return new LongToken(token);
    }

    private static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
    }

    /** Range whose right bound is the +∞ sentinel (Murmur3 MINIMUM). */
    private static Range<Token> rangeToMax(long left)
    {
        return new Range<>(tk(left), MIN);
    }

    /** Range whose left bound is the +∞ sentinel; with right = finite this is the bottom slice. */
    private static Range<Token> rangeFromMin(long right)
    {
        return new Range<>(MIN, tk(right));
    }

    /** Collect the set of shards returned by a forEach-like method. */
    private static Set<Shard> collect(java.util.function.Consumer<Consumer<Shard>> producer)
    {
        Set<Shard> hits = new HashSet<>();
        producer.accept(hits::add);
        return hits;
    }

    private static Set<Shard> intersecting(ShardIntervalBTree map, Range<Token> query)
    {
        return collect(c -> map.forEachIntersecting(query, c));
    }

    private static Set<Shard> covering(ShardIntervalBTree map, Token token)
    {
        return collect(c -> map.forEachCovering(token, c));
    }

    /**
     * Build a {@link Shard} skeleton suitable for indexing - enough state for
     * {@code ShardIntervalMap} (range + sinceEpoch + identity) without requiring
     * a running mutation journal.
     */
    private static Shard shard(Range<Token> range, long sinceEpoch)
    {
        return new Shard(/* localNodeId    */ 1,
                         /* keyspace       */ "ks",
                         /* sinceEpoch     */ sinceEpoch,
                         /* range          */ range,
                         /* participants   */ PARTICIPANTS,
                         /* logs           */ new NonBlockingHashMapLong<>(),
                         /* currentLocal   */ null,
                         /* logIdProvider  */ () -> 0L,
                         /* onNewLog       */ (s, l) -> {});
    }

    @Test
    public void testWithSingleShardLookup()
    {
        Shard s = shard(range(0, 100), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        assertFalse(map.isEmpty());

        // (left, right] semantics
        assertNull(map.latestShardCovering(tk(0)));        // exclusive on left
        assertSame(s, map.latestShardCovering(tk(1)));
        assertSame(s, map.latestShardCovering(tk(50)));
        assertSame(s, map.latestShardCovering(tk(100)));   // inclusive on right
        assertNull(map.latestShardCovering(tk(101)));
    }

    @Test
    public void testNonOverlappingShards()
    {
        Shard a = shard(range(0, 100), 1L);
        Shard b = shard(range(100, 200), 1L);
        Shard c = shard(range(200, 300), 1L);

        ShardIntervalBTree map = new ShardIntervalBTree().with(a).with(b).with(c);

        assertSame(a, map.latestShardCovering(tk(50)));
        assertSame(a, map.latestShardCovering(tk(100)));   // (0, 100] wins on the boundary
        assertSame(b, map.latestShardCovering(tk(150)));
        assertSame(b, map.latestShardCovering(tk(200)));
        assertSame(c, map.latestShardCovering(tk(250)));
        assertNull(map.latestShardCovering(tk(301)));
    }

    @Test
    public void testNewestShardWinsOnEpoch()
    {
        Shard older = shard(range(0, 100), 1L);
        Shard newer = shard(range(0, 100), 5L);

        ShardIntervalBTree map = new ShardIntervalBTree().with(older).with(newer);

        assertSame(newer, map.latestShardCovering(tk(50)));

        // both are reachable via forEach
        Set<Shard> hits = new HashSet<>();
        map.forEachCovering(tk(50), hits::add);
        assertEquals(Set.of(older, newer), hits);
    }

    @Test
    public void testForEachAll()
    {
        Shard a = shard(range(0, 100), 1L);
        Shard b = shard(range(100, 200), 1L);

        ShardIntervalBTree map = new ShardIntervalBTree().with(a).with(b);

        List<Shard> seen = new ArrayList<>();
        map.forEach(seen::add);
        assertEquals(Set.of(a, b), new HashSet<>(seen));
    }

    @Test
    public void testWithoutShard()
    {
        Shard a = shard(range(0, 100), 1L);
        Shard b = shard(range(100, 200), 1L);

        ShardIntervalBTree map = new ShardIntervalBTree().with(a).with(b);
        ShardIntervalBTree less = map.without(a);

        // the original snapshot is untouched (immutability)
        assertSame(a, map.latestShardCovering(tk(50)));

        assertNull(less.latestShardCovering(tk(50)));
        assertSame(b, less.latestShardCovering(tk(150)));
    }

    @Test
    public void testWithDuplicateThrows()
    {
        Shard a = shard(range(0, 100), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(a);
        try
        {
            map.with(a);
            fail("expected IllegalStateException for duplicate shard");
        }
        catch (IllegalStateException expected) {}
    }

    @Test
    public void testWithoutMissingThrows()
    {
        ShardIntervalBTree map = new ShardIntervalBTree();
        Shard a = shard(range(0, 100), 1L);
        try
        {
            map.without(a);
            fail("expected IllegalStateException for missing shard");
        }
        catch (IllegalStateException expected) {}
    }

    @Test
    public void testImmutabilityOfWithAndWithout()
    {
        Shard a = shard(range(0, 100), 1L);
        Shard b = shard(range(100, 200), 1L);

        ShardIntervalBTree empty = new ShardIntervalBTree();
        ShardIntervalBTree one = empty.with(a);
        ShardIntervalBTree two = one.with(b);
        ShardIntervalBTree back = two.without(b);

        assertTrue(empty.isEmpty());
        assertNotNull(one.latestShardCovering(tk(50)));
        assertNotNull(two.latestShardCovering(tk(150)));
        assertNull(back.latestShardCovering(tk(150)));
        assertSame(a, back.latestShardCovering(tk(50)));
    }

    // ---------------------------------------------------------------------
    // Same-range / multi-epoch handling
    // ---------------------------------------------------------------------

    /** Two shards sharing a range at different epochs coexist and are distinguished by sinceEpoch. */
    @Test
    public void testSameRangeDifferentEpochBothStored()
    {
        Shard olderS = shard(range(0, 100), 1L);
        Shard newerS = shard(range(0, 100), 5L);

        ShardIntervalBTree map = new ShardIntervalBTree().with(olderS).with(newerS);

        assertSame(newerS, map.latestShardCovering(tk(50)));
        assertEquals(Set.of(olderS, newerS), covering(map, tk(50)));
        assertEquals(Set.of(olderS, newerS), intersecting(map, range(10, 90)));

        // Removing one leaves the other intact.
        ShardIntervalBTree lessNew = map.without(newerS);
        assertSame(olderS, lessNew.latestShardCovering(tk(50)));
        assertEquals(Set.of(olderS), covering(lessNew, tk(50)));

        ShardIntervalBTree lessOld = map.without(olderS);
        assertSame(newerS, lessOld.latestShardCovering(tk(50)));
        assertEquals(Set.of(newerS), covering(lessOld, tk(50)));
    }

    // ---------------------------------------------------------------------
    // forEachIntersecting: empty tree
    // ---------------------------------------------------------------------

    @Test
    public void testIntersectingEmptyTree()
    {
        ShardIntervalBTree map = new ShardIntervalBTree();
        assertEquals(Set.of(), intersecting(map, range(0, 100)));
        assertEquals(Set.of(), intersecting(map, rangeToMax(0)));
    }

    // ---------------------------------------------------------------------
    // forEachIntersecting: basic overlap shapes
    // ---------------------------------------------------------------------

    @Test
    public void testIntersectingOverlapShapes()
    {
        Shard a = shard(range(  0, 100), 1L);
        Shard b = shard(range(100, 200), 1L);
        Shard c = shard(range(200, 300), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(a).with(b).with(c);

        // query entirely within a single shard
        assertEquals(Set.of(a), intersecting(map, range(10, 50)));

        // query spanning two shards
        assertEquals(Set.of(a, b), intersecting(map, range(50, 150)));

        // query spanning all three shards
        assertEquals(Set.of(a, b, c), intersecting(map, range(50, 250)));

        // query equal to one of the shards
        assertEquals(Set.of(a), intersecting(map, range(0, 100)));

        // query strictly to the right of all shards
        assertEquals(Set.of(), intersecting(map, range(300, 400)));

        // query strictly to the left (note: tk -1 is a perfectly valid finite token)
        assertEquals(Set.of(), intersecting(map, range(-100, -10)));
    }

    // ---------------------------------------------------------------------
    // forEachIntersecting: tie cases on (left, right] semantics
    // ---------------------------------------------------------------------

    /**
     * Regression for bug 1: query.left (exclusive) == shard.right (inclusive) is NOT overlap.
     * Before the fix, RangeQueryComparators.startWithEndSeeker used keyStartWithEnd which
     * returned 0 on a tie (point-query convention), causing a false positive here.
     */
    @Test
    public void testIntersectingBoundary_QueryLeftEqualsShardRight()
    {
        Shard s = shard(range(5, 10), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        // query (10, 20] starts strictly after shard ends at 10; they do NOT overlap
        assertEquals(Set.of(), intersecting(map, range(10, 20)));

        // But a query that still contains 10 on its right-inclusive side DOES overlap
        assertEquals(Set.of(s), intersecting(map, range(9, 20)));
    }

    /**
     * Mirror of the above: query.right (inclusive) == shard.left (exclusive) is NOT overlap.
     * This was already correct in the original code; the test guards against regressions.
     */
    @Test
    public void testIntersectingBoundary_QueryRightEqualsShardLeft()
    {
        Shard s = shard(range(10, 20), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        // query (0, 10] ends exactly at shard's exclusive left; they do NOT overlap
        assertEquals(Set.of(), intersecting(map, range(0, 10)));

        // Shifting right by one makes them overlap (at token 11).
        assertEquals(Set.of(s), intersecting(map, range(0, 11)));
    }

    /** Adjacent shards both boundary-cased at once. */
    @Test
    public void testIntersectingAdjacentShardBoundary()
    {
        Shard a = shard(range(  0, 100), 1L);
        Shard b = shard(range(100, 200), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(a).with(b);

        // Query (99, 100] sits entirely inside a (includes token 100 which is a's inclusive right).
        assertEquals(Set.of(a), intersecting(map, range(99, 100)));

        // Query (100, 101] sits entirely inside b (excludes token 100 via its exclusive left).
        assertEquals(Set.of(b), intersecting(map, range(100, 101)));

        // Query (99, 101] straddles the boundary: both shards match.
        assertEquals(Set.of(a, b), intersecting(map, range(99, 101)));
    }

    // ---------------------------------------------------------------------
    // forEachIntersecting: min-token (+∞) sentinel handling
    // ---------------------------------------------------------------------

    /**
     * Regression for bug 2: a query whose right bound is the min-token sentinel (= +∞)
     * must still correctly intersect shards whose ranges lie above the query's left.
     * Before the fix, endWithStartSeeker used raw Token.compareTo which treats min as
     * -∞, producing a false negative.
     */
    @Test
    public void testIntersectingQueryRightIsMin()
    {
        Shard a = shard(range(  0, 100), 1L);
        Shard b = shard(range(100, 200), 1L);
        Shard c = shard(range(200, 300), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(a).with(b).with(c);

        // Query (10, +∞] must return all three shards.
        assertEquals(Set.of(a, b, c), intersecting(map, rangeToMax(10)));

        // Query (150, +∞] starts inside b and continues past everything on the right.
        assertEquals(Set.of(b, c), intersecting(map, rangeToMax(150)));

        // Query (300, +∞] starts strictly above all shards.
        assertEquals(Set.of(), intersecting(map, rangeToMax(300)));

        // Boundary: query (100, +∞] must exclude a (whose right is 100, inclusive but
        // coincident with the query's exclusive left).
        assertEquals(Set.of(b, c), intersecting(map, rangeToMax(100)));
    }

    /**
     * Shards whose right bound is the min-token sentinel (= +∞) must be matched
     * by queries that fall anywhere in their range. This exercises the storage side
     * of the +∞ convention through compareTokenToEnd / endWithEndSorter.
     */
    @Test
    public void testIntersectingShardRightIsMin()
    {
        // shard covers (100, +∞]
        Shard s = shard(rangeToMax(100), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        // finite window strictly above the shard's left
        assertEquals(Set.of(s), intersecting(map, range(200, 300)));
        // window that starts before the shard and reaches inside it
        assertEquals(Set.of(s), intersecting(map, range(50, 150)));
        // window entirely below the shard
        assertEquals(Set.of(), intersecting(map, range(0, 50)));
        // window whose right bound equals shard's exclusive left
        assertEquals(Set.of(), intersecting(map, range(0, 100)));
        // query reaching to +∞
        assertEquals(Set.of(s), intersecting(map, rangeToMax(200)));

        // point queries:
        assertEquals(Set.of(s), covering(map, tk(200)));
        assertEquals(Set.of(),  covering(map, tk(100)));        // exclusive left
        assertEquals(Set.of(),  covering(map, tk(50)));
    }

    /**
     * Full-ring shard (min, min] must be accepted by with() (it is not "truly"
     * wrap-around) and match every finite query.
     */
    @Test
    public void testIntersectingFullRingShard()
    {
        Shard s = shard(new Range<>(MIN, MIN), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        assertEquals(Set.of(s), intersecting(map, range(0, 100)));
        assertEquals(Set.of(s), intersecting(map, range(-100, -1)));
        assertEquals(Set.of(s), intersecting(map, rangeToMax(0)));
        assertEquals(Set.of(s), intersecting(map, rangeFromMin(100)));
    }

    // ---------------------------------------------------------------------
    // forEachIntersecting: wrap-around guard
    // ---------------------------------------------------------------------

    @Test
    public void testIntersectingRejectsTrulyWrapAroundQuery()
    {
        ShardIntervalBTree map = new ShardIntervalBTree().with(shard(range(0, 100), 1L));
        // (200, 100] truly wraps around (right is finite, smaller than left).
        try
        {
            map.forEachIntersecting(new Range<>(tk(200), tk(100)), s -> fail("should have thrown"));
            fail("expected IllegalArgumentException for wrap-around query");
        }
        catch (IllegalArgumentException expected) {}
    }

    @Test
    public void testIntersectingAcceptsQueryWithMinRight()
    {
        // (50, +∞] is not "truly" wrap-around and must be accepted.
        ShardIntervalBTree map = new ShardIntervalBTree().with(shard(range(0, 100), 1L));
        assertEquals(Set.of(map.latestShardCovering(tk(50))), intersecting(map, rangeToMax(50)));
    }

    // ---------------------------------------------------------------------
    // forEachCovering: boundary pin-downs (regression guards)
    // ---------------------------------------------------------------------

    @Test
    public void testCoveringBoundaries()
    {
        Shard s = shard(range(0, 100), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        assertEquals(Set.of(),  covering(map, tk(0)));     // exclusive left
        assertEquals(Set.of(s), covering(map, tk(1)));
        assertEquals(Set.of(s), covering(map, tk(100)));   // inclusive right
        assertEquals(Set.of(),  covering(map, tk(101)));
    }

    @Test
    public void testCoveringAtMinToken()
    {
        // For both a finite shard and a +∞-right shard, the min token is never
        // covered (conceptually it's a sentinel outside the ring).
        Shard finite = shard(range(0, 100), 1L);
        Shard toMax  = shard(rangeToMax(100), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(finite).with(toMax);

        assertEquals(Set.of(), covering(map, MIN));
        assertNull(map.latestShardCovering(MIN));
    }

    // ---------------------------------------------------------------------
    // get(Range, sinceEpoch): exact (range, sinceEpoch) lookup
    // ---------------------------------------------------------------------

    @Test
    public void testGetEmptyTreeReturnsNull()
    {
        ShardIntervalBTree map = new ShardIntervalBTree();
        assertNull(map.get(range(0, 100), 1L));
        assertNull(map.get(rangeToMax(0), 1L));
    }

    @Test
    public void testGetExactMatch()
    {
        Shard a = shard(range(  0, 100), 1L);
        Shard b = shard(range(100, 200), 2L);
        Shard c = shard(range(200, 300), 3L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(a).with(b).with(c);

        assertSame(a, map.get(range(  0, 100), 1L));
        assertSame(b, map.get(range(100, 200), 2L));
        assertSame(c, map.get(range(200, 300), 3L));
    }

    @Test
    public void testGetRangeMatchesButEpochDiffersReturnsNull()
    {
        Shard s = shard(range(0, 100), 5L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        // exact range, wrong sinceEpoch
        assertNull(map.get(range(0, 100), 1L));
        assertNull(map.get(range(0, 100), 4L));
        assertNull(map.get(range(0, 100), 6L));

        // sanity: the right epoch still works
        assertSame(s, map.get(range(0, 100), 5L));
    }

    @Test
    public void testGetEpochMatchesButRangeDiffersReturnsNull()
    {
        Shard s = shard(range(0, 100), 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        // overlapping but not equal: subset, superset, shifted, adjacent
        assertNull(map.get(range( 10,  90), 1L)); // strict subset
        assertNull(map.get(range(-10, 110), 1L)); // strict superset
        assertNull(map.get(range( 50, 150), 1L)); // shifted right, partial overlap
        assertNull(map.get(range(-50,  50), 1L)); // shifted left, partial overlap
        assertNull(map.get(range(100, 200), 1L)); // adjacent (no overlap on (left, right] semantics)
        assertNull(map.get(range(200, 300), 1L)); // disjoint

        // sanity: exact equality still works
        assertSame(s, map.get(range(0, 100), 1L));
    }

    @Test
    public void testGetSameRangeDifferentEpochs()
    {
        Shard older = shard(range(0, 100), 1L);
        Shard newer = shard(range(0, 100), 5L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(older).with(newer);

        assertSame(older, map.get(range(0, 100), 1L));
        assertSame(newer, map.get(range(0, 100), 5L));
        // No shard exists at this epoch even though the range matches.
        assertNull(map.get(range(0, 100), 3L));
    }

    @Test
    public void testGetWithRangeReachingMin()
    {
        // shard covers (100, +∞]
        Shard s = shard(rangeToMax(100), 7L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        assertSame(s, map.get(rangeToMax(100), 7L));
        assertNull(map.get(rangeToMax(100), 6L));        // wrong epoch
        assertNull(map.get(rangeToMax( 99), 7L));        // different left
        assertNull(map.get(range(100, 200), 7L));        // different right (not +∞)
    }

    @Test
    public void testGetFullRingShard()
    {
        Range<Token> fullRing = new Range<>(MIN, MIN);
        Shard s = shard(fullRing, 1L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(s);

        assertSame(s, map.get(fullRing, 1L));
        assertNull(map.get(fullRing, 2L));               // wrong epoch
        assertNull(map.get(rangeToMax(0), 1L));          // not the same range
        assertNull(map.get(rangeFromMin(100), 1L));      // not the same range
    }

    @Test
    public void testGetAfterWithoutReturnsNull()
    {
        Shard a = shard(range(0, 100), 1L);
        Shard b = shard(range(100, 200), 2L);
        ShardIntervalBTree map = new ShardIntervalBTree().with(a).with(b);

        assertSame(a, map.get(range(0, 100), 1L));

        ShardIntervalBTree less = map.without(a);
        assertNull(less.get(range(0, 100), 1L));
        // the original snapshot is untouched (immutability)
        assertSame(a, map.get(range(0, 100), 1L));
        // unrelated entries are still reachable
        assertSame(b, less.get(range(100, 200), 2L));
    }
}
