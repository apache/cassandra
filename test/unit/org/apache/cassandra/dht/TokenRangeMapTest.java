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
package org.apache.cassandra.dht;

import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.CassandraTestBase;
import org.apache.cassandra.CassandraTestBase.DDDaemonInitialization;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@DDDaemonInitialization
public class TokenRangeMapTest extends CassandraTestBase
{
    private static Token tok(long v)
    {
        return new LongToken(v);
    }

    private static Range<Token> range(long left, long right)
    {
        return new Range<>(tok(left), tok(right));
    }

    @Test
    public void testCreateAndGet()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        assertEquals("A", map.get(tok(0)));
        assertEquals("A", map.get(tok(100)));
        assertEquals("A", map.get(tok(-100)));
        assertEquals(1, map.intervalCount());
    }

    @Test
    public void testSetSingleRange()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");

        // Set (10, 20] → B
        map = map.set(range(10, 20), "B");

        assertEquals("A", map.get(tok(5)));
        assertEquals("A", map.get(tok(10)));  // 10 is exclusive lower bound
        assertEquals("B", map.get(tok(15)));
        assertEquals("B", map.get(tok(20)));  // 20 is inclusive upper bound
        assertEquals("A", map.get(tok(25)));
    }

    @Test
    public void testSetAdjacentRanges()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");

        map = map.set(range(10, 20), "B");
        map = map.set(range(20, 30), "C");

        assertEquals("A", map.get(tok(10)));
        assertEquals("B", map.get(tok(15)));
        assertEquals("B", map.get(tok(20)));
        assertEquals("C", map.get(tok(25)));
        assertEquals("C", map.get(tok(30)));
        assertEquals("A", map.get(tok(35)));
    }

    @Test
    public void testSetOverlappingRange()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(10, 30), "B");

        // Now set an overlapping range that splits B
        map = map.set(range(15, 25), "C");

        assertEquals("A", map.get(tok(10)));
        assertEquals("B", map.get(tok(12)));
        assertEquals("B", map.get(tok(15)));  // 15 is exclusive
        assertEquals("C", map.get(tok(20)));
        assertEquals("C", map.get(tok(25)));  // 25 is inclusive
        assertEquals("B", map.get(tok(28)));
        assertEquals("A", map.get(tok(35)));
    }

    @Test
    public void testSetMergesAdjacentSameValue()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(10, 30), "B");

        // Set a sub-range back to A — the prefix and suffix should merge with surrounding A
        map = map.set(range(10, 30), "A");

        assertTrue(map.allEqual("A"));
        assertEquals(1, map.intervalCount());
    }

    @Test
    public void testSetPartialMerge()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(10, 20), "B");
        map = map.set(range(20, 30), "B");

        // The two B ranges should merge
        assertEquals("A", map.get(tok(10)));
        assertEquals("B", map.get(tok(15)));
        assertEquals("B", map.get(tok(25)));
        assertEquals("A", map.get(tok(35)));
        assertEquals(3, map.intervalCount()); // A, B, A
    }

    @Test
    public void testSetExactBoundaryMatch()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(10, 20), "B");

        // Set the exact same range to a different value
        map = map.set(range(10, 20), "C");

        assertEquals("A", map.get(tok(10)));
        assertEquals("C", map.get(tok(15)));
        assertEquals("C", map.get(tok(20)));
        assertEquals("A", map.get(tok(25)));
    }

    @Test
    public void testSetSupersetRange()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(10, 20), "B");

        // Set a superset range
        map = map.set(range(5, 25), "C");

        assertEquals("A", map.get(tok(3)));
        assertEquals("A", map.get(tok(5)));   // exclusive
        assertEquals("C", map.get(tok(10)));
        assertEquals("C", map.get(tok(15)));
        assertEquals("C", map.get(tok(20)));
        assertEquals("C", map.get(tok(25)));
        assertEquals("A", map.get(tok(30)));
    }

    @Test
    public void testAllMatch()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        assertTrue(map.allEqual("A"));
        assertFalse(map.allEqual("B"));

        map = map.set(range(10, 20), "B");
        assertFalse(map.allEqual("A"));
        assertFalse(map.allEqual("B"));

        map = map.set(range(10, 20), "A");
        assertTrue(map.allEqual("A"));
    }

    @Test
    public void testAllMatchPredicate()
    {
        TokenRangeMap<Integer> map = TokenRangeMap.create(0);
        assertTrue(map.allMatch(v -> v >= 0));

        map = map.set(range(10, 20), 5);
        assertTrue(map.allMatch(v -> v >= 0));

        map = map.set(range(20, 30), -1);
        assertFalse(map.allMatch(v -> v >= 0));
    }

    @Test
    public void testMultipleNonOverlappingSets()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(10, 20), "B");
        map = map.set(range(30, 40), "C");
        map = map.set(range(50, 60), "D");

        assertEquals("A", map.get(tok(5)));
        assertEquals("B", map.get(tok(15)));
        assertEquals("A", map.get(tok(25)));
        assertEquals("C", map.get(tok(35)));
        assertEquals("A", map.get(tok(45)));
        assertEquals("D", map.get(tok(55)));
        assertEquals("A", map.get(tok(65)));
    }

    @Test
    public void testSetWithNormalizedRanges()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");

        NormalizedRanges<Token> ranges = NormalizedRanges.normalizedRanges(Arrays.asList(range(10, 20), range(30, 40)));
        map = map.set(ranges, "B");

        assertEquals("A", map.get(tok(5)));
        assertEquals("B", map.get(tok(15)));
        assertEquals("A", map.get(tok(25)));
        assertEquals("B", map.get(tok(35)));
        assertEquals("A", map.get(tok(45)));
    }

    @Test
    public void testFailoverStateTransitionPattern()
    {
        // Simulates the KeyspaceFailoverState lifecycle:
        // Start: all ranges in TRANSITION_ACK
        // Some ranges move to TRANSITION, then to NORMAL
        TokenRangeMap<String> map = TokenRangeMap.create("TRANSITION_ACK");

        // Move (10, 30] to TRANSITION
        map = map.set(range(10, 30), "TRANSITION");
        assertEquals("TRANSITION_ACK", map.get(tok(5)));
        assertEquals("TRANSITION", map.get(tok(20)));
        assertEquals("TRANSITION_ACK", map.get(tok(40)));

        // Move (10, 30] to NORMAL
        map = map.set(range(10, 30), "NORMAL");
        assertEquals("TRANSITION_ACK", map.get(tok(5)));
        assertEquals("NORMAL", map.get(tok(20)));
        assertEquals("TRANSITION_ACK", map.get(tok(40)));

        // Move the rest: (-50, 10] and (30, 50] to TRANSITION, then NORMAL
        NormalizedRanges<Token> remaining = NormalizedRanges.normalizedRanges(Arrays.asList(
            range(-50, 10),
            range(30, 50)
        ));
        map = map.set(remaining, "TRANSITION");
        assertEquals("TRANSITION", map.get(tok(0)));
        assertEquals("NORMAL", map.get(tok(20)));
        assertEquals("TRANSITION", map.get(tok(40)));

        map = map.set(remaining, "NORMAL");
        assertEquals("NORMAL", map.get(tok(0)));
        assertEquals("NORMAL", map.get(tok(20)));
        assertEquals("NORMAL", map.get(tok(40)));
    }

    @Test
    public void testSetWrappingRange()
    {
        // (5000, -5000] wraps around: covers (5000, MAX] and (MIN, -5000]
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(5000, -5000), "B");

        assertEquals("B", map.get(tok(6000)));    // above 5000 → in range
        assertEquals("B", map.get(tok(-6000)));   // below -5000 → in range
        assertEquals("A", map.get(tok(0)));        // between -5000 and 5000 → not in range
        assertEquals("A", map.get(tok(3000)));
        assertEquals("A", map.get(tok(-3000)));
        assertEquals("A", map.get(tok(5000)));     // 5000 is exclusive lower bound
        assertEquals("B", map.get(tok(-5000)));    // -5000 is inclusive upper bound
    }

    @Test
    public void testSetWrappingRangeOverExisting()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(1000, 2000), "B");
        map = map.set(range(8000, 9000), "C");

        // Wrapping range that overlaps with C but not B
        map = map.set(range(5000, -5000), "X");

        assertEquals("A", map.get(tok(0)));        // between -5000 and 1000
        assertEquals("B", map.get(tok(1500)));     // B is untouched
        assertEquals("A", map.get(tok(3000)));     // between B and wrapping range
        assertEquals("X", map.get(tok(6000)));     // in wrapping range
        assertEquals("X", map.get(tok(8500)));     // C was overwritten by X
        assertEquals("X", map.get(tok(-6000)));    // in wrapping range (below -5000)
        assertEquals("A", map.get(tok(-3000)));    // not in wrapping range
    }

    @Test
    public void testSetWrappingRangeThenSetBack()
    {
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(5000, -5000), "B");

        // Set a portion within the wrapping range back to A
        map = map.set(range(7000, 9000), "A");

        assertEquals("B", map.get(tok(6000)));     // still B
        assertEquals("A", map.get(tok(8000)));     // set back to A
        assertEquals("B", map.get(tok(10000)));    // still B
        assertEquals("B", map.get(tok(-6000)));    // still B (other half of wrap)
        assertEquals("A", map.get(tok(0)));         // was never B
    }

    @Test
    public void testSetFullRingWrappingRange()
    {
        // (MIN, MIN] is the full ring
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        Token min = IPartitioner.global().getMinimumToken();
        map = map.set(new Range<>(min, min), "B");

        assertTrue(map.allEqual("B"));
        assertEquals("B", map.get(tok(0)));
        assertEquals("B", map.get(tok(5000)));
        assertEquals("B", map.get(tok(-5000)));
    }

    @Test
    public void testWrappingRangeMergesCorrectly()
    {
        // Set two wrapping ranges with the same value — should merge
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(5000, -5000), "B");
        map = map.set(range(-5000, 5000), "B");

        assertTrue(map.allEqual("B"));
        assertEquals(1, map.intervalCount());
    }

    @Test
    public void testBoundaryWrappingRange()
    {
        // (5000, MIN] is not a true wrap-around: MIN is just the inclusive upper bound
        // (end of ring) of the last interval, so this is a single segment (5000, MIN].
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(new Range<>(tok(5000), Murmur3Partitioner.instance.getMinimumToken()), "B");

        assertEquals("A", map.get(tok(5000)));
        assertEquals("B", map.get(tok(5001)));
        assertEquals("B", map.get(Murmur3Partitioner.instance.getMinimumToken()));
        assertEquals("A", map.get(tok(Long.MIN_VALUE + 1)));

        assertEquals("A", map.get(tok(0)));
        assertEquals("A", map.get(tok(-5000)));
        assertEquals(2, map.intervalCount()); // (MIN,5000]=A, (5000,MIN]=B
    }

    @Test
    public void testGetMinimumTokenDefaultMap()
    {
        // On a single-value map, get(MIN) resolves to that value (the sole/last interval).
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        assertEquals("A", map.get(Murmur3Partitioner.instance.getMinimumToken()));
    }

    @Test
    public void testGetMinimumTokenTrulyWrappingRange()
    {
        // (5000, -5000] truly wraps; MIN falls in the wrap segment (5000, MIN] portion.
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(5000, -5000), "B");

        assertEquals("B", map.get(Murmur3Partitioner.instance.getMinimumToken()));
        assertEquals("B", map.get(tok(Long.MAX_VALUE))); // top of ring, in (5000, MIN]
        assertEquals(3, map.intervalCount()); // (MIN,-5000]=B, (-5000,5000]=A, (5000,MIN]=B
    }

    @Test
    public void testBoundaryRangeNegativeLeft()
    {
        // (-5000, MIN]: prefix (MIN, -5000] stays A, suffix (-5000, MIN] becomes B.
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(new Range<>(tok(-5000), Murmur3Partitioner.instance.getMinimumToken()), "B");

        assertEquals("A", map.get(tok(-6000)));   // (MIN, -5000]
        assertEquals("A", map.get(tok(-5000)));   // inclusive upper bound of the A segment
        assertEquals("B", map.get(tok(-4999)));   // just above → B
        assertEquals("B", map.get(tok(0)));        // (-5000, MIN]
        assertEquals("B", map.get(tok(5000)));
        assertEquals("B", map.get(Murmur3Partitioner.instance.getMinimumToken()));
        assertEquals(2, map.intervalCount());
    }

    @Test
    public void testBoundaryRangeOverExisting()
    {
        // Boundary form (X, MIN] applied over pre-existing non-wrapping ranges.
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        map = map.set(range(1000, 2000), "X");
        map = map.set(new Range<>(tok(5000), Murmur3Partitioner.instance.getMinimumToken()), "B");

        assertEquals("A", map.get(tok(500)));      // (MIN, 1000]
        assertEquals("X", map.get(tok(1500)));     // (1000, 2000] untouched
        assertEquals("A", map.get(tok(3000)));     // (2000, 5000]
        assertEquals("B", map.get(tok(6000)));     // (5000, MIN]
        assertEquals("B", map.get(Murmur3Partitioner.instance.getMinimumToken()));
        assertEquals("A", map.get(tok(-3000)));    // still below 5000 → A, not in (5000, MIN]
        assertEquals(4, map.intervalCount());
    }

    @Test
    public void testBoundaryRangeMergeAcrossWrap()
    {
        // (MIN, 5000] then (5000, MIN] with the same value should merge into the full ring.
        TokenRangeMap<String> map = TokenRangeMap.create("A");
        Token min = Murmur3Partitioner.instance.getMinimumToken();
        map = map.set(new Range<>(min, tok(5000)), "B");
        map = map.set(new Range<>(tok(5000), min), "B");

        assertTrue(map.allEqual("B"));
        assertEquals(1, map.intervalCount());
    }

    @Test
    public void testEquals()
    {
        TokenRangeMap<String> a = TokenRangeMap.create("X");
        TokenRangeMap<String> b = TokenRangeMap.create("X");
        assertEquals(a, b);

        a = a.set(range(10, 20), "Y");
        b = b.set(range(10, 20), "Y");
        assertEquals(a, b);
    }
}
