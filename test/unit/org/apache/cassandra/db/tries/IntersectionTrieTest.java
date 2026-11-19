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

package org.apache.cassandra.db.tries;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.googlecode.concurrenttrees.common.Iterables;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryTrie;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.apache.cassandra.db.tries.TrieUtil.toBound;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class IntersectionTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static final int COUNT = 15000;
    Random rand = new Random();
    int seed = rand.nextInt();
    final static int bitsNeeded = 4;

    @Parameterized.Parameters(name = "bits per transition {0} dropDanglingZero {1}")
    public static List<Object[]> data()
    {
        return IntStream.rangeClosed(1, bitsNeeded)
                        .boxed()
                        .flatMap(x -> Stream.of(new Object[] { x, false},
                                                new Object[] {x, true}))
                        .collect(Collectors.toList());
    }

    @Parameterized.Parameter(0)
    public int bits = bitsNeeded;

    @Parameterized.Parameter(1)
    public boolean dropDanglingZeros = false;

    public static final Trie.CollectionMergeResolver<Integer> RESOLVER = new Trie.CollectionMergeResolver<>()
    {
        public Integer resolve(Collection<Integer> contents)
        {
            return contents.iterator().next();
        }

        public Integer resolve(Integer b1, Integer b2)
        {
            return b1;
        }
    };

    public interface RangeOp<T>
    {
        Trie<T> apply(Trie<T> t, ByteComparable left, ByteComparable right);
    }

    @Test
    public void testIntersectRangeDirect() throws Exception
    {
        testIntersectRange(COUNT, Trie::subtrie);
    }

    @Test
    public void testIntersectRangesOneDirect() throws Exception
    {
        testIntersectRange(COUNT, (t, l, r) -> t.intersect(TrieSet.ranges(VERSION, l, r)));
    }

    public void testIntersectRange(int count, RangeOp<ByteBuffer> op) throws Exception
    {
        System.out.format("intersectrange seed %d\n", ++seed);
        rand.setSeed(seed);
        Preencoded[] src1 = generateKeys(rand, count);
        NavigableMap<Preencoded, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));

        Trie<ByteBuffer> t1 = makeInMemoryTrie(src1, content1, true);

        checkEqualRange(content1, t1, null, null, op);
        checkEqualRange(content1, t1, TrieUtil.generateKeyBound(rand), null, op);
        checkEqualRange(content1, t1, null, TrieUtil.generateKeyBound(rand), op);

        Preencoded l = rand.nextBoolean() ? TrieUtil.generateKeyBound(rand) : toBound(src1[rand.nextInt(src1.length)]);
        Preencoded r = rand.nextBoolean() ? TrieUtil.generateKeyBound(rand) : toBound(src1[rand.nextInt(src1.length)]);
        int cmp = ByteComparable.compare(l, r, VERSION);
        if (cmp > 0)
        {
            Preencoded t = l;l = r;r = t; // swap
        }

        checkEqualRange(content1, t1, l, r, op);
    }

    public void checkEqualRange(NavigableMap<Preencoded, ByteBuffer> content1,
                                Trie<ByteBuffer> t1,
                                Preencoded l,
                                Preencoded r,
                                RangeOp<ByteBuffer> op)
    {
        System.out.format("Intersection with [%s:%s]\n", asString(l), asString(r));
        NavigableMap<Preencoded, ByteBuffer> imap = TrieUtil.boundedMap(content1, l, true, r, false);

        Trie<ByteBuffer> intersection = op.apply(t1, l, r);

        assertMapEquals(intersection, imap, Direction.FORWARD);
        assertMapEquals(intersection, imap, Direction.REVERSE);
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie, Direction direction)
    {
        return Iterables.toList(trie.values(direction));
    }

    private Trie<Integer> fromList(int... list) throws TrieSpaceExhaustedException
    {
        InMemoryTrie<Integer> trie = InMemoryTrie.shortLivedOrdered(VERSION);
        for (int i : list)
        {
            trie.putRecursive(of(i), i, (ex, n) -> n);
        }
        return trie;
    }

    /** Creates a {@link ByteComparable} for the provided value by splitting the integer in sequences of "bits" bits. */
    private ByteComparable of(int value)
    {
        // TODO: Also in all other tests of this type
        assert value >= 0 && value <= Byte.MAX_VALUE;

        byte[] splitBytes = new byte[(bitsNeeded + bits - 1) / bits];
        int pos = 0;
        int mask = (1 << bits) - 1;
        for (int i = bitsNeeded - bits; i > 0; i -= bits)
            splitBytes[pos++] = (byte) ((value >> i) & mask);

        splitBytes[pos++] = (byte) (value & mask);
        if (dropDanglingZeros)
        {
            while (pos > 0 && splitBytes[pos - 1] == 0)
                --pos;
        }
        return ByteComparable.preencoded(VERSION, splitBytes, 0, pos);
    }

    @Test
    public void testSimpleSubtrie() throws TrieSpaceExhaustedException
    {
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

            testIntersection("", asList(3, 4, 5, 6), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(3), of(7)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, null, of(7)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8, 9), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(3), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, null, null));

            testIntersection("", asList(), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(7), of(7)));
        }
    }

    @Test
    public void testRangeOnSubtrie() throws TrieSpaceExhaustedException
    {
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // non-overlapping
            testIntersection("", asList(), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, of(4), of(7)));
            // touching, i.e. still non-overlapping
            testIntersection("", asList(), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, of(3), of(7)));
            // overlapping 1
            testIntersection("", asList(2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, of(2), of(7)));
            // overlapping 2
            testIntersection("", asList(1, 2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, of(1), of(7)));
            // covered
            testIntersection("", asList(0, 1, 2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), of(7)));
            // covered 2
            testIntersection("", asList(1, 2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(1), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), of(7)));
            // nulls
            testIntersection("", asList(1, 2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(1), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, null, null));

            // null left
            testIntersection("", asList(1, 2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(1), of(7)),
                             TrieSet.rangeExclusiveEnd(VERSION, null, of(3)));

            // null left contained
            testIntersection("", asList(1, 2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(1), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, null, of(7)));

            // null right
            testIntersection("", asList(1, 2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, of(1), null));

            // null right contained
            testIntersection("", asList(1, 2), trie,
                             TrieSet.rangeExclusiveEnd(VERSION, of(1), of(3)),
                             TrieSet.rangeExclusiveEnd(VERSION, of(0), null));
        }
    }

    @Test
    public void testSimpleRanges() throws TrieSpaceExhaustedException
    {
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

            testIntersection("", asList(3, 4, 5, 6), trie,
                             TrieSet.ranges(VERSION, of(3), of(7)));

            testIntersection("", asList(3), trie,
                             TrieSet.ranges(VERSION, of(3), of(4)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6), trie,
                             TrieSet.ranges(VERSION, null, of(7)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8, 9), trie,
                             TrieSet.ranges(VERSION, of(3), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie,
                             TrieSet.ranges(VERSION, null, null));

            testIntersection("", asList(3, 4, 5, 7, 8), trie,
                             TrieSet.ranges(VERSION, of(3), of(6), of(7), of(9)));

            testIntersection("", asList(3, 7, 8), trie,
                             TrieSet.ranges(VERSION, of(3), of(4), of(7), of(9)));

            testIntersection("", asList(3, 7, 8), trie,
                             TrieSet.ranges(VERSION, of(3), of(4), of(7), of(9), of(12), of(15)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 7, 8), trie,
                             TrieSet.ranges(VERSION, null, of(6), of(7), of(9)));

            testIntersection("", asList(3, 4, 5, 7, 8, 9), trie,
                             TrieSet.ranges(VERSION, of(3), of(6), of(7), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 7, 8, 9), trie,
                             TrieSet.ranges(VERSION, null, of(6), of(7), null));

            // Test some touching slices.
            testIntersection("", asList(3, 4, 5, 6, 7, 8), trie,
                             TrieSet.ranges(VERSION, of(3), of(6), of(6), of(9)));

            testIntersection("", asList(3, 4, 5, 7, 8), trie,
                             TrieSet.ranges(VERSION, of(3), of(6), of(6), of(6), of(7), of(9)));
        }
    }

    @Test
    public void testRangesOnRangesOne() throws TrieSpaceExhaustedException
    {
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

            // non-overlapping
            testIntersection("non-overlapping", asList(), trie,
                             TrieSet.ranges(VERSION, of(0), of(4)),
                             TrieSet.ranges(VERSION, of(4), of(8)));
            // touching
            testIntersection("touching", asList(3), trie,
                             TrieSet.ranges(VERSION, of(0), of(4)),
                             TrieSet.ranges(VERSION, of(3), of(8)));
            // overlapping 1
            testIntersection("overlapping A", asList(2, 3), trie,
                             TrieSet.ranges(VERSION, of(0), of(4)),
                             TrieSet.ranges(VERSION, of(2), of(8)));
            // overlapping 2
            testIntersection("overlapping B", asList(1, 2, 3), trie,
                             TrieSet.ranges(VERSION, of(0), of(4)),
                             TrieSet.ranges(VERSION, of(1), of(8)));
            // covered
            testIntersection("covered same end A", asList(0, 1, 2, 3), trie,
                             TrieSet.ranges(VERSION, of(0), of(4)),
                             TrieSet.ranges(VERSION, of(0), of(8)));
            // covered 2
            testIntersection("covered same end B", asList(4, 5, 6, 7), trie,
                             TrieSet.ranges(VERSION, of(4), of(8)),
                             TrieSet.ranges(VERSION, of(0), of(8)));
            // covered 3
            testIntersection("covered", asList(1, 2, 3), trie,
                             TrieSet.ranges(VERSION, of(1), of(4)),
                             TrieSet.ranges(VERSION, of(0), of(8)));
        }
    }

    @Test
    public void testRangesOnRanges() throws TrieSpaceExhaustedException
    {
        testIntersections(fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14));
    }

    @Test
    public void testRangesOnMerge() throws TrieSpaceExhaustedException
    {

        testIntersections(Trie.merge(ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                                      fromList(4, 6, 7, 9, 10, 11, 12, 13)),
                                     RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge2() throws TrieSpaceExhaustedException
    {
        {
            List<Trie<Integer>> inputs = ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                                          fromList(4, 6, 7, 9, 10, 11, 12, 13));
            testIntersections(dir -> new CollectionMergeCursor.Plain<>(RESOLVER, dir, inputs, Trie::cursor));
        }
    }

    @Test
    public void testRangesOnCollectionMerge3() throws TrieSpaceExhaustedException
    {
        testIntersections(Trie.merge(
            ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                             fromList(4, 6, 9, 10),
                             fromList(4, 7, 11, 12, 13)),
            RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge10() throws TrieSpaceExhaustedException
    {
        testIntersections(Trie.merge(
            ImmutableList.of(fromList(0, 14),
                             fromList(1, 2),
                             fromList(2, 13),
                             fromList(3),
                             fromList(4, 7),
                             fromList(5, 9, 12),
                             fromList(6, 8),
                             fromList(7),
                             fromList(8),
                             fromList(10, 11)),
            RESOLVER));
    }

    private void testIntersections(Trie<Integer> trie)
    {
        testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14), trie);

        TrieSet set1 = TrieSet.ranges(VERSION, null, of(4), of(5), of(9), of(12), null);
        TrieSet set2 = TrieSet.ranges(VERSION, of(2), of(7), of(8), of(10), of(12), of(14));
        TrieSet set3 = TrieSet.ranges(VERSION, of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));

        testIntersections(trie, set1, set2, set3);

        testSetAlgebraIntersection(trie);
    }

    private void testSetAlgebraIntersection(Trie<Integer> trie)
    {
        TrieSet set1 = TrieSet.rangeExclusiveEnd(VERSION, null, of(3))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(2), of(4)))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(5), of(7)))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(7), of(9)))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(14), of(15)))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(12), null));
        TrieSet set2 = TrieSet.rangeExclusiveEnd(VERSION, of(2), of(7))
                              .union(TrieSet.ranges(VERSION, null, of(8), of(10), null).negation())
                              .union(TrieSet.ranges(VERSION, of(8), of(10), of(12), of(14)));
        TrieSet set3 = TrieSet.rangeExclusiveEnd(VERSION, of(1), of(2))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(3), of(4)))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(5), of(6)))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(7), of(8)))
                              .union(TrieSet.rangeExclusiveEnd(VERSION, of(9), of(10)));

        testIntersections(trie, set1, set2, set3);
    }

    private void testIntersections(Trie<Integer> trie, TrieSet set1, TrieSet set2, TrieSet set3)
    {
        testIntersection("1", asList(0, 1, 2, 3, 5, 6, 7, 8, 12, 13, 14), trie, set1);

        testIntersection("2", asList(2, 3, 4, 5, 6, 8, 9, 12, 13), trie, set2);

        testIntersection("3", asList(1, 3, 5, 7, 9), trie, set3);

        testIntersection("12", asList(2, 3, 5, 6, 8, 12, 13), trie, set1, set2);

        testIntersection("13", asList(1, 3, 5, 7), trie, set1, set3);

        testIntersection("23", asList(3, 5, 9), trie, set2, set3);

        testIntersection("123", asList(3, 5), trie, set1, set2, set3);
    }

    public void testIntersection(String message, List<Integer> expected, Trie<Integer> trie, TrieSet... sets)
    {
        testIntersectionTries(message, expected, trie, sets);
        testIntersectionSets(message + " setix", expected, trie, TrieSet.rangeExclusiveEnd(VERSION, null, null), sets);
        testIntersectionTriesByRangeApplyTo(message + " applyTo", expected, trie, sets);
        testIntersectionTriesByMixed(message + " applyTo", expected, trie, sets);
        testIntersectionInMemoryTrieDelete(message + " delete", expected, trie, sets);
    }

    public void checkEqual(String message, List<Integer> expected, Trie<Integer> trie)
    {
        assertEquals(message + " forward", expected, toList(trie, Direction.FORWARD));
        assertEquals(message + " reverse", expected.stream()
                                                   .sorted(Comparator.<Integer>naturalOrder().reversed())
                                                   .collect(Collectors.toList()),
                     toList(trie, Direction.REVERSE));
    }

    public void testIntersectionSets(String message, List<Integer> expected, Trie<Integer> trie, TrieSet intersectedSet, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            checkEqual(message + " b" + bits, expected, trie.intersectSlicing(intersectedSet));
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersectionSets(message + ' ' + toRemove, expected,
                                     trie,
                                     intersectedSet.intersection(set),
                                     Arrays.stream(sets)
                                           .filter(x -> x != set)
                                           .toArray(TrieSet[]::new)
                );
            }
        }
    }

    public void testIntersectionTries(String message, List<Integer> expected, Trie<Integer> trie, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            checkEqual(message + " b" + bits, expected, trie);
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersectionTries(message + ' ' + toRemove, expected,
                                      trie.intersectSlicing(set),
                                      Arrays.stream(sets)
                                                .filter(x -> x != set)
                                                .toArray(TrieSet[]::new)
                );
            }
        }
    }

    public void testIntersectionTriesByMixed(String message, List<Integer> expected, Trie<Integer> trie, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            checkEqual(message + " b" + bits, expected, trie);
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                Trie<Integer> next;
                if (((toRemove ^ sets.length ^ bits) & 1) == 0)
                    next = applySet(set, trie);
                else
                    next = trie.intersectSlicing(set);

                testIntersectionTriesByMixed(message + ' ' + toRemove, expected,
                                             next,
                                             Arrays.stream(sets)
                                                   .filter(x -> x != set)
                                                   .toArray(TrieSet[]::new)
                );
            }
        }
    }

    public void testIntersectionTriesByRangeApplyTo(String message, List<Integer> expected, Trie<Integer> trie, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            checkEqual(message + " b" + bits, expected, trie);
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersectionTriesByRangeApplyTo(message + ' ' + toRemove, expected,
                                                    applySet(set, trie),
                                                    Arrays.stream(sets)
                                                          .filter(x -> x != set)
                                                          .toArray(TrieSet[]::new)
                );
            }
        }
    }

    private <T> Trie<T> applySet(TrieSet set, Trie<T> trie)
    {
        // Convert the set to a range trie of its negation to apply deletion to anything that is not in the set.
        // Do this by reinterpreting the cursor and avoiding verification
        // (instead of e.g. RangeTrie.fromSet(set, TrieSetCursor.RangeState.END_START_PREFIX)),
        // because some of the sets we use here are open and thus technically not valid range tries.
        RangeTrie<TrieSetCursor.RangeState> setAsRangeTrie = new RangeTrie<>()
        {
            @Override
            public RangeCursor<TrieSetCursor.RangeState> makeCursor(Direction direction)
            {
                throw new AssertionError();
            }

            @Override
            public RangeCursor<TrieSetCursor.RangeState> cursor(Direction direction)
            {
                // We are overriding cursor to disable debug verification (the source cursor is already checked by
                // TrieSet.cursor()).
                // We also want to negate the cursor in order to delete anything that is not contained in the set.
                return set.cursor(direction).negated();
            }
        };
        return setAsRangeTrie.applyTo(trie, (range, value) -> range.applicableAfter ? null : value);
    }

    private static InMemoryTrie<Integer> duplicateTrie(Trie<Integer> trie)
    {
        try
        {
            InMemoryTrie<Integer> dupe = InMemoryTrie.shortLivedOrdered(VERSION);
            dupe.apply(trie, (x, y) -> y, Predicates.alwaysFalse());
            return dupe;
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    public void testIntersectionInMemoryTrieDelete(String message, List<Integer> expected, Trie<Integer> trie, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            checkEqual(message + " b" + bits, expected, trie);
        }
        else
        {
            try
            {
                for (int toRemove = 0; toRemove < sets.length; ++toRemove)
                {
                    TrieSet set = sets[toRemove];
                    InMemoryTrie<Integer> ix = duplicateTrie(trie);
                    ix.delete(set.negation());
                    testIntersectionInMemoryTrieDelete(message + ' ' + toRemove, expected,
                                                       ix,
                                                       Arrays.stream(sets)
                                                             .filter(x -> x != set)
                                                             .toArray(TrieSet[]::new)
                    );
                }
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }
    }


    @Test
    public void testReturnsContentOnPrefix() throws TrieSpaceExhaustedException
    {
        TrieSet set = TrieSet.branch(VERSION, TrieUtil.directComparable("abc"));
        Trie<String> trie = TrieUtil.directTrie("a", "aa", "ab", "abc", "cd");
        Trie<String> expected = TrieUtil.directTrie("a", "ab", "abc");
        assertMapEquals(expected.entrySet(Direction.FORWARD), trie.intersect(set).entrySet(Direction.FORWARD), TrieUtil.FORWARD_COMPARATOR);
        assertMapEquals(expected.entrySet(Direction.REVERSE), trie.intersect(set).entrySet(Direction.REVERSE), TrieUtil.REVERSE_COMPARATOR);
        assertEquals(expected.process(Direction.FORWARD, new TrieDumper.Plain<>(Object::toString)), trie.intersect(set).dump());
    }

    @Test
    public void testReturnsBranchContents() throws TrieSpaceExhaustedException
    {
        TrieSet set = TrieSet.branch(VERSION, TrieUtil.directComparable("abc"));
        Trie<String> trie = TrieUtil.directTrie("aaa", "abc", "abce", "abcfff", "bcd");
        Trie<String> expected = TrieUtil.directTrie("abc", "abce", "abcfff");
        assertMapEquals(expected.entrySet(Direction.FORWARD), trie.intersect(set).entrySet(Direction.FORWARD), TrieUtil.FORWARD_COMPARATOR);
        assertMapEquals(expected.entrySet(Direction.REVERSE), trie.intersect(set).entrySet(Direction.REVERSE), TrieUtil.REVERSE_COMPARATOR);
        assertEquals(expected.process(Direction.FORWARD, new TrieDumper.Plain<>(Object::toString)), trie.intersect(set).dump());
    }

    @Test
    public void testRangeUnderCoveredBranch()
    {
        TrieSet set1 = TrieSet.branch(VERSION, TrieUtil.directComparable("b"));
        TrieSet set2 = TrieUtil.directRanges("aa", "ab", "bc", "bd", "ce", "cf");
        TrieSet expected = TrieUtil.directRanges("bc", "bd");
        assertEquals(expected.dump(), set1.intersection(set2).dump());
    }

    @Test
    public void testRangeUnderCoveredRoot()
    {
        TrieSet set1 = TrieSet.branch(VERSION, ByteComparable.EMPTY);
        TrieSet set2 = TrieUtil.directRanges("aa", "ab", "bc", "bd", "ce", "cf");
        TrieSet expected = set2;
        assertEquals(expected.dump(), set1.intersection(set2).dump());
    }
}
