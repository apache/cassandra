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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.assertSameContent;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryTrie;
import static org.apache.cassandra.db.tries.TrieUtil.FORWARD_COMPARATOR;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.apache.cassandra.db.tries.TrieUtil.singleLevelIntTrie;
import static org.apache.cassandra.db.tries.TrieUtil.toBound;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.EMPTY;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;

public class SubtrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    public static final Preencoded[] BOUNDARIES = toByteComparable(new String[]{
    "test1",
    "test11",
    "test12",
    "test13",
    "test2",
    "test21",
    "test",
    "te",
    "s",
    "q",
    "\000",
    "\377",
    "\377\000",
    "\000\377",
    "\000\000",
    "\000\000\000",
    "\000\000\377",
    "\377\377"
    });
    public static final Preencoded[] KEYS = toByteComparable(new String[]{
    "test1",
    "test2",
    "test55",
    "test123",
    "test124",
    "test12",
    "test21",
    "test",
    "te",
    "tease",
    "sort",
    "sorting",
    "square",
    "\377\000",
    "\000\377",
    "\000\000",
    "\000\000\000",
    "\000\000\377",
    "\377\377"
    });

    private static final int COUNT = 15000;
    Random rand = new Random();

    @Test
    public void testIntersectRangeDirect()
    {
        testIntersectRange(COUNT);
    }

    public void testIntersectRange(int count)
    {
        Preencoded[] src1 = generateKeys(rand, count);
        NavigableMap<Preencoded, ByteBuffer> content1 = new TreeMap<>(FORWARD_COMPARATOR);

        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);

        checkEqualRange(content1, trie1, null, true, null, true);
        checkEqualRange(content1, trie1, TrieUtil.generateKey(rand), true, null, true);
        checkEqualRange(content1, trie1, null, true, TrieUtil.generateKey(rand), true);
        for (int i = 0; i < 4; ++i)
        {
            Preencoded l = rand.nextBoolean() ? TrieUtil.generateKey(rand) : src1[rand.nextInt(src1.length)];
            Preencoded r = rand.nextBoolean() ? TrieUtil.generateKey(rand) : src1[rand.nextInt(src1.length)];
            int cmp = ByteComparable.compare(l, r, VERSION);
            if (cmp > 0)
            {
                Preencoded t = l;
                l = r;
                r = t; // swap
            }

            boolean includeLeft = (i & 1) != 0 || cmp == 0;
            boolean includeRight = (i & 2) != 0 || cmp == 0;
            checkEqualRange(content1, trie1, l, includeLeft, r, includeRight);
            checkEqualRange(content1, trie1, null, includeLeft, r, includeRight);
            checkEqualRange(content1, trie1, l, includeLeft, null, includeRight);
        }
    }

    private static Preencoded[] toByteComparable(String[] keys)
    {
        return Arrays.stream(keys)
                     .map(TrieUtil::comparable)
                     .toArray(Preencoded[]::new);
    }

    @Test
    public void testSingletonSubtrie()
    {
        testSingletonSubtrie(key -> Trie.singleton(key, VERSION, true));
    }

    @Test
    public void testSingletonSubtrieTailForward()
    {
        testSingletonSubtrie(key -> Trie.singleton(key, VERSION, true)
                                        .cursor(Direction.FORWARD)::tailCursor);
    }

    @Test
    public void testSingletonSubtrieTailReverse()
    {
        testSingletonSubtrie(key -> Trie.singleton(key, VERSION, true)
                                        .cursor(Direction.REVERSE)::tailCursor);
    }

    @Test
    public void testPrefixToEmptySubtrie()
    {
        testSingletonSubtrie(key -> Trie.singleton(EMPTY, VERSION, true).prefixedBy(key));
    }

    @Test
    public void testEmptyPrefixSubtrie()
    {
        testSingletonSubtrie(key -> Trie.singleton(key, VERSION, true).prefixedBy(EMPTY));
    }

    @Test
    public void testSplitPrefixSubtrie()
    {
        testSingletonSubtrie(key ->
                             {
                                 byte[] bytes = key.asByteComparableArray(VERSION);
                                 int cut = bytes.length / 2;
                                 Preencoded k1 = ByteComparable.preencoded(VERSION, bytes, 0, cut);
                                 Preencoded k2 = ByteComparable.preencoded(VERSION, bytes, cut, bytes.length - cut);
                                 return Trie.singleton(k2, VERSION, true).prefixedBy(k1);
                             });
    }

    public void testSingletonSubtrie(Function<ByteComparable, Trie<Boolean>> make)
    {
        Arrays.sort(BOUNDARIES, (a, b) -> ByteComparable.compare(a, b, VERSION));
        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            Preencoded l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                Preencoded r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];

                for (int i = li == ri ? 3 : 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;

                    for (Preencoded key : KEYS)
                    {
                        int cmp1 = l != null ? ByteComparable.compare(key, l, VERSION) : 1;
                        int cmp2 = r != null ? ByteComparable.compare(r, key, VERSION) : 1;
                        Trie<Boolean> ix = make.apply(key).subtrie(toBound(l, !includeLeft), toBound(r, includeRight));
                        boolean expected = true;
                        if (cmp1 < 0 || cmp1 == 0 && !includeLeft)
                            expected = false;
                        if (cmp2 < 0 || cmp2 == 0 && !includeRight)
                            expected = false;

                        try
                        {
                            assertEquals(expected, Iterables.getFirst(ix.values(), false));
                        }
                        catch (Throwable t)
                        {
                            System.err.println("Intersection");
                            System.err.println(ix.dump());
                            Assert.fail(String.format("Failed on range %s%s,%s%s key %s\n%s\n",
                                                      includeLeft ? "[" : "(",
                                                      l != null ? l.byteComparableAsString(VERSION) : null,
                                                      r != null ? r.byteComparableAsString(VERSION) : null,
                                                      includeRight ? "]" : ")",
                                                      key.byteComparableAsString(VERSION),
                                                      t));
                        }

                        try
                        {
                            assertEquals(expected, Iterables.getFirst(ix.values(Direction.REVERSE), false));
                        }
                        catch (Throwable t)
                        {
                            System.err.println("Intersection REV");
                            System.err.println(ix.cursor(Direction.REVERSE).process(new TrieDumper.Plain<>(Object::toString)));
                            Assert.fail(String.format("Failed on range %s%s,%s%s REV key %s\n%s\n",
                                                      includeLeft ? "[" : "(",
                                                      l != null ? l.byteComparableAsString(VERSION) : null,
                                                      r != null ? r.byteComparableAsString(VERSION) : null,
                                                      includeRight ? "]" : ")",
                                                      key.byteComparableAsString(VERSION),
                                                      t));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testMemtableSubtrie()
    {
        Arrays.sort(BOUNDARIES, FORWARD_COMPARATOR);
        NavigableMap<Preencoded, ByteBuffer> content1 = new TreeMap<>(FORWARD_COMPARATOR);
        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(KEYS, content1, true);

        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            Preencoded l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                Preencoded r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];
                for (int i = 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;
                    if ((!includeLeft || !includeRight) && li == ri)
                        continue;
                    checkEqualRange(content1, trie1, l, includeLeft, r, includeRight);
                }
            }
        }
    }

    @Test
    public void testMergeSubtrie()
    {
        testMergeSubtrie(2);
    }

    @Test
    public void testCollectionMergeSubtrie3()
    {
        testMergeSubtrie(3);
    }

    @Test
    public void testCollectionMergeSubtrie5()
    {
        testMergeSubtrie(5);
    }

    public void testMergeSubtrie(int mergeCount)
    {
        Arrays.sort(BOUNDARIES, FORWARD_COMPARATOR);
        NavigableMap<Preencoded, ByteBuffer> content1 = new TreeMap<>(FORWARD_COMPARATOR);
        List<Trie<ByteBuffer>> tries = new ArrayList<>();
        for (int i = 0; i < mergeCount; ++i)
        {
            tries.add(makeInMemoryTrie(Arrays.copyOfRange(KEYS,
                                                           KEYS.length * i / mergeCount,
                                                           KEYS.length * (i + 1) / mergeCount),
                                       content1,
                                       true));
        }
        Trie<ByteBuffer> trie1 = Trie.mergeDistinct(tries);

        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            Preencoded l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                Preencoded r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];
                for (int i = 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;
                    if ((!includeLeft || !includeRight) && li == ri)
                        continue;
                    checkEqualRange(content1, trie1, l, includeLeft, r, includeRight);
                }
            }
        }
    }

    public void checkEqualRange(NavigableMap<Preencoded, ByteBuffer> content1,
                                Trie<ByteBuffer> t1,
                                Preencoded l,
                                boolean includeLeft,
                                Preencoded r,
                                boolean includeRight)
    {
        System.out.println(String.format("Intersection with %s%s:%s%s", includeLeft ? "[" : "(", asString(l), asString(r), includeRight ? "]" : ")"));
        SortedMap<Preencoded, ByteBuffer> imap = TrieUtil.boundedMap(content1, l, includeLeft, r, includeRight);
        Trie<ByteBuffer> intersection = t1.subtrie(toBound(l, !includeLeft), toBound(r, includeRight));
        try
        {
            assertSameContent(intersection, imap);
        }
        catch (AssertionError e)
        {
            System.out.println("\n" + t1.dump(ByteBufferUtil::bytesToHex));

            System.out.println("\n" + intersection.dump(ByteBufferUtil::bytesToHex));
            throw e;
        }

        if (l == null || r == null)
            return;

        // Test intersecting intersection.
        intersection = t1.subtrie(toBound(l, !includeLeft), null).subtrie(null, toBound(r, includeRight));
        assertSameContent(intersection, imap);

        intersection = t1.subtrie(null, toBound(r, includeRight)).subtrie(toBound(l, !includeLeft), null);
        assertSameContent(intersection, imap);
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie, Direction direction)
    {
        return Streams.stream(trie.values(direction)).collect(Collectors.toList());
    }

    /** Creates a single byte {@link ByteComparable} with the provide value */
    private static ByteComparable of(int value)
    {
        assert value >= 0 && value <= Byte.MAX_VALUE;
        return ByteComparable.preencoded(VERSION, new byte[]{ (byte)value });
    }

    List<Integer> maybeReversed(Direction direction, List<Integer> list)
    {
        if (direction.isForward())
            return list;
        List<Integer> reversed = new ArrayList<>(list);
        reversed.sort((x, y) -> x == -1 ? -1 : y == -1 ? 1 : Integer.compare(y, x));
        return reversed;
    }

    void assertTrieEquals(List<Integer> expected, Trie<Integer> trie)
    {
        assertEquals(expected, toList(trie, Direction.FORWARD));
        assertEquals(maybeReversed(Direction.REVERSE, expected), toList(trie, Direction.REVERSE));
    }

    @Test
    public void testSimpleIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10, false);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(of(3), of(7));
        assertTrieEquals(asList(-1, 3, 4, 5, 6, 7), intersection);
    }

    @Test
    public void testSimpleLeftIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10, false);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(of(3), null);
        assertTrieEquals(asList(-1, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleRightIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10, false);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(null, of(7));
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7), intersection);
    }

    @Test
    public void testSimpleNoIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10, false);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(null, null);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleEmptyIntersectionLeft()
    {
        Trie<Integer> trie = singleLevelIntTrie(10, false);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(ByteComparable.EMPTY, null);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);

        intersection = trie.subtrie(ByteComparable.EMPTY, ByteComparable.EMPTY);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleEmptyIntersectionRight()
    {
        Trie<Integer> trie = singleLevelIntTrie(10, false);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(null, ByteComparable.EMPTY);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);

        intersection = trie.subtrie(ByteComparable.EMPTY, ByteComparable.EMPTY);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSubtrieOnSubtrie()
    {
        Trie<Integer> trie = singleLevelIntTrie(15, false);

        // non-overlapping
        Trie<Integer> intersection = trie.subtrie(of(0), of(4)).subtrie(of(5), of(8));
        assertTrieEquals(asList(-1), intersection);
        // touching
        intersection = trie.subtrie(of(0), of(3)).subtrie(of(3), of(8));
        assertTrieEquals(asList(-1, 3), intersection);
        // overlapping 1
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(2), of(8));
        assertTrieEquals(asList(-1, 2, 3, 4), intersection);
        // overlapping 2
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(1), of(8));
        assertTrieEquals(asList(-1, 1, 2, 3, 4), intersection);
        // covered
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(0), of(8));
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4), intersection);
        // covered 2
        intersection = trie.subtrie(of(4), of(8)).subtrie(of(0), of(8));
        assertTrieEquals(asList(-1, 4, 5, 6, 7, 8), intersection);
    }

    @Test
    public void testIntersectedIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(15, false);

        // non-overlapping
        Trie<Integer> intersection = trie.intersect(TrieSet.rangeInclusiveEnd(VERSION, of(0), of(4)))
                                         .intersect(TrieSet.rangeInclusiveEnd(VERSION, of(5), of(8)));
        assertTrieEquals(asList(-1), intersection);
        // touching
        intersection = trie.intersect(TrieSet.rangeInclusiveEnd(VERSION, of(0), of(3)))
                           .intersect(TrieSet.rangeInclusiveEnd(VERSION, of(3), of(8)));
        assertTrieEquals(asList(-1, 3), intersection);
        // overlapping 1
        intersection = trie.intersect(TrieSet.rangeInclusiveEnd(VERSION, of(0), of(4)))
                           .intersect(TrieSet.rangeInclusiveEnd(VERSION, of(2), of(8)));
        assertTrieEquals(asList(-1, 2, 3, 4), intersection);
        // overlapping 2
        intersection = trie.intersect(TrieSet.rangeInclusiveEnd(VERSION, of(0), of(4)))
                           .intersect(TrieSet.rangeInclusiveEnd(VERSION, of(1), of(8)));
        assertTrieEquals(asList(-1, 1, 2, 3, 4), intersection);
        // covered
        intersection = trie.intersect(TrieSet.rangeInclusiveEnd(VERSION, of(0), of(4)))
                           .intersect(TrieSet.rangeInclusiveEnd(VERSION, of(0), of(8)));
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4), intersection);
        // covered 2
        intersection = trie.intersect(TrieSet.rangeInclusiveEnd(VERSION, of(4), of(8)))
                           .intersect(TrieSet.rangeInclusiveEnd(VERSION, of(0), of(8)));
        assertTrieEquals(asList(-1, 4, 5, 6, 7, 8), intersection);
    }
}
