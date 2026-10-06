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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import com.googlecode.concurrenttrees.common.Iterables;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryTrie;
import static org.apache.cassandra.db.tries.TrieUtil.FORWARD_COMPARATOR;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.assertSameContent;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;

public class SlicedTrieTest
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
                     .map(x -> ByteComparable.preencoded(VERSION, x.getBytes(StandardCharsets.UTF_8)))
                     .toArray(Preencoded[]::new);
    }

    static boolean isTruePrefix(ByteComparable b, ByteComparable key)
    {
        return ByteComparable.compare(b, key, VERSION) != 0 && key.byteComparableAsString(VERSION).startsWith(b.byteComparableAsString(VERSION));
    }

    @Test
    public void testSingletonSlice()
    {
        Arrays.sort(BOUNDARIES, (a, b) -> ByteComparable.compare(a, b, VERSION));
        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            Preencoded l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                Preencoded r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];
                if (l != null && r != null && (isTruePrefix(l, r) || isTruePrefix(r, l)))
                    continue; // prefixes in bounds are not supported

                for (int i = li == ri ? 3 : 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;

                    for (Preencoded key : KEYS)
                    {
                        int cmp1 = l != null ? !isTruePrefix(l, key) ? ByteComparable.compare(key, l, VERSION) : 0 : 1;
                        int cmp2 = r != null ? !isTruePrefix(r, key) ? ByteComparable.compare(r, key, VERSION) : 0 : 1;
                        Trie<Boolean> ix = Trie.singleton(key, VERSION, true).slice(l, includeLeft, r, includeRight);
                        boolean expected = true;
                        if (cmp1 < 0 || cmp1 == 0 && !includeLeft)
                            expected = false;
                        if (cmp2 < 0 || cmp2 == 0 && !includeRight)
                            expected = false;
                        boolean actual = com.google.common.collect.Iterables.getFirst(ix.values(), false);
                        if (expected != actual)
                        {
                            System.err.println("Intersection");
                            System.err.println(ix.dump());
                            Assert.fail(String.format("Failed on range %s%s,%s%s key %s expected %s got %s\n",
                                                      includeLeft ? "[" : "(",
                                                      l != null ? l.byteComparableAsString(VERSION) : null,
                                                      r != null ? r.byteComparableAsString(VERSION) : null,
                                                      includeRight ? "]" : ")",
                                                      key.byteComparableAsString(VERSION),
                                                      expected,
                                                      actual));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testMemtableSlice()
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
    public void testMergeSlice()
    {
        testMergeSlice(2);
    }

    @Test
    public void testCollectionMergeSlice3()
    {
        testMergeSlice(3);
    }

    @Test
    public void testCollectionMergeSlice5()
    {
        testMergeSlice(5);
    }

    public void testMergeSlice(int mergeCount)
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
        if (l != null && r != null && (isTruePrefix(l, r) || isTruePrefix(r, l)))
            return; // prefixes not supported in key bounds

        System.out.println(String.format("Intersection with %s%s:%s%s", includeLeft ? "[" : "(", asString(l), asString(r), includeRight ? "]" : ")"));
        SortedMap<Preencoded, ByteBuffer> imap = TrieUtil.boundedMap(content1, l, includeLeft, r, includeRight);
        Trie<ByteBuffer> intersection = t1.slice(l, includeLeft, r, includeRight);
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
        intersection = t1.slice(l, includeLeft, null, false).slice(null, false, r, includeRight);
        assertSameContent(intersection, imap);

        intersection = t1.slice(null, false, r, includeRight).slice(l, includeLeft, null, false);
        assertSameContent(intersection, imap);
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie, Direction direction)
    {
        return Iterables.toList(trie.values(direction));
    }

    /**
     * Creates a simple trie with a root having the provided number of childs, where each child is a leaf whose content
     * is simply the value of the transition leading to it.
     *
     * In other words, {@code singleLevelIntTrie(4)} creates the following trie:
     *       Root
     * t= 0  1  2  3
     *    |  |  |  |
     *    0  1  2  3
     */
    private static Trie<Integer> singleLevelIntTrie(int childs)
    {
        return new Trie<Integer>()
        {
            @Override
            public Cursor<Integer> makeCursor(Direction direction)
            {
                return new SingleLevelCursor(direction);
            }

            class SingleLevelCursor implements Cursor<Integer>
            {
                final Direction direction;
                int current;

                SingleLevelCursor(Direction direction)
                {
                    this.direction = direction;
                    current = direction.select(-1, childs);
                }

                @Override
                public int advance()
                {
                    current += direction.increase;
                    return depth();
                }

                @Override
                public int skipTo(int depth, int transition)
                {
                    if (depth > 1)
                        return advance();
                    if (depth < 1)
                        transition = direction.select(childs, -1);

                    if (direction.isForward())
                        current = Math.max(0, transition);
                    else
                        current = Math.min(childs - 1, transition);

                    return depth();
                }

                @Override
                public int depth()
                {
                    if (current == direction.select(-1, childs))
                        return 0;
                    if (direction.inLoop(current, 0, childs - 1))
                        return 1;
                    return -1;
                }

                @Override
                public int incomingTransition()
                {
                    return current >= childs ? -1 : current;
                }

                @Override
                public Integer content()
                {
                    return current == direction.select(-1, childs) ? -1 : current;
                }

                @Override
                public Direction direction()
                {
                    return direction;
                }

                @Override
                public ByteComparable.Version byteComparableVersion()
                {
                    return VERSION;
                }

                @Override
                public Cursor<Integer> tailCursor(Direction d)
                {
                    throw new UnsupportedOperationException("tailTrie on test cursor");
                }
            }
        };
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
    public void testSimpleIntersectionII()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(of(3), true, of(7), true);
        assertTrieEquals(asList(3, 4, 5, 6, 7), intersection);
    }

    @Test
    public void testSimpleIntersectionEI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(of(3), false, of(7), true);
        assertTrieEquals(asList(4, 5, 6, 7), intersection);
    }

    @Test
    public void testSimpleIntersectionIE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(of(3), true, of(7), false);
        assertTrieEquals(asList(3, 4, 5, 6), intersection);
    }

    @Test
    public void testSimpleIntersectionEE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(of(3), false, of(7), false);
        assertTrieEquals(asList(4, 5, 6), intersection);
    }

    @Test
    public void testSimpleLeftIntersectionE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(of(3), false, null, true);
        assertTrieEquals(asList(4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleLeftIntersectionI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(of(3), true, null, true);
        assertTrieEquals(asList(3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleRightIntersectionE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(null, true, of(7), false);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6), intersection);
    }

    @Test
    public void testSimpleRightIntersectionI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(null, true, of(7), true);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7), intersection);
    }

    @Test
    public void testSimpleNoIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(null, true, null, true);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);

        // The two boolean flags don't have a meaning when the bound does not exist. For completeness, also test
        // with them set to false.
        intersection = trie.slice(null, false, null, false);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleEmptyIntersectionLeft()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(ByteComparable.EMPTY, true, null, true);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);

        // Not currently supported
//        intersection = trie.slice(ByteComparable.EMPTY, false, null, true);
//        assertTrieEquals(asList(), intersection);
    }

    @Test
    public void testSimpleEmptyIntersectionRight()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.slice(null, true, ByteComparable.EMPTY, true);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);

        // Not currently supported
//        intersection = trie.slice(null, true, ByteComparable.EMPTY, false);
//        assertTrieEquals(asList(), intersection);

        intersection = trie.slice(ByteComparable.EMPTY, true, ByteComparable.EMPTY, true);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);

        // (empty, empty) is an invalid call as the "(empty" is greater than "empty)"
    }

    @Test
    public void testSubtrieOnSubtrie()
    {
        Trie<Integer> trie = singleLevelIntTrie(15);

        // non-overlapping
        ByteComparable left9 = of(0);
        Trie<Integer> integerTrie6 = trie.slice(left9, true, of(4), false);
        ByteComparable left10 = of(4);
        Trie<Integer> intersection = integerTrie6.slice(left10, true, of(8), false);
        assertTrieEquals(asList(), intersection);
        // touching
        Trie<Integer> integerTrie5 = trie.slice(of(0), true, of(3), true);
        ByteComparable left8 = of(3);
        intersection = integerTrie5.slice(left8, true, of(8), false);
        assertTrieEquals(asList(3), intersection);
        // overlapping 1
        ByteComparable left6 = of(0);
        Trie<Integer> integerTrie4 = trie.slice(left6, true, of(4), false);
        ByteComparable left7 = of(2);
        intersection = integerTrie4.slice(left7, true, of(8), false);
        assertTrieEquals(asList(2, 3), intersection);
        // overlapping 2
        ByteComparable left4 = of(0);
        Trie<Integer> integerTrie3 = trie.slice(left4, true, of(4), false);
        ByteComparable left5 = of(1);
        intersection = integerTrie3.slice(left5, true, of(8), false);
        assertTrieEquals(asList(1, 2, 3), intersection);
        // covered
        ByteComparable left2 = of(0);
        Trie<Integer> integerTrie2 = trie.slice(left2, true, of(4), false);
        ByteComparable left3 = of(0);
        intersection = integerTrie2.slice(left3, true, of(8), false);
        assertTrieEquals(asList(0, 1, 2, 3), intersection);
        // covered 2
        Trie<Integer> integerTrie1 = trie.slice(of(4), true, of(8), true);
        ByteComparable left1 = of(0);
        intersection = integerTrie1.slice(left1, true, of(8), false);
        assertTrieEquals(asList(4, 5, 6, 7), intersection);
        // covered 3
        Trie<Integer> integerTrie = trie.slice(of(1), false, of(4), true);
        ByteComparable left = of(0);
        intersection = integerTrie.slice(left, true, of(8), false);
        assertTrieEquals(asList(2, 3, 4), intersection);
    }
}
