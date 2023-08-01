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
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.asString;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.assertSameContent;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.generateKeys;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryTrie;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class SlicedTrieTest
{
    public static final ByteComparable[] BOUNDARIES = toByteComparable(new String[]{
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
    "\777",
    "\777\000",
    "\000\777",
    "\000\000",
    "\000\000\000",
    "\000\000\777",
    "\777\777"
    });
    public static final ByteComparable[] KEYS = toByteComparable(new String[]{
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
    "\777\000",
    "\000\777",
    "\000\000",
    "\000\000\000",
    "\000\000\777",
    "\777\777"
    });
    public static final Comparator<ByteComparable> BYTE_COMPARABLE_COMPARATOR = (bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, Trie.BYTE_COMPARABLE_VERSION);
    private static final int COUNT = 15000;
    Random rand = new Random();

    @Test
    public void testIntersectRangeDirect()
    {
        testIntersectRange(COUNT);
    }

    public void testIntersectRange(int count)
    {
        ByteComparable[] src1 = generateKeys(rand, count);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, Trie.BYTE_COMPARABLE_VERSION));

        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);

        checkEqualRange(content1, trie1, null, true, null, true);
        checkEqualRange(content1, trie1, InMemoryTrieTestBase.generateKey(rand), true, null, true);
        checkEqualRange(content1, trie1, null, true, InMemoryTrieTestBase.generateKey(rand), true);
        for (int i = 0; i < 4; ++i)
        {
            ByteComparable l = rand.nextBoolean() ? InMemoryTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
            ByteComparable r = rand.nextBoolean() ? InMemoryTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
            int cmp = ByteComparable.compare(l, r, Trie.BYTE_COMPARABLE_VERSION);
            if (cmp > 0)
            {
                ByteComparable t = l;
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

    private static ByteComparable[] toByteComparable(String[] keys)
    {
        return Arrays.stream(keys)
                     .map(x -> ByteComparable.fixedLength(x.getBytes(StandardCharsets.UTF_8)))
                     .toArray(ByteComparable[]::new);
    }

    @Test
    public void testSingletonSubtrie()
    {
        Arrays.sort(BOUNDARIES, (a, b) -> ByteComparable.compare(a, b, ByteComparable.Version.OSS50));
        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            ByteComparable l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                ByteComparable r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];

                for (int i = li == ri ? 3 : 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;

                    for (ByteComparable key : KEYS)
                    {
                        int cmp1 = l != null ? ByteComparable.compare(key, l, ByteComparable.Version.OSS50) : 1;
                        int cmp2 = r != null ? ByteComparable.compare(r, key, ByteComparable.Version.OSS50) : 1;
                        Trie<Boolean> ix = new SlicedTrie<>(Trie.singleton(key, true), l, includeLeft, r, includeRight);
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
                                                      l != null ? l.byteComparableAsString(ByteComparable.Version.OSS50) : null,
                                                      r != null ? r.byteComparableAsString(ByteComparable.Version.OSS50) : null,
                                                      includeRight ? "]" : ")",
                                                      key.byteComparableAsString(ByteComparable.Version.OSS50),
                                                      expected,
                                                      actual));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testMemtableSubtrie()
    {
        Arrays.sort(BOUNDARIES, BYTE_COMPARABLE_COMPARATOR);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(KEYS, content1, true);

        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            ByteComparable l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                ByteComparable r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];
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
        Arrays.sort(BOUNDARIES, BYTE_COMPARABLE_COMPARATOR);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
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
            ByteComparable l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                ByteComparable r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];
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

    public void checkEqualRange(NavigableMap<ByteComparable, ByteBuffer> content1,
                                Trie<ByteBuffer> t1,
                                ByteComparable l,
                                boolean includeLeft,
                                ByteComparable r,
                                boolean includeRight)
    {
        System.out.println(String.format("Intersection with %s%s:%s%s", includeLeft ? "[" : "(", asString(l), asString(r), includeRight ? "]" : ")"));
        SortedMap<ByteComparable, ByteBuffer> imap = l == null
                                                     ? r == null
                                                       ? content1
                                                       : content1.headMap(r, includeRight)
                                                     : r == null
                                                       ? content1.tailMap(l, includeLeft)
                                                       : content1.subMap(l, includeLeft, r, includeRight);

        Trie<ByteBuffer> intersection = t1.subtrie(l, includeLeft, r, includeRight);
        assertSameContent(intersection, imap);

        if (l == null || r == null)
            return;

        // Test intersecting intersection.
        intersection = t1.subtrie(l, includeLeft, null, false).subtrie(null, false, r, includeRight);
        assertSameContent(intersection, imap);

        intersection = t1.subtrie(null, false, r, includeRight).subtrie(l, includeLeft, null, false);
        assertSameContent(intersection, imap);
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie)
    {
        return Iterables.toList(trie.values());
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
            protected Cursor<Integer> cursor()
            {
                return new singleLevelCursor();
            }

            class singleLevelCursor implements Cursor<Integer>
            {
                int current = -1;

                @Override
                public int advance()
                {
                    ++current;
                    return depth();
                }

                @Override
                public int skipChildren()
                {
                    return advance();
                }

                @Override
                public int depth()
                {
                    if (current == -1)
                        return 0;
                    if (current < childs)
                        return 1;
                    return -1;
                }

                @Override
                public int incomingTransition()
                {
                    return current;
                }

                @Override
                public Integer content()
                {
                    return current;
                }
            }
        };
    }

    /** Creates a single byte {@link ByteComparable} with the provide value */
    private static ByteComparable of(int value)
    {
        assert value >= 0 && value <= Byte.MAX_VALUE;
        return ByteComparable.fixedLength(new byte[]{ (byte)value });
    }

    @Test
    public void testSimpleIntersectionII()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), true, of(7), true);
        assertEquals(asList(3, 4, 5, 6, 7), toList(intersection));
    }

    @Test
    public void testSimpleIntersectionEI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), false, of(7), true);
        assertEquals(asList(4, 5, 6, 7), toList(intersection));
    }

    @Test
    public void testSimpleIntersectionIE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), true, of(7), false);
        assertEquals(asList(3, 4, 5, 6), toList(intersection));
    }

    @Test
    public void testSimpleIntersectionEE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), false, of(7), false);
        assertEquals(asList(4, 5, 6), toList(intersection));
    }

    @Test
    public void testSimpleLeftIntersectionE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), false, null, true);
        assertEquals(asList(4, 5, 6, 7, 8, 9), toList(intersection));
    }

    @Test
    public void testSimpleLeftIntersectionI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), true, null, true);
        assertEquals(asList(3, 4, 5, 6, 7, 8, 9), toList(intersection));
    }

    @Test
    public void testSimpleRightIntersectionE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(null, true, of(7), false);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6), toList(intersection));
    }

    @Test
    public void testSimpleRightIntersectionI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(null, true, of(7), true);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7), toList(intersection));
    }

    @Test
    public void testSimpleNoIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(null, true, null, true);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(intersection));

        // The two boolean flags don't have a meaning when the bound does not exist. For completeness, also test
        // with them set to false.
        intersection = trie.subtrie(null, false, null, false);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(intersection));
    }

    @Test
    public void testSimpleEmptyIntersectionLeft()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(ByteComparable.EMPTY, true, null, true);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, false, null, true);
        assertEquals(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, true, of(5), true);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, false, of(5), true);
        assertEquals(asList(0, 1, 2, 3, 4, 5), toList(intersection));

    }

    @Test
    public void testSimpleEmptyIntersectionRight()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(null, true, ByteComparable.EMPTY, true);
        assertEquals(asList(-1), toList(intersection));

        intersection = trie.subtrie(null, true, ByteComparable.EMPTY, false);
        assertEquals(asList(), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, true, ByteComparable.EMPTY, true);
        assertEquals(asList(-1), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, false, ByteComparable.EMPTY, true);
        assertEquals(asList(), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, true, ByteComparable.EMPTY, false);
        assertEquals(asList(), toList(intersection));

        // (empty, empty) is an invalid call as the "(empty" is greater than "empty)"
    }

    @Test
    public void testSubtrieOnSubtrie()
    {
        Trie<Integer> trie = singleLevelIntTrie(15);

        // non-overlapping
        Trie<Integer> intersection = trie.subtrie(of(0), of(4)).subtrie(of(4), of(8));
        assertEquals(asList(), toList(intersection));
        // touching
        intersection = trie.subtrie(of(0), true, of(3), true).subtrie(of(3), of(8));
        assertEquals(asList(3), toList(intersection));
        // overlapping 1
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(2), of(8));
        assertEquals(asList(2, 3), toList(intersection));
        // overlapping 2
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(1), of(8));
        assertEquals(asList(1, 2, 3), toList(intersection));
        // covered
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(0), of(8));
        assertEquals(asList(0, 1, 2, 3), toList(intersection));
        // covered 2
        intersection = trie.subtrie(of(4), true, of(8), true).subtrie(of(0), of(8));
        assertEquals(asList(4, 5, 6, 7), toList(intersection));
        // covered 3
        intersection = trie.subtrie(of(1), false, of(4), true).subtrie(of(0), of(8));
        assertEquals(asList(2, 3, 4), toList(intersection));
    }
}
