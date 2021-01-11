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
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.MemtableTrieTestBase.asString;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.assertMapEquals;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.assertSameContent;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.generateKeys;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.makeMemtableTrie;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class SetIntersectionTrieTest
{
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

        MemtableTrie<ByteBuffer> trie1 = makeMemtableTrie(src1, content1, true);

        checkEqualRange(content1, trie1, null, true, null, true);
        checkEqualRange(content1, trie1, MemtableTrieTestBase.generateKey(rand), true, null, true);
        checkEqualRange(content1, trie1, null, true, MemtableTrieTestBase.generateKey(rand), true);
        for (int i = 0; i < 4; ++i)
        {
            ByteComparable l = rand.nextBoolean() ? MemtableTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
            ByteComparable r = rand.nextBoolean() ? MemtableTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
            int cmp = ByteComparable.compare(l, r, Trie.BYTE_COMPARABLE_VERSION);
            if (cmp > 0)
            {
                ByteComparable t = l;l = r;r = t; // swap
            }

            boolean includeLeft = (i & 1) != 0;
            boolean includeRight = (i & 2) != 0;
            if (!includeLeft && !includeRight && cmp == 0)
                includeRight = true;
            checkEqualRange(content1, trie1, l, includeLeft, r, includeRight);
        }
    }

    public void checkEqualRange(NavigableMap<ByteComparable, ByteBuffer> content1,
                                Trie<ByteBuffer> t1,
                                ByteComparable l,
                                boolean includeLeft,
                                ByteComparable r,
                                boolean includeRight)
    {
        System.out.format("Intersection with %s%s:%s%s\n", includeLeft ? "[" : "(", asString(l), asString(r), includeRight ? "]" : ")");
        SortedMap<ByteComparable, ByteBuffer> imap = l == null
                                                     ? r == null
                                                       ? content1
                                                       : content1.headMap(r, includeRight)
                                                     : r == null
                                                       ? content1.tailMap(l, includeLeft)
                                                       : content1.subMap(l, includeLeft, r, includeRight);
        Trie<ByteBuffer> intersection = t1.subtrie(l, includeLeft, r, includeRight);

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
            protected <L> Node<Integer, L> root()
            {
                return new RootNode<>();
            }

            /** Root node of the trie: has {@code childs} transition, each leading to a {@link LeafNode} whose content
             * is the value of the transition. */
            class RootNode<L> extends Node<Integer, L>
            {
                RootNode()
                {
                    super(null);
                    currentTransition = 0;
                }

                public Remaining startIteration()
                {
                    currentTransition = 0;
                    return childs == 0 ? null : (childs == 1 ? Remaining.ONE : Remaining.MULTIPLE);
                }

                public Remaining advanceIteration()
                {
                    return ++currentTransition >= childs ? null : Remaining.MULTIPLE;
                }

                public Node<Integer, L> getCurrentChild(L parent)
                {
                    return new LeafNode<>(parent, currentTransition);
                }

                public Integer content()
                {
                    return null;
                }

                @Override
                public String toString()
                {
                    return String.format("ROOT(t=%s, parent=%s)", currentTransition, parentLink);
                }
            }

            /** Leaf nodes: no children but a content corresponding ot the transition leading to them */
            class LeafNode<L> extends NoChildrenNode<Integer, L>
            {
                private final int value;

                LeafNode(L parent, int value)
                {
                    super(parent);
                    this.value = value;
                }

                public Integer content()
                {
                    return value;
                }

                @Override
                public String toString()
                {
                    return String.format("LEAF(%d, parent=%s)", value, parentLink);
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
    public void testSimpleIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), true, of(7), true);
        // Currently returns [4, 5, 6, 7, 8, 9], which "looks" wrong.
        assertEquals(asList(3, 4, 5, 6, 7), toList(intersection));
    }
}
