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
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.addToInMemoryTrie;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryTrie;
import static org.apache.cassandra.db.tries.MergeTrieTest.removeDuplicates;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;

public class CollectionMergeTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static final int COUNT = 15000;
    private static final Random rand = new Random();

    @Test
    public void testDirect()
    {
        Preencoded[] src1 = TrieUtil.generateKeys(rand, COUNT);
        Preencoded[] src2 = TrieUtil.generateKeys(rand, COUNT);
        SortedMap<Preencoded, ByteBuffer> content1 = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        SortedMap<Preencoded, ByteBuffer> content2 = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);

        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);
        InMemoryTrie<ByteBuffer> trie2 = makeInMemoryTrie(src2, content2, true);

        content1.putAll(content2);
        // construct directly, trie.merge() will defer to mergeWith on two sources
        Trie<ByteBuffer> union = makeCollectionMergeTrie(trie1, trie2);

        TrieUtil.assertSameContent(union, content1);
    }

    @Test
    public void testWithDuplicates()
    {
        Preencoded[] src1 = TrieUtil.generateKeys(rand, COUNT);
        Preencoded[] src2 = TrieUtil.generateKeys(rand, COUNT);
        SortedMap<Preencoded, ByteBuffer> content1 = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        SortedMap<Preencoded, ByteBuffer> content2 = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);

        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);
        InMemoryTrie<ByteBuffer> trie2 = makeInMemoryTrie(src2, content2, true);

        addToInMemoryTrie(TrieUtil.generateKeys(new Random(5), COUNT), content1, trie1, true);
        addToInMemoryTrie(TrieUtil.generateKeys(new Random(5), COUNT), content2, trie2, true);

        content1.putAll(content2);
        Trie<ByteBuffer> union = makeCollectionMergeTrie(trie1, trie2);

        TrieUtil.assertSameContent(union, content1);
    }

    private static Trie<ByteBuffer> makeCollectionMergeTrie(InMemoryTrie<ByteBuffer>... tries)
    {
        return dir -> new CollectionMergeCursor.Plain<>(x -> x.iterator().next(), dir, List.of(tries), Trie::cursor);
    }

    @Test
    public void testDistinct()
    {
        Preencoded[] src1 = TrieUtil.generateKeys(rand, COUNT);
        SortedMap<Preencoded, ByteBuffer> content1 = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);

        Preencoded[] src2 = TrieUtil.generateKeys(rand, COUNT);
        src2 = removeDuplicates(src2, content1);
        SortedMap<Preencoded, ByteBuffer> content2 = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        InMemoryTrie<ByteBuffer> trie2 = makeInMemoryTrie(src2, content2, true);

        content1.putAll(content2);
        Trie<ByteBuffer> union = mergeDistinctTrie(ImmutableList.of(trie1, trie2));

        TrieUtil.assertSameContent(union, content1);
    }

    private static <T> Trie<T> mergeDistinctTrie(Collection<? extends Trie<T>> sources)
    {
        // This duplicates the code in the private Trie.mergeDistinctTrie
        return new Trie<T>()
        {
            @Override
            public Cursor<T> makeCursor(Direction direction)
            {
                return new CollectionMergeCursor.Plain<>(Trie.throwingResolver(), direction, sources, Trie::cursor);
            }

            @Override
            public Iterable<T> valuesUnordered()
            {
                return Iterables.concat(Iterables.transform(sources, Trie::valuesUnordered));
            }
        };
    }

    @Test
    public void testMultiple()
    {
        for (int i = 0; i < 10; ++i)
        {
            testMultiple(rand.nextInt(10) + 5, COUNT / 10);
        }
    }

    @Test
    public void testMerge1()
    {
        testMultiple(1, COUNT / 10);
    }

    @Test
    public void testMerge2()
    {
        testMultiple(2, COUNT / 10);
    }

    @Test
    public void testMerge3()
    {
        testMultiple(3, COUNT / 10);
    }

    @Test
    public void testMerge5()
    {
        testMultiple(5, COUNT / 10);
    }

    public void testMultiple(int mergeCount, int count)
    {
        testMultipleDistinct(mergeCount, count);
        testMultipleWithDuplicates(mergeCount, count);
    }

    public void testMultipleDistinct(int mergeCount, int count)
    {
        List<Trie<ByteBuffer>> tries = new ArrayList<>(mergeCount);
        SortedMap<Preencoded, ByteBuffer> content = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);

        for (int i = 0; i < mergeCount; ++i)
        {
            Preencoded[] src = removeDuplicates(TrieUtil.generateKeys(rand, count), content);
            Trie<ByteBuffer> trie = makeInMemoryTrie(src, content, true);
            tries.add(trie);
        }

        Trie<ByteBuffer> union = Trie.mergeDistinct(tries);
        TrieUtil.assertSameContent(union, content);
    }

    public void testMultipleWithDuplicates(int mergeCount, int count)
    {
        List<Trie<ByteBuffer>> tries = new ArrayList<>(mergeCount);
        SortedMap<Preencoded, ByteBuffer> content = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);

        for (int i = 0; i < mergeCount; ++i)
        {
            Preencoded[] src = TrieUtil.generateKeys(rand, count);
            Trie<ByteBuffer> trie = makeInMemoryTrie(src, content, true);
            tries.add(trie);
        }

        Trie<ByteBuffer> union = Trie.merge(tries, x -> x.iterator().next());
        TrieUtil.assertSameContent(union, content);

        try
        {
            union = Trie.mergeDistinct(tries);
            TrieUtil.assertSameContent(union, content);
            Assert.fail("Expected assertion error for duplicate keys.");
        }
        catch (AssertionError e)
        {
            // correct path
        }
    }
}
