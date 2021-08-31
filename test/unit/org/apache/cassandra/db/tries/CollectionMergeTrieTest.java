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
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.MemtableTrieTestBase.*;
import static org.apache.cassandra.db.tries.MergeTrieTest.removeDuplicates;

public class CollectionMergeTrieTest
{
    private static final int COUNT = 15000;
    Random rand = new Random();

    @Test
    public void testDirect()
    {
        ByteComparable[] src1 = generateKeys(rand, COUNT);
        ByteComparable[] src2 = generateKeys(rand, COUNT);
        SortedMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        SortedMap<ByteComparable, ByteBuffer> content2 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));

        MemtableTrie<ByteBuffer> trie1 = makeMemtableTrie(src1, content1, true);
        MemtableTrie<ByteBuffer> trie2 = makeMemtableTrie(src2, content2, true);

        content1.putAll(content2);
        // construct directly, trie.merge() will defer to mergeWith on two sources
        Trie<ByteBuffer> union = new CollectionMergeTrie<>(ImmutableList.of(trie1, trie2), x -> x.iterator().next());

        assertSameContent(union, content1);
    }

    @Test
    public void testWithDuplicates()
    {
        ByteComparable[] src1 = generateKeys(rand, COUNT);
        ByteComparable[] src2 = generateKeys(rand, COUNT);
        SortedMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        SortedMap<ByteComparable, ByteBuffer> content2 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));

        MemtableTrie<ByteBuffer> trie1 = makeMemtableTrie(src1, content1, true);
        MemtableTrie<ByteBuffer> trie2 = makeMemtableTrie(src2, content2, true);

        addToMemtableTrie(generateKeys(new Random(5), COUNT), content1, trie1, true);
        addToMemtableTrie(generateKeys(new Random(5), COUNT), content2, trie2, true);

        content1.putAll(content2);
        Trie<ByteBuffer> union = new CollectionMergeTrie<>(ImmutableList.of(trie1, trie2), x -> x.iterator().next());

        assertSameContent(union, content1);
    }

    @Test
    public void testDistinct()
    {
        ByteComparable[] src1 = generateKeys(rand, COUNT);
        SortedMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        MemtableTrie<ByteBuffer> trie1 = makeMemtableTrie(src1, content1, true);

        ByteComparable[] src2 = generateKeys(rand, COUNT);
        src2 = removeDuplicates(src2, content1);
        SortedMap<ByteComparable, ByteBuffer> content2 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        MemtableTrie<ByteBuffer> trie2 = makeMemtableTrie(src2, content2, true);

        content1.putAll(content2);
        Trie<ByteBuffer> union = new CollectionMergeTrie.Distinct<>(ImmutableList.of(trie1, trie2));

        assertSameContent(union, content1);
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

    @Test
    public void testMerge0()
    {
        testMultiple(0, COUNT / 10);
    }

    public void testMultiple(int mergeCount, int count)
    {
        testMultipleDistinct(mergeCount, count);
        testMultipleWithDuplicates(mergeCount, count);
    }

    public void testMultipleDistinct(int mergeCount, int count)
    {
        List<Trie<ByteBuffer>> tries = new ArrayList<>(mergeCount);
        SortedMap<ByteComparable, ByteBuffer> content = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));

        for (int i = 0; i < mergeCount; ++i)
        {
            ByteComparable[] src = removeDuplicates(generateKeys(rand, count), content);
            Trie<ByteBuffer> trie = makeMemtableTrie(src, content, true);
            tries.add(trie);
        }

        Trie<ByteBuffer> union = Trie.mergeDistinct(tries);

        assertSameContent(union, content);
    }

    public void testMultipleWithDuplicates(int mergeCount, int count)
    {
        List<Trie<ByteBuffer>> tries = new ArrayList<>(mergeCount);
        SortedMap<ByteComparable, ByteBuffer> content = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));

        for (int i = 0; i < mergeCount; ++i)
        {
            ByteComparable[] src = generateKeys(rand, count);
            Trie<ByteBuffer> trie = makeMemtableTrie(src, content, true);
            tries.add(trie);
        }

        Trie<ByteBuffer> union = Trie.merge(tries, x -> x.iterator().next());

        assertSameContent(union, content);
    }
}
