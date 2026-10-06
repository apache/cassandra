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
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.addToInMemoryTrie;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.assertSameContent;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryTrie;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;

public class MergeTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static final int COUNT = 15000;
    Random rand = new Random();

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
        Trie<ByteBuffer> union = trie1.mergeWith(trie2, (x, y) -> x);

        assertSameContent(union, content1);
    }

    @Test
    public void testWithDuplicates()
    {
        Preencoded[] src1 = TrieUtil.generateKeys(rand, COUNT);
        Preencoded[] src2 = TrieUtil.generateKeys(rand, COUNT);
        SortedMap<Preencoded, ByteBuffer> content1 = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        SortedMap<Preencoded, ByteBuffer> content2 = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);

        InMemoryTrie trie1 = makeInMemoryTrie(src1, content1, true);
        InMemoryTrie trie2 = makeInMemoryTrie(src2, content2, true);

        addToInMemoryTrie(TrieUtil.generateKeys(new Random(5), COUNT), content1, trie1, true);
        addToInMemoryTrie(TrieUtil.generateKeys(new Random(5), COUNT), content2, trie2, true);

        content1.putAll(content2);
        Trie<ByteBuffer> union = trie1.mergeWith(trie2, (x, y) -> y);

        TrieUtil.assertSameContent(union, content1);
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
        Trie<ByteBuffer> union = Trie.mergeDistinct(trie1, trie2);

        TrieUtil.assertSameContent(union, content1);
    }

    static Preencoded[] removeDuplicates(Preencoded[] keys, SortedMap<Preencoded, ByteBuffer> content1)
    {
        return Arrays.stream(keys)
                     .filter(key -> !content1.containsKey(key))
                     .toArray(Preencoded[]::new);
    }
}
