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

import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.*;

public class MergeTrieTest
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

        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);
        InMemoryTrie<ByteBuffer> trie2 = makeInMemoryTrie(src2, content2, true);

        content1.putAll(content2);
        Trie<ByteBuffer> union = trie1.mergeWith(trie2, (x, y) -> x);

        assertSameContent(union, content1);
    }

    @Test
    public void testWithDuplicates()
    {
        ByteComparable[] src1 = generateKeys(rand, COUNT);
        ByteComparable[] src2 = generateKeys(rand, COUNT);
        SortedMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        SortedMap<ByteComparable, ByteBuffer> content2 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));

        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);
        InMemoryTrie<ByteBuffer> trie2 = makeInMemoryTrie(src2, content2, true);

        addToInMemoryTrie(generateKeys(new Random(5), COUNT), content1, trie1, true);
        addToInMemoryTrie(generateKeys(new Random(5), COUNT), content2, trie2, true);

        content1.putAll(content2);
        Trie<ByteBuffer> union = trie1.mergeWith(trie2, (x, y) -> y);

        assertSameContent(union, content1);
    }

    @Test
    public void testDistinct()
    {
        ByteComparable[] src1 = generateKeys(rand, COUNT);
        SortedMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);

        ByteComparable[] src2 = generateKeys(rand, COUNT);
        src2 = removeDuplicates(src2, content1);
        SortedMap<ByteComparable, ByteBuffer> content2 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        InMemoryTrie<ByteBuffer> trie2 = makeInMemoryTrie(src2, content2, true);

        content1.putAll(content2);
        Trie<ByteBuffer> union = new MergeTrie.Distinct<>(trie1, trie2);

        assertSameContent(union, content1);
    }

    static ByteComparable[] removeDuplicates(ByteComparable[] keys, SortedMap<ByteComparable, ByteBuffer> content1)
    {
        return Arrays.stream(keys)
                     .filter(key -> !content1.containsKey(key))
                     .toArray(ByteComparable[]::new);
    }
}
