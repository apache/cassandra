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
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;

public class IntersectedTailsTest
{
    static final BiFunction<TrieSetCursor.RangeState, ByteBuffer, ByteBuffer> applyDeletion = (d, v) -> d.applicableAfter ? null : v;
    @SuppressWarnings("rawtypes")
    static final Trie.CollectionMergeResolver onlySame = x ->
    {
        if (x.stream().distinct().count() != 1)
            throw new AssertionError();
        return x.iterator().next();
    };

    @BeforeClass
    public static void setup()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
        InMemoryTrieTestBase.strategy = InMemoryTrieTestBase.ReuseStrategy.SHORT_LIVED_ORDERED;
        InMemoryTrieTestBase.reverseComparator = InMemoryTrieTestBase.forwardComparator.reversed();
    }

    @Test
    public void testRangeApplyCursor()
    {
        testIntersectedTails((trie, set) ->
                             {
                                 RangeTrie<TrieSetCursor.RangeState> setAsRangeTrie = RangeTrie.fromSet(set, TrieSetCursor.RangeState.CONTAINED);
                                 return setAsRangeTrie.applyTo(trie,
                                                               applyDeletion);
                             });
    }


    @Test
    public void testIntersection()
    {
        testIntersectedTails((trie, set) -> trie.intersectSlicing(set.negation()));
    }

    @Test
    public void testDeletionAwareMerge()
    {
        testIntersectedTails((trie, set) ->
                             {
                                 RangeTrie<TrieSetCursor.RangeState> setAsRangeTrie = RangeTrie.fromSet(set, TrieSetCursor.RangeState.CONTAINED);
                                 DeletionAwareTrie<ByteBuffer, TrieSetCursor.RangeState> data = DeletionAwareTrie.wrap(trie);
                                 DeletionAwareTrie<ByteBuffer, TrieSetCursor.RangeState> deletions = DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, VERSION, setAsRangeTrie);
                                 return data.mergeWith(deletions, DeletionAwareTrie.throwingResolver(), DeletionAwareTrie.throwingResolver(), applyDeletion, true)
                                            .contentOnlyTrie();
                             });
    }

    @Test
    public void testDeletionAwareCollectionMerge()
    {
        testIntersectedTails((trie, set) ->
                             {
                                 RangeTrie<TrieSetCursor.RangeState> setAsRangeTrie = RangeTrie.fromSet(set, TrieSetCursor.RangeState.CONTAINED);
                                 DeletionAwareTrie<ByteBuffer, TrieSetCursor.RangeState> data = DeletionAwareTrie.wrap(trie);
                                 DeletionAwareTrie<ByteBuffer, TrieSetCursor.RangeState> deletions = DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, VERSION, setAsRangeTrie);
                                 //noinspection unchecked
                                 return DeletionAwareTrie.merge(List.of(data, deletions, data, deletions),
                                                                onlySame,
                                                                onlySame,
                                                                applyDeletion,
                                                                true)
                                            .contentOnlyTrie();
                             });
    }

    public void testIntersectedTails(BiFunction<Trie<ByteBuffer>, TrieSet, Trie<ByteBuffer>> deleter)
    {
        String[] data = new String[]{ "abc", "acd", "ade", "adgh", "adhi", "adij",
                                      "bbc", "bcd", "bde", "bdgh", "bdhi", "bdij",
                                      "dbc", "dcd", "dde", "ddgh", "ddhi", "ddij",
                                      "ebc", "ecd", "ede", "edgh", "edhi", "edij",
                                      "gbc", "gcd", "gde", "gdgh", "gdhi", "gdij" };
        String[] ranges = new String[]{ "a", "a",
                                        "c", "c",
                                        "eaa", "ede",
                                        "edjj", "ef",
                                        "ga", "gb",
                                        "gd", "gg" };

        String[] expected = new String[] {"bbc", "bcd", "bde", "bdgh", "bdhi", "bdij",
                                          "dbc", "dcd", "dde", "ddgh", "ddhi", "ddij",
                                          "edgh", "edhi", "edij",
                                          "gcd" };

        SortedMap<ByteComparable.Preencoded, ByteBuffer> expectedAsMap = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        InMemoryTrie<ByteBuffer> trie = InMemoryTrieTestBase.makeInMemoryTrie(Arrays.stream(data)
                                                                                    .map(TrieUtil::directComparable)
                                                                                    .toArray(ByteComparable.Preencoded[]::new),
                                                                              expectedAsMap,
                                                                              true);

        TrieSet set = TrieSet.ranges(TrieUtil.VERSION, true, true, Arrays.stream(ranges)
                                                                         .map(TrieUtil::directComparable)
                                                                         .toArray(ByteComparable.Preencoded[]::new));
        Trie<ByteBuffer> del = deleter.apply(trie, set);

        expectedAsMap.clear();
        InMemoryTrie<ByteBuffer> expTrie = InMemoryTrieTestBase.makeInMemoryTrie(Arrays.stream(expected)
                                                                                       .map(TrieUtil::directComparable)
                                                                                       .toArray(ByteComparable.Preencoded[]::new),
                                                                                 expectedAsMap,
                                                                                 true);
        TrieUtil.assertSameContent(del, expectedAsMap);

        // Check a random selection of tails.
        Random r = new Random(1);
        for (int i = 0; i < 10000; ++i)
        {
            String k = data[r.nextInt(data.length)];
            String f = k.substring(0, r.nextInt(k.length() + 1)); // "" to all of k

            Trie<ByteBuffer> expTail = expTrie.tailTrie(TrieUtil.directComparable(f));
            Trie<ByteBuffer> tail = del.tailTrie(TrieUtil.directComparable(f));
            checkSameTries(expectedAsMap, expTail, tail, f);

            // Take a tail of the tail too.
            String s = k.substring(f.length(), f.length() + r.nextInt(k.length() - f.length() + 1));
            expTail = expTrie.tailTrie(TrieUtil.directComparable(f + s));
            if (tail != null)
                tail = tail.tailTrie(TrieUtil.directComparable(s));
            checkSameTries(expectedAsMap, expTail, tail, f + s);
        }
    }

    private static void checkSameTries(SortedMap<ByteComparable.Preencoded, ByteBuffer> expectedAsMap, Trie<ByteBuffer> expTail, Trie<ByteBuffer> tail, String k)
    {
        expectedAsMap.clear();
        if (expTail != null)
        {
            for (var entries : expTail.entrySet())
                expectedAsMap.put(entries.getKey(), entries.getValue());
        }

        if (tail == null)
            tail = Trie.empty(VERSION);

        try
        {
            TrieUtil.assertSameContent(tail, expectedAsMap);
        }
        catch (Throwable t)
        {
            System.err.println("Prefix " + k);
            TrieUtil.dumpToOut(tail);
            System.err.println(expectedAsMap);
            throw t;
        }
    }
}
