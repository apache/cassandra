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
import java.util.AbstractMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.google.common.collect.Streams;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.MapValuesTest.TrieWithContent;
import static org.apache.cassandra.db.tries.MapValuesTest.VERSION;
import static org.apache.cassandra.db.tries.MapValuesTest.createIntegerTrie;
import static org.apache.cassandra.db.tries.MapValuesTest.keyValueListGen;
import static org.apache.cassandra.db.tries.MapValuesTest.testNullFilteredEntries;
import static org.apache.cassandra.db.tries.MapValuesTest.testTailTries;
import static org.apache.cassandra.db.tries.MapValuesTest.verifyMappedValues;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;

/// Property-based tests for [Trie#mapValues] using QuickTheories framework.
///
/// Tests that the `mapValues` operation correctly transforms all content in a trie
/// through a mapping function while preserving the trie structure and key ordering.
/// Also tests various types of filtered iteration.
public class FilteringTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static <T> T filter(Class<T> clazz, Object o)
    {
        return clazz.isInstance(o) ? clazz.cast(o) : null;
    }

    /**
     * Test multi-typed conversion followed by filtering in different ways.
     */
    @Test
    public void testTypesAndFiltering()
    {
        qt().forAll(keyValueListGen())
            .checkAssert(keyValues -> {
                if (keyValues.isEmpty())
                    return; // Skip empty case

                TrieWithContent<Integer> trieWithContent = createIntegerTrie(keyValues);

                // Chain multiple mappings
                Function<Integer, Object> multiTypeMapper = x -> {
                    switch (x % 3)
                    {
                        case 0:
                            return x;
                        case 1:
                            return Integer.toString(x);
                        default:
                            return ByteBuffer.wrap(Integer.toString(x).getBytes());
                    }
                };

                Trie<Object> mappedTrie = trieWithContent.trie.mapValues(multiTypeMapper);

                // Verify initial mapping
                verifyMappedValues(mappedTrie, trieWithContent.content, multiTypeMapper);
                testNullFilteredEntries(trieWithContent, multiTypeMapper);

                checkFiltering(mappedTrie, multiTypeMapper, trieWithContent, Integer.class);
                checkFiltering(mappedTrie, multiTypeMapper, trieWithContent, String.class);
                checkFiltering(mappedTrie, multiTypeMapper, trieWithContent, ByteBuffer.class);
            });
    }

    private <T> void checkFiltering(Trie<Object> mappedTrie,
                                    Function<Integer, Object> multiTypeMapper,
                                    TrieWithContent<Integer> trieWithContent,
                                    Class<T> clazz)
    {
        Function<Object, T> extractor = x -> filter(clazz, x);
        Trie<T> stringTrie = mappedTrie.mapValues(extractor);
        Function<Integer, T> filteredMapper = multiTypeMapper.andThen(extractor);
        verifyMappedValues(stringTrie, trieWithContent.content, filteredMapper);

        // check filtered values
        assertEquals(trieWithContent.content.values()
                                            .stream()
                                            .map(filteredMapper)
                                            .filter(Predicates.notNull())
                                            .collect(Collectors.toList()),
                     Streams.stream(mappedTrie.filteredValues(Direction.FORWARD, clazz)).collect(Collectors.toList()));
        assertEquals(trieWithContent.content.descendingMap()
                                            .values()
                                            .stream()
                                            .map(filteredMapper)
                                            .filter(Predicates.notNull())
                                            .collect(Collectors.toList()),
                     Streams.stream(mappedTrie.filteredValues(Direction.REVERSE, clazz)).collect(Collectors.toList()));

        // check filtered entrySet
        assertEquals(trieWithContent.content.entrySet()
                                            .stream()
                                            .map(x -> map(x, filteredMapper))
                                            .filter(Predicates.notNull())
                                            .collect(Collectors.toList()),
                     Streams.stream(mappedTrie.filteredEntrySet(Direction.FORWARD, clazz)).collect(Collectors.toList()));
        assertEquals(trieWithContent.content.descendingMap()
                                            .entrySet()
                                            .stream()
                                            .map(x -> map(x, filteredMapper))
                                            .filter(Predicates.notNull())
                                            .collect(Collectors.toList()),
                     Streams.stream(mappedTrie.filteredEntrySet(Direction.REVERSE, clazz)).collect(Collectors.toList()));

        testNullFilteredEntries(trieWithContent, filteredMapper);
        testTailTries(mappedTrie, clazz, trieWithContent.content, filteredMapper);
        testDanglingMetadataCleaner(mappedTrie, clazz);
    }

    private static <T, V extends T> void testDanglingMetadataCleaner(Trie<T> trie, Class<V> clazz)
    {
        NavigableMap<ByteComparable.Preencoded, T> survivors = new TreeMap<>();
        for (var en : trie.entrySet())
        {
            Trie<T> tail = trie.tailTrie(en.getKey());
            assert tail != null;
            if (tail.filteredValuesIterator(Direction.FORWARD, clazz).hasNext())
                survivors.put(en.getKey(), en.getValue());
        }

        // set up in-memory trie with dangling non-clazz clean-up
        InMemoryTrie<T> copy = new InMemoryTrie<>(VERSION,
                                                  BufferType.ON_HEAP,
                                                  InMemoryBaseTrie.ExpectedLifetime.SHORT,
                                                  null,
                                                  true,
                                                  clazz::isInstance);
        try
        {
            copy.mutator((x, y) -> y,
                         (Predicate<InMemoryBaseTrie.NodeFeatures<T>>) x -> false)
                .apply(trie);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new RuntimeException(e);
        }

        assertEquals(survivors.entrySet()
                              .stream()
                              .collect(Collectors.toList()),
                     Streams.stream(copy.entrySet())
                            .collect(Collectors.toList()));
    }

    private <K, V1, V2> Map.Entry<K, V2> map(Map.Entry<K, V1> en, Function<V1, V2> mapper)
    {
        V2 value = mapper.apply(en.getValue());
        if (value == null)
            return null;
        return new AbstractMap.SimpleEntry<>(en.getKey(), value);
    }
}
