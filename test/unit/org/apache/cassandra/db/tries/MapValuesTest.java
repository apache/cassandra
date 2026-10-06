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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import org.junit.BeforeClass;
import org.junit.Test;
import org.quicktheories.core.Gen;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;

/// Property-based tests for [Trie#mapValues] using QuickTheories framework.
///
/// Tests that the `mapValues` operation correctly transforms all content in a trie
/// through a mapping function while preserving the trie structure and key ordering.
/// Also tests various types of filtered iteration.
public class MapValuesTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static final int MAX_KEYS = 100;
    static final int MAX_VALUE = 1000;
    static final ByteComparable.Version VERSION = TrieUtil.VERSION;

    /**
     * Generator for random ByteBuffer values.
     */
    static Gen<ByteBuffer> byteBufferGen()
    {
        return integers().between(0, MAX_VALUE)
                        .map(i -> {
                            ByteBuffer buf = ByteBuffer.allocate(4);
                            buf.putInt(i);
                            buf.flip();
                            return buf;
                        });
    }

    /**
     * Generator for lists of key-value pairs.
     */
    static Gen<List<KeyValue>> keyValueListGen()
    {
        return lists().of(integers().between(0, MAX_VALUE)
                                   .zip(byteBufferGen(), KeyValue::new))
                     .ofSizeBetween(0, MAX_KEYS);
    }

    /**
     * Helper to create and populate a trie with integer values from key-value pairs.
     */
    static TrieWithContent<Integer> createIntegerTrie(List<KeyValue> keyValues)
    {
        InMemoryTrie<Integer> trie = InMemoryTrie.shortLivedOrdered(VERSION);
        NavigableMap<ByteComparable.Preencoded, Integer> content = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        
        try
        {
            for (KeyValue kv : keyValues)
            {
                Integer value = kv.integer;
                trie.putSingleton(kv.key, value, (existing, update) -> update, true);
                content.put(kv.key, value);
            }
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new RuntimeException(e);
        }
        
        return new TrieWithContent<>(trie, content);
    }

    /**
     * Helper to create and populate a trie with ByteBuffer values from key-value pairs.
     */
    static TrieWithContent<ByteBuffer> createByteBufferTrie(List<KeyValue> keyValues)
    {
        InMemoryTrie<ByteBuffer> trie = InMemoryTrie.shortLivedOrdered(VERSION);
        NavigableMap<ByteComparable.Preencoded, ByteBuffer> content = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        
        try
        {
            for (KeyValue kv : keyValues)
            {
                ByteBuffer value = kv.value;
                trie.putSingleton(kv.key, value, (existing, update) -> update, true);
                content.put(kv.key, value);
            }
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new RuntimeException(e);
        }
        
        return new TrieWithContent<>(trie, content);
    }

    /**
     * Verifies that all values in the mapped trie match the expected transformed values.
     */
    static <T, V> void verifyMappedValues(Trie<V> mappedTrie,
                                          NavigableMap<ByteComparable.Preencoded, T> sourceContent,
                                          Function<T, V> mapper)
    {
        for (Map.Entry<ByteComparable.Preencoded, T> entry : sourceContent.entrySet())
        {
            T originalValue = entry.getValue();
            V expectedMappedValue = mapper.apply(originalValue);

            V actualMappedValue = mappedTrie.get(entry.getKey());
            assertEquals("Mapped value should match expected", expectedMappedValue, actualMappedValue);

            Trie<V> tail = mappedTrie.tailTrie(entry.getKey());
            assertNotNull("Tail trie should exist for key", tail);
            
            actualMappedValue = tail.get(ByteComparable.EMPTY);
            assertEquals("Mapped value in tail should match expected", expectedMappedValue, actualMappedValue);

            if (expectedMappedValue != null)
            {
                actualMappedValue = Iterables.getFirst(tail.values(), null);
                assertEquals("First value in tail should match expected", expectedMappedValue, actualMappedValue);

                actualMappedValue = Iterables.getLast(tail.values(Direction.REVERSE), null);
                assertEquals("Last value in tail should match expected", expectedMappedValue, actualMappedValue);
            } // otherwise  iteration will skip over the null
        }

        assertEquals(sourceContent.values()
                                  .stream()
                                  .map(mapper)
                                  .filter(Predicates.notNull())
                                  .collect(Collectors.toList()),
                     Streams.stream(mappedTrie.values()).collect(Collectors.toList()));
        assertEquals(sourceContent.descendingMap()
                                  .values()
                                  .stream()
                                  .map(mapper)
                                  .filter(Predicates.notNull())
                                  .collect(Collectors.toList()),
                     Streams.stream(mappedTrie.values(Direction.REVERSE)).collect(Collectors.toList()));

        testTailTries(mappedTrie, null, sourceContent, mapper);
    }

    static <T, V, Q> void testTailTries(Trie<V> mappedTrie,
                                        Class<Q> clazz, NavigableMap<ByteComparable.Preencoded, T> sourceContent,
                                        Function<T, Q> mapper)
    {
        // test tailTries. We must account for the fact that when a prefix is reported the code will skip over all
        // descendants.
        Set<String> keys = new HashSet<>();
        String prev = null;
        for (var en : sourceContent.entrySet())
        {
            if (mapper.apply(en.getValue()) == null) // skip over null mappings
                continue;

            String string = en.getKey().toString();
            if (prev != null && string.startsWith(prev)) // skip descentants
                continue;
            keys.add(string);
            prev = string;
        }
        TreeMap<ByteComparable.Preencoded, T> tailFilteredMap = new TreeMap<>(sourceContent);
        for (var it = tailFilteredMap.keySet().iterator(); it.hasNext();)
        {
            var v = it.next();
            if (!keys.contains(v.toString()))
                it.remove();
        }


        assertEquals(tailFilteredMap.values()
                                    .stream()
                                    .map(mapper)
                                    .filter(Predicates.notNull())
                                    .collect(Collectors.toList()),
                     Streams.stream(mappedTrie.tailTries(Direction.FORWARD, clazz != null ? clazz::isInstance : Predicates.alwaysTrue()))
                            .map(t -> t.getValue().get(ByteComparable.EMPTY))
                            .collect(Collectors.toList()));
        // We can't do reverse iteration because the content in an ordered trie is presented on the return path where
        // we can't take a tail trie.
    }

    static class NullFilteredEntries<T, V> extends TrieEntriesIterator.WithNullFiltering<T, V>
    {
        private final Function<T, V> mapper;

        NullFilteredEntries(Function<T, V> mapper, BaseTrie<T, ?, ?> trie, Direction direction)
        {
            super(trie, direction);
            this.mapper = mapper;
        }

        @Override
        protected V mapContent(T content, byte[] bytes, int byteLength)
        {
            return mapper.apply(content);
        }
    }

    /**
     * Helper class to hold a trie and its expected content together.
     */
    static class TrieWithContent<T>
    {
        final InMemoryTrie<T> trie;
        final NavigableMap<ByteComparable.Preencoded, T> content;

        TrieWithContent(InMemoryTrie<T> trie, NavigableMap<ByteComparable.Preencoded, T> content)
        {
            this.trie = trie;
            this.content = content;
        }
    }

    @Test
    public void testMapValuesInteger()
    {
        qt().forAll(keyValueListGen())
            .checkAssert(keyValues -> {
                if (keyValues.isEmpty())
                    return; // Skip empty case
                    
                TrieWithContent<Integer> trieWithContent = createIntegerTrie(keyValues);
                
                // Apply mapping function: multiply by 2
                Function<Integer, Integer> mapper = x -> x * 2;
                Trie<Integer> mappedTrie = trieWithContent.trie.mapValues(mapper);

                // Verify all values are correctly mapped
                verifyMappedValues(mappedTrie, trieWithContent.content, mapper);

                testNullFilteredEntries(trieWithContent, mapper);
            });
    }

    static <T, V> void testNullFilteredEntries(TrieWithContent<T> trieWithContent, Function<T, V> mapper)
    {
        assertEquals(trieWithContent.content.values()
                                            .stream()
                                            .map(mapper)
                                            .filter(Predicates.notNull())
                                            .collect(Collectors.toList()),
                     Streams.stream(new NullFilteredEntries<>(mapper, trieWithContent.trie, Direction.FORWARD))
                            .collect(Collectors.toList()));
        assertEquals(trieWithContent.content.descendingMap()
                                            .values()
                                            .stream()
                                            .map(mapper)
                                            .filter(Predicates.notNull())
                                            .collect(Collectors.toList()),
                     Streams.stream(new NullFilteredEntries<>(mapper, trieWithContent.trie, Direction.REVERSE))
                            .collect(Collectors.toList()));
    }

    @Test
    public void testMapValuesByteBuffer()
    {
        qt().forAll(keyValueListGen())
            .checkAssert(keyValues -> {
                if (keyValues.isEmpty())
                    return; // Skip empty case
                    
                TrieWithContent<ByteBuffer> trieWithContent = createByteBufferTrie(keyValues);

                // Apply mapping function: extract integer and add 100
                Function<ByteBuffer, Integer> mapper = buf -> {
                    ByteBuffer duplicate = buf.duplicate();
                    return duplicate.getInt() + 100;
                };
                Trie<Integer> mappedTrie = trieWithContent.trie.mapValues(mapper);

                // Verify all values are correctly mapped
                verifyMappedValues(mappedTrie, trieWithContent.content, mapper);

                testNullFilteredEntries(trieWithContent, mapper);
            });
    }

    @Test
    public void testMapValuesStrings()
    {
        qt().forAll(keyValueListGen())
            .checkAssert(keyValues -> {
                if (keyValues.isEmpty())
                    return; // Skip empty case

                TrieWithContent<Integer> trieWithContent = createIntegerTrie(keyValues);

                // Apply mapping
                Function<Integer, String> mapper = x -> "value_" + x;
                Trie<String> mappedTrie = trieWithContent.trie.mapValues(mapper);

                // Verify keys with content are mapped
                verifyMappedValues(mappedTrie, trieWithContent.content, mapper);

                testNullFilteredEntries(trieWithContent, mapper);
            });
    }

    /**
     * Test that chained mapValues operations work correctly.
     */
    @Test
    public void testChainedMapValues()
    {
        qt().forAll(keyValueListGen())
            .checkAssert(keyValues -> {
                if (keyValues.isEmpty())
                    return; // Skip empty case

                TrieWithContent<Integer> trieWithContent = createIntegerTrie(keyValues);

                // Chain multiple mappings
                Function<Integer, Integer> mapper1 = x -> x * 2;
                Function<Integer, String> mapper2 = x -> "value_" + x;
                Function<String, Integer> mapper3 = s -> s.length();

                Trie<Integer> mappedTrie = trieWithContent.trie.mapValues(mapper1)
                                                              .mapValues(mapper2)
                                                              .mapValues(mapper3);

                // Compute combined mapper for verification
                Function<Integer, Integer> combinedMapper = x -> mapper3.apply(mapper2.apply(mapper1.apply(x)));
                
                // Verify chained mapping
                verifyMappedValues(mappedTrie, trieWithContent.content, combinedMapper);

                testNullFilteredEntries(trieWithContent, combinedMapper);
            });
    }


    @Test
    public void testMappingMergeWith()
    {
        qt().forAll(keyValueListGen(), keyValueListGen())
            .checkAssert((keyValues1, keyValues2) -> {
                TrieWithContent<Integer> trieWithContent1 = createIntegerTrie(keyValues1);
                TrieWithContent<ByteBuffer> trieWithContent2 = createByteBufferTrie(keyValues2);

                BiFunction<Integer, ByteBuffer, String> mapper = (x, y) ->
                                                                 x != null ? y != null ? "" + (x * 2) + ":" + ByteBufferUtil.bytesToHex(y)
                                                                                       : "" + (x * 2)
                                                                           : y != null ? ByteBufferUtil.bytesToHex(y)
                                                                                       : null;

                Trie<String> mappedTrie = trieWithContent1.trie.mappingMergeWith(trieWithContent2.trie, mapper);
                var mergedContent = mergeAndMapContent(trieWithContent1.content, trieWithContent2.content, mapper);

                // Verify all values are correctly mapped
                verifyMappedValues(mappedTrie, mergedContent, x -> x);
            });
    }

    private static <K, T, S, R>
    NavigableMap<K, R> mergeAndMapContent(NavigableMap<K, T> c1, NavigableMap<K, S> c2, BiFunction<T, S, R> mapper)
    {
        NavigableMap<K, R> mergedContent = new TreeMap<>();
        for (K key : Sets.union(c1.keySet(), c2.keySet()))
            mergedContent.put(key, mapper.apply(c1.get(key), c2.get(key)));
        return mergedContent;
    }


    /**
     * Helper class to hold key-value pairs for testing.
     */
    static class KeyValue
    {
        final ByteComparable.Preencoded key;
        final int integer;
        final ByteBuffer value;

        KeyValue(int intKey, ByteBuffer value)
        {
            this.integer = intKey;
            this.key = TrieUtil.generateKeyAllowingPrefixes(new Random(intKey));
            this.value = value;
        }

        @Override
        public String toString()
        {
            return "KeyValue{" +
                   "key=" + key.byteComparableAsString(key.encodingVersion()) +
                   ", integer=" + integer +
                   ", value=" + ByteBufferUtil.bytesToHex(value) +
                                               '}';
        }
    }
}
