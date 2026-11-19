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
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.collect.Iterables;
import com.google.common.primitives.Bytes;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.checkGet;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.assertIterablesEqual;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.apache.cassandra.db.tries.TrieUtil.generateKey;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class PrefixTailTestBase<T extends InMemoryBaseTrie<Object>, Q extends BaseTrie<Object, ?, Q>>
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static final int COUNT_TAIL = 5000;
    private static final int COUNT_HEAD = 25;
    public static final Comparator<Preencoded> BYTE_COMPARABLE_COMPARATOR = (a, b) -> ByteComparable.compare(a, b, VERSION);
    Random rand = new Random();

    static final InMemoryBaseTrie.UpsertTransformer<Object, Object> THROWING_UPSERT = (e, u) -> {
        if (e != null) throw new AssertionError();
        return u;
    };

    static final Function<Object, String> CONTENT_TO_STRING = x -> x instanceof ByteBuffer
                                                                   ? ByteBufferUtil.bytesToHex((ByteBuffer) x)
                                                                   : x.toString();

    static class Tail
    {
        byte[] prefix;
        NavigableMap<Preencoded, ByteBuffer> data;

        public Tail(byte[] prefix, NavigableMap<Preencoded, ByteBuffer> map)
        {
            this.prefix = prefix;
            this.data = map;
        }

        public String toString()
        {
            return "Tail{" + ByteBufferUtil.bytesToHex(ByteBuffer.wrap(prefix)) + '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Tail tail = (Tail) o;
            return Arrays.equals(prefix, tail.prefix) && Objects.equals(data, tail.data);
        }
    }

    static <T> T getRootContent(BaseTrie<T, ?, ?> trie)
    {
        return trie.get(ByteComparable.EMPTY);
    }

    @Test
    public void testPrefixTail() throws Exception
    {
        testPrefixTail(1, false);
    }

    @Test
    public void testPrefixTailMerge2InHead() throws Exception
    {
        testPrefixTail(2, false);
    }

    @Test
    public void testPrefixTailMerge2InTail() throws Exception
    {
        testPrefixTail(2, true);
    }

    @Test
    public void testPrefixTailMerge5InHead() throws Exception
    {
        testPrefixTail(5, false);
    }

    @Test
    public void testPrefixTailMerge5InTail() throws Exception
    {
        testPrefixTail(5, true);
    }

    static Tail combineTails(Object x, Object y)
    {
        // Cast failure is a test problem
        Tail tx = (Tail) x;
        Tail ty = (Tail) y;
        var map = new TreeMap<Preencoded, ByteBuffer>(BYTE_COMPARABLE_COMPARATOR);
        map.putAll(tx.data);
        map.putAll(ty.data);
        return new Tail(tx.prefix, map);
    }

    public void testPrefixTail(int splits, boolean splitInTail) throws Exception
    {
        Preencoded[] prefixes = generateKeys(rand, COUNT_HEAD);

        NavigableMap<Preencoded, Tail> data = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
        final Q trie = splitInTail ? prepareSplitInTailTrie(splits, prefixes, data)
                                   : prepareSplitInHeadTrie(splits, prefixes, data);
//        System.out.println(trie.dump(CONTENT_TO_STRING));

        // Test tailTrie for known prefix
        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            Tail t = data.get(prefixes[i]);
            Q tail = trie.tailTrie(prefixes[i]);
            assertEquals(t, getRootContent(tail));
            checkContent(processContent(tail), t.data);
        }

        // Test tail iteration for given class
        for (Direction td : Direction.values())
        {
            long count = 0;
            for (var en : trie.tailTries(td, Tail.class))
            {
                System.out.println(en.getKey().byteComparableAsString(VERSION));
                Q tail = en.getValue();
                Tail t = data.get(en.getKey());
                assertNotNull(t);
                assertEquals(t, getRootContent(tail));
                checkContent(processContent(tail), t.data);
                ++count;
            }
            assertEquals(COUNT_HEAD, count);
        }

        // test a sample of tail slices
        for (int i = rand.nextInt(7); i < COUNT_HEAD; i += 1 + rand.nextInt(7))
        {
            Tail t = data.get(prefixes[i]);
            int keyCount = t.data.size();
            int firstIndex = rand.nextInt(keyCount - 1);
            int lastIndex = firstIndex + rand.nextInt(keyCount - firstIndex);
            Preencoded first = rand.nextInt(5) > 0 ? Iterables.get(t.data.keySet(), firstIndex) : null;
            Preencoded last = rand.nextInt(5) > 0 ? Iterables.get(t.data.keySet(), lastIndex) : null;
            Preencoded prefix = prefixes[i];
            final ByteComparable leftWithPrefix = concat(prefix, first, rand.nextBoolean() ? data.lowerKey(prefix)
                                                                                           : null);
            final ByteComparable rightWithPrefix = concat(prefix, last, rand.nextBoolean() ? data.higherKey(prefix)
                                                                                           : null);
            System.out.println("Between " + (leftWithPrefix == null ? "null" : leftWithPrefix.byteComparableAsString(VERSION)) + " and " + (rightWithPrefix == null ? "null" : rightWithPrefix.byteComparableAsString(VERSION)));
            Q tail = trie.subtrie(leftWithPrefix,
                                  rightWithPrefix)
                         .tailTrie(prefixes[i]);
            assertEquals(t, getRootContent(tail));
            checkContent(processContent(tail), subMap(t.data, first, last));
        }

        // Test processSkippingBranches variations
        for (Direction td : Direction.values())
        {
            final AtomicLong count = new AtomicLong(0);
            trie.forEachValueSkippingBranches(td, v -> count.incrementAndGet());
            assertEquals(COUNT_HEAD, count.get());

            count.set(0);
            trie.forEachEntrySkippingBranches(td, (key, tail) ->
            {
                assertArrayEquals(((Tail) tail).prefix, key.asByteComparableArray(VERSION));
                count.incrementAndGet();
            });
            assertEquals(COUNT_HEAD, count.get());
        }

        // Test filteredValues and filteredEntrySet
        for (Direction td : Direction.values())
        {
            long count = 0;
            for (Tail t : trie.filteredValues(td, Tail.class))
                ++count;
            assertEquals(COUNT_HEAD, count);

            count = 0;
            for (var en : trie.filteredEntrySet(td, Tail.class))
            {
                assertArrayEquals(en.getValue().prefix, en.getKey().asByteComparableArray(VERSION));
                ++count;
            }
            assertEquals(COUNT_HEAD, count);

            count = 0;
            for (var it = new TrieEntriesIterator.WithNullFiltering<Object, Tail>(trie, td)
            {
                @Override
                protected Tail mapContent(Object content, byte[] bytes, int byteLength)
                {
                    if (!(content instanceof Tail))
                        return null;

                    Tail tail = (Tail) content;
                    assertArrayEquals(tail.prefix, Arrays.copyOf(bytes, byteLength));
                    return tail;
                }
            }; it.hasNext(); )
            {
                assertNotNull(it.next());
                ++count;
            }
            assertEquals(COUNT_HEAD, count);
        }
    }

    private static void checkContent(Trie<ByteBuffer> tail, NavigableMap<Preencoded, ByteBuffer> data)
    {
        assertMapEquals(tail.entryIterator(Direction.FORWARD),
                        data.entrySet().iterator());
        assertIterablesEqual(tail.values(Direction.FORWARD),
                             data.values());
        // As the keys are prefix-free, reverse iteration is the inverse of forward.
        assertMapEquals(tail.entryIterator(Direction.REVERSE),
                        data.descendingMap().entrySet().iterator());
        assertIterablesEqual(tail.values(Direction.REVERSE),
                             data.descendingMap().values());
        checkGet(tail, data);
    }

    private static <K, V> NavigableMap<K, V> subMap(NavigableMap<K, V> data, K left, K right)
    {
        // Subtries are always inclusive.
        if (left == null)
            return right == null ? data : data.headMap(right, true);
        else
            return right == null
                   ? data.tailMap(left, true)
                   : data.subMap(left, true, right, true);
    }

    private static ByteComparable concat(ByteComparable a, ByteComparable b, ByteComparable ifBNull)
    {
        if (b == null)
            return ifBNull;
        return ByteComparable.preencoded(VERSION,
                                         Bytes.concat(a.asByteComparableArray(VERSION),
                                                      b.asByteComparableArray(VERSION)));
    }

    interface UpsertTransformerWithKeys
    {
        Object apply(Object existing, Object update, InMemoryBaseTrie.Mutator mutator);
    }

    abstract T[] makeArray(int length);
    abstract T makeInMemoryTrie();
    abstract void applyPrefixed(T destination, ByteComparable prefix, T tail, InMemoryBaseTrie.UpsertTransformer<Object, Object> upsertTransformer) throws TrieSpaceExhaustedException;
    abstract void apply(T destination, Q tail, UpsertTransformerWithKeys upsertTransformer) throws TrieSpaceExhaustedException;
    abstract Q merge(T[] tries, Trie.CollectionMergeResolver<Object> resolver);
    abstract Q cast(T inMemoryTrie);
    abstract void addToInMemoryTrie(Preencoded[] src, NavigableMap<Preencoded, ByteBuffer> content, T tail);
    abstract void addNthToInMemoryTrie(Preencoded[] src, NavigableMap<Preencoded, ByteBuffer> content, T tail, int splits, int k);
    abstract Trie<ByteBuffer> processContent(Q trie);

    private Q prepareSplitInTailTrie(int splits, Preencoded[] prefixes, Map<Preencoded, Tail> data) throws TrieSpaceExhaustedException
    {
        T[] tries = makeArray(splits);
        for (int i = 0; i < splits; ++i)
            tries[i] = makeInMemoryTrie();
        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            Preencoded[] src = generateKeys(rand, COUNT_TAIL);
            NavigableMap<Preencoded, ByteBuffer> allContent = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
            for (int k = 0; k < splits; ++k)
            {
                NavigableMap<Preencoded, ByteBuffer> content = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
                T tail = makeInMemoryTrie();
                addNthToInMemoryTrie(src, content, tail, splits, k);

                Tail t = new Tail(prefixes[i].asByteComparableArray(VERSION), content);
                allContent.putAll(content);
                tail.putRecursive(ByteComparable.EMPTY, t, THROWING_UPSERT);
//            System.out.println(tail.dump(CONTENT_TO_STRING));

                applyPrefixed(tries[k], prefixes[i], tail, THROWING_UPSERT);
            }
            Tail t = new Tail(prefixes[i].asByteComparableArray(VERSION), allContent);
            data.put(ByteComparable.preencoded(VERSION, t.prefix), t);
        }

        return merge(tries, c -> c.stream().reduce(PrefixTailTestBase::combineTails).get());
    }


    private Q prepareSplitInHeadTrie(int splits, Preencoded[] prefixes, Map<Preencoded, Tail> data) throws TrieSpaceExhaustedException
    {
        T[] tries = makeArray(splits);
        for (int i = 0; i < splits; ++i)
            tries[i] = makeInMemoryTrie();
        int trieIndex = 0;
        for (int i = 0; i < prefixes.length; ++i)
        {
            Preencoded[] src = generateKeys(rand, COUNT_TAIL);

            NavigableMap<Preencoded, ByteBuffer> content = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
            T tail = makeInMemoryTrie();
            addToInMemoryTrie(src, content, tail);

            Tail t = new Tail(prefixes[i].asByteComparableArray(VERSION), content);
            tail.putRecursive(ByteComparable.EMPTY, t, THROWING_UPSERT);
//            System.out.println(tail.dump(CONTENT_TO_STRING));
            applyPrefixed(tries[trieIndex], prefixes[i], tail, THROWING_UPSERT);

            data.put(ByteComparable.preencoded(VERSION, t.prefix), t);
            trieIndex = (trieIndex + 1) % splits;
        }

        return merge(tries, Trie.throwingResolver());
    }

    // also do same prefix updates

    @Test
    public void testTailMerge() throws Exception
    {
        ByteComparable prefix = generateKey(rand);
        T trie = makeInMemoryTrie();
        NavigableMap<Preencoded, ByteBuffer> content = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);

        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            Preencoded[] src = generateKeys(rand, COUNT_TAIL);
            T tail = makeInMemoryTrie();
            addToInMemoryTrie(src, content, tail);
//                        System.out.println(tail.dump(CONTENT_TO_STRING));
            tail.putRecursive(ByteComparable.EMPTY, 1, THROWING_UPSERT);
            applyPrefixed(trie, prefix, tail,
                       (x, y) -> x instanceof Integer ? (Integer) x + (Integer) y : y);
        }

//                System.out.println(trie.dump(CONTENT_TO_STRING));

        Q tail = cast(trie).tailTrie(prefix);
        assertEquals(COUNT_HEAD, ((Integer) getRootContent(tail)).intValue());
        checkContent(processContent(tail), content);


        // Test tail iteration for metadata
        long count = 0;
        for (var en : cast(trie).tailTries(Direction.FORWARD, Integer.class))
        {
            System.out.println(en.getKey().byteComparableAsString(VERSION));
            Q tt = en.getValue();
            assertNotNull(tt);
            assertEquals(COUNT_HEAD, ((Integer) getRootContent(tail)).intValue());
            checkContent(processContent(tt), content);
            ++count;
        }
        assertEquals(1, count);
    }

    @Test
    public void testKeyProducer() throws Exception
    {

        testKeyProducer(generateKeys(rand, COUNT_HEAD));
    }

    @Test
    public void testKeyProducerMarkedRoot() throws Exception
    {
        // Check that path construction works correctly also when the root is the starting position.
        testKeyProducer(new Preencoded[] { Preencoded.EMPTY.preencode(VERSION) });
    }

    private void testKeyProducer(Preencoded[] prefixes) throws TrieSpaceExhaustedException
    {
        NavigableMap<Preencoded, Tail> data = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
        final Q trie = prepareSplitInHeadTrie(1, prefixes, data);
//        System.out.println(trie.dump(CONTENT_TO_STRING));

        T dest = makeInMemoryTrie();
        InclusionChecker checker = new InclusionChecker();
        apply(dest, trie, checker);
        assertEquals("", checker.output.toString());
    }

    static class InclusionChecker implements UpsertTransformerWithKeys
    {
        Tail currentTail = null;
        StringBuilder output = new StringBuilder();

        @Override
        public Object apply(Object existing, Object update, InMemoryBaseTrie.Mutator mutator)
        {
            if (existing != null)
                output.append("Non-null existing\n");

            byte[] tailPath = mutator.getCurrentKeyBytesToNearestAncestorSatisfying(Tail.class::isInstance);
            byte[] fullPath = mutator.getCurrentKeyBytes();
            String tail = Hex.bytesToHex(tailPath);
            String full = Hex.bytesToHex(fullPath);
            if (!full.endsWith(tail))
            {
                output.append("Tail " + tail + " is not suffix of full path " + full + "\n");
                return update; // can't continue
            }

            String msg = "\n@key " + full.substring(0, full.length() - tail.length()) + ":" + tail + "\n";

            if (update instanceof Tail)
            {
                // At
                if (tailPath.length != fullPath.length)
                    output.append("Prefix not empty on tail root").append(msg);
                Tail t = (Tail) update;
                if (!Arrays.equals(t.prefix, fullPath))
                    output.append("Tail root path expected ").append(Hex.bytesToHex(t.prefix)).append(msg);
                currentTail = t;
            }
            else
            {
                if (currentTail == null)
                    output.append("Null currentTail").append(msg);

                if (update instanceof ByteBuffer)
                {
                    byte[] prefix = Arrays.copyOfRange(fullPath, 0, fullPath.length - tailPath.length);
                    if (!Arrays.equals(currentTail.prefix, prefix))
                        output.append("Prefix expected " + Hex.bytesToHex(currentTail.prefix) + msg);
                }
                else if (update instanceof TestRangeState)
                {
                    if (!Arrays.equals(currentTail.prefix, fullPath) || !Arrays.equals(currentTail.prefix, tailPath))
                        output.append("Prefix expected ").append(Hex.bytesToHex(currentTail.prefix)).append(msg);
                    // on deletions the tail path is queried separately
                    tailPath = ((InMemoryDeletionAwareTrie.Mutator) mutator).getDeletionBranchKeyBytes();
                }

                ByteBuffer updateAsBuf = null;

                if (update instanceof TestRangeState)
                {
                    TestRangeState rs = (TestRangeState) update;
                    if (rs.leftSide >= 0)
                        updateAsBuf = ByteBufferUtil.bytes(rs.leftSide);
                    else if (rs.rightSide >= 0)
                        updateAsBuf = ByteBufferUtil.bytes(rs.rightSide);
                    else
                        output.append("Invalid range state ").append(rs);
                }
                else if (update instanceof ByteBuffer)
                    updateAsBuf = (ByteBuffer) update;
                else
                    output.append("Not ByteBuffer or TestRangeState ").append(update).append(msg);

                ByteBuffer expected = currentTail.data.get(ByteComparable.preencoded(VERSION, tailPath));
                if (expected == null)
                    output.append("Suffix not found").append(msg);
                else if (!expected.equals(updateAsBuf))
                    output.append("Data mismatch ").append(ByteBufferUtil.bytesToHex(updateAsBuf)).append(" expected ").append(ByteBufferUtil.bytesToHex(expected)).append(msg);
            }
            return update;
        }
    }
}
