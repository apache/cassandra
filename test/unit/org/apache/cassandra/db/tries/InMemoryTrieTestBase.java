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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public abstract class InMemoryTrieTestBase
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    // Set this to true (in combination with smaller count) to dump the tries while debugging a problem.
    // Do not commit the code with VERBOSE = true.
    static final boolean VERBOSE = false;

    private static final int COUNT = 100000;

    Random rand = new Random();

    abstract boolean usePut();

    static ByteComparable invert(ByteComparable b)
    {
        return version -> invert(b.asComparableBytes(version));
    }

    static ByteSource invert(ByteSource src)
    {
        return () ->
        {
            int v = src.next();
            if (v == ByteSource.END_OF_STREAM)
                return v;
            return v ^ 0xFF;
        };
    }

    @Test
    public void testSingle()
    {
        Preencoded e = TrieUtil.comparable("test");
        InMemoryTrie<String> trie = strategy.create();
        putSimpleResolve(trie, e, "test", (x, y) -> y);
        System.out.println("Trie " + trie.dump());
        assertEquals("test", trie.get(e));
        assertEquals(null, trie.get(TrieUtil.comparable("teste")));
    }

    public enum ReuseStrategy
    {
        SHORT_LIVED
        {
            <T> InMemoryTrie<T> create()
            {
                return InMemoryTrie.shortLived(VERSION);
            }
        },
        LONG_LIVED
        {
            <T> InMemoryTrie<T> create()
            {
                return InMemoryTrie.longLived(VERSION, BufferType.OFF_HEAP, null);
            }
        },
        SHORT_LIVED_ORDERED
        {
            <T> InMemoryTrie<T> create()
            {
                return InMemoryTrie.shortLivedOrdered(VERSION);
            }
        };

        abstract <T> InMemoryTrie<T> create();
    }

    @Parameterized.Parameters(name="{0}")
    public static List<Object[]> generateData()
    {
        var list = new ArrayList<Object[]>();
        for (var s : ReuseStrategy.values())
            list.add(new Object[] {s});
        return list;
    }

    @Parameterized.Parameter(0)
    public static ReuseStrategy strategy = ReuseStrategy.SHORT_LIVED;

    public static Comparator<Preencoded> forwardComparator =
        (bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION);
    public static Comparator<Preencoded> reverseComparator =
        (bytes1, bytes2) -> ByteComparable.compare(invert(bytes1), invert(bytes2), VERSION);

    @Test
    public void testSplitMulti()
    {
        testEntries(new String[] { "testing", "tests", "trials", "trial", "aaaa", "aaaab", "abdddd", "abeeee" });
    }

    @Test
    public void testSplitMultiBug()
    {
        testEntriesHex(new String[] { "0c4143aeff", "0c4143ae69ff" });
    }


    @Test
    public void testSparse00bug()
    {
        String[] tests = new String[] {
        "40bd256e6fd2adafc44033303000",
        "40bdd47ec043641f2b403131323400",
        "40bd00bf5ae8cf9d1d403133323800",
        };
        InMemoryTrie<String> trie = strategy.create();
        for (String test : tests)
        {
            Preencoded e = ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(test));
            System.out.println("Adding " + asString(e) + ": " + test);
            putSimpleResolve(trie, e, test, (x, y) -> y);
        }

        System.out.println(trie.dump());

        for (String test : tests)
            assertEquals(test, trie.get(ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(test))));

        Arrays.sort(tests);

        int idx = 0;
        for (String s : trie.values())
        {
            if (s != tests[idx])
                throw new AssertionError("" + s + "!=" + tests[idx]);
            ++idx;
        }
        assertEquals(tests.length, idx);
    }

    @Test
    public void testUpdateContent()
    {
        String[] tests = new String[] {"testing", "tests", "trials", "trial", "testing", "trial", "trial"};
        String[] values = new String[] {"testing", "tests", "trials", "trial", "t2", "x2", "y2"};
        InMemoryTrie<String> trie = strategy.create();
        for (int i = 0; i < tests.length; ++i)
        {
            String test = tests[i];
            String v = values[i];
            Preencoded e = TrieUtil.comparable(test);
            System.out.println("Adding " + asString(e) + ": " + v);
            putSimpleResolve(trie, e, v, (x, y) -> "" + x + y);
            System.out.println("Trie " + trie.dump());
        }

        for (int i = 0; i < tests.length; ++i)
        {
            String test = tests[i];
            assertEquals(Stream.iterate(0, x -> x + 1)
                               .limit(tests.length)
                               .filter(x -> tests[x] == test)
                               .map(x -> values[x])
                               .reduce("", (x, y) -> "" + x + y),
                         trie.get(TrieUtil.comparable(test)));
        }
    }

    @Test
    public void testEntriesNullChildBug() throws TrieSpaceExhaustedException
    {
        Object[] trieDef = new Object[]
                           {
                               new Object[] { // 0
                                              ByteBufferUtil.bytes(1), // 01
                                              ByteBufferUtil.bytes(2)  // 02
                               },
                               // If requestChild returns null, bad things can happen (DB-2982)
                               null, // 1
                               ByteBufferUtil.bytes(3), // 2
                               new Object[] {  // 3
                                               ByteBufferUtil.bytes(4), // 30
                                               // Also try null on the Remaining.ONE path
                                               null // 31
                               },
                               ByteBufferUtil.bytes(5), // 4
                               // Also test requestUniqueDescendant returning null
                               new Object[] { // 5
                                              new Object[] { // 50
                                                             new Object[] { // 500
                                                                            null // 5000
                                                             }
                                              }
                               },
                               ByteBufferUtil.bytes(6) // 6
                           };

        SortedMap<Preencoded, ByteBuffer> expected = new TreeMap<>(forwardComparator);
        expected.put(comparable("00"), ByteBufferUtil.bytes(1));
        expected.put(comparable("01"), ByteBufferUtil.bytes(2));
        expected.put(comparable("2"), ByteBufferUtil.bytes(3));
        expected.put(comparable("30"), ByteBufferUtil.bytes(4));
        expected.put(comparable("4"), ByteBufferUtil.bytes(5));
        expected.put(comparable("6"), ByteBufferUtil.bytes(6));

        Trie<ByteBuffer> trie = TrieUtil.specifiedTrie(trieDef);
        System.out.println(trie.dump());
        assertSameContent(trie, expected);

        InMemoryTrie<ByteBuffer> inmem = strategy.create();
        inmem.apply(trie, (x, y) -> y, Predicates.alwaysFalse());
        System.out.println(inmem.dump());
        assertSameContent(inmem, expected);
    }

    static Preencoded comparable(String s)
    {
        ByteBuffer b = ByteBufferUtil.bytes(s);
        return ByteComparable.preencoded(VERSION, b);
    }

    @Test
    public void testDirect()
    {
        Preencoded[] src = TrieUtil.generateKeys(rand, COUNT);
        SortedMap<Preencoded, ByteBuffer> content = new TreeMap<>(forwardComparator);
        InMemoryTrie<ByteBuffer> trie = makeInMemoryTrie(src, content, usePut());
        int keysize = Arrays.stream(src)
                            .mapToInt(src1 -> ByteComparable.length(src1, VERSION))
                            .sum();
        long ts = ObjectSizes.measureDeep(content);
        long onh = ObjectSizes.measureDeep(((ContentManagerPojo<?>) trie.contentManager).contentArrays);
        System.out.format("Trie size on heap %,d off heap %,d measured %,d keys %,d treemap %,d\n",
                          trie.usedSizeOnHeap(), trie.usedSizeOffHeap(), onh, keysize, ts);
        System.out.format("per entry on heap %.2f off heap %.2f measured %.2f keys %.2f treemap %.2f\n",
                          trie.usedSizeOnHeap() * 1.0 / COUNT, trie.usedSizeOffHeap() * 1.0 / COUNT, onh * 1.0 / COUNT, keysize * 1.0 / COUNT, ts * 1.0 / COUNT);
        if (VERBOSE)
            System.out.println("Trie " + trie.dump(ByteBufferUtil::bytesToHex));

        assertSameContent(trie, content);
        checkGet(trie, content);

        trie.discardBuffers();
    }

    @Test
    public void testPrefixEvolution()
    {
        testEntries(new String[] { "testing",
                                   "test",
                                   "tests",
                                   "tester",
                                   "testers",
                                   // test changing type with prefix
                                   "types",
                                   "types1",
                                   "types",
                                   "types2",
                                   "types3",
                                   "types4",
                                   "types",
                                   "types5",
                                   "types6",
                                   "types7",
                                   "types8",
                                   "types",
                                   // test adding prefix to chain
                                   "chain123",
                                   "chain",
                                   // test adding prefix to sparse
                                   "sparse1",
                                   "sparse2",
                                   "sparse3",
                                   "sparse",
                                   // test adding prefix to split
                                   "split1",
                                   "split2",
                                   "split3",
                                   "split4",
                                   "split5",
                                   "split6",
                                   "split7",
                                   "split8",
                                   "split"
        });
    }

    @Test
    public void testPrefixUnsafeChain()
    {
        // Make sure prefixes on inside a chain aren't overwritten by embedded metadata node.

        testEntries(new String[] { "test89012345678901234567890",
                                   "test8",
                                   "test89",
                                   "test890",
                                   "test8901",
                                   "test89012",
                                   "test890123",
                                   "test8901234",
                                   });
    }

    @Test
    public void testSkipToPositionForSkippingBranchesOnMax()
    {
        testEntriesHex(
            // chain parent ending in 00
            "aaaaaa00", "aaaaaa0000", "aaaaaa00ab",
            // chain parent ending in FF
            "bbbbbbff", "bbbbbbffff", "bbbbbbffab",
            // sparse parent
            "cc00", "cc80", "ccff", "cc00ab", "cc80ab", "ccffab",
            // split parent
            "dd00", "dd80", "ddf0", "dd00ab", "dd80ab", "ddf0ab",
            "dd04", "dd84", "ddf4", "dd04ab", "dd84ab", "ddf4ab",
            "dd08", "dd88", "ddf8", "dd08ab", "dd88ab", "ddf8ab",
            "dd0f", "dd8f", "ddff", "dd0fab", "dd8fab", "ddffab"
        );
    }

    private void testEntries(String... tests)
    {
        for (Function<String, Preencoded> mapping :
                ImmutableList.<Function<String, Preencoded>>of(TrieUtil::comparable,
                                                                   s -> ByteComparable.preencoded(VERSION, s.getBytes())))
        {
            testEntries(tests, mapping);
        }
    }

    private void testEntriesHex(String... tests)
    {
        testEntries(tests, s -> ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s)));
        // Run the other translations just in case.
        testEntries(tests);
    }

    private void testEntries(String[] tests, Function<String, Preencoded> mapping)

    {
        InMemoryTrie<String> trie = strategy.create();
        for (String test : tests)
        {
            Preencoded e = mapping.apply(test);
            System.out.println("Adding " + asString(e) + ": " + test);
            putSimpleResolve(trie, e, test, (x, y) -> y);
            if (VERBOSE)
                System.out.println("Trie\n" + trie.dump());
        }

        for (String test : tests)
            assertEquals(test, trie.get(mapping.apply(test)));

        if (strategy == ReuseStrategy.SHORT_LIVED_ORDERED)
            testSkipOverBranch(tests, mapping, trie);

        testDeletions(tests, mapping, trie);

        randomizedTestEntries(tests, mapping, trie);
    }

    private void testSkipOverBranch(String[] testsAsStrings, Function<String, Preencoded> mapping, InMemoryTrie<String> trie)
    {
        testsAsStrings = Arrays.copyOf(testsAsStrings, testsAsStrings.length);
        Arrays.sort(testsAsStrings, (x, y) -> mapping.apply(x).compareTo(mapping.apply(y)));
        Preencoded[] tests = Arrays.stream(testsAsStrings).map(mapping).toArray(Preencoded[]::new);
        for (int testIndex = 0; testIndex < tests.length; ++testIndex)
        {
            Preencoded key = tests[testIndex];
            for (Direction d : Direction.values())
            {
                Cursor<String> c = trie.cursor(d);
                assertTrue(c.descendAlong(key.getPreencodedBytes()) || !d.isForward());

                long skipBranch = Cursor.positionForSkippingBranch(c.encodedPosition());
                long pos = c.skipTo(skipBranch);
                boolean exhausted = Cursor.isExhausted(pos);

                String next = exhausted ? null : c.content();
                if (next == null && !exhausted)
                    next = c.advanceToContent(null);
                int nextIndex = testIndex + d.increase;
                while (d.inLoop(nextIndex, 0, tests.length - 1) && isPrefix(key, tests[nextIndex]))
                    nextIndex += d.increase;
                String expected = d.inLoop(nextIndex, 0, tests.length - 1) ? testsAsStrings[nextIndex] : null;
                assertEquals("Value after skipping branch at " + testsAsStrings[testIndex] + " " + d, expected, next);
            }
        }
    }

    static boolean isPrefix(Preencoded prefix, Preencoded value)
    {
        ByteSource ps = prefix.getPreencodedBytes();
        ByteSource vs = value.getPreencodedBytes();
        while (true)
        {
            int nextp = ps.next();
            int nextv = vs.next();
            if (nextp == ByteSource.END_OF_STREAM)
                return true;
            if (nextp != nextv)
                return false;
        }
    }

    private void testDeletions(String[] tests, Function<String, Preencoded> mapping, InMemoryTrie<String> trie)
    {
        System.out.println("\nDeleting all entries");
        List<String> toDelete = Arrays.stream(tests).distinct().collect(Collectors.toList());
        while (!toDelete.isEmpty())
        {
            int index = rand.nextInt(toDelete.size());
            String entry = toDelete.remove(index);
            Preencoded e = mapping.apply(entry);
            System.out.println("Deleting " + asString(e) + ": " + entry);
            delete(trie, e);
            if (VERBOSE)
                System.out.println("Trie\n" + trie.dump());

            for (String test : toDelete)
                assertEquals(test, trie.get(mapping.apply(test)));
        }
        assertTrue(trie.isEmpty());
        if (((BufferManagerMultibuf) trie.bufferManager).cellAllocator instanceof MemoryAllocationStrategy.OpOrderReuseStrategy)
        {
            assertEquals(0L, trie.bufferManager.usedBufferSpace());
            assertEquals(0L, ((ContentManagerPojo<?>) trie.contentManager).usedObjectSpace());
        }
    }

    private void randomizedTestEntries(String[] tests, Function<String, Preencoded> mapping, InMemoryTrie<String> trie)
    {
        System.out.println("\nRandomized insert and delete");
        List<String> toInsert = Arrays.stream(tests).distinct().collect(Collectors.toList());
        List<String> inserted = new ArrayList<>();

        while (!toInsert.isEmpty())
        {
            if (rand.nextDouble() > 0.35)
            {
                // Insert one value
                int index = rand.nextInt(toInsert.size());
                String entry = toInsert.remove(index);
                Preencoded e = mapping.apply(entry);
                System.out.println("Adding " + asString(e) + ": " + entry);
                putSimpleResolve(trie, e, entry, (x, y) -> y);
                if (VERBOSE)
                    System.out.println("Trie\n" + trie.dump());
                inserted.add(entry);
            }
            else if (!inserted.isEmpty())
            {
                // Delete one value
                int index = rand.nextInt(inserted.size());
                String entry = inserted.remove(index);
                Preencoded e = mapping.apply(entry);
                System.out.println("Deleting " + asString(e) + ": " + entry);
                delete(trie, e);
                if (VERBOSE)
                    System.out.println("Trie\n" + trie.dump());
                toInsert.add(entry);
            }

            for (String test : inserted)
                assertEquals(test, trie.get(mapping.apply(test)));
            for (String test: toInsert)
                assertEquals(null, trie.get(mapping.apply(test)));
        }
    }

    static InMemoryTrie<ByteBuffer> makeInMemoryTrie(Preencoded[] src,
                                                     Map<Preencoded, ByteBuffer> content,
                                                     boolean usePut)

    {
        InMemoryTrie<ByteBuffer> trie = strategy.create();
        addToInMemoryTrie(src, content, trie, usePut);
        return trie;
    }

    static void addToInMemoryTrie(Preencoded[] src,
                                  Map<Preencoded, ByteBuffer> content,
                                  InMemoryTrie<? super ByteBuffer> trie,
                                  boolean usePut)

    {
        for (Preencoded b : src)
            addToInMemoryTrie(content, trie, usePut, b);
    }

    static void addNthToInMemoryTrie(Preencoded[] src,
                                     Map<Preencoded, ByteBuffer> content,
                                     InMemoryTrie<? super ByteBuffer> trie,
                                     boolean usePut,
                                     int divisor,
                                     int remainder)

    {
        int i = 0;
        for (Preencoded b : src)
        {
            if (i++ % divisor != remainder)
                continue;

            addToInMemoryTrie(content, trie, usePut, b);
        }
    }

    private static void addToInMemoryTrie(Map<Preencoded, ByteBuffer> content, InMemoryTrie<? super ByteBuffer> trie, boolean usePut, Preencoded b)
    {
        // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
        // (so that all sources have the same value).
        int payload = asString(b).hashCode();
        ByteBuffer v = ByteBufferUtil.bytes(payload);
        content.put(b, v);
        if (VERBOSE)
            System.out.println("Adding " + asString(b) + ": " + ByteBufferUtil.bytesToHex(v));
        putSimpleResolve(trie, b, v, (x, y) -> y, usePut);
        if (VERBOSE)
            System.out.println(trie.dump(x -> string(x)));
    }

    static void addToMap(Preencoded[] src,
                         Map<Preencoded, ByteBuffer> content)

    {
        for (Preencoded b : src)
        {
            // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
            // (so that all sources have the same value).
            int payload = asString(b).hashCode();
            ByteBuffer v = ByteBufferUtil.bytes(payload);
            content.put(b, v);
        }
    }

    static String string(Object x)
    {
        return x instanceof ByteBuffer
               ? ByteBufferUtil.bytesToHex((ByteBuffer) x)
               : x instanceof Preencoded
                 ? ((Preencoded) x).byteComparableAsString(VERSION)
                 : x.toString();
    }

    static void checkGet(BaseTrie<? super ByteBuffer, ?, ?> trie, Map<Preencoded, ByteBuffer> items)
    {
        for (Map.Entry<Preencoded, ByteBuffer> en : items.entrySet())
        {
            if (VERBOSE)
                System.out.println("Checking " + asString(en.getKey()) + ": " + ByteBufferUtil.bytesToHex(en.getValue()));
            assertEquals(en.getValue(), trie.get(en.getKey()));
        }
    }

    static void assertSameContent(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map)
    {
        assertMapEquals(trie, map, Direction.FORWARD);
        assertForEachEntryEquals(trie, map, Direction.FORWARD);
        assertValuesEqual(trie, map);
        assertForEachValueEquals(trie, map);
        assertUnorderedValuesEqual(trie, map);
        assertMapEquals(trie, map, Direction.REVERSE);
        assertForEachEntryEquals(trie, map, Direction.REVERSE);
        checkGet(trie, map);
    }

    private static void assertValuesEqual(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map)
    {
        assertIterablesEqual(trie.values(), map.values());
    }

    private static void assertUnorderedValuesEqual(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map)
    {
        Multiset<ByteBuffer> unordered = HashMultiset.create();
        StringBuilder errors = new StringBuilder();
        for (ByteBuffer b : trie.valuesUnordered())
            unordered.add(b);

        for (ByteBuffer b : map.values())
            if (!unordered.remove(b))
                errors.append("\nMissing value in valuesUnordered: " + ByteBufferUtil.bytesToHex(b));

        for (ByteBuffer b : unordered)
            errors.append("\nExtra value in valuesUnordered: " + ByteBufferUtil.bytesToHex(b));

        assertEquals("", errors.toString());
    }

    static Collection<Preencoded> maybeReversed(Direction direction, Collection<Preencoded> data)
    {
        return direction.isForward() ? data : reorderBy(data, reverseComparator);
    }

    static <V> Map<Preencoded, V> maybeReversed(Direction direction, Map<Preencoded, V> data)
    {
        return direction.isForward() ? data : reorderBy(data, reverseComparator);
    }

    private static <V> Map<Preencoded, V> reorderBy(Map<Preencoded, V> data, Comparator<Preencoded> comparator)
    {
        Map<Preencoded, V> newMap = new TreeMap<>(comparator);
        newMap.putAll(data);
        return newMap;
    }

    private static void assertForEachEntryEquals(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map, Direction direction)
    {
        Iterator<Map.Entry<Preencoded, ByteBuffer>> it = maybeReversed(direction, map).entrySet().iterator();
        trie.forEachEntry(direction, (key, value) -> {
            Assert.assertTrue("Map exhausted first, key " + asString(key), it.hasNext());
            Map.Entry<Preencoded, ByteBuffer> entry = it.next();
            assertEquals(0, ByteComparable.compare(entry.getKey(), key, VERSION));
            assertEquals(entry.getValue(), value);
        });
        Assert.assertFalse("Trie exhausted first", it.hasNext());
    }

    private static void assertForEachValueEquals(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map)
    {
        Iterator<ByteBuffer> it = map.values().iterator();
        trie.forEachValue(value -> {
            Assert.assertTrue("Map exhausted first, value " + ByteBufferUtil.bytesToHex(value), it.hasNext());
            ByteBuffer entry = it.next();
            assertEquals(entry, value);
        });
        Assert.assertFalse("Trie exhausted first", it.hasNext());
    }

    static void assertMapEquals(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map, Direction direction)
    {
        assertMapEquals(trie.entryIterator(direction), maybeReversed(direction, map).entrySet().iterator());
    }

    static <E> Collection<E> reorderBy(Collection<E> original, Comparator<E> comparator)
    {
        List<E> list = original.stream().collect(Collectors.toList());
        list.sort(comparator);
        return list;
    }

    static <B extends Preencoded, C extends Preencoded>
    void assertMapEquals(Iterator<Map.Entry<B, ByteBuffer>> it1, Iterator<Map.Entry<C, ByteBuffer>> it2)
    {
        List<Preencoded> failedAt = new ArrayList<>();
        StringBuilder b = new StringBuilder();
        while (it1.hasNext() && it2.hasNext())
        {
            Map.Entry<? extends Preencoded, ByteBuffer> en1 = it1.next();
            Map.Entry<? extends Preencoded, ByteBuffer> en2 = it2.next();
            b.append(String.format("TreeSet %s:%s\n", asString(en2.getKey()), ByteBufferUtil.bytesToHex(en2.getValue())));
            b.append(String.format("Trie    %s:%s\n", asString(en1.getKey()), ByteBufferUtil.bytesToHex(en1.getValue())));
            if (ByteComparable.compare(en1.getKey(), en2.getKey(), VERSION) != 0 || ByteBufferUtil.compareUnsigned(en1.getValue(), en2.getValue()) != 0)
                failedAt.add(en1.getKey());
        }
        while (it1.hasNext())
        {
            Map.Entry<? extends Preencoded, ByteBuffer> en1 = it1.next();
            b.append(String.format("Trie    %s:%s\n", asString(en1.getKey()), ByteBufferUtil.bytesToHex(en1.getValue())));
            failedAt.add(en1.getKey());
        }
        while (it2.hasNext())
        {
            Map.Entry<? extends Preencoded, ByteBuffer> en2 = it2.next();
            b.append(String.format("TreeSet %s:%s\n", asString(en2.getKey()), ByteBufferUtil.bytesToHex(en2.getValue())));
            failedAt.add(en2.getKey());
        }
        if (!failedAt.isEmpty())
        {
            String message = "Failed at " + Lists.transform(failedAt, InMemoryTrieTestBase::asString);
            System.err.println(message);
            System.err.println(b);
            Assert.fail(message);
        }
    }

    static <E extends Comparable<E>> void assertIterablesEqual(Iterable<E> expectedIterable, Iterable<E> actualIterable)
    {
        Iterator<E> expected = expectedIterable.iterator();
        Iterator<E> actual = actualIterable.iterator();
        while (actual.hasNext() && expected.hasNext())
        {
            Assert.assertEquals(actual.next(), expected.next());
        }
        if (expected.hasNext())
            Assert.fail("Remaing values in expected, starting with " + expected.next());
        else if (actual.hasNext())
            Assert.fail("Remaing values in actual, starting with " + actual.next());
    }

    static String asString(Preencoded bc)
    {
        return bc != null ? bc.byteComparableAsString(VERSION) : "null";
    }

    <T> void putSimpleResolve(InMemoryTrie<T> trie,
                              Preencoded key,
                              T value,
                              Trie.MergeResolver<T> resolver)
    {
        putSimpleResolve(trie, key, value, resolver, usePut());
    }

    static <T> void putSimpleResolve(InMemoryTrie<T> trie,
                                        Preencoded key,
                                        T value,
                                        Trie.MergeResolver<T> resolver,
                                        boolean usePut)
    {
        try
        {
            trie.putSingleton(key,
                              value,
                              (existing, update) -> existing != null ? resolver.resolve(existing, update) : update,
                              usePut);
        }
        catch (TrieSpaceExhaustedException e)
        {
            // Should not happen, test stays well below size limit.
            throw new AssertionError(e);
        }
    }

    <T> void delete(InMemoryTrie<T> trie, ByteComparable key)
    {
        delete(trie, key, usePut());
    }

    static <T> void delete(InMemoryTrie<T> trie, ByteComparable key, boolean usePut)
    {
        try
        {
            trie.putSingleton(key,
                              Boolean.TRUE,
                              (existing, update) -> update ? null : existing,
                              usePut);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw Throwables.propagate(e);
        }
    }
}
