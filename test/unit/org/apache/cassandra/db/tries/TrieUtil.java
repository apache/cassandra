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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import org.junit.Assert;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.utils.bytecomparable.ByteComparable.EMPTY;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TrieUtil
{
    static final ByteComparable.Version VERSION = ByteComparable.Version.OSS50;
    public static final Comparator<Preencoded> REVERSE_COMPARATOR = (bytes1, bytes2) -> ByteComparable.compare(invert(bytes1), invert(bytes2), VERSION);
    public static final Comparator<Preencoded> FORWARD_COMPARATOR = (bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION);
    private static final int KEY_CHOICE = 25;
    private static final int MIN_LENGTH = 10;
    private static final int MAX_LENGTH = 50;

    static Map<String, String> toStringMap(BaseTrie<?, ?, ?> trie, Direction direction)
    {
        return Streams.stream(trie.entryIterator(direction))
                      .collect(Collectors.toMap(x -> asString(x.getKey()),
                                                x -> x.getValue().toString(),
                                                (x, y) -> '(' + x + ',' + y + ')',
                                                LinkedHashMap::new));
    }

    static <T> Map<String, String> toStringMap(Map<Preencoded, T> map, Function<T, ?> mapper)
    {
        return map.entrySet()
                  .stream()
                  .collect(Collectors.toMap(x -> asString(x.getKey()),
                                            x -> mapper.apply(x.getValue()).toString(),
                                            (x, y) -> '(' + x + ',' + y + ')',
                                            LinkedHashMap::new));
    }

    static <T> void assertMapEquals(Iterable<Map.Entry<Preencoded, T>> actual,
                                    Iterable<Map.Entry<Preencoded, T>> expected,
                                    Comparator<Preencoded> comparator)
    {
        Map<String, String> values1 = collectAsStrings(actual, comparator);
        Map<String, String> values2 = collectAsStrings(expected, comparator);
        assertMapEquals(values1, values2);
    }

    static void assertMapEquals(Map<String, String> actual, Map<String, String> expected)
    {
        if (actual.equals(expected))
            return;

        // If the maps are not equal, we want to print out the differences in a way that is easy to read.
        final Set<String> allKeys = Sets.union(actual.keySet(), expected.keySet());
        Set<String> keyDifference = allKeys.stream()
                                           .filter(k -> !Objects.equal(actual.get(k), expected.get(k)))
                                           .collect(Collectors.toCollection(TreeSet::new));
        System.err.println("All data");
        dumpDiff(actual, expected, allKeys);
        System.err.println("\nDifferences");
        dumpDiff(actual, expected, keyDifference);
        fail("Maps are not equal at " + keyDifference);
    }

    private static void dumpDiff(Map<String, String> actual, Map<String, String> expected, Set<String> set)
    {
        for (String key : set)
        {
            String v1 = actual.get(key);
            if (v1 != null)
                System.err.println(String.format("Actual   %s:%s", key, v1));
            String v2 = expected.get(key);
            if (v2 != null)
                System.err.println(String.format("Expected %s:%s", key, v2));
        }
    }

    private static <T> Map<String, String> collectAsStrings(Iterable<Map.Entry<Preencoded, T>> container,
                                                            Comparator<Preencoded> comparator)
    {
        var map = new LinkedHashMap<String, String>();
        Preencoded prevKey = null;
        for (var e : container)
        {
            var key = e.getKey();
            if (prevKey != null && comparator.compare(prevKey, key) >= 0)
                fail("Keys are not sorted: " + asString(prevKey) + " >= " + asString(key));
            prevKey = key;
            map.put(asString(key), e.getValue().toString());
        }
        return map;
    }

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

    static SpecStackEntry makeSpecStackEntry(Direction direction, Object spec, SpecStackEntry parent)
    {
        assert !(spec instanceof Pair);
        if (spec instanceof Object[])
        {
            final Object[] specArray = (Object[]) spec;
            return new SpecStackEntry(specArray, null, parent, direction.select(-1, specArray.length));
        }
        else
            return new SpecStackEntry(new Object[0], spec, parent, direction.select(-1, 1));

    }

    static <T> Trie<T> specifiedTrie(Object[] nodeDef)
    {
        return dir -> new CursorFromSpec<>(nodeDef, dir);
    }

    static Preencoded directComparable(String s)
    {
        ByteBuffer b = ByteBufferUtil.bytes(s);
        return ByteComparable.preencoded(VERSION, b);
    }

    @VisibleForTesting
    static Preencoded comparable(String s)
    {
        return ((ByteComparable) (v -> ByteSource.withTerminator(ByteSource.TERMINATOR, ByteSource.of(s, v)))).preencode(VERSION);
    }

    static void assertSameContent(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map)
    {
        // Don't use a loop for the direction to see it in the stack path in case of failure.
        assertMapEquals(trie, map, Direction.FORWARD);
        assertForEachEntryEquals(trie, map, Direction.FORWARD);
        assertValuesEqual(trie, map, Direction.FORWARD);
        assertForEachValueEquals(trie, map, Direction.FORWARD);

        assertMapEquals(trie, map, Direction.REVERSE);
        assertForEachEntryEquals(trie, map, Direction.REVERSE);
        assertValuesEqual(trie, map, Direction.REVERSE);
        assertForEachValueEquals(trie, map, Direction.REVERSE);

        assertUnorderedValuesEqual(trie, map);
        checkGet(trie, map);
    }

    static void checkGet(Trie<ByteBuffer> trie, Map<Preencoded, ByteBuffer> items)
    {
        for (Map.Entry<Preencoded, ByteBuffer> en : items.entrySet())
        {
            assertEquals(en.getValue(), trie.get(en.getKey()));
        }
    }

    private static void assertValuesEqual(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map, Direction direction)
    {
        assertIterablesEqual(trie.values(direction), maybeReversed(direction, map).values());
    }

    private static void assertUnorderedValuesEqual(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map)
    {
        Multiset<ByteBuffer> unordered = HashMultiset.create();
        StringBuilder errors = new StringBuilder();
        for (ByteBuffer b : trie.valuesUnordered())
            unordered.add(b);

        for (ByteBuffer b : map.values())
            if (!unordered.remove(b))
                errors.append("\nMissing value in valuesUnordered: ").append(ByteBufferUtil.bytesToHex(b));

        for (ByteBuffer b : unordered)
            errors.append("\nExtra value in valuesUnordered: ").append(ByteBufferUtil.bytesToHex(b));

        assertEquals("", errors.toString());
    }

    static <V> Map<Preencoded, V> maybeReversed(Direction direction, Map<Preencoded, V> data)
    {
        return direction.isForward() ? data : reorderBy(data, REVERSE_COMPARATOR);
    }

    static <V> Map<Preencoded, V> reorderBy(Map<Preencoded, V> data, Comparator<Preencoded> comparator)
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

    private static void assertForEachValueEquals(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map, Direction direction)
    {
        Iterator<ByteBuffer> it = maybeReversed(direction, map).values().iterator();
        trie.forEachValue(direction, value -> {
            Assert.assertTrue("Map exhausted first, value " + ByteBufferUtil.bytesToHex(value), it.hasNext());
            ByteBuffer entry = it.next();
            assertEquals("Map " + ByteBufferUtil.bytesToHex(entry) + " vs trie " + ByteBufferUtil.bytesToHex(value), entry, value);
        });
        Assert.assertFalse("Trie exhausted first", it.hasNext());
    }

    static void assertMapEquals(Trie<ByteBuffer> trie, SortedMap<Preencoded, ByteBuffer> map, Direction direction)
    {
        assertMapEquals(trie.entryIterator(direction), maybeReversed(direction, map).entrySet().iterator());
    }

    static void assertMapEquals(Iterator<Map.Entry<Preencoded, ByteBuffer>> actual,
                                Iterator<Map.Entry<Preencoded, ByteBuffer>> expected)
    {
        List<Preencoded> failedAt = new ArrayList<>();
        StringBuilder b = new StringBuilder();
        while (actual.hasNext() && expected.hasNext())
        {
            Map.Entry<Preencoded, ByteBuffer> en1 = actual.next();
            Map.Entry<Preencoded, ByteBuffer> en2 = expected.next();
            b.append(String.format("Expected %s:%s\n", asString(en2.getKey()), ByteBufferUtil.bytesToHex(en2.getValue())));
            b.append(String.format("Actual   %s:%s\n", asString(en1.getKey()), ByteBufferUtil.bytesToHex(en1.getValue())));
            if (ByteComparable.compare(en1.getKey(), en2.getKey(), VERSION) != 0 || ByteBufferUtil.compareUnsigned(en1.getValue(), en2.getValue()) != 0)
                failedAt.add(en1.getKey());
        }
        while (actual.hasNext())
        {
            Map.Entry<Preencoded, ByteBuffer> en1 = actual.next();
            b.append(String.format("Actual   %s:%s\n", asString(en1.getKey()), ByteBufferUtil.bytesToHex(en1.getValue())));
            failedAt.add(en1.getKey());
        }
        while (expected.hasNext())
        {
            Map.Entry<Preencoded, ByteBuffer> en2 = expected.next();
            b.append(String.format("Expected %s:%s\n", asString(en2.getKey()), ByteBufferUtil.bytesToHex(en2.getValue())));
            failedAt.add(en2.getKey());
        }
        if (!failedAt.isEmpty())
        {
            String message = "Failed at " + Lists.transform(failedAt, TrieUtil::asString);
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

    static Preencoded[] generateKeys(Random rand, int count)
    {
        Preencoded[] sources = new Preencoded[count];
        TreeSet<Preencoded> added = new TreeSet<>(FORWARD_COMPARATOR);
        for (int i = 0; i < count; ++i)
        {
            sources[i] = generateKey(rand);
            if (!added.add(sources[i]))
                --i;
        }

        // note: not sorted!
        return sources;
    }

    static Preencoded generateKeyAllowingPrefixes(Random rand)
    {
        return generateKey(rand, MIN_LENGTH, MAX_LENGTH, -1);
    }

    static Preencoded generateKey(Random rand)
    {
        return generateKey(rand, MIN_LENGTH, MAX_LENGTH, ByteSource.TERMINATOR);
    }

    static Preencoded generateKeyBound(Random rand)
    {
        return generateKey(rand, MIN_LENGTH, MAX_LENGTH, ByteSource.LT_NEXT_COMPONENT);
    }

    static Preencoded generateKey(Random rand, int minLength, int maxLength, int terminator)
    {
        int len = rand.nextInt(maxLength - minLength + 1) + minLength;
        byte[] bytes = new byte[len];
        int p = 0;
        int length = bytes.length;
        while (p < length)
        {
            int seed = rand.nextInt(KEY_CHOICE);
            Random r2 = new Random(seed);
            int m = r2.nextInt(5) + 2 + p;
            if (m > length)
                m = length;
            while (p < m)
                bytes[p++] = (byte) r2.nextInt(256);
        }
        if (terminator != -1)
            return ((ByteComparable)(v -> ByteSource.withTerminator(terminator, ByteSource.of(bytes, v)))).preencode(VERSION);
        else
            return ByteComparable.preencoded(VERSION, bytes);
    }

    public static <T> Trie<T> withRootMetadata(Trie<T> wrapped, T metadata)
    {
        return wrapped.mergeWith(Trie.singleton(ByteComparable.EMPTY, VERSION, metadata), Trie.throwingResolver());
    }

    public static <S extends RangeState<S>> RangeTrie<S> withRootMetadata(RangeTrie<S> wrapped, S metadata)
    {
        return wrapped.mergeWith(RangeTrie.point(ByteComparable.EMPTY, VERSION, true, metadata), Trie.throwingResolver());
    }

    public static <T, D extends RangeState<D>> DeletionAwareTrie<T, D> withRootMetadata(DeletionAwareTrie<T, D> wrapped, T metadata)
    {
        return wrapped.mergeWith(DeletionAwareTrie.singleton(ByteComparable.EMPTY, VERSION, metadata),
                                 Trie.throwingResolver(),
                                 Trie.throwingResolver(),
                                 (d,t) -> { throw new AssertionError(); },
                                 false);
    }

    static Trie<String> directTrie(String... points) throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = InMemoryTrie.shortLived(VERSION);
        for (String s : points)
            trie.putRecursive(directComparable(s), s, (ex, n) -> n);
        return trie;
    }

    static TrieSet directRanges(String... ranges)
    {
        return TrieSet.ranges(VERSION, true, true, Arrays.stream(ranges)
                                                         .map(TrieUtil::directComparable)
                                                         .toArray(ByteComparable[]::new));
    }

    static RangeTrie<TestRangeState> directRangeTrie(int value, String... keys)
    {
        return RangeTrie.fromSet(directRanges(keys), new TestRangeState(EMPTY, value, value));
    }

    static RangeTrie<TestRangeState> directRangeTrie(String... keys)
    {
        return directRangeTrie(1, keys);
    }

    static void verifyEqualRangeTries(RangeTrie<TestRangeState> trie, RangeTrie<TestRangeState> expected)
    {
//        System.out.println("Actual:  \n" + trie.dump(TestRangeState::toStringNoPosition));
//        System.out.println("Expected:\n" + expected.cursor(Direction.FORWARD).process(new TrieDumper.Plain<>(TestRangeState::toStringNoPosition)));
        assertMapEquals(TestRangeState.toStringMap(trie, Direction.FORWARD),
                        TestRangeState.toStringMap(expected, Direction.FORWARD));
        assertMapEquals(TestRangeState.toStringMap(trie, Direction.REVERSE),
                        TestRangeState.toStringMap(expected, Direction.REVERSE));
    }

    static Preencoded toBound(Preencoded bc)
    {
        return toBound(bc, false);
    }

    static Preencoded toBound(Preencoded bc, boolean greater)
    {
        if (bc == null)
            return null;

        byte[] data = bc.getPreencodedBytes().remainingBytesToArray();
        data[data.length - 1] = (byte) (greater ? ByteSource.GT_NEXT_COMPONENT : ByteSource.LT_NEXT_COMPONENT);
        return ByteComparable.preencoded(bc.encodingVersion(), data);
    }

    static String asString(ByteComparable bc)
    {
        return bc != null ? bc.byteComparableAsString(VERSION) : "null";
    }

    static <T> NavigableMap<Preencoded, T> boundedMap(NavigableMap<Preencoded, T> sourceMap, Preencoded ll, boolean includeLeft, Preencoded rr, boolean includeRight)
    {
        // Our slice has somewhat different semantics:
        // - prefixes are not supported, i.e. a range like (a, aaa) cannot be used
        // - inclusivity extends to the branches of each bound
        Preencoded l = !includeLeft ? nudge(ll) : ll;
        Preencoded r = includeRight ? nudge(rr) : rr;

        return l == null
               ? r == null
                 ? sourceMap
                 : sourceMap.headMap(r, false)
               : r == null
                 ? sourceMap.tailMap(l, true)
                 : sourceMap.subMap(l, true, r, false);
    }

    static Preencoded nudge(Preencoded v)
    {
        if (v == null)
            return null;

        byte[] data = v.getPreencodedBytes().remainingBytesToArray();
        int len = data.length;
        while (len > 0 && data[len-1] == -1)
            --len;

        if (len == 0)
            return null;

        ++data[len - 1];
        return ByteComparable.preencoded(v.encodingVersion(), data, 0, len);
    }

    /// Creates a simple trie with a root having the provided number of childs, where each child is a leaf whose content
    /// is simply the value of the transition leading to it.
    /// In other words, `singleLevelIntTrie(4)` creates the following trie:
    /// ```
    ///       Root
    /// t= 0  1  2  3
    ///    |  |  |  |
    ///    0  1  2  3
    /// ```
    static Trie<Integer> singleLevelIntTrie(int childs, boolean sliceCompatibleContent)
    {
        return new Trie<>()
        {
            @Override
            public Cursor<Integer> makeCursor(Direction direction)
            {
                return new SingleLevelCursor(direction);
            }

            class SingleLevelCursor implements Cursor<Integer>
            {
                final Direction direction;
                int current;
                final boolean presentContentOnReturnPath;

                SingleLevelCursor(Direction direction)
                {
                    this.direction = direction;
                    current = direction.select(-1, childs);
                    presentContentOnReturnPath = !direction.isForward() && sliceCompatibleContent;
                }

                @Override
                public long advance()
                {
                    current += direction.increase;
                    return encodedPosition();
                }

                @Override
                public long skipTo(long encodedSkipPosition)
                {
                    int depth = Cursor.depth(encodedSkipPosition);
                    int transition = Cursor.incomingTransition(encodedSkipPosition);

                    if (Cursor.isOnReturnPath(encodedSkipPosition) && !presentContentOnReturnPath)
                        transition += direction.increase;

                    if (depth > 1)
                        return advance();
                    if (depth < 1)
                        transition = exhausted();

                    if (direction.isForward())
                        current = Math.max(0, transition);
                    else
                        current = Math.min(childs - 1, transition);

                    return encodedPosition();
                }

                int exhausted()
                {
                    int lastPos = direction.select(childs, -1);
                    if (presentContentOnReturnPath)
                        lastPos += direction.increase;
                    return lastPos;
                }

                @Override
                public long encodedPosition()
                {
                    if (current == direction.select(-1, childs))
                        return Cursor.rootPosition(direction);
                    else if (presentContentOnReturnPath && current == direction.select(childs, -1))
                        return Cursor.rootPosition(direction) | Cursor.ON_RETURN_PATH_BIT;
                    else if (direction.inLoop(current, 0, childs - 1))
                        return Cursor.encode(1, current, direction) |
                               (presentContentOnReturnPath ? Cursor.ON_RETURN_PATH_BIT : 0);
                    return Cursor.exhaustedPosition(direction);
                }

                @Override
                public Integer content()
                {
                    if (presentContentOnReturnPath != Cursor.isOnReturnPath(encodedPosition()))
                        return null;
                    return current == childs ? -1 : current;
                }

                @Override
                public ByteComparable.Version byteComparableVersion()
                {
                    return VERSION;
                }

                @Override
                public Cursor<Integer> tailCursor(Direction d)
                {
                    if (current == -1)
                        return makeCursor(d);
                    else
                        throw new UnsupportedOperationException("tailTrie on test cursor");
                }
            }
        };
    }

    static String dump(BaseTrie<?, ?, ?> s, Direction direction)
    {
        return s.process(direction, new TrieDumper.Plain<>(Object::toString));
    }

    static void dumpToOut(BaseTrie<?, ?, ?> s)
    {
        System.out.println("Forward:");
        System.out.println(dump(s, Direction.FORWARD));
        System.out.println("Reverse:");
        System.out.println(dump(s, Direction.REVERSE));
    }

    /// Assert two tries are equal, performing exactly the same walks in both cursor directions.
    public static<T, D extends RangeState<D>> void assertTriesEqual(DeletionAwareTrie<T, D> expected, DeletionAwareTrie<T, D> actual)
    {
        if (expected == null || actual == null)
        {
            assertEquals(expected, actual);
            return;
        }

        for (Direction dir : Direction.values())
            try
            {
                assertCursorWalksEqual(expected.cursor(dir), actual.cursor(dir));
            }
            catch (Throwable t)
            {
                System.err.println("Expected\n" + expected.cursor(dir).process(new TrieDumper.DeletionAware<>(Object::toString, Object::toString)));
                System.err.println("Actual\n" + actual.cursor(dir).process(new TrieDumper.DeletionAware<>(Object::toString, Object::toString)));
                throw t;
            }
    }

    public static<T, D extends RangeState<D>> void assertCursorWalksEqual(DeletionAwareCursor<T, D> expected, DeletionAwareCursor<T, D> actual)
    {
        long expectedPos = expected.encodedPosition();
        long actualPos = actual.encodedPosition();
        while (true)
        {
            assertEquals("Actual pos " + Cursor.toString(actualPos) + " != " + Cursor.toString(expectedPos), expectedPos, actualPos);
            if (Cursor.isExhausted(actualPos))
                return;
            T expectedContent = expected.content();
            T actualContent = actual.content();
            assertEquals("Content", expectedContent, actualContent);
            RangeCursor<D> expectedDeletionBranch = expected.deletionBranchCursor(Cursor.direction(expectedPos));
            RangeCursor<D> actualDeletionBranch = actual.deletionBranchCursor(Cursor.direction(actualPos));
            assertEquals("Deletion branch present", expectedDeletionBranch != null, actualDeletionBranch != null);
            if (expectedDeletionBranch != null)
                assertCursorWalksEqual(expectedDeletionBranch, actualDeletionBranch);
            expectedPos = expected.advance();
            actualPos = actual.advance();
        }
    }

    public static<T> void assertCursorWalksEqual(Cursor<T> expected, Cursor<T> actual)
    {
        long expectedPos = expected.encodedPosition();
        long actualPos = actual.encodedPosition();
        while (true)
        {
            assertEquals("Actual pos " + Cursor.toString(actualPos) + " != " + Cursor.toString(expectedPos), expectedPos, actualPos);
            if (Cursor.isExhausted(actualPos))
                return;
            T expectedContent = expected.content();
            T actualContent = actual.content();
            assertEquals("Content", expectedContent, actualContent);
            expectedPos = expected.advance();
            actualPos = actual.advance();
        }
    }


    /// Assert two tries are the same with respect to their content and deletion branches and their paths. Uses
    /// `advanceToContent` in order to ignore unproductive branches.
    public static<T, D extends RangeState<D>> void assertTrieContentEqual(DeletionAwareTrie<T, D> expected, DeletionAwareTrie<T, D> actual)
    {
        if (expected == null && actual == null)
            return;

        if (actual == null)
            actual = DeletionAwareTrie.empty(VERSION);
        if (expected == null)
            expected = DeletionAwareTrie.empty(VERSION);

        for (Direction dir : Direction.values())
            try
            {
                assertCursorContentEqual(expected.cursor(dir), actual.cursor(dir));
            }
            catch (Throwable t)
            {
                System.err.println("Expected\n" + expected.cursor(dir).process(new TrieDumper.DeletionAware<>(Object::toString, Object::toString)));
                System.err.println("Actual\n" + actual.cursor(dir).process(new TrieDumper.DeletionAware<>(Object::toString, Object::toString)));
                throw t;
            }
    }

    private static <T, D extends RangeState<D>> void assertCursorContentEqual(DeletionAwareCursor<T, D> expected, DeletionAwareCursor<T, D> actual)
    {
        TriePathReconstructor expectedPath = new TriePathReconstructor();
        TriePathReconstructor actualPath = new TriePathReconstructor();
        long expectedPos = expected.encodedPosition();
        long actualPos = actual.encodedPosition();
        T expectedContent = expected.content();
        T actualContent = actual.content();
        RangeCursor<D> expectedDeletionBranch = expected.deletionBranchCursor(expected.direction());
        RangeCursor<D> actualDeletionBranch = actual.deletionBranchCursor(actual.direction());
        while (true)
        {
            assertEquals("Actual pos " + Cursor.toString(actualPos) + " != " + Cursor.toString(expectedPos), expectedPos, actualPos);
            assertEquals("Path", Hex.bytesToHex(expectedPath.getTrimmedPathBytes()), Hex.bytesToHex(actualPath.getTrimmedPathBytes()));
            assertEquals("Content", expectedContent, actualContent);
            if (expectedDeletionBranch != null ^ actualDeletionBranch != null)
            {
                if (expectedDeletionBranch == null)
                    expectedDeletionBranch = RangeCursor.empty(expected.direction(), VERSION);
                else if (actualDeletionBranch == null)
                    actualDeletionBranch = RangeCursor.empty(actual.direction(), VERSION);
            }
            assertEquals("Deletion branch present", expectedDeletionBranch != null, actualDeletionBranch != null);
            if (expectedDeletionBranch != null)
                assertCursorContentEqual(expectedDeletionBranch, actualDeletionBranch);

            if (Cursor.isExhausted(actualPos))
                return;

            expectedContent = null;
            expectedDeletionBranch = null;
            do
            {
                expectedPos = advanceMultipleIntoReceiver(expected, expectedPath, expectedPos);
                if (!Cursor.isExhausted(expectedPos))
                {
                    expectedContent = expected.content();
                    expectedDeletionBranch = expected.deletionBranchCursor(expected.direction());
                }
                else
                    break;
            }
            while (expectedContent == null && expectedDeletionBranch == null);

            actualContent = null;
            actualDeletionBranch = null;
            do
            {
                actualPos = advanceMultipleIntoReceiver(actual, actualPath, actualPos);
                if (!Cursor.isExhausted(actualPos))
                {
                    actualContent = actual.content();
                    actualDeletionBranch = actual.deletionBranchCursor(actual.direction());
                }
                else
                    break;
            }
            while (actualContent == null && actualDeletionBranch == null);
        }

    }

    private static <T> long advanceMultipleIntoReceiver(Cursor<T> cursor, Cursor.ResettingTransitionsReceiver receiver, long prevPosition)
    {
        long currPosition = cursor.advanceMultiple(receiver);
        if (Cursor.ascended(currPosition, prevPosition))
        {
            int depth = Cursor.depth(currPosition);
            if (depth > 0)
            {
                receiver.resetPathLength(depth - 1);
                receiver.addPathByte(Cursor.incomingTransition(currPosition));
            }
            else
            {
                receiver.resetPathLength(0);
            }
        }
        else
            receiver.addPathByte(Cursor.incomingTransition(currPosition));

        if (Cursor.isOnReturnPath(currPosition))
            receiver.onReturnPath();
        return currPosition;
    }

    private static <T> void assertCursorContentEqual(Cursor<T> expected, Cursor<T> actual)
    {
        TriePathReconstructor expectedPath = new TriePathReconstructor();
        TriePathReconstructor actualPath = new TriePathReconstructor();
        T expectedContent = expected.content();
        T actualContent = actual.content();
        while (true)
        {
            long expectedPos = expected.encodedPosition();
            long actualPos = actual.encodedPosition();
            assertEquals("Actual pos " + Cursor.toString(actualPos) + " != " + Cursor.toString(expectedPos), expectedPos, actualPos);
            if (Cursor.isExhausted(actualPos))
                return;
            assertEquals("Path", Hex.bytesToHex(expectedPath.getTrimmedPathBytes()), Hex.bytesToHex(actualPath.getTrimmedPathBytes()));
            assertEquals("Content", expectedContent, actualContent);
            expectedContent = expected.advanceToContent(expectedPath);
            actualContent = actual.advanceToContent(actualPath);
        }
    }

    static class SpecStackEntry
    {
        Object[] children;
        int curChild;
        Object content;
        SpecStackEntry parent;

        public SpecStackEntry(Object[] spec, Object content, SpecStackEntry parent, int curChild)
        {
            this.children = spec;
            this.content = content;
            this.parent = parent;
            this.curChild = curChild;
        }
    }

    public static class CursorFromSpec<T> implements Cursor<T>
    {
        SpecStackEntry stack;
        Direction direction;
        long position;

        CursorFromSpec(Object[] spec, Direction direction)
        {
            this.direction = direction;
            stack = makeSpecStackEntry(direction, spec, null);
            position = Cursor.rootPosition(direction);
        }

        @Override
        public long advance()
        {
            SpecStackEntry current = stack;
            Object child;
            int depth = Cursor.depth(position);
            do
            {
                while (current != null
                       && (current.children.length == 0
                           || !direction.inLoop(current.curChild += direction.increase, 0, current.children.length - 1)))
                {
                    current = current.parent;
                    --depth;
                }
                if (current == null)
                {
                    stack = null;
                    return position = Cursor.exhaustedPosition(direction);
                }

                child = current.children[current.curChild];
            }
            while (child == null);
            stack = makeSpecStackEntry(direction, child, current);

            return position = encode(++depth);
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            int skipDepth = Cursor.depth(encodedSkipPosition);
            int skipTransition = Cursor.incomingTransition(encodedSkipPosition);
            if (Cursor.isOnReturnPath(encodedSkipPosition))
                skipTransition += direction.increase;
            int depth = Cursor.depth(position);
            assert skipDepth <= depth + 1 : "skipTo descends more than one level";

            while (stack != null && skipDepth <= depth)
            {
                --depth;
                stack = stack.parent;
            }
            if (stack == null)
            {
                return position = Cursor.exhaustedPosition(direction);
            }

            int index = skipTransition - 0x30;
            assert direction.gt(index, stack.curChild) : "Backwards skipTo";
            if (direction.gt(index, direction.select(stack.children.length - 1, 0)))
            {
                --depth;
                stack = stack.parent;
                return advance();
            }
            stack.curChild = index - direction.increase;
            return advance();
        }

        @Override
        public long encodedPosition()
        {
            return position;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T content()
        {
            return (T) stack.content;
        }

        private long encode(int depth)
        {
            return Cursor.encode(depth, stack.parent.curChild + 0x30, direction);
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return VERSION;
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public String toString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(Cursor.toString(position));
            if (stack.content != null)
                stringBuilder.append(" content ")
                             .append(stack.content);
            stringBuilder.append(" children ");
            stringBuilder.append(IntStream.range(0, stack.children.length)
                                          .filter(i -> stack.children[i] != null)
                                          .mapToObj(i -> (i + 1 == stack.curChild ? "*" : "") + (char) (i + 0x30))
                                          .reduce("", (x, y) -> x + y));
            return stringBuilder.toString();
        }
    }
}
