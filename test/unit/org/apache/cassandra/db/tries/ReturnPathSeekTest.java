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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ReturnPathSeekTest
{
    static final String[] tests = new String[] {"", "testing", "test", "tests", "tested", "tesa", "tesz"};

    @Parameterized.Parameter(0)
    public boolean inMemorySingleton = true;
    @Parameterized.Parameter(1)
    public boolean useUnion = false;
    @Parameterized.Parameter(2)
    public boolean intersectTrie = false;

    @Parameterized.Parameters(name="inMemorySingleton={0}, useUnion={1}, intersectTrie={2}")
    public static List<Object[]> generateData()
    {
        var list = new ArrayList<Object[]>();
        for (boolean inMemorySingleton : new boolean[] {true, false})
            for (boolean useUnion : new boolean[] {true, false})
                for (boolean intersectTrie : new boolean[] {true, false})
                    list.add(new Object[] {inMemorySingleton, useUnion, intersectTrie});
        return list;
    }

    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    Trie<String> makeSingleton(ByteComparable.Preencoded e, String v) throws TrieSpaceExhaustedException
    {
        if (inMemorySingleton)
        {
            InMemoryTrie<String> tt = InMemoryTrie.shortLivedOrdered(VERSION);
            tt.putRecursive(e, v, (x, y) -> y);
            return tt;
        }
        else
            return Trie.singletonOrdered(e, e.encodingVersion(), v);
    }

    @Test
    public void testSingle() throws TrieSpaceExhaustedException
    {
        Assume.assumeFalse("Test can't use union", useUnion);
        for (String test : tests)
        {
            ByteComparable.Preencoded e = TrieUtil.directComparable(test);
            System.out.println("Testing singleton " + asString(e) + ": " + test);
            Trie<String> tt = makeSingleton(e, test);
            if (intersectTrie)
                tt = tt.slice(e, true, e, true); // ineffective but might mess it up

            testOrderedSeek(test, e, tt);
            assertEquals(test, Iterators.getOnlyElement(tt.valueIterator(Direction.FORWARD)));
            assertEquals(test, Iterators.getOnlyElement(tt.valueIterator(Direction.REVERSE)));
        }
    }

    @Test
    public void testMultiple() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = useUnion ? buildMergeTrie() : buildInMemoryTrie();

        System.out.println("Forward\n" + trie.dump());
        System.out.println("Reverse\n" + trie.process(Direction.REVERSE, new TrieDumper.Plain<>(Objects::toString)));

        Set<String> expected = new HashSet<>(Arrays.asList(tests));
        assertEquals(expected, Streams.stream(trie.values(Direction.FORWARD)).collect(Collectors.toSet()));
        assertEquals(expected, Streams.stream(trie.values(Direction.REVERSE)).collect(Collectors.toSet()));

        for (String test : tests)
        {
            ByteComparable.Preencoded e = TrieUtil.directComparable(test);
            System.out.println("Testing " + asString(e) + ": " + test);
            Trie<String> tt = trie;
            if (intersectTrie)
                tt = tt.slice(e, true, e, true);
            testOrderedSeek(test, e, tt);
        }
    }

    private InMemoryTrie<String> buildInMemoryTrie() throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = InMemoryTrie.shortLivedOrdered(VERSION);
        for (String test : tests)
        {
            ByteComparable.Preencoded e = TrieUtil.directComparable(test);
            System.out.println("Adding " + asString(e) + ": " + test);
            if (inMemorySingleton)
                trie.putRecursive(e, test, (x, y) -> y);
            else
                trie.mutator((String x, String y) -> y, Predicates.alwaysFalse()).apply(makeSingleton(e, test));
        }
        return trie;
    }

    private Trie<String> buildMergeTrie() throws TrieSpaceExhaustedException
    {
        List<Trie<String>> tries = new ArrayList<>();
        for (String test : tests)
        {
            ByteComparable.Preencoded e = TrieUtil.directComparable(test);
            System.out.println("Adding " + asString(e) + ": " + test);
            tries.add(makeSingleton(e, test));
        }
        return Trie.merge(tries, Trie.throwingResolver());
    }

    private static void testOrderedSeek(String value, ByteComparable.Preencoded key, Trie<String> trie)
    {
        assertEquals(value, trie.get(key)); // FORWARD direction
        testForwardSeekBeyond(value, key, trie);
        testReversedOrderedGet(value, key, trie);
        testReversedGetBefore(value, key, trie);
    }

    private static <T> boolean descendAlongToReturnPath(Cursor<T> cursor, ByteSource bytes)
    {
        long position = cursor.encodedPosition();
        int next = bytes.next();
        if (next == ByteSource.END_OF_STREAM)
        {
            long nextPosition = position | Cursor.ON_RETURN_PATH_BIT;
            if (Cursor.compare(position, nextPosition) == 0)
                return true;
            return Cursor.compare(cursor.skipTo(nextPosition), nextPosition) == 0;
        }
        while (next != ByteSource.END_OF_STREAM)
        {
            long nextPosition = Cursor.positionForDescentWithByte(position, next);
            next = bytes.next();
            if (next == ByteSource.END_OF_STREAM)
                nextPosition |= Cursor.ON_RETURN_PATH_BIT;
            if (Cursor.compare(cursor.skipTo(nextPosition), nextPosition) != 0)
                return false;
            position = nextPosition;
        }
        return true;
    }

    private static void testReversedOrderedGet(String test, ByteComparable.Preencoded e, Trie<String> trie)
    {
        Cursor<String> c = trie.cursor(Direction.REVERSE);
        assertTrue(descendAlongToReturnPath(c, e.getPreencodedBytes()));
        assertEquals(test, c.content());
    }

    private static void testReversedGetBefore(String test, ByteComparable.Preencoded e, Trie<String> trie)
    {
        Cursor<String> c = trie.cursor(Direction.REVERSE);
        assertFalse(c.descendAlong(e.getPreencodedBytes()) && c.content() == test);
    }

    private static void testForwardSeekBeyond(String test, ByteComparable.Preencoded e, Trie<String> trie)
    {
        Cursor<String> c = trie.cursor(Direction.FORWARD);
        assertFalse(descendAlongToReturnPath(c, e.getPreencodedBytes()));
    }

    @Test
    public void testSeeksAfterDescent() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = useUnion ? buildMergeTrie() : buildInMemoryTrie();

        for (String first : tests)
        {
            for (String second : tests)
            {
                if (first.compareTo(second) >= 0)
                    continue;

                Trie<String> tt = trie;
                if (intersectTrie)
                    tt = tt.subtrie(TrieUtil.directComparable(first), TrieUtil.directComparable(second));
                testSeekAfterDescent(first, second, tt);
            }
        }
    }

    @Test
    public void testSeeksAfterDescentOnPair() throws TrieSpaceExhaustedException
    {
        Assume.assumeTrue("Test can't not use union", useUnion);
        for (String first : tests)
        {
            for (String second : tests)
            {
                if (first.compareTo(second) >= 0)
                    continue;

                ByteComparable.Preencoded firstKey = TrieUtil.directComparable(first);
                ByteComparable.Preencoded secondKey = TrieUtil.directComparable(second);

                Trie<String> tt = makeSingleton(firstKey, first).mergeWith(makeSingleton(secondKey, second),
                                                                           Trie.throwingResolver());
                if (intersectTrie)
                    tt = tt.subtrie(firstKey, secondKey); // ineffective but might mess it up

                testSeekAfterDescent(first, second, tt);
            }
        }
    }

    private static void testSeekAfterDescent(String first, String second, Trie<String> trie)
    {
        System.out.println("Testing " + second + " to " + first + " reverse");
        ByteComparable.Preencoded firstKey = TrieUtil.directComparable(first);
        ByteComparable.Preencoded secondKey = TrieUtil.directComparable(second);

        Cursor<String> cursor = trie.cursor(Direction.REVERSE);
        descendAlongToReturnPath(cursor, secondKey.getPreencodedBytes());
        assertTrue(advanceByDifference(secondKey, firstKey, cursor));
        assertEquals(first, cursor.content());

        System.out.println("Testing " + first + " to " + second + " forward");
        cursor = trie.cursor(Direction.FORWARD);
        descendAlongToReturnPath(cursor, firstKey.getPreencodedBytes());
        assertFalse(advanceByDifference(firstKey, secondKey, cursor));
    }

    private static boolean advanceByDifference(ByteComparable.Preencoded from, ByteComparable.Preencoded to, Cursor<String> cursor)
    {
        int depth = 0;
        ByteSource.Peekable keyTo = to.getPreencodedBytes();
        ByteSource.Peekable keyFrom = from.getPreencodedBytes();
        int lastToByte = 0; // root return path
        while (keyTo.peek() == keyFrom.peek())
        {
            ++depth;
            lastToByte = keyTo.next();
            keyFrom.next();
        }
        int next = keyTo.next();
        Direction direction = Cursor.direction(cursor.encodedPosition());
        if (next == ByteSource.END_OF_STREAM)
        {
            long skipPosition = Cursor.encode(depth, lastToByte, direction) | Cursor.ON_RETURN_PATH_BIT;
            if (Cursor.compare(skipPosition, cursor.encodedPosition()) < 0)
                return false; // already beyond target
            long advancedPosition;
            if (Cursor.compare(skipPosition, cursor.encodedPosition()) == 0)
                advancedPosition = skipPosition;
            else
                advancedPosition = cursor.skipTo(skipPosition);
            return Cursor.compare(advancedPosition, skipPosition) == 0;
        }
        else
        {
            long skipPosition = Cursor.encode(depth + 1, next, direction);
            if (keyTo.peek() == ByteSource.END_OF_STREAM)
                skipPosition |= Cursor.ON_RETURN_PATH_BIT;
            if (Cursor.compare(skipPosition, cursor.encodedPosition()) < 0)
                return false; // already beyond target
            long advancedPosition;
            if (Cursor.compare(skipPosition, cursor.encodedPosition()) == 0)
                advancedPosition = skipPosition;
            else
                advancedPosition = cursor.skipTo(skipPosition);
            if (Cursor.compare(advancedPosition, skipPosition) != 0)
                return false;
            return descendAlongToReturnPath(cursor, keyTo);
        }
    }
}
