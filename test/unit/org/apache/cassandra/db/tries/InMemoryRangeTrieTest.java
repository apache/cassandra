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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class InMemoryRangeTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static int delTime;

    @Parameterized.Parameter(0)
    public static boolean addTerminators = false;

    @Parameterized.Parameter(1)
    public static boolean forceCopy = false;

    @Parameterized.Parameters(name = "addTerminators: {0} forceCopy: {1}")
    public static Object[] parameters()
    {
        return new Object[][]
               {
                   new Object[]{ false, false },
                   new Object[]{ false, true },
                   new Object[]{ true, false },
                   new Object[]{ true, true },
               };
    }

    @Before
    public void init()
    {
        delTime = 99;
    }

    static TestRangeState toMarker(String string)
    {
        return toMarker(string, delTime--);
    }

    static TestRangeState toMarker(String string, int delTime)
    {
        return new TestRangeState(TrieUtil.directComparable(string), delTime, delTime);
    }

    static TestRangeState addMarkerStrings(TestRangeState a, TestRangeState b)
    {
        if (a == null)
            return b;

        TestRangeState c = TestRangeState.combine(a, b);
        return new TestRangeState(TrieUtil.directComparable(fromMarker(a) + fromMarker(b)),
                                  c.appliesAfter,
                                  c.leftSide,
                                  c.rightSide);
    }

    static String fromMarker(TestRangeState marker)
    {
        if (marker == null)
            return null;
        return new String(marker.position.asByteComparableArray(VERSION), StandardCharsets.UTF_8);
    }

    @Test
    public void testSingle()
    {
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        putRange(trie, "test", toMarker("test"), (x, y) -> y);
        System.out.println("Trie " + trie.dump());
        assertEquals("test", fromMarker(trie.applicableRange(key("test"))));
        assertEquals(null, fromMarker(trie.applicableRange(key("tezt"))));
        assertEquals(null, fromMarker(trie.applicableRange(key("tast"))));
    }

    @Test
    public void testSplitMulti()
    {
        testEntries("testing", "tests", "trials", "trial", "aaaa", "aaaab", "abdddd", "abeeee");
    }

    @Test
    public void testSplitMultiBug()
    {
        testEntriesHex(new String[]{ "0c4143aeff", "0c4143ae69ff" });
    }

    @Test
    public void testUpdateContent()
    {
        String[] tests = new String[]{ "testing", "tests", "trials", "trial", "testing", "trial", "trial" };
        String[] values = new String[]{ "testing", "tests", "trials", "trial", "t2", "x2", "y2" };
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        for (int i = 0; i < tests.length; ++i)
        {
            String test = tests[i];
            String v = values[i];
            System.out.println("Adding " + test + ": " + v);
            putRange(trie, test, toMarker(v), InMemoryRangeTrieTest::addMarkerStrings);
            System.out.println("Trie " + trie.dump());
        }

        for (int i = 0; i < tests.length; ++i)
        {
            String test = tests[i];
            assertEquals(IntStream.range(0, tests.length)
                                  .filter(x -> test.startsWith(tests[x]))
                                  .mapToObj(x -> values[x])
                                  .collect(Collectors.joining()),
                         fromMarker(trie.applicableRange(key(TrieUtil.directComparable(test)))));
        }
    }

    @Test
    public void testApplyUpdate() throws TrieSpaceExhaustedException
    {
        // Ranges in pairs. They may touch but not overlap (putRecursive can't handle covering ranges).
        String[] bounds = new String[]{ "<aaa", ">aaa", "<touch", ">touchAfter", ">touchAfter", "<touchBefore", "<touchBefore", "<touchTwice", "<touchTwice", ">touchTwice", ">touchTwice", ">touch" };
        String[] tests = Arrays.stream(bounds).map(x -> x.substring(1)).toArray(String[]::new);
        Boolean[] after = Arrays.stream(bounds).map(x -> x.charAt(0) == '>').toArray(Boolean[]::new);
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        for (int i = 0; i < tests.length; i += 2)
        {
            System.out.println("Adding " + bounds[i] + "-" + bounds[i + 1]);

            trie.apply(RangeTrie.range(bound(tests[i], after[i]), !after[i], bound(tests[i+1], after[i+1]), after[i+1], VERSION, toMarker(bounds[i], i)),
                       InMemoryRangeTrieTest::addMarkerStrings,
                       x -> forceCopy);
            System.out.println("Trie " + trie.dump());
        }

        for (int i = 0; i < tests.length; ++i)
        {
            String bound = bounds[i];
            assertEquals(IntStream.range(0, tests.length)
                                  .filter(x -> bounds[x].equals(bound))
                                  // The ranges have the left side as the marker string
                                  .mapToObj(x -> bounds[x & ~1])
                               .collect(Collectors.joining()),
                         fromMarker(get(trie, bound(tests[i], after[i]), after[i])));
        }
    }

    @Test
    public void testPutRecursiveUpdate() throws TrieSpaceExhaustedException
    {
        // Ranges in pairs. They may touch but not overlap (putRecursive can't handle covering ranges).
        String[] bounds = new String[]{ "<aaa", ">aaa", "<touch", ">touchAfter", ">touchAfter", "<touchBefore", "<touchBefore", "<touchTwice", "<touchTwice", ">touchTwice", ">touchTwice", ">touch" };
        String[] tests = Arrays.stream(bounds).map(x -> x.substring(1)).toArray(String[]::new);
        Boolean[] after = Arrays.stream(bounds).map(x -> x.charAt(0) == '>').toArray(Boolean[]::new);
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        for (int i = 0; i < tests.length; i += 2)
        {
            System.out.println("Adding " + bounds[i] + "-" + bounds[i + 1]);
            trie.putRecursive(bound(tests[i], after[i]),
                              toMarker(bounds[i], i).asBoundary(Direction.FORWARD),
                              after[i],
                              InMemoryRangeTrieTest::addMarkerStrings);
            trie.putRecursive(bound(tests[i + 1], after[i + 1]),
                              toMarker(bounds[i + 1], i).asBoundary(Direction.REVERSE),
                              after[i + 1],
                              InMemoryRangeTrieTest::addMarkerStrings);
            System.out.println("Trie " + trie.dump());
        }

        for (int i = 0; i < tests.length; ++i)
        {
            String bound = bounds[i];
            assertEquals(Arrays.stream(bounds)
                               .filter(x -> x.equals(bound))
                               .collect(Collectors.joining()),
                         fromMarker(get(trie, bound(tests[i], after[i]), after[i])));
        }
    }

    @Test
    public void testMultipathApplyEE() throws TrieSpaceExhaustedException
    {
        testMultipathApply(true, false,
                           new String[]{ "abc", "ade", "a", "bcd", "bcd", "bceeeee", "bce", "bd" });
    }

    @Test
    public void testMultipathApplyIE() throws TrieSpaceExhaustedException
    {
        // repetitions are acceptable, but our test will fail because the entry there will end up dropped
        testMultipathApply(false, false,
                           new String[]{ "a", "ade", "b", "bcd", "bce", "bceeeee", "bcf", "bd" });
    }

    @Test
    public void testMultipathApplyEI() throws TrieSpaceExhaustedException
    {
        testMultipathApply(true, true,
                           new String[]{ "abc", "ab", "a", "bcd", "bceeeee", "bce", "bcf", "bd" });
    }

    @Test
    public void testMultipathApplyII() throws TrieSpaceExhaustedException
    {
        testMultipathApply(false, true,
                           new String[]{ "a", "abc", "ade", "a", "bcd", "bcd", "bce", "bceeeeee", "bddddddd", "bd", "efg", "hik" });
    }

    private void testMultipathApply(boolean startsAfter, boolean endsAfter, String[] tests) throws TrieSpaceExhaustedException
    {
        ByteComparable[] keys = IntStream.range(0, tests.length)
                                         .mapToObj(x -> bound(tests[x], x % 2 == 0 ? startsAfter : endsAfter))
                                         .toArray(ByteComparable[]::new);

        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        trie.apply(RangeTrie.fromSet(TrieSet.ranges(VERSION, !startsAfter, endsAfter, keys),
                                     toMarker("marker", 1)),
                   InMemoryRangeTrieTest::addMarkerStrings,
                   x -> forceCopy);
        System.out.println("Trie " + trie.dump());

        for (int i = 0; i < tests.length; ++i)
        {
            boolean after = i % 2 == 0 ? startsAfter : endsAfter;
            assertEquals("for key " + (after ? ">" : "<") + tests[i], "marker",
                         fromMarker(get(trie, bound(tests[i], after), after)));
        }
    }

    private <T> T get(BaseTrie<T, ?, ?> trie, ByteComparable key, boolean after)
    {
        Cursor<T> cursor = trie.cursor(Direction.FORWARD);
        ByteSource bytes = key.asComparableBytes(cursor.byteComparableVersion());
        int next = bytes.next();
        long position = cursor.encodedPosition();
        while (next != ByteSource.END_OF_STREAM)
        {
            long nextPosition = Cursor.positionForDescentWithByte(position, next);
            next = bytes.next();
            if (after && next == ByteSource.END_OF_STREAM)
                nextPosition |= Cursor.ON_RETURN_PATH_BIT;
            if (Cursor.compare(cursor.skipTo(nextPosition), nextPosition) != 0)
                return null;
            position = nextPosition;
        }
        return cursor.content();
    }

    private ByteComparable bound(String s, boolean after)
    {
        return after ? rightBound(s) : leftBound(s);
    }

    private void testEntries(String... tests)
    {
        testEntries(tests, InMemoryRangeTrieTest::bc);
    }

    private void testEntriesHex(String[] tests)
    {
        testEntries(tests, s -> ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s)));
        // Run the other translations just in case.
        testEntries(tests);
    }

    private void testEntries(String[] tests, Function<String, ByteComparable> mapping)

    {
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        for (String test : tests)
        {
            ByteComparable e = mapping.apply(test);
            System.out.println("Adding " + asString(e) + ": " + test);
            putRange(trie, e, toMarker(test), (x, y) -> TestRangeState.combine(y, x));
            System.out.println("Trie\n" + trie.dump());
        }

        for (String test : tests)
        {
            // Entries with greater delTime override ones with smaller. So we will match the leftmost key in the list.
            String expected = Arrays.stream(tests).filter(test::startsWith).findFirst().get();
            assertEquals(expected, fromMarker(trie.applicableRange(key(mapping.apply(test)))));
        }
    }

    static String asString(ByteComparable bc)
    {
        return bc != null ? bc.byteComparableAsString(VERSION) : "null";
    }

    @Test
    public void testCursorDeletionBeforeNearest() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("aaebc", "aaecd"),
                                            "aa", "aaec",
                                            strings("aabc", "aacd"));
    }

    @Test
    public void testCursorRangeDeletionCoversPosition() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("aaabc", "aaacde", "bcd", "cde"),
                                            "aaa", "aaacd",
                                            strings("a_", "ab"));
    }

    @Test
    public void testCursorBranchDeletionCoversPosition() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("aaabc", "aaacde", "bcd", "cde"),
                                            "aaa", "aaacd",
                                            strings("aa", "aa"));
    }

    private String[] strings(String... strings)
    {
        return strings;
    }

    private void testCursorsWithInterveningDeletions(String[] preparations,
                                                     String leftPos,
                                                     String rightPos,
                                                     String[] insertions)
    throws TrieSpaceExhaustedException
    {
        // Note: if position matches a boundary we may get a false negative when looking for it because it will be on
        // the return path in one of the directions. If any of these checks fails, it is a test error, please make
        // sure the queried positions are not boundaries.
        assertFalse(Arrays.asList(preparations).contains(leftPos));
        assertFalse(Arrays.asList(preparations).contains(rightPos));
        assertFalse(Arrays.asList(insertions).contains(leftPos));
        assertFalse(Arrays.asList(insertions).contains(rightPos));

        // New deletions supercede old
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, false, 1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, true, 1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, false, 1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, true, 1);

        // New deletions addition to old
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, false, -1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, true, -1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, false, -1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, true, -1);

        // New deletions group with old
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, false, 0);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, true, 0);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, false, 0);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, true, 0);
    }

    private void testCursorsWithInterveningDeletions(String[] preparations,
                                                     String leftPos,
                                                     String rightPos,
                                                     String[] insertions,
                                                     Direction dir,
                                                     boolean useSkip,
                                                     int delTimeIncrease)
    throws TrieSpaceExhaustedException
    {
        // Note: ranges are inserted one pair at a time, with changing delTime.
        delTime = 100;
        if (!dir.isForward() && rightPos.startsWith(leftPos))
        {
            String t = leftPos;
            leftPos = rightPos;
            rightPos = t; // swap left and right as prefixes are always before
        }

        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        insertRanges(trie, preparations, delTimeIncrease);

        final String current = dir.select(leftPos, rightPos);
        RangeCursor<TestRangeState> c = trie.cursor(dir);
        TriePathReconstructor paths = new TriePathReconstructor();
        boolean found;
        if (useSkip)
            found = c.descendAlong(bc(current).asComparableBytes(VERSION));
        else
            found = advanceTo(c, bc(current), paths);

        assertTrue(found);

        insertRanges(trie, insertions, delTimeIncrease);

        // Even if the branch c is on is deleted, we should be able to continue iterating it and finding the right data.
        String target = dir.select(rightPos, leftPos);
        if (useSkip)
            found = skipByDifference(c, bc(current), bc(target));
        else
            found = advanceTo(c, bc(target), paths);

        assertTrue(found);

        while (!Cursor.isExhausted(c.advanceMultiple(null)))
        {
        }    // let the verification cursor check the correctness of the iteration
}

    ByteComparable maybeInvert(ByteComparable bc, Direction dir)
    {
        return dir.isForward() ? bc : InMemoryTriePutTest.invert(bc);
    }

    private boolean advanceTo(RangeCursor<TestRangeState> c, ByteComparable target, TriePathReconstructor paths)
    {
        int cmp;
        Direction dir = c.direction();
        while (true)
        {
            cmp = ByteComparable.compare(maybeInvert(target, dir), maybeInvert(ByteComparable.preencoded(VERSION, paths.keyBytes, 0, paths.keyPos), dir), VERSION);
            if (cmp == 0)
                return true;
            if (cmp < 0)
                return false;
            if (Cursor.isExhausted(c.advance()))
                return false; // exhausted

            long position = c.encodedPosition();
            paths.resetPathLength(Cursor.depth(position) - 1);
            paths.addPathByte(Cursor.incomingTransition(position));
        }
    }

    private boolean skipByDifference(Cursor<?> cursor, ByteComparable a, ByteComparable b)
    {
        ByteSource.Peekable sa = ByteSource.peekable(a.asComparableBytes(VERSION));
        ByteSource.Peekable sb = ByteSource.peekable(b.asComparableBytes(VERSION));
        int depth = 0;
        while (sa.peek() == sb.peek())
        {
            sa.next();
            sb.next();
            ++depth;
        }

        final int nextByte = sb.next();
        long skipPosition = Cursor.encode(depth + 1, nextByte, cursor.direction());
        long skippedPosition = cursor.skipTo(skipPosition);
        if (Cursor.compare(skippedPosition, skipPosition) != 0)
            return false;
        return cursor.descendAlong(sb);
    }

    private void insertRanges(InMemoryRangeTrie<TestRangeState> trie, String[] insertions, int delTimeIncrease) throws TrieSpaceExhaustedException
    {
        for (int i = 0; i < insertions.length; i += 2)
        {
            ByteComparable left = leftBound(insertions[i]);
            ByteComparable right = rightBound(insertions[i + 1]);
            trie.apply(RangeTrie.range(left, true, right, true, TrieUtil.VERSION, toMarker(insertions[i], delTime)),
                       (existing, update) -> existing == null ? update : TestRangeState.combine(existing, update),
                       delTimeIncrease >= 0 ? x -> forceCopy : Predicates.alwaysTrue()); // if we delete covered branches, we should be okay with no force copying
            delTime += delTimeIncrease;
        }
        System.out.println("After inserting " + Arrays.toString(insertions) + ":\n" + trie.dump());
    }

    static ByteComparable withTerminator(int terminator, ByteComparable bc)
    {
        if (addTerminators)
            return v -> ByteSource.append(bc.asComparableBytes(v), terminator);
        else
            return bc;
    }

    private static ByteComparable bc(String s)
    {
        return addTerminators ? ByteComparable.preencoded(VERSION, s.getBytes()) : TrieUtil.directComparable(s);
    }

    static ByteComparable leftBound(String s)
    {
        return leftBound(bc(s));
    }

    static ByteComparable rightBound(String s)
    {
        return rightBound(bc(s));
    }

    static ByteComparable key(String s)
    {
        return key(bc(s));
    }

    static ByteComparable leftBound(ByteComparable bc)
    {
        return withTerminator(0x00, bc);
    }

    static ByteComparable rightBound(ByteComparable bc)
    {
        return withTerminator(0xFF, bc);
    }

    static ByteComparable key(ByteComparable bc)
    {
        return withTerminator(0x80, bc);
    }


    static <S extends RangeState<S>> void putRange(InMemoryRangeTrie<S> trie,
                                                   String s,
                                                   S value,
                                                   Trie.MergeResolver<S> resolver)
    {
        putRange(trie, bc(s), value, resolver);
    }

    static <S extends RangeState<S>> void putRange(InMemoryRangeTrie<S> trie,
                                                   ByteComparable key,
                                                   S value,
                                                   Trie.MergeResolver<S> resolver)
    {
        try
        {
            trie.apply(RangeTrie.range(leftBound(key), true, rightBound(key), true, TrieUtil.VERSION, value),
                       (existing, update) -> existing != null ? resolver.resolve(existing, update) : update,
                       x -> forceCopy);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw Throwables.propagate(e);
        }
    }
}
