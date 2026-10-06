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
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryRangeTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    int delTime;

    static TestRangeState toMarker(String string)
    {
        return toMarker(string, 1);
    }

    static TestRangeState toMarker(String string, int delTime)
    {
        return new TestRangeState(TrieUtil.directComparable(string), delTime, delTime, delTime, false);
    }

    static TestRangeState addMarkerStrings(TestRangeState a, TestRangeState b)
    {
        assert a.leftSide == b.leftSide;
        assert a.at == b.at;
        assert a.rightSide == b.rightSide;
        assert a.isBoundary == b.isBoundary;
        return new TestRangeState(TrieUtil.directComparable(fromMarker(a) + fromMarker(b)),
                                  a.leftSide,
                                  a.at,
                                  a.rightSide,
                                  a.isBoundary);
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
        ByteComparable e = TrieUtil.directComparable("test");
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        putRange(trie, e, toMarker("test"), (x, y) -> y);
        System.out.println("Trie " + trie.dump());
        assertEquals("test", fromMarker(trie.applicableRange(key(e))));
        assertEquals(null, fromMarker(trie.applicableRange(key(TrieUtil.directComparable("tezt")))));
        assertEquals(null, fromMarker(trie.applicableRange(key(TrieUtil.directComparable("tast")))));
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
            ByteComparable e = TrieUtil.directComparable(test);
            System.out.println("Adding " + asString(e) + ": " + v);
            putRange(trie, e, toMarker(v), InMemoryRangeTrieTest::addMarkerStrings);
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
                         fromMarker(trie.applicableRange(key(TrieUtil.directComparable(test)))));
        }
    }

    private void testEntries(String... tests)
    {
        for (Function<String, ByteComparable> mapping :
        ImmutableList.<Function<String, ByteComparable>>of(TrieUtil::comparable,
                                                           s -> ByteComparable.preencoded(VERSION, s.getBytes())))
        {
            testEntries(tests, mapping);
        }
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
            putRange(trie, e, toMarker(test), (x, y) -> y);
            System.out.println("Trie\n" + trie.dump());
        }

        for (String test : tests)
            assertEquals(test, fromMarker(trie.applicableRange(key(mapping.apply(test)))));
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
        testCursorsWithInterveningDeletions(strings("aaabc", "aaacd", "bcd", "cde"),
                                            "aaa", "aaacd",
                                            strings("a_", "ab"));
    }

    @Test
    public void testCursorBranchDeletionCoversPosition() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("aaabc", "aaacd", "bcd", "cde"),
                                            "aaa", "aaacd",
                                            strings("a_", "ab"));
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
            found = c.descendAlong(ByteSource.peekable(TrieUtil.directComparable(current).asComparableBytes(VERSION)));
        else
            found = advanceTo(c, TrieUtil.directComparable(current), paths);

        if (delTimeIncrease > 0)
            assertTrue(found);

        insertRanges(trie, insertions, delTimeIncrease);

        String target = dir.select(rightPos, leftPos);
        if (found)
        {
            if (useSkip)
                found = skipByDifference(c, TrieUtil.directComparable(current), TrieUtil.directComparable(target));
            else
                found = advanceTo(c, TrieUtil.directComparable(target), paths);
        }
        else
        {
            // nested entries may be gone if deleted by parent. If so, just try to skip to target for a new cursor.
            c = trie.cursor(dir);
            paths = new TriePathReconstructor();
            if (useSkip)
                found = c.descendAlong(TrieUtil.directComparable(current).asComparableBytes(VERSION));
            else
                found = advanceTo(c, TrieUtil.directComparable(current), paths);
        }

        if (delTimeIncrease > 0)
            assertTrue(found);

        if (found)
            while (c.advanceMultiple(null) != -1)
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
            if (c.advance() < 0)
                return false; // exhausted

            paths.resetPathLength(c.depth() - 1);
            paths.addPathByte(c.incomingTransition());
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
        int skippedDepth = cursor.skipTo(depth + 1, nextByte);
        if (skippedDepth != depth + 1 || cursor.incomingTransition() != nextByte)
            return false;
        return cursor.descendAlong(sb);
    }

    private void insertRanges(InMemoryRangeTrie<TestRangeState> trie, String[] insertions, int delTimeIncrease) throws TrieSpaceExhaustedException
    {
        for (int i = 0; i < insertions.length; i += 2)
        {
            trie.apply(RangeTrie.range(TrieUtil.directComparable(insertions[i]),
                                       TrieUtil.directComparable(insertions[i + 1]),
                                       VERSION,
                                       toMarker(insertions[i], delTime)),
                       (existing, update) -> existing == null ? update : TestRangeState.combine(existing, update),
                       delTimeIncrease >= 0 ? Predicates.alwaysFalse() : Predicates.alwaysTrue()); // if we delete covered branches, we should be okay with no force copying
//                       Predicates.alwaysTrue());
            delTime += delTimeIncrease;
        }
        System.out.println("After inserting " + Arrays.toString(insertions) + ":\n" + trie.dump());
    }

    static ByteComparable withTerminator(int terminator, ByteComparable bc)
    {
        return ver -> ByteSource.withTerminator(terminator, bc.asComparableBytes(ver));
    }

    static ByteComparable leftBound(ByteComparable bc)
    {
        return withTerminator(ByteSource.LT_NEXT_COMPONENT, bc);
    }

    static ByteComparable rightBound(ByteComparable bc)
    {
        return withTerminator(ByteSource.GT_NEXT_COMPONENT, bc);
    }

    static ByteComparable key(ByteComparable bc)
    {
        return withTerminator(ByteSource.TERMINATOR, bc);
    }



    static <S extends RangeState<S>> void putRange(InMemoryRangeTrie<S> trie,
                                                   ByteComparable key,
                                                   S value,
                                                   Trie.MergeResolver<S> resolver)
    {
        try
        {
            trie.apply(RangeTrie.range(leftBound(key), rightBound(key), VERSION, value),
                       (existing, update) -> existing != null ? resolver.resolve(existing, update) : update,
                       Predicates.alwaysFalse());
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw Throwables.propagate(e);
        }
    }
}
