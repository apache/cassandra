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

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Streams;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/// Range state used for testing range tries. It is a general implementation of [RangeState] state that can represent
/// any combination of deletions before, after, as well as at a specific point. It will also hold a position, which is
/// not necessary for the trie logic, but makes it possible to describe a range trie using a list of [TestRangeState]
/// as well as perform some operations on it (see [RangeTrieMergeTest#mergeLists]).
class TestRangeState implements RangeState<TestRangeState>
{
    final ByteComparable position;
    final boolean appliesAfter; // part of the position, needs to be remapped for comparisons

    final int leftSide;
    final int rightSide;

    final TestRangeState leftState;
    final TestRangeState rightState;

    TestRangeState(ByteComparable position, int leftSide, int rightSide)
    {
        // By default put left starts (which do not have a left side) before branch and ends (which do not have a right
        // side) after the branch, and leave switches to the left side.
        this(position, rightSide < 0, leftSide, rightSide);
    }

    TestRangeState(ByteComparable position, boolean appliesAfter, int leftSide, int rightSide)
    {
        this.position = position;
        this.appliesAfter = appliesAfter;
        this.leftSide = leftSide;
        this.rightSide = rightSide;
        if (leftSide == rightSide)
        {
            this.leftState = this;
            this.rightState = this;
        }
        else
        {
            this.leftState = leftSide >= 0 ? new TestRangeState(position, false, leftSide, leftSide) : null;
            this.rightState = rightSide >= 0 ? new TestRangeState(position, false, rightSide, rightSide) : null;
        }
    }

    static TestRangeState combine(TestRangeState m1, TestRangeState m2)
    {
        return combineCollection(Arrays.asList(m1, m2));
    }


    public static TestRangeState combineCollection(Collection<TestRangeState> rangeStates)
    {
        int newLeft = -1;
        int newRight = -1;
        ByteComparable position = null;
        boolean appliesAfter = false;
        for (TestRangeState marker : rangeStates)
        {
            newLeft = Math.max(newLeft, marker.leftSide);
            newRight = Math.max(newRight, marker.rightSide);
            position = marker.position;
            appliesAfter = marker.appliesAfter;
        }
        if (newLeft < 0 && newRight < 0)
            return null;

        return new TestRangeState(position, appliesAfter, newLeft, newRight);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(position, leftSide, rightSide);
    }

    @Override
    public String toString()
    {
        return toString('"' + toString(position, appliesAfter) + '"');
    }

    public String toStringNoPosition()
    {
        return toString("X");
    }

    public String toString(String positionString)
    {

        return (leftSide >= 0 ? leftSide + "<" : "") +
               positionString +
               (rightSide >= 0 ? "<" + rightSide : "") +
               (isBoundary() ? "" : " not reportable");
    }

    @Override
    public boolean isBoundary()
    {
        return leftSide != rightSide;
    }

    public TestRangeState toContent()
    {
        return isBoundary() ? this : null;
    }

    @Override
    public TestRangeState precedingState(Direction direction)
    {
        return direction.select(leftState, rightState);
    }

    @Override
    public TestRangeState succedingState(Direction direction)
    {
        return direction.select(rightState, leftState);
    }

    @Override
    public TestRangeState restrict(boolean applicableBefore, boolean applicableAfter)
    {
        assert isBoundary();
        if ((applicableBefore || leftSide < 0) && (applicableAfter || rightSide < 0))
            return this;
        int newLeft = applicableBefore ? leftSide : -1;
        int newRight = applicableAfter ? rightSide : -1;
        if (newLeft >= 0 || newRight >= 0)
            return new TestRangeState(position, appliesAfter, newLeft, newRight);
        else
            return null;
    }

    @Override
    public TestRangeState asBoundary(Direction direction)
    {
        assert !isBoundary();
        final boolean isForward = direction.isForward();
        int newLeft = !isForward ? leftSide : -1;
        int newRight = isForward ? rightSide : -1;
        return new TestRangeState(position, appliesAfter, newLeft, newRight);
    }

    static String toString(ByteComparable position, boolean appliesAfter)
    {
        if (position == null)
            return "null";
        return position.byteComparableAsString(TrieUtil.VERSION) + (appliesAfter ? "↑" : "");
    }

    static List<TestRangeState> verify(List<TestRangeState> markers)
    {
        int active = -1;
        TestRangeState prev = null;
        for (TestRangeState marker : markers)
        {
            if (prev != null && prev.position != null && marker != null && marker.position != null)
                assertTrue("Order violation " + toString(prev.position, prev.appliesAfter) + " vs " + toString(marker.position, marker.appliesAfter),
                           ByteComparable.compare(prev.position, marker.position, TrieUtil.VERSION) < 0 ||
                           ByteComparable.compare(prev.position, marker.position, TrieUtil.VERSION) == 0 && !prev.appliesAfter && marker.appliesAfter);

            if (marker != null)
                assertEquals("Range close violation", active, marker.leftSide);
            else
                assertEquals("Open range at end", null, active);

            assertTrue(marker.leftSide != marker.rightSide);
            prev = marker;
            active = marker.rightSide;
        }
        assertEquals("Unclosed range", -1, active);
        return markers;
    }

    static class TestRangeStateIterator extends TrieEntriesIterator<TestRangeState, TestRangeState>
    {
        boolean onReturnPath;

        TestRangeStateIterator(RangeTrie<TestRangeState> trie, Direction direction)
        {
            super(trie.cursor(direction), Predicates.alwaysTrue());
        }

        @Override
        public void onReturnPath()
        {
            onReturnPath = true;
        }

        @Override
        protected TestRangeState mapContent(TestRangeState content, byte[] bytes, int byteLength)
        {
            ByteComparable key = ByteComparable.preencoded(byteComparableVersion(), Arrays.copyOf(bytes, byteLength));
            boolean appliesAfter = onReturnPath == direction().isForward();
            onReturnPath = false;
            return remap(content, key, appliesAfter);
        }
    }

    /**
     * Extract the values of the provided trie into a list.
     */
    static List<TestRangeState> toList(RangeTrie<TestRangeState> trie, Direction direction)
    {
        return Streams.stream(new TestRangeStateIterator(trie, direction))
                      .collect(Collectors.toList());
    }

    /**
     * Extract the values of the provided trie into a map.
     */
    static Map<String, String> toStringMap(RangeTrie<TestRangeState> trie, Direction direction)
    {
        return Streams.stream(new TestRangeStateIterator(trie, direction))
                      .collect(Collectors.toMap(x -> TrieUtil.asString(x.position),
                                                x -> x.toString(),
                                                (x, y) -> '(' + x + ',' + ')',
                                                LinkedHashMap::new));
    }

    static TestRangeState remap(TestRangeState dm, ByteComparable newKey, boolean appliesAfter)
    {
        return new TestRangeState(newKey, appliesAfter, dm.leftSide, dm.rightSide);
    }

    static InMemoryRangeTrie<TestRangeState> fromList(List<TestRangeState> list)
    {
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(TrieUtil.VERSION);
        for (TestRangeState i : list)
        {
            try
            {
                ByteComparable pos = i.position;
                if (pos == null)
                    pos = ByteComparable.EMPTY;
                trie.putRecursive(pos, i, i.appliesAfter, (ex, n) -> n);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw Throwables.propagate(e);
            }
        }
        return trie;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
            return false;
        TestRangeState otherMarker = (TestRangeState) other;
        return otherMarker.leftSide == leftSide && otherMarker.rightSide == rightSide;
    }
}
