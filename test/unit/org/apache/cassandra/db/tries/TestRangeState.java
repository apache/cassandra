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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
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
    final int leftSide;
    final int rightSide;
    final int at;

    final boolean isBoundary;
    final TestRangeState leftState;
    final TestRangeState rightState;

    TestRangeState(ByteComparable position, int leftSide, int at, int rightSide, boolean isBoundary)
    {
        this.position = position;
        this.leftSide = leftSide;
        this.at = at;
        this.rightSide = rightSide;
        this.isBoundary = isBoundary;
        if (leftSide == rightSide && !isBoundary)
        {
            this.leftState = this;
            this.rightState = this;
        }
        else
        {
            this.leftState = leftSide >= 0 ? new TestRangeState(position, leftSide, leftSide, leftSide, false) : null;
            this.rightState = rightSide >= 0 ? new TestRangeState(position, rightSide, rightSide, rightSide, false) : null;
        }
    }

    static TestRangeState combine(TestRangeState m1, TestRangeState m2)
    {
        return combineCollection(Arrays.asList(m1, m2));
    }


    public static TestRangeState combineCollection(Collection<TestRangeState> rangeStates)
    {
        int newLeft = -1;
        int newAt = -1;
        int newRight = -1;
        boolean isBoundary = false;
        ByteComparable position = null;
        for (TestRangeState marker : rangeStates)
        {
            newLeft = Math.max(newLeft, marker.leftSide);
            newAt = Math.max(newAt, marker.at);
            newRight = Math.max(newRight, marker.rightSide);
            position = marker.position;
            isBoundary |= marker.isBoundary;
        }
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;
        isBoundary &= newLeft != newRight || newLeft != newAt;

        return new TestRangeState(position, newLeft, newAt, newRight, isBoundary);
    }

    TestRangeState withPoint(int value)
    {
        return new TestRangeState(position, leftSide, value, rightSide, isBoundary);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(position, leftSide, at, rightSide);
    }

    @Override
    public String toString()
    {
        return toString('"' + toString(position) + '"');
    }

    public String toStringNoPosition()
    {
        return toString("X");
    }

    public String toString(String positionString)
    {
        boolean hasAt = at >= 0 && at != leftSide && at != rightSide;
        String left = leftSide != at ? "<" : "<=";
        String right = rightSide != at ? "<" : "<=";

        return (leftSide >= 0 ? leftSide + left : "") +
               positionString +
               (hasAt ? "=" + at : "") +
               (rightSide >= 0 ? right + rightSide : "") +
               (isBoundary ? "" : " not reportable");
    }

    @Override
    public boolean isBoundary()
    {
        return isBoundary;
    }

    public TestRangeState toContent()
    {
        return isBoundary ? this : null;
    }

    @Override
    public TestRangeState precedingState(Direction direction)
    {
        return direction.select(leftState, rightState);
    }

    @Override
    public TestRangeState restrict(boolean applicableBefore, boolean applicableAfter)
    {
        assert isBoundary;
        if ((applicableBefore || leftSide < 0) && (applicableAfter || rightSide < 0))
            return this;
        int newLeft = applicableBefore ? leftSide : -1;
        int newRight = applicableAfter ? rightSide : -1;
        if (newLeft >= 0 || newRight >= 0 || at >= 0)
            return new TestRangeState(position, newLeft, at, newRight, true);
        else
            return null;
    }

    @Override
    public TestRangeState asBoundary(Direction direction)
    {
        assert !isBoundary;
        final boolean isForward = direction.isForward();
        int newLeft = !isForward ? leftSide : -1;
        int newRight = isForward ? rightSide : -1;
        return new TestRangeState(position, newLeft, at, newRight, true);
    }

    @Override
    public TestRangeState asPoint()
    {
        return new TestRangeState(position, -1, at, -1, true);
    }

    static String toString(ByteComparable position)
    {
        if (position == null)
            return "null";
        return position.byteComparableAsString(TrieUtil.VERSION);
    }

    static List<TestRangeState> verify(List<TestRangeState> markers)
    {
        int active = -1;
        ByteComparable prev = null;
        for (TestRangeState marker : markers)
        {
            assertTrue("Order violation " + toString(prev) + " vs " + toString(marker.position),
                       prev == null || ByteComparable.compare(prev, marker.position, TrieUtil.VERSION) < 0);
            assertEquals("Range close violation", active, marker.leftSide);
            assertTrue(marker.at != marker.leftSide || marker.at != marker.rightSide);
            prev = marker.position;
            active = marker.rightSide;
        }
        assertEquals("Unclosed range", -1, active);
        return markers;
    }


    /**
     * Extract the values of the provided trie into a list.
     */
    static List<TestRangeState> toList(RangeTrie<TestRangeState> trie, Direction direction)
    {
        return Streams.stream(trie.entryIterator(direction))
                      .map(en -> remap(en.getValue(), en.getKey()))
                      .collect(Collectors.toList());
    }

    static TestRangeState remap(TestRangeState dm, ByteComparable newKey)
    {
        return new TestRangeState(newKey, dm.leftSide, dm.at, dm.rightSide, dm.isBoundary);
    }

    static Map.Entry<ByteComparable.Preencoded, TestRangeState> remap(Map.Entry<ByteComparable.Preencoded, TestRangeState> entry)
    {
        return Maps.immutableEntry(entry.getKey(), remap(entry.getValue(), entry.getKey()));
    }

    static InMemoryRangeTrie<TestRangeState> fromList(List<TestRangeState> list)
    {
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(TrieUtil.VERSION);
        for (TestRangeState i : list)
        {
            try
            {
                trie.putRecursive(i.position, i, (ex, n) -> n);
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
        return otherMarker.leftSide == leftSide && otherMarker.at == at && otherMarker.rightSide == rightSide;
    }
}
