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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// A cursor for a [TrieSet] that represents a set of ranges.
///
/// Ranges in this trie design always include all prefixes and all descendants of the start and end points. That is,
/// the range of ("abc", "afg") includes "a", "ab", "abc", "af, "afg", as well as any string staring with "abc", "ac",
/// "ad", "ae", "afg". The range of ("abc", "abc") includes "a", "ab", "abc" and any string starting with "abc".
///
/// Ranges that contain prefixes (e.g. ("ab", "abc")) are invalid as they cannot be specified without violating order in
/// one of the directions (i.e. "ab" is before "abc" in forward order, but not after it in reverse).
/// The reason for these restrictions is to ensure that all individual ranges are contiguous spans when iterated in
/// forward as well as backward order. This in turn makes it possible to have simple range trie and intersection
/// implementations where the representations of range trie sets do not differ when iterated in the two directions.
///
/// These types of ranges are actually preferable to us, because we use prefix-free keys with terminators that leave
/// room for greater- and less-than positions, and at prefix nodes we store metadata applicable to the whole branch.
///
/// The ranges are specified by passing a sequence of boundaries, where each even boundary is the opening boundary, and
/// the odd ones are the closing boundary for a range. The boundaries must be in order and cannot overlap, but it is
/// possible to repeat a boundary, e.g.
/// - `[a, a] == point a`: an even number of repeats starting at an even position specifies a point
///    (i.e. the set of all prefixes and all descendants of that key)
/// - `[a, b, b, c] == [a, c]`: an even number of repeats which starts at an odd position is ignored
/// - `[a, a, a, b] == [a, b]`: an odd number of repeats is the same as a single copy
///
/// If the first boundary is null, the range is open on the left, and if the last non-null boundary is on an even
/// position (i.e. if the array has an even length and its last boundary is null, or if that last boundary is cut off),
/// the range is open on the right:
/// - `[null, a]` covers all keys smaller than `a`, plus the prefixes and descendants of `a`
/// - `[a, null]` or `[a]` covers all keys greater than `a`, plus the prefixes and descendants of `a`
class RangesCursor implements TrieSetCursor
{
    private final ByteComparable.Version byteComparableVersion;
    private final Direction direction;

    /// The current index in the boundaries array. This is the input that will be advanced on the next [#advance] call.
    /// When the current boundary is exhausted, it will advance (in the direction of the cursor). When it surpasses
    /// [#completedIdx], the cursor is exhausted.
    private int currentIdx;
    /// The index at which the ranges end. Depending on the direction of iteration, the end or start of the array,
    /// adjusted to remove null boundaries.
    private final int completedIdx;
    /// The next byte for all boundaries.
    int[] nexts;
    /// The depth (processed number of bytes) for all boundaries.
    int[] depths;
    /// Byte sources producing the rest of the bytes of the boundaries.
    ByteSource[] sources;
    /// The currently reached depth (reported to the user). This is usually `depths[currentIdx] - 1`.
    int currentDepth;
    /// The current incoming transition.
    int currentTransition;
    /// Current range state, returned by [#state].
    RangeState currentState;

    public static RangesCursor create(Direction direction, ByteComparable.Version byteComparableVersion, ByteComparable... boundaries)
    {
        // handle empty array (== full range) and nulls at the ends (same as not there, odd length == open end range)
        int length = boundaries.length;

        if (length > 1 && boundaries[length - 1] == null)
            --length;

        int first = 0;
        if (length > 0 && boundaries[0] == null)
            first = 1;

        if (first >= length) // no boundaries on either side, report END_START_PREFIX on root and exhausted state
            return new RangesCursor(direction, byteComparableVersion,
                                    null, null, null,
                                    1, 1,
                                    0, -1,
                                    RangeState.END_START_PREFIX);

        int[] nexts = new int[length];
        int[] depths = new int[length];
        ByteSource[] sources = new ByteSource[length];
        for (int i = first; i < length; ++i)
        {
            depths[i] = 1;
            if (boundaries[i] == null)
                throw new AssertionError("Null can only be used as the first or last boundary.");

            sources[i] = boundaries[i].asComparableBytes(byteComparableVersion);
            nexts[i] = sources[i].next();
        }

        RangesCursor cursor = new RangesCursor(direction, byteComparableVersion,
                                               nexts, depths, sources,
                                               first, length,
                                               0, -1,
                                               RangeState.START_END_PREFIX);
        cursor.skipCompletedAndSelectContained(nexts[cursor.currentIdx], cursor.completedIdx);
        return cursor;
    }

    private RangesCursor(Direction direction,
                         ByteComparable.Version byteComparableVersion,
                         int[] nexts,
                         int[] depths,
                         ByteSource[] sources,
                         int firstIdxInclusive,
                         int lastIdxExclusive,
                         int currentDepth,
                         int currentTransition,
                         RangeState currentState)
    {
        this.byteComparableVersion = byteComparableVersion;
        this.direction = direction;
        this.nexts = nexts;
        this.depths = depths;
        this.sources = sources;
        this.currentIdx = direction.select(firstIdxInclusive, lastIdxExclusive - 1);
        this.completedIdx = direction.select(lastIdxExclusive - 1, firstIdxInclusive);
        this.currentDepth = currentDepth;
        this.currentTransition = currentTransition;
        this.currentState = currentState;
    }

    @Override
    public int depth()
    {
        return currentDepth;
    }

    @Override
    public int incomingTransition()
    {
        return currentTransition;
    }

    @Override
    public RangeState state()
    {
        return currentState;
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    @Override
    public int advance()
    {
        if (direction.gt(currentIdx, completedIdx))
            return exhausted();

        // Advance the current idx
        currentTransition = nexts[currentIdx];
        currentDepth = depths[currentIdx]++;
        int next = ByteSource.END_OF_STREAM;
        if (currentTransition != ByteSource.END_OF_STREAM)
            next = nexts[currentIdx] = sources[currentIdx].next();

        // Also advance all others that are at the same position and have the same next byte.
        int endIdx = currentIdx + direction.increase;
        while (direction.le(endIdx, completedIdx)
               && depths[endIdx] == currentDepth && nexts[endIdx] == currentTransition)
        {
            depths[endIdx]++;
            nexts[endIdx] = sources[endIdx].next();
            endIdx += direction.increase;
        }

        return skipCompletedAndSelectContained(next, endIdx - direction.increase);
    }

    private int skipCompletedAndSelectContained(int next, int endIdx)
    {
        int containedSelection = 0;
        // in reverse direction the roles of current and end idx are swapped
        if ((direction.select(currentIdx, endIdx) & 1) != 0) // even left index means not valid before
            containedSelection |= RangeState.APPLICABLE_BEFORE;

        if ((direction.select(endIdx, currentIdx) & 1) == 0) // even end index means not valid after
            containedSelection |= RangeState.APPLICABLE_AFTER;

        if (next == ByteSource.END_OF_STREAM)
        {
            containedSelection |= RangeState.IS_BOUNDARY; // exact match, point and children included; reportable node
            while (direction.le(currentIdx, endIdx))
            {
                assert nexts[currentIdx] == ByteSource.END_OF_STREAM : "Prefixes are not allowed in trie ranges.";
                currentIdx += direction.increase;
            }
        }
        currentState = RangeState.values()[containedSelection];
        return currentDepth;
    }

    // Note: Sets don't need `advanceMultiple` because they are meant to apply as a restriction on other tries,
    // and the combined walks necessary to implement such restrictions can only proceed one step at a time.
    // Once the restriction identifies that a branch in covered by the set, it can use the trie's `advanceMultiple`
    // method.

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        while (direction.le(currentIdx, completedIdx)
               && (depths[currentIdx] > skipDepth ||
                   depths[currentIdx] == skipDepth && direction.lt(nexts[currentIdx], skipTransition)))
            currentIdx += direction.increase;
        return advance();
    }

    private int exhausted()
    {
        currentDepth = -1;
        currentTransition = -1;
        return skipCompletedAndSelectContained(0, completedIdx);
    }

    @Override
    public TrieSetCursor tailCursor(Direction direction)
    {
        return tailCopyOf(this, direction);
    }

    private static RangesCursor tailCopyOf(RangesCursor copyFrom, Direction newDirection)
    {
        Direction copyDirection = copyFrom.direction;
        int startInclusive = copyFrom.currentIdx;
        int endExclusive = findEndOfMatchingValues(copyDirection, startInclusive, copyFrom.completedIdx, copyFrom.depths, copyFrom.currentDepth);

        if (startInclusive == endExclusive)
            return boundaryMatchingCursor(copyFrom, newDirection);

        int endInclusive = endExclusive - copyDirection.increase;
        int firstInclusive = copyDirection.select(startInclusive, endInclusive);
        int lastExclusive = copyDirection.select(endInclusive, startInclusive) + 1;

        // Duplicate all boundaries that are positioned at the current point (if they are not, they cannot affect the
        // tail trie).
        // From the left we can only drop an even number of boundaries.
        int firstAdjusted = firstInclusive & -2;
        ByteSource[] sources = new ByteSource[lastExclusive - firstAdjusted];
        final int[] depths = Arrays.copyOfRange(copyFrom.depths, firstAdjusted, lastExclusive);
        final int startDepth = copyFrom.currentDepth;
        for (int i = firstInclusive; i < lastExclusive; ++i)
        {
            if (copyFrom.sources[i] != null)
            {
                ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.sources[i]);
                copyFrom.sources[i] = dupe;
                sources[i - firstAdjusted] = dupe.duplicate();
            }
            depths[i - firstAdjusted] -= startDepth;
        }
        return new RangesCursor(newDirection,
                                copyFrom.byteComparableVersion,
                                Arrays.copyOfRange(copyFrom.nexts, firstAdjusted, lastExclusive),
                                depths,
                                sources,
                                firstInclusive - firstAdjusted,
                                lastExclusive - firstAdjusted,
                                0,
                                -1,
                                copyFrom.currentState);
    }

    private static int findEndOfMatchingValues(Direction direction, int startInclusive, int completedInclusive, int[] depths, int currentDepth)
    {
        int endExclusive;
        for (endExclusive = startInclusive;
             direction.le(endExclusive, completedInclusive) && depths[endExclusive] > currentDepth;
             endExclusive += direction.increase)
        {
        }
        return endExclusive;
    }

    private static RangesCursor boundaryMatchingCursor(RangesCursor copyFrom, Direction newDirection)
    {
        // There are no further ranges to follow. The current state is the only thing we need to present, but
        // we need to make sure we present the right final state after advancing over the current state.
        // Prepare a combination of current and completed index that produces true or false precedingIncluded() result
        // on the exhausted() call.
        final RangeState state = copyFrom.currentState;
        // This gives us the included/excluded state after the current position.
        int currentIdx = state.precedingIncluded(newDirection.opposite()) ? 1 : 0;

        return new RangesCursor(newDirection,
                                copyFrom.byteComparableVersion,
                                null,
                                null,
                                null,
                                currentIdx,
                                currentIdx,
                                0,
                                -1,
                                state);
    }
}
