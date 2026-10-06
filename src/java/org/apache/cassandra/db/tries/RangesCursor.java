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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// A cursor for a [TrieSet] that represents a set of ranges.
///
/// A range is the span between two keys, which can be taken to be inclusive or exclusive of the endpoints and all
/// their descendants. For example, the range `[abc; afg]` includes "abc", "af", "afg", as well as any string staring
/// with "abc", "ad", "afg". The range `[abc; abc]` is the branch of "abc", i.e. the set containing all string
/// starting with "abc". The range `(abc; afg)` includes "abd", "ac", "afe" and all strings starting with these prefixes,
/// but not anything starting with "abc" or "afg".
///
/// If one of the two bounds is a prefix, the included part of the descendants of the prefix are restricted to only
/// those that fall between the bounds, e.g. `[a; abc]` includes "a", "ab" as well as all strings starting with "aa",
/// "abb" or "abc", but not those starting with "abd". The range `(abc; a]` contains all strings starting with "abd",
/// "ac" among others, but not "abc" or any string starting with "abc".
///
/// The inclusivity is implemented by using positions before or after the branch in iteration order, which we
/// denote e.g. "abc<" as the point before the point "abc" and its branch, and "abc>" for the point after the branch of
/// "abc". In other words, these positions are such that `abc< < X < abc>` for any string X that starts with "abc".
/// When walking a trie using a cursor in the forward direction, the "<" positions are reported on the descent path,
/// while the ">" positions are reported on the ascent path, and vice versa in reverse.
///
/// As a corollary of this order, ranges like `(a; abc]` or `(a; a)` are invalid, because the right side (resp. "abc>"
/// and "a<") orders smaller than the left ("a>").
///
/// The ranges are specified by passing a sequence of boundaries, where each even boundary is the opening boundary, and
/// the odd ones are the closing boundary for a range. The class also takes an argument for the inclusivity of open
/// and close boundaries which applies to all boundaries of the type.
///
/// The boundaries given must be in order and cannot overlap, but it is possible to repeat a boundary when they are the
/// same thing or cover a branch because of inclusivity, e.g.
/// - `[a, a] == branch a`: an even number of repeats starting at an even position with inclusivity on both sides
///    specifies a branch (i.e. the set of all descendants of that key)
/// - `[a, b), [b, c) == [a, c)`: an even number of repeats is ignored when the inclusivity is complementary
///    (e.g. inclusive-start-exclusive-end)
/// - `[a, a), [a, b) == [a, b)`: an odd number of repeats is the same as a single copy for complementary inclusivity
/// - `(a, b), (b, c) == (a, c) - branch b`: an even number of repeats which starts at an odd position specifies an
///    excluded branch when both sides are exclusive.
///
/// If the first boundary is null, the range is open on the left, and if the last non-null boundary is on an even
/// position (i.e. if the array has an even length and its last boundary is null, or if that last boundary is cut off),
/// the range is open on the right:
/// - `[null, a]` covers all keys smaller than `a`
/// - `[a, null]` or `[a]` covers all keys greater than `a`
class RangesCursor implements TrieSetCursor
{
    private final ByteComparable.Version byteComparableVersion;

    /// The current index in the boundaries array. This is the input that will be advanced on the next [#advance] call.
    /// When the current boundary is exhausted, it will advance. When it reaches [#endIdx], the cursor is
    /// exhausted.
    private int currentIdx;
    /// The index at which the ranges end. The end or start of the array, adjusted to remove null boundaries.
    private final int endIdx;
    /// The next position for all boundaries.
    long[] nextPositions;

    /// Byte sources producing the rest of the bytes of the boundaries.
    ByteSource.Peekable[] sources;
    /// The current position (reported to the user). This is usually obtained from `nextPositions[currentIdx]` before
    /// the current keys are advanced.
    long currentPosition;
    /// Current range state, returned by [#state].
    RangeState currentState;

    static final int STARTS_AFTER = 1;
    static final int ENDS_AFTER = 2;

    /// Bit mask specifying the positioning of left and right bounds in relation to the covered branch as a combination
    /// of [#STARTS_AFTER] and [#ENDS_AFTER] bits. This determines inclusivity: the left sides are inclusive if the
    /// `STARTS_AFTER` is not set (i.e. the boundary is to the left/before the key), and the right sides are inclusive
    /// if `ENDS_AFTER` is set (i.e. the boundary is to the right/after the key).
    final int endsAfterMask;

    public static RangesCursor full(Direction direction, ByteComparable.Version byteComparableVersion)
    {
        return create(direction, byteComparableVersion, ENDS_AFTER, null, null);
    }

    public static RangesCursor create(Direction direction, ByteComparable.Version byteComparableVersion, boolean startsInclusive, boolean endsInclusive, ByteComparable... boundaries)
    {
        return create(direction, byteComparableVersion, (startsInclusive ? 0 : STARTS_AFTER) | (endsInclusive ? ENDS_AFTER : 0), boundaries);
    }

    public static RangesCursor create(Direction direction, ByteComparable.Version byteComparableVersion, int endsAfterMask, ByteComparable... boundaries)
    {
        long rootPosition = Cursor.rootPosition(direction);

        int length = boundaries.length;

        int arrayLength = (length + 1) & ~1;
        long[] nextPositions = new long[arrayLength];

        ByteSource.Peekable[] sources = new ByteSource.Peekable[arrayLength];
        for (int i = 0; i < arrayLength; ++i)
        {
            ByteComparable boundary = i < length ? boundaries[i] : null;
            int destIndex = direction.select(i, arrayLength - i - 1);
            if (boundary != null)
            {
                sources[destIndex] = boundary.asPeekableBytes(byteComparableVersion);
                nextPositions[destIndex] = maybeOnReturnPath(sources, endsAfterMask, direction, rootPosition, destIndex);
            }
            else
            {
                // Unspecified bounds are the same as empty string, inclusive.
                assert destIndex == 0 || destIndex == arrayLength - 1;
                sources[destIndex] = ByteSource.Peekable.EMPTY;
                nextPositions[destIndex] = maybeOnReturnPath(sources, ENDS_AFTER, direction, rootPosition, destIndex);
            }
        }

        // If we have a set that is empty because the first start position is after the root branch, shortcut this to
        // plain empty set to avoid reporting NOT_CONTAINED on the return path.
        if (arrayLength > 0 && nextPositions[0] == (rootPosition | ON_RETURN_PATH_BIT))
        {
            return new RangesCursor(byteComparableVersion,
                                    0,
                                    null, null,
                                    0, 0,
                                    rootPosition,
                                    RangeState.NOT_CONTAINED);
        }

        RangesCursor cursor = new RangesCursor(byteComparableVersion,
                                               endsAfterMask,
                                               nextPositions, sources,
                                               0, arrayLength,
                                               rootPosition,
                                               RangeState.NOT_CONTAINED);
        cursor.advanceBoundariesAndSelectState(rootPosition);
        return cursor;
    }

    private RangesCursor(ByteComparable.Version byteComparableVersion,
                         int endsAfterMask,
                         long[] nextPositions,
                         ByteSource.Peekable[] sources,
                         int startIdx,
                         int endIdxExclusive,
                         long currentPosition,
                         RangeState currentState)
    {
        this.byteComparableVersion = byteComparableVersion;
        this.nextPositions = nextPositions;
        this.sources = sources;
        this.currentIdx = startIdx;
        this.endIdx = endIdxExclusive;
        this.currentPosition = currentPosition;
        this.currentState = currentState;
        this.endsAfterMask = endsAfterMask;
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public RangeState state()
    {
        return currentState;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    @Override
    public long advance()
    {
        if (currentIdx >= endIdx)
            return exhausted();

        return advanceBoundariesAndSelectState(nextPositions[currentIdx]);
    }

    private long advanceBoundariesAndSelectState(long nextPosition)
    {
        // Note: currentIdx may be outside of range when this is called (e.g. for an empty set/tail), which is why we
        // take the next position as an argument rather than get it from nextPositions[currentIdx].

        // In reverse direction before and after are swapped.
        Direction direction = Cursor.direction(nextPosition);
        int containedSelection = 0;

        // Even left key index means not contained before.
        if ((currentIdx & 1) != 0)
            containedSelection |= direction.select(RangeState.APPLICABLE_BEFORE, RangeState.APPLICABLE_AFTER);

        // We need to advance all the keys that match the selected next position to prepare them for the next iteration.
        for (int advancingIdx = currentIdx; advancingIdx < this.endIdx; ++advancingIdx)
        {
            long cmp = Cursor.compare(nextPosition, nextPositions[advancingIdx]);
            assert cmp <= 0 : "Invalid order of range boundaries";
            if (cmp < 0) // This key is beyond our position, we are done.
                break;

            int nextByte = sources[advancingIdx].next();
            if (nextByte == ByteSource.END_OF_STREAM)
            {
                // If a key (or more than one) ends here, advance currentIdx. Its new value will determine whether the
                // succeeding side is included in the set.
                assert currentIdx == advancingIdx : "Invalid order of range boundaries";
                ++currentIdx;
            }
            else
                nextPositions[advancingIdx] = maybeOnReturnPath(Cursor.positionForDescentWithByte(nextPosition, nextByte),
                                                                advancingIdx,
                                                                direction);
        }

        // Even left key index after consuming the cursors that end here means not contained after.
        if ((currentIdx & 1) != 0)
            containedSelection |= direction.select(RangeState.APPLICABLE_AFTER, RangeState.APPLICABLE_BEFORE);

        currentState = RangeState.values()[containedSelection];
        currentPosition = nextPosition;
        return nextPosition;
    }

    private long maybeOnReturnPath(long nextPosition, int index, Direction direction)
    {
        return maybeOnReturnPath(sources, endsAfterMask, direction, nextPosition, index);
    }

    /// Adjusts the position for keys that end after the current byte, in order to put the boundaries at the right
    /// place with respect to descendant branches.
    ///
    /// This means, for example, placing inclusive right boundaries on the return path for forward iteration.
    private static long maybeOnReturnPath(ByteSource.Peekable[] sources,
                                          int endsAfterMask,
                                          Direction direction,
                                          long nextPosition,
                                          int index)
    {
        // Fast path when the inclusivity options ask for no return path positions.
        if (endsAfterMask == direction.select(0, STARTS_AFTER | ENDS_AFTER))
            return nextPosition;

        if (sources[index].peek() != ByteSource.END_OF_STREAM)
            return nextPosition;

        int bitInMask = index & 1;
        if (!direction.isForward()) // Ends and starts are swapped when going in reverse
            bitInMask ^= 1;
        boolean placeAfter = (endsAfterMask & (1 << bitInMask)) != 0;
        if (placeAfter == direction.isForward()) // Return path is to the left when going in reverse
            return nextPosition | ON_RETURN_PATH_BIT;
        else
            return nextPosition;
    }

    // Note: Sets don't need `advanceMultiple` because they are meant to apply as a restriction on other tries,
    // and the combined walks necessary to implement such restrictions can only proceed one step at a time.
    // Once the restriction identifies that a branch in covered by the set, it can use the trie's `advanceMultiple`
    // method.

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        // Since individual keys are singletons, if the skip positition is beyond the prepared next position for a key,
        // we are done with that key. So drop all such keys and advance with the first that is at or beyond the
        // requested position.
        while (currentIdx < endIdx && Cursor.compare(nextPositions[currentIdx], encodedSkipPosition) < 0)
            currentIdx++;
        return advance();
    }

    private long exhausted()
    {
        currentIdx = endIdx;
        currentState = RangeState.NOT_CONTAINED;
        currentPosition = Cursor.exhaustedPosition(currentPosition);
        return currentPosition;
    }

    @Override
    public TrieSetCursor tailCursor(Direction direction)
    {
        return tailCopyOf(this, direction);
    }

    ByteSource.Duplicatable duplicateSource(int index)
    {
        ByteSource.Peekable src = sources[index];
        if (src == null)
            return null;

        if (!(src instanceof ByteSource.Duplicatable))
            sources[index] = src = ByteSource.duplicatable(src);

        ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) src;
        return duplicatableSource.duplicate();
    }

    private static RangesCursor tailCopyOf(RangesCursor copyFrom, Direction newDirection)
    {
        assert !Cursor.isOnReturnPath(copyFrom.currentPosition)
            : "Cannot take tail of a position " + Cursor.toString(copyFrom.currentPosition) + " on the return path.";
        boolean directionMatches = newDirection == copyFrom.direction();

        // Calculate the span of boundaries that are still active for the tail, not including any matching return path
        // (the latter has the same effect as the set being open-ended at this tail).
        int startInclusive = copyFrom.currentIdx;
        int endExclusive = startInclusive;
        while (endExclusive < copyFrom.endIdx &&
               Cursor.compare(copyFrom.nextPositions[endExclusive],
                              copyFrom.currentPosition | ON_RETURN_PATH_BIT) < 0)
             ++endExclusive;

        // We can only drop an even number of boundaries on either size. Expand the indexes to make them even.
        int arrayStart = startInclusive & ~1;
        int arrayEnd = ((endExclusive + 1) & ~1);
        // Note: if endExclusive == startInclusive, arrayEnd - arrayStart is 0 if branch is not included, 2 if included
        // (i.e. startInclusive is odd).

        final long depthDiff = Cursor.depthCorrectionValue(copyFrom.currentPosition);
        ByteSource.Peekable[] sources = new ByteSource.Peekable[arrayEnd - arrayStart];
        final long[] nextPositions = new long[arrayEnd - arrayStart];

        int newStartIdx;

        // Duplicate all selected boundaries, adjust depths and reverse the order if the direction doesn't match.
        if (directionMatches)
        {
            for (int i = startInclusive; i < endExclusive; ++i)
            {
                sources[i - arrayStart] = copyFrom.duplicateSource(i);
                nextPositions[i - arrayStart] = copyFrom.nextPositions[i] - depthDiff;
            }
            newStartIdx = startInclusive - arrayStart;
        }
        else
        {
            for (int i = startInclusive; i < endExclusive; ++i)
            {
                int destIndex = arrayEnd - 1 - i;
                sources[destIndex] = copyFrom.duplicateSource(i);
                nextPositions[destIndex] = (copyFrom.nextPositions[i] - depthDiff) ^ TRANSITION_MASK;
                if (sources[destIndex].peek() == ByteSource.END_OF_STREAM)
                    nextPositions[destIndex] ^= ON_RETURN_PATH_BIT;
            }
            newStartIdx = arrayEnd - endExclusive;
        }

        // Determine the state the root needs to present.
        boolean startIsContained = (newStartIdx & 1) != 0;
        RangeState rootState = startIsContained ? newDirection.select(RangeState.START, RangeState.END)
                                                : RangeState.NOT_CONTAINED;
        long rootPosition = Cursor.rootPosition(newDirection);

        // Add an onReturnPath root position for open-ended sets.
        int last = nextPositions.length - 1;
        if (last > 0 && sources[last] == null)
        {
            sources[last] = ByteSource.EMPTY;
            nextPositions[last] = rootPosition | ON_RETURN_PATH_BIT;
        }

        return new RangesCursor(copyFrom.byteComparableVersion,
                                copyFrom.endsAfterMask,
                                nextPositions,
                                sources,
                                newStartIdx,
                                nextPositions.length,
                                rootPosition,
                                rootState);
    }
}
