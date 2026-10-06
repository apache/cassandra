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

/// The cursor implementation of [RangeTrie].
///
/// The main difference between normal and range cursors is the addition of a [#precedingState] method, which returns a
/// range for prefixes of start or end positions and is used to determine whether a position that has been skipped to
/// falls inside one of the trie's ranges.
///
/// As an example, consider the following range trie:
///
/// ```
/// a ->
///   b -> start of del1              (left: null, right: del1)
/// c ->
///   d -> switch from del1 to del2   (left: del1, right: del2)
///   g ->
///     h -> end,del2                 (left: del2, right: null)
/// ```
///
/// If we advance through this trie, it is easy to keep track of the covering deletion, as we walk through the
/// boundaries before entering the range. However, if we skip into the trie, we will not see the boundaries.
/// Imagine a cursor starts at the root and attempts to skip to "b". We need to be able to notice that "b" is covered by
/// the "ab-cd" range with deletion del1. This is achieved by using [#precedingState]. In this case skipping to "b"
/// (in forward direction) will position the cursor on "c"; because the positions to the left of "c" are covered by
/// del1, the [#precedingState] the cursor should report must be a covering non-boundary state corresponding to `del1`.
///
/// If, on the other hand, we perform a skip in the reverse direction that reaches the same state, the cursor should
/// report `null` as its state (e.g. performing a `skipTo` from the root with character "d" will land in "c" whose state
/// correctly determines that there is no covering deletion for "d").
///
/// For further details, see the range trie section in [Trie.md](./Trie.md).
interface RangeCursor<S extends RangeState<S>> extends Cursor<S>
{
    /// Returns a range that covers positions before this in iteration order, including this position if `content()` is
    /// null. This is the range that is active at (i.e. covers) a position that was skipped to, when the range trie
    /// jumps past the requested position or does not have content.
    ///
    /// The returned value must be a non-boundary state (i.e. `precedingState().isBoundary()` must always be `false`)
    /// and must return itself for its `precedingState` in both directions.
    default S precedingState()
    {
        final S state = state();
        if (state == null)
            return null;
        return state.precedingState(direction());
    }

    /// The range state at the current position. This is either a reportable marker (if the cursor is positioned at a
    /// range boundary), or the covering state that applies to this position and the ones preceding it (up to the
    /// closest range marker preceding the current position). This carries information for both [#content] and
    /// [#precedingState] and is used to reduce the amount of work done to obtain both values.
    ///
    /// Typically, we would have `state() == content() != null ? content() : precedingState()`, but the state could be
    /// richer for some types of trie; for example, [TrieSetCursor] also includes information about the state following
    /// in the states returned for prefix positions.
    ///
    /// This can be null when no range is active before the current position.
    S state();

    /// Content is only returned for boundary positions.
    /// Note that if `content()` is non-null, `precedingState()` does not apply to this exact position.
    @Override
    default S content()
    {
        final S state = state();
        if (state == null)
            return null;
        return state.isBoundary() ? state : null;
    }

    @Override
    RangeCursor<S> tailCursor(Direction direction);

    /// Corresponding method to tailCursor above applicable when this cursor is ahead.
    /// Returns a full-range cursor returning [#precedingState()].
    default RangeCursor<S> precedingStateCursor(Direction direction)
    {
        // Note: this uses `precedingState` in the current direction, which must be the same as the preceding state we
        // would get if we walked to the same state in the opposite direction.
        return new Empty<>(precedingState(), byteComparableVersion(), direction);
    }

    class Empty<S extends RangeState<S>> extends Cursor.Empty<S> implements RangeCursor<S>
    {
        final S coveringState;

        public Empty(S coveringState, ByteComparable.Version version, Direction direction)
        {
            super(direction, version);
            this.coveringState = coveringState;
        }

        @Override
        public S state()
        {
            return coveringState;
        }

        @Override
        public S content()
        {
            return null;
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            return new RangeCursor.Empty<>(coveringState, byteComparableVersion(), direction);
        }
    }

    static <S extends RangeState<S>> RangeCursor<S> empty(Direction direction, ByteComparable.Version version)
    {
        return new Empty<S>(null, version, direction);
    }

    class Done<S extends RangeState<S>> implements RangeCursor<S>
    {
        final Direction direction;
        final ByteComparable.Version version;

        public Done(Direction direction, ByteComparable.Version version)
        {
            this.direction = direction;
            this.version = version;
        }

        @Override
        public S state()
        {
            return null;
        }

        @Override
        public int depth()
        {
            return -1;
        }

        @Override
        public int incomingTransition()
        {
            return -1;
        }

        @Override
        public S content()
        {
            return null;
        }

        @Override
        public Direction direction()
        {
            return direction;
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return version;
        }

        @Override
        public int advance()
        {
            return -1;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return -1;
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            return this;
        }
    }

    static <S extends RangeState<S>> RangeCursor<S> done(Direction direction, ByteComparable.Version version)
    {
        return new Done<>(direction, version);
    }

    class FromSet<S extends RangeState<S>> implements RangeCursor<S>
    {
        final TrieSetCursor source;
        final S marker;

        FromSet(TrieSetCursor source, S marker)
        {
            this.source = source;
            this.marker = marker;
        }

        @Override
        public S state()
        {
            return source.state().applyToCoveringState(marker, direction());
        }

        @Override
        public int depth()
        {
            return source.depth();
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public Direction direction()
        {
            return source.direction();
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public int advance()
        {
            return source.advance();
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return source.skipTo(skipDepth, skipTransition);
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            return new FromSet<>(source.tailCursor(direction), marker);
        }
    }
}
