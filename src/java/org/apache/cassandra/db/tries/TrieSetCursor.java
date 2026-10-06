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

/// The implementation of a [TrieSet].
///
/// In addition to the functionality of normal trie cursors, set cursors also produce a [#state] that describes the
/// coverage of trie sections to the left and right of the cursor position. This is necessary to be able to identify
/// coverage after a [#skipTo] operation, where the set cursor jumps to a position beyond the requested one.
interface TrieSetCursor extends RangeCursor<TrieSetCursor.RangeState>
{
    /// This type describes the state at a given cursor position. It describes the coverage of the positions before and
    /// after the current in forward iteration order and whether the node is a set boundary that starts or ends an
    /// included range.
    enum RangeState implements org.apache.cassandra.db.tries.RangeState<RangeState>
    {
        // Note: the states must be ordered so that
        //   `values()[applicableBefore * APPLICABLE_BEFORE + applicableAfter * APPLICABLE_AFTER]`
        // produces a state with the requested flags

        /// The cursor is at a prefix of some start boundary, and the branches before it as well as the current point
        /// are not included in the set.
        NOT_CONTAINED(false, false),
        /// The cursor is positioned at an end boundary. Branches to the left of this are covered by the set.
        /// The current position and any position to the right in lexicographic order (including descendants of the
        /// current position) until the next boundary are excluded.
        END(true, false),
        /// The cursor is positioned at a start boundary. The current position as well as any position to the right
        /// in lexicographic order (including descendants of the current position) up to the next boundary are covered
        /// by the set. Branches to the left of this position are excluded.
        START(false, true),
        /// The cursor is positioned inside a covered range.
        CONTAINED(true, true);

        public static final int APPLICABLE_BEFORE = 1 << 0;
        public static final int APPLICABLE_AFTER  = 1 << 1;

        /// Whether the set contains the positions before the cursor's in forward iteration order.
        final boolean applicableBefore;
        /// Whether the set contains the positions after the cursor's in iteration order, starting with the specific
        /// node and the children of the current node.
        final boolean applicableAfter;

        RangeState(boolean applicableBefore, boolean applicableAfter)
        {
            this.applicableBefore = applicableBefore;
            this.applicableAfter = applicableAfter;
        }

        /// Whether the positions preceding the current in iteration order are included in the set.
        public boolean precedingIncluded(Direction direction)
        {
            return direction.select(applicableBefore, applicableAfter);
        }

        /// Whether the positions following the current in iteration order are included in the set.
        public boolean succeedingIncluded(Direction direction)
        {
            return direction.select(applicableAfter, applicableBefore);
        }

        /// Whether the current position is a range boundary.
        public boolean isBoundary()
        {
            return applicableBefore != applicableAfter;
        }

        public RangeState toContent()
        {
            return isBoundary() ? this : null;
        }

        /// Return an "intersection" state for the combination of two states, i.e. the ranges covered by both states.
        public RangeState intersect(RangeState other)
        {
            return values()[ordinal() & other.ordinal()];
        }

        /// Return a "union" state for the combination of two states, i.e. the ranges covered by at least one of the states.
        public RangeState union(RangeState other)
        {
            return values()[ordinal() | other.ordinal()];
        }

        /// Return the negated state, i.e. the state that corresponds to flipped areas of coverage to the left
        /// and right, thus also exchanging start and end boundaries. See [Negated] for more details.
        public RangeState negation()
        {
            return values()[ordinal() ^ (APPLICABLE_BEFORE | APPLICABLE_AFTER)];
        }

        public static RangeState fromProperties(boolean applicableBefore, boolean applicableAfter)
        {
            return values()[(applicableBefore ? APPLICABLE_BEFORE : 0) |
                            (applicableAfter ? APPLICABLE_AFTER : 0)];
        }

        // Implementations of methods from the general RangeState interface (used to treat sets as range tries)

        @Override
        public RangeState precedingState(Direction direction)
        {
            return precedingIncluded(direction) ? CONTAINED : null;
        }

        @Override
        public RangeState succedingState(Direction direction)
        {
            return succeedingIncluded(direction) ? CONTAINED : null;
        }

        @Override
        public RangeState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            return fromProperties(this.applicableBefore && applicableBefore,
                                  this.applicableAfter && applicableAfter);
        }

        @Override
        public RangeState asBoundary(Direction direction)
        {
            final boolean isForward = direction.isForward();
            return fromProperties(this.applicableBefore && !isForward,
                                  this.applicableAfter && isForward);
        }


        public <S extends org.apache.cassandra.db.tries.RangeState<S>>
        S applyToCoveringState(S srcState)
        {
            switch (this)
            {
                case START:
                    return srcState.asBoundary(Direction.FORWARD);
                case END:
                    return srcState.asBoundary(Direction.REVERSE);
                case CONTAINED:
                    return srcState;
                case NOT_CONTAINED:
                    return null;
                default:
                    throw new AssertionError();
            }
        }
    }

    /// The range state of the trie cursor at this point.
    RangeState state();

    /// Returns whether the set includes the positions before the current in iteration order, but after any earlier
    /// position of this cursor, including any position requested by a [#skipTo] call, where this cursor advanced beyond
    /// that position.
    default boolean precedingIncluded()
    {
        return state().precedingIncluded(direction());
    }

    @Override
    default RangeState content()
    {
        return state().toContent();
    }

    @Override
    TrieSetCursor tailCursor(Direction direction);

    @Override
    default TrieSetCursor precedingStateCursor(Direction direction)
    {
        if (precedingIncluded()) // preceding in the direction of this cursor
            return RangesCursor.full(direction, byteComparableVersion());
        else
            return null;
    }

    /// Returns a negated version of this cursor, covering the complement of the key space.
    default TrieSetCursor negated()
    {
        return new Negated(this);
    }

    /// Negation of trie set cursors.
    ///
    /// Achieved by simply inverting the [#state()] values, but it must also correct the root state, including by
    /// adding or dropping a return path to the root state.
    class Negated implements TrieSetCursor
    {
        final TrieSetCursor source;

        enum Overriding
        {
            NONE, ROOT, ROOT_RETURN, EXHAUSTED
        }
        Overriding overriding;

        Negated(TrieSetCursor source)
        {
            this.source = source;
            overriding = Overriding.ROOT;
        }

        @Override
        public long encodedPosition()
        {
            long encodedPosition = source.encodedPosition();
            switch (overriding)
            {
                case ROOT_RETURN:
                    return Cursor.rootReturnPosition(encodedPosition);
                case EXHAUSTED:
                    return Cursor.exhaustedPosition(encodedPosition);
                case ROOT:
                case NONE:
                default:
                    return encodedPosition;
            }
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public RangeState state()
        {
            switch (overriding)
            {
                case ROOT:
                    return source.state().negation().asBoundary(direction());
                case ROOT_RETURN:
                    return direction().select(RangeState.END, RangeState.START);
                case EXHAUSTED:
                    return RangeState.NOT_CONTAINED;
                case NONE:
                default:
                    return source.state().negation();
            }
        }

        long checkOverride(long encodedPosition)
        {
            int depth = Cursor.depth(encodedPosition);
            if (depth > 0)
            {
                overriding = Overriding.NONE;
                return encodedPosition;
            }
            else if (depth == 0)
            {
                // If we are ascending to the root on the return path, it is done to close an active deletion which
                // we no longer have. Go directly to exhausted.
                assert Cursor.isOnReturnPath(encodedPosition);
                overriding = Overriding.EXHAUSTED;
                return encodedPosition();
            }
            else // depth < 0
            {
                // If we went directly to exhausted, we have an active deletion. Insert a root position on the return
                // path to close it.
                assert Cursor.isExhausted(encodedPosition);
                overriding = Overriding.ROOT_RETURN;
                return encodedPosition();
            }
        }

        @Override
        public long advance()
        {
            switch (overriding)
            {
                case ROOT_RETURN:
                    overriding = Overriding.EXHAUSTED;
                    return encodedPosition();
                default:
                    return checkOverride(source.advance());
            }
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            if (Cursor.isExhausted(encodedSkipPosition) || overriding == Overriding.ROOT_RETURN)
            {
                overriding = Overriding.EXHAUSTED;
                return encodedPosition();
            }
            else
                return checkOverride(source.skipTo(encodedSkipPosition));
        }

        // Sets don't implement advanceMultiple as they are only meant to limit data tries.

        @Override
        public TrieSetCursor tailCursor(Direction direction)
        {
            assert !Cursor.isOnReturnPath(encodedPosition()) : "tailCursor called on the return path";
            assert !Cursor.isExhausted(encodedPosition()) : "tailCursor on exhausted cursor";
            return new Negated(source.tailCursor(direction));
        }
    }
}
