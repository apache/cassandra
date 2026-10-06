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
    /// after the current in forward order, whether the node is boundary (and thus applies to this point and all its
    /// descendants) and also describes the type of boundary (e.g. start/end).
    enum RangeState implements org.apache.cassandra.db.tries.RangeState<RangeState>
    {
        // Note: the states must be ordered so that
        //   `values()[applicableBefore * APPLICABLE_BEFORE + applicableAfter * APPLICABLE_AFTER + isBoundary * IS_BOUNDARY]`
        // produces a state with the requested flags

        /// The cursor is at a prefix of a contained range, and neither the branches to the left or right are contained.
        START_END_PREFIX(false, false, false),
        /// The cursor is positioned at a prefix of an end boundary, inside a covered range on the left.
        END_PREFIX(true, false, false),
        /// The cursor is positioned at a prefix of a start boundary. The branches to the right are covered.
        START_PREFIX(false, true, false),
        /// The cursor is positioned inside a covered range, on a prefix of an excluded sub-range.
        END_START_PREFIX(true, true, false),
        /// The cursor is positioned at a "point" boundary, i.e. only the descendants of the boundary are covered,
        /// branches to the left or right are not contained.
        POINT(false, false, true),
        /// The cursor is positioned at an end boundary. Branches to the left, as well as descendants of this point are
        /// covered by the set.
        END(true, false, true),
        /// The cursor is positioned at a start boundary. Branches to the right, as well as descendants of this point
        /// are covered by the set.
        START(false, true, true),
        /// The cursor is positioned at a non-effective boundary (an end boundary for the previous range, as well as
        /// a start for the next). Branches before, after and below this point is covered.
        COVERED(true, true, true);

        public static final int APPLICABLE_BEFORE = 1 << 0;
        public static final int APPLICABLE_AFTER  = 1 << 1;
        public static final int IS_BOUNDARY       = 1 << 2;

        /// Whether the set applied to positions before the cursor's in forward order.
        final boolean applicableBefore;
        /// Whether the set applied to positions after the cursor's in forward order.
        final boolean applicableAfter;
        /// Whether this marker specifies a boundary point. Boundary points are reported as content.
        final boolean isBoundary;

        RangeState(boolean applicableBefore, boolean applicableAfter, boolean isBoundary)
        {
            this.applicableBefore = applicableBefore;
            this.applicableAfter = applicableAfter;
            this.isBoundary = isBoundary;
        }

        /// Whether the positions preceding the current in iteration order are included in the set.
        public boolean precedingIncluded(Direction direction)
        {
            return direction.select(applicableBefore, applicableAfter);
        }

        /// Whether the current position is a range boundary. This also means that the descendant branch is fully
        /// included in the set.
        public boolean isBoundary()
        {
            return isBoundary;
        }

        public RangeState toContent()
        {
            return isBoundary ? this : null;
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

        /// Return the "weakly negated" state, i.e. the state that corresponds to flipped areas of coverage to the left
        /// and right, and the boundary points. See [TrieSet#weakNegation] for more details.
        public RangeState weakNegation()
        {
            return values()[ordinal() ^ (APPLICABLE_BEFORE | APPLICABLE_AFTER)];
        }

        public static RangeState fromProperties(boolean applicableBefore, boolean applicableAfter, boolean isBoundary)
        {
            return values()[(applicableBefore ? APPLICABLE_BEFORE : 0) |
                            (applicableAfter ? APPLICABLE_AFTER : 0) |
                            (isBoundary ? IS_BOUNDARY : 0)];
        }

        // RangeState implementations (used for verification)

        @Override
        public RangeState precedingState(Direction direction)
        {
            return precedingIncluded(direction) ? END_START_PREFIX : null;
        }

        @Override
        public RangeState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            return fromProperties(this.applicableBefore && applicableBefore,
                                  this.applicableAfter && applicableAfter,
                                  this.isBoundary);
        }

        @Override
        public RangeState asBoundary(Direction direction)
        {
            final boolean isForward = direction.isForward();
            return fromProperties(this.applicableBefore && !isForward,
                                  this.applicableAfter && isForward,
                                  true);
        }


        public <S extends org.apache.cassandra.db.tries.RangeState<S>>
        S applyToCoveringState(S srcState, Direction direction)
        {
            switch (this)
            {
                case POINT:
                    return srcState.asPoint();
                case COVERED:
                    return srcState;
                case START:
                    return srcState.asBoundary(Direction.FORWARD);
                case END:
                    return srcState.asBoundary(Direction.REVERSE);
                default:
                    return precedingIncluded(direction) ? srcState : null;
            }

        }
    }

    /// The range state of the trie cursor at this point.
    RangeState state();

    /// Returns whether the set includes the positions before the current in iteration order, but after any earlier
    /// position of this cursor, including any position requested by a [#skipTo] call, where this cursor advanced beyond
    /// that position.
    ///
    /// Note that this may also be true when the cursor is in an exhausted state, as well as immediately
    /// after cursor construction, signifying, respectively, right and left unbounded ranges.
    default boolean precedingIncluded()
    {
        return state().precedingIncluded(direction());
    }

    /// Returns whether the set fully includes all descendants of the current position. This is true for all boundary
    /// points.
    default boolean branchIncluded()
    {
        return state().isBoundary;
    }

    @Override
    default RangeState content()
    {
        return state().toContent();
    }

    @Override
    TrieSetCursor tailCursor(Direction direction);

    /// Returns a negated version of this cursor (where every returned state is inverted).
    default TrieSetCursor negated()
    {
        return new Negated(this);
    }

    /// Negation of trie set cursors.
    ///
    /// Achieved by simply inverting the [#state()] values.
    class Negated implements TrieSetCursor
    {
        final TrieSetCursor source;

        Negated(TrieSetCursor source)
        {
            this.source = source;
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
        public RangeState state()
        {
            return source.state().weakNegation();
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

        // Sets don't implement advanceMultiple as they are only meant to limit data tries.

        @Override
        public TrieSetCursor tailCursor(Direction direction)
        {
            return new Negated(source.tailCursor(direction));
        }
    }

    static TrieSetCursor empty(Direction direction, ByteComparable.Version version)
    {
        return new Empty(TrieSetCursor.RangeState.START_END_PREFIX, version, direction);
    }

    class Empty extends Cursor.Empty<RangeState> implements TrieSetCursor
    {
        final RangeState coveringState;

        public Empty(RangeState coveringState, ByteComparable.Version version, Direction direction)
        {
            super(direction, version);
            this.coveringState = coveringState;
        }

        @Override
        public RangeState state()
        {
            return coveringState;
        }

        @Override
        public RangeState content()
        {
            return null;
        }

        @Override
        public TrieSetCursor tailCursor(Direction direction)
        {
            return new TrieSetCursor.Empty(coveringState, byteComparableVersion(), direction);
        }
    }
}
