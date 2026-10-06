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

import java.util.function.BiFunction;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// Cursor interface for deletion-aware tries that provides access to both live data and deletion branches.
///
/// This cursor extends the basic [Cursor] interface to support the dual nature of deletion-aware tries,
/// where live data and deletion information coexist in a unified structure. The cursor walks the live
/// data portion of the trie while providing access to deletion branches through the
/// [#deletionBranchCursor] method.
///
/// The cursor behaves like a standard trie cursor for live data, supporting all standard navigation and
/// content access operations inherited from [Cursor]. It can also be used as a plain trie cursor for
/// processing or iteration methods and classes, in which case only live data will be presented.
///
/// At any position, this cursor can provide access to deletion branches through [#deletionBranchCursor],
/// which returns a [RangeCursor] covering deletion ranges rooted at the current position. The deletion
/// information is only reachable after taking and following deletion branches. When a consumer is interested
/// in the deletion information, it can be merged into the main tree using [LiveAndDeletionsMergeCursor] or
/// presented as the full range trie via [DeletionsTrieCursor].
///
/// Deletion-aware cursors must maintain strict structural invariants to ensure correctness and efficiency:
///
/// **Non-Overlapping Deletion Branches**: No deletion branch can be covered by another deletion branch.
/// When a deletion branch exists at a given node, all descendants of that node must have null deletion
/// branches. This prevents nested deletion scopes and simplifies merge algorithms.
///
/// **Well-Formed Deletion Branches**: Each deletion branch must be a properly constructed range trie:
/// - It cannot start or end with an active deletion (no open-ended ranges at boundaries).
/// - Every deletion opened by an entry must be closed by the next entry.
/// - Preceding state must be correctly reported for all positions.
///
/// **Deletion Consistency**: There cannot be live entries in the trie that are deleted by deletion
/// branches in the same trie. This ensures that the trie represents a consistent view where deletions
/// have been properly applied.
///
/// @param <T> The content type for live data in the trie
/// @param <D> The deletion marker type, must extend `RangeState`
public interface DeletionAwareCursor<T, D extends RangeState<D>> extends Cursor<T>
{
    /// Returns the deletion branch rooted at the current cursor position, if any.
    ///
    /// This method provides access to deletion information associated with the current position in the
    /// trie and nodes below it. The deletion branch is represented as a [RangeCursor] that can cover
    /// ranges of keys with deletion markers. It is presented as a tail cursor for the current position,
    /// i.e. it starts with depth 0 and cannot extend beyond the current position.
    ///
    /// When this method returns a non-null deletion branch, the source cursor is not allowed to return another deletion
    /// branch in the covered branch. In other words, for any given path in the trie there must be at most one node
    /// where [#deletionBranchCursor] is non-null.
    ///
    /// @param direction The direction for traversing the deletion branch.
    /// @return A range cursor for deletions at this position, or null if no deletion branch is defined at this level.
    RangeCursor<D> deletionBranchCursor(Direction direction);

    @Override
    DeletionAwareCursor<T, D> tailCursor(Direction direction);


    /// Process the trie using the given [DeletionAwareWalker], providing access to both live and
    /// deletion branches.
    default <R> R process(DeletionAwareWalker<? super T, ? super D, R> walker)
    {
        assertFresh();
        long currentPosition = encodedPosition();

        while (true)
        {
            T content = content();   // handle content on the root node
            if (content != null)
                walker.content(content);
            RangeCursor<D> deletionBranch = deletionBranchCursor(direction());
            if (deletionBranch != null && walker.enterDeletionsBranch())
            {
                processDeletionBranch(walker, deletionBranch);
                walker.exitDeletionsBranch();
            }

            long prevPosition = currentPosition;
            currentPosition = advanceMultiple(walker);
            if (Cursor.isExhausted(currentPosition))
                break;
            if (Cursor.ascended(currentPosition, prevPosition))
                walker.resetPathLength(Cursor.depth(currentPosition) - 1);
            walker.addPathByte(Cursor.incomingTransition(currentPosition));
        }

        return walker.complete();
    }

    /// Process a deletion branch using the given walker.
    private static <D> void processDeletionBranch(DeletionAwareWalker<?, ? super D, ?> walker, Cursor<D> cursor)
    {
        cursor.assertFresh();
        D content = cursor.content();   // handle content on the root node
        if (content == null)
            content = cursor.advanceToContent(walker);

        while (content != null)
        {
            walker.deletionMarker(content);
            content = cursor.advanceToContent(walker);
        }
    }

    /// Walker interface extended to also process deletion branches.
    interface DeletionAwareWalker<T, D, R> extends Walker<T, R>
    {
        /// Called when a deletion branch is found. Return false to skip over it, or true to use to descend inside
        /// it. If this returns true, this walker will go through the deletion branch, call [#deletionMarker] for
        /// all content of the deletion branch and exit the branch by calling [#exitDeletionsBranch] when it is
        /// exhausted, after which it will start the walk over the data branch of this node.
        ///
        /// Note that the depth given by [#resetPathLength] in the deletion branch will be relative to the root of the
        /// deletion branch. See [TrieEntriesWalker.DeletionAware] for an example of handling this.
        default boolean enterDeletionsBranch()
        {
            // do nothing by default
            return true;
        }

        /// Called for every deletion marker found in the deletion branch.
        void deletionMarker(D marker);

        /// Called when the deletion branch is exited.
        default void exitDeletionsBranch()
        {
            // do nothing by default
        }
    }

    /// A cursor merging the live data and deletion markers of a deletion-aware trie into a combined trie.
    class LiveAndDeletionsMergeCursor<T, D extends RangeState<D>, Z>
    extends FlexibleMergeCursor.WithMappedContent<T, D, DeletionAwareCursor<T, D>, RangeCursor<D>, Z>
    {
        LiveAndDeletionsMergeCursor(BiFunction<T, D, Z> resolver, DeletionAwareCursor<T, D> c1)
        {
            super(resolver, c1);
            postAdvance(encodedPosition());
        }

        LiveAndDeletionsMergeCursor(BiFunction<T, D, Z> resolver, DeletionAwareCursor<T, D> c1, RangeCursor<D> c2)
        {
            super(resolver, c1, c2);
            postAdvance(encodedPosition());
        }

        @Override
        long postAdvance(long encodedPosition)
        {
            if (state == State.C1_ONLY)
            {
                RangeCursor<D> deletionsBranch = c1.deletionBranchCursor(direction());
                if (deletionsBranch != null)
                    addCursor(deletionsBranch);
            }
            return encodedPosition;
        }

        @Override
        public LiveAndDeletionsMergeCursor<T, D, Z> tailCursor(Direction direction)
        {
            switch (state)
            {
                case C1_ONLY:
                    return new LiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction));
                case AT_C2:
                    return new LiveAndDeletionsMergeCursor<>(resolver, new DeletionAwareCursor.Empty<>(direction, byteComparableVersion()), c2.tailCursor(direction));
                case AT_C1:
                    return new LiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction), c2.precedingStateCursor(direction));
                case AT_BOTH:
                    return new LiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction), c2.tailCursor(direction));
                default:
                    throw new AssertionError();
            }
        }

        /// Returns an unmerged tail cursor that includes the data and deletion branches applicable to the current
        /// point. Used by [TrieTailsIterator.DeletionAware].
        ///
        /// @param includeCoveringDeletion If false, any covering deletion will not be included in the tail deletion
        ///        branch, including the internal ranges where the covering deletion applies.
        public DeletionAwareTrie<T, D> deletionAwareTail(boolean includeCoveringDeletion)
        {
            if (Cursor.isOnReturnPath(encodedPosition()))
                return null;

            switch (state)
            {
                case C1_ONLY:
                    return combineTails(c1, null);
                case AT_C2:
                    return combineTails(null, includeCoveringDeletion ? c2 : dropCoveringDeletions(c2));
                case AT_C1:
                    return combineTails(c1, includeCoveringDeletion ? c2.precedingStateCursor(direction()) : null);
                case AT_BOTH:
                    return combineTails(c1, includeCoveringDeletion ? c2 : dropCoveringDeletions(c2));
                default:
                    throw new AssertionError();
            }
        }
    }

    /// Returns a wrapped version of the given cursor that drops any deletion that applies to the current point as
    /// a covering deletion. Used to prepare a cursor for taking its tail when `ignoreCoveringDeletion` is false.
    static <D extends RangeState<D>> RangeCursor<D> dropCoveringDeletions(RangeCursor<D> cursor)
    {
        D state = cursor.state();
        if (state == null)
            return cursor;
        // If a covering state applies, it must be the left side of the state.
        D preceeding = state.precedingState(cursor.direction());
        if (preceeding == null)
            return cursor;
        return new ContentMappingCursor.Range<>(s -> dropDeletion(s, preceeding), cursor);
    }

    private static <D extends RangeState<D>> D dropDeletion(D state, D toDrop)
    {
        if (state.isBoundary())
        {
            boolean dropLeft = toDrop.equals(state.precedingState(Direction.FORWARD));
            boolean dropRight = toDrop.equals(state.succedingState(Direction.FORWARD));
            if (!dropLeft && !dropRight)
                return state;
            return state.restrict(!dropLeft, !dropRight);
        }
        else
            return state.equals(toDrop) ? null : state;
    }

    /// Returns a tail trie formed by combining the tail tries of the positions of the given live and deletion cursors,
    /// correcting for any of the arguments being null.
    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> combineTails(DeletionAwareCursor<T, D> c, RangeCursor<D> deletionBranch)
    {
        if (c == null && deletionBranch == null)
            return null;

        // Create a trie cursor now to make sure changes to c or deletionBranch do not affect it.
        DeletionAwareCursor<T, D> cursor = combineTailCursors(c, deletionBranch);

        return dir -> cursor.tailCursor(dir);
    }

    private static <T, D extends RangeState<D>>
    DeletionAwareCursor<T, D> combineTailCursors(DeletionAwareCursor<T, D> c, RangeCursor<D> deletionBranch)
    {
        if (c != null)
        {
            Direction direction = c.direction();
            if (deletionBranch != null)
                return new PrefixedCursor.DeletionAwareSeparately<>(ByteComparable.EMPTY,
                                                                    c.tailCursor(direction),
                                                                    deletionBranch.tailCursor(direction));
            else
                return c.tailCursor(direction);
        }
        else if (deletionBranch != null)
        {
            // fix the position of the deletion branch
            Direction direction = deletionBranch.direction();
            deletionBranch = deletionBranch.tailCursor(direction);
            return new SingletonCursor.DeletionBranch<>(direction,
                                                        ByteSource.EMPTY,
                                                        deletionBranch.byteComparableVersion(),
                                                        deletionBranch::tailCursor);
        }
        else
            return null;
    }

    /// A variant of [LiveAndDeletionsMergeCursor] that can be asked to stop issuing deletion markers.
    class SwitchableLiveAndDeletionsMergeCursor<T, D extends RangeState<D>, Z>
    extends LiveAndDeletionsMergeCursor<T, D, Z>
    implements DeletionAwareTrie.DeletionsStopControl
    {
        boolean stopIssuingDeletions;

        SwitchableLiveAndDeletionsMergeCursor(BiFunction<T, D, Z> resolver, DeletionAwareCursor<T, D> c1)
        {
            super(resolver, c1);
            this.stopIssuingDeletions = false;
        }

        SwitchableLiveAndDeletionsMergeCursor(BiFunction<T, D, Z> resolver, DeletionAwareCursor<T, D> c1, boolean stopIssuingDeletions)
        {
            super(resolver, c1);
            this.stopIssuingDeletions = stopIssuingDeletions;
        }

        SwitchableLiveAndDeletionsMergeCursor(BiFunction<T, D, Z> resolver, DeletionAwareCursor<T, D> c1, RangeCursor<D> c2)
        {
            super(resolver, c1, c2);
            this.stopIssuingDeletions = false;
        }

        public void stopIssuingDeletions(ResettingTransitionsReceiver receiver)
        {
            stopIssuingDeletions = true;
            // drop any already open deletion branch
            switch (state)
            {
                case AT_C2:
                    // we need to exit the deletion branch at the next advance
                    c2 = RangeCursor.empty(direction(), byteComparableVersion());
                    break;
                default:
                    state = State.C1_ONLY;
                    c2 = null;
                    break;
            }
        }

        @Override
        long postAdvance(long encodedPosition)
        {
            if (stopIssuingDeletions)
                return encodedPosition;
            return super.postAdvance(encodedPosition);
        }

        @Override
        public SwitchableLiveAndDeletionsMergeCursor<T, D, Z> tailCursor(Direction direction)
        {
            switch (state)
            {
                case C1_ONLY:
                    return new SwitchableLiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction), stopIssuingDeletions);
                case AT_C2:
                    // If stopIssuingDeletions was just set, c2 is empty thus we return an empty cursor as expected.
                    return new SwitchableLiveAndDeletionsMergeCursor<>(resolver, new DeletionAwareCursor.Empty<>(direction, byteComparableVersion()), c2.tailCursor(direction));
                    // we can't reach any of the other states if stopIssuingDeletions is true
                case AT_C1:
                    return new SwitchableLiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction), c2.precedingStateCursor(direction));
                case AT_BOTH:
                    return new SwitchableLiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction), c2.tailCursor(direction));
                default:
                    throw new AssertionError();
            }
        }
    }

    /// A cursor presenting the deletion markers of a deletion-aware trie.
    ///
    /// This cursor combines all deletion branches into a single trie. Because it is not known where a deletion branch
    /// can be introduced, this cursor has to walk all nodes of the live trie that are not covered by a deletion branch,
    /// returning (likely a lot of) unproductive branches where a deletion is not defined.
    class DeletionsTrieCursor<T, D extends RangeState<D>>
    extends FlexibleMergeCursor<DeletionAwareCursor<T, D>, RangeCursor<D>, D> implements RangeCursor<D>
    {
        DeletionsTrieCursor(DeletionAwareCursor<T, D> c1)
        {
            super(c1);
            postAdvance(encodedPosition());
        }

        @Override
        public D state()
        {
            return c2 != null ? c2.state() : null;
        }

        @Override
        public D precedingState()
        {
            return c2 != null ? c2.precedingState() : null;
        }

        @Override
        public D content()
        {
            return c2 != null ? c2.content() : null;
        }

        @Override
        long postAdvance(long encodedPosition)
        {
            switch (state)
            {
                case AT_C2:
                    // already in deletion branch
                    break;
                case C1_ONLY:
                    RangeCursor<D> deletionsBranch = c1.deletionBranchCursor(direction());
                    if (deletionsBranch != null)
                    {
                        addCursor(deletionsBranch);
                        // deletion branches cannot be nested; skip past the current position in the main trie as we
                        // don't need to further track it inside this branch
                        c1.skipTo(Cursor.positionForSkippingBranch(encodedPosition));
                        state = State.AT_C2;
                    }
                    break;
                default:
                    throw new AssertionError("Deletion branch extends above its introduction");
            }
            return encodedPosition;
        }

        @Override
        public RangeCursor<D> tailCursor(Direction direction)
        {
            switch (state)
            {
                case AT_C2:
                    return c2.tailCursor(direction);
                case C1_ONLY:
                    return new DeletionsTrieCursor<>(c1.tailCursor(direction));
                default:
                    throw new AssertionError("Deletion branch extends above its introduction");
            }
        }
    }

    class Empty<T, D extends RangeState<D>>
    extends Cursor.Empty<T> implements DeletionAwareCursor<T, D>
    {
        public Empty(Direction direction, ByteComparable.Version byteComparableVersion)
        {
            super(direction, byteComparableVersion);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return null;
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new DeletionAwareCursor.Empty<>(direction, byteComparableVersion());
        }
    }

    class Wrapping<T, D extends RangeState<D>> implements DeletionAwareCursor<T, D>
    {
        final Cursor<T> source;

        public Wrapping(Cursor<T> source)
        {
            this.source = source;
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return null;
        }

        @Override
        public long encodedPosition()
        {
            return source.encodedPosition();
        }

        @Override
        public T content()
        {
            return source.content();
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public long advance()
        {
            return source.advance();
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return source.advanceMultiple(receiver);
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            return source.skipTo(encodedSkipPosition);
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new Wrapping<>(source.tailCursor(direction));
        }
    }
}
