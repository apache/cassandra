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


    /// Process the trie using the given [DeletionAwareTrie.DeletionAwareWalker], providing access to both live and
    /// deletion branches.
    default <R> R process(DeletionAwareTrie.DeletionAwareWalker<? super T, ? super D, R> walker)
    {
        assertFresh();
        long currentPosition = encodedPosition();

        while (true)
        {
            RangeCursor<D> deletionBranch = deletionBranchCursor(direction());
            if (deletionBranch != null && walker.enterDeletionsBranch())
            {
                processDeletionBranch(walker, deletionBranch);
                walker.exitDeletionsBranch();
            }
            T content = content();   // handle content on the root node
            if (content != null)
                walker.content(content);

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
    private static <D> void processDeletionBranch(DeletionAwareTrie.DeletionAwareWalker<?, ? super D, ?> walker, Cursor<D> cursor)
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
                    // we can't reach any of the other states if stopIssuingDeletions is true
                case AT_C2:
                    return new SwitchableLiveAndDeletionsMergeCursor<>(resolver, new DeletionAwareCursor.Empty<>(direction, byteComparableVersion()), c2.tailCursor(direction));
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
