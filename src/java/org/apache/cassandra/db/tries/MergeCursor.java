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

/// A merged view of two trie cursors.
///
/// This is accomplished by walking the two cursors in parallel; the merged cursor takes the position and features of the
/// smaller and advances with it; when the two cursors are equal, both are advanced.
///
/// Crucial for the efficiency of this is the fact that when they are advanced like this, we can compare cursors'
/// positions by their `depth` descending and then `incomingTransition` ascending.
/// See [Trie.md](./Trie.md) for further details.
abstract class MergeCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    final Trie.MergeResolver<T> resolver;

    final C c1;
    final C c2;

    boolean atC1;
    boolean atC2;

    MergeCursor(Trie.MergeResolver<T> resolver, C c1, C c2)
    {
        c1.assertFresh();
        c2.assertFresh();
        this.resolver = resolver;
        this.c1 = c1;
        this.c2 = c2;
        atC1 = atC2 = true;
    }

    @Override
    public long advance()
    {
        return checkOrder(atC1 ? c1.advance() : c1.encodedPosition(),
                          atC2 ? c2.advance() : c2.encodedPosition());
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        return checkOrder(atC1 ? c1.skipTo(encodedSkipPosition) : c1.skipToWhenAhead(encodedSkipPosition),
                          atC2 ? c2.skipTo(encodedSkipPosition) : c2.skipToWhenAhead(encodedSkipPosition));
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atC1 && atC2)
            return checkOrder(c1.advance(), c2.advance());

        // If we are in a branch that's only covered by one of the sources, we can use its advanceMultiple as it is
        // only different from advance if it takes multiple steps down, which does not change the order of the
        // cursors.
        // Since it might ascend, we still have to check the order after the call.
        if (atC1)
            return checkOrder(c1.advanceMultiple(receiver), c2.encodedPosition());
        else // atC2
            return checkOrder(c1.encodedPosition(), c2.advanceMultiple(receiver));
    }

    long checkOrder(long c1pos, long c2pos)
    {
        long cmp = Cursor.compare(c1pos, c2pos);
        atC1 = cmp <= 0;
        atC2 = cmp >= 0;
        return atC1 ? c1pos : c2pos;
    }

    @Override
    public long encodedPosition()
    {
        return atC1 ? c1.encodedPosition() : c2.encodedPosition();
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        assert c1.byteComparableVersion() == c2.byteComparableVersion() :
            "Merging cursors with different byteComparableVersions: " +
            c1.byteComparableVersion() + " vs " + c2.byteComparableVersion();
        return c1.byteComparableVersion();
    }

    /// Merge implementation for [Trie]
    static class Plain<T> extends MergeCursor<T, Cursor<T>>
    {
        Plain(Trie.MergeResolver<T> resolver, Cursor<T> c1, Cursor<T> c2)
        {
            super(resolver, c1, c2);
        }

        @Override
        public T content()
        {
            T mc = atC2 ? c2.content() : null;
            T nc = atC1 ? c1.content() : null;
            if (mc == null)
                return nc;
            else if (nc == null)
                return mc;
            else
                return resolver.resolve(nc, mc);
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            if (atC1 && atC2)
                return new Plain<>(resolver, c1.tailCursor(direction), c2.tailCursor(direction));
            else if (atC1)
                return c1.tailCursor(direction);
            else if (atC2)
                return c2.tailCursor(direction);
            else
                throw new AssertionError();
        }
    }

    /// Merge implementation for [RangeTrie]
    static class Range<S extends RangeState<S>> extends MergeCursor<S, RangeCursor<S>> implements RangeCursor<S>
    {
        private S state;
        boolean stateCollected;

        Range(Trie.MergeResolver<S> resolver, RangeCursor<S> c1, RangeCursor<S> c2)
        {
            super(resolver, c1, c2);
        }

        @Override
        public S state()
        {
            if (!stateCollected)
            {
                S state1 = atC1 ? c1.state() : c1.precedingState();
                S state2 = atC2 ? c2.state() : c2.precedingState();
                if (state1 == null)
                    state = state2;
                else if (state2 == null)
                    state = state1;
                else
                    state = resolver.resolve(state1, state2);
                stateCollected = true;
            }
            return state;
        }

        @Override
        public long advance()
        {
            stateCollected = false;
            return super.advance();
        }

        @Override
        public long skipTo(long encodedSkipTransition)
        {
            stateCollected = false;
            return super.skipTo(encodedSkipTransition);
        }

        @Override
        public long advanceMultiple(Cursor.TransitionsReceiver receiver)
        {
            stateCollected = false;
            return super.advanceMultiple(receiver);
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            if (atC1 && atC2)
                return new Range<>(resolver, c1.tailCursor(direction), c2.tailCursor(direction));
            else if (atC1)
                return makeMerge(resolver, c1.tailCursor(direction), c2.precedingStateCursor(direction));
            else if (atC2)
                return makeMerge(resolver, c1.precedingStateCursor(direction), c2.tailCursor(direction));
            else
                throw new AssertionError();
        }

        private static <S extends RangeState<S>> RangeCursor<S> makeMerge(Trie.MergeResolver<S> resolver, RangeCursor<S> c1, RangeCursor<S> c2)
        {
            if (c1 == null)
                return c2;
            if (c2 == null)
                return c1;
            return new Range<>(resolver, c1, c2);
        }
    }

    /// Deletion-aware merge cursor that efficiently merges two deletion-aware tries.
    /// This cursor handles the complex task of merging both live data and deletion metadata
    /// from two deletion-aware sources. It supports an important optimization via the
    /// `deletionsAtFixedPoints` flag.
    static class DeletionAware<T, D extends RangeState<D>>
    extends MergeCursor<T, DeletionAwareMergeSource<T, D, D>> implements DeletionAwareCursor<T, D>
    {
        final Trie.MergeResolver<D> deletionResolver;

        /// Tracks the depth at which deletion branches were introduced to avoid redundant processing.
        /// Set to -1 when no deletion branches are active.
        int deletionBranchDepth = -1;

        /// @see DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints
        final boolean deletionsAtFixedPoints;

        /// Creates a deletion-aware merge cursor with configurable deletion optimization.
        ///
        /// @param mergeResolver resolver for merging live data content
        /// @param deletionResolver resolver for merging deletion metadata
        /// @param deleter function to apply deletions to live data
        /// @param c1 first deletion-aware cursor
        /// @param c2 second deletion-aware cursor
        /// @param deletionsAtFixedPoints See [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints]
        DeletionAware(Trie.MergeResolver<T> mergeResolver,
                      Trie.MergeResolver<D> deletionResolver,
                      BiFunction<D, T, T> deleter,
                      DeletionAwareCursor<T, D> c1,
                      DeletionAwareCursor<T, D> c2,
                      boolean deletionsAtFixedPoints)
        {
            this(mergeResolver,
                 deletionResolver,
                 new DeletionAwareMergeSource<>(deleter, c1),
                 new DeletionAwareMergeSource<>(deleter, c2),
                 deletionsAtFixedPoints);
            // We will add deletion sources to the above as we find them.
            maybeAddDeletionsBranch(this.c1.encodedPosition());
        }

        DeletionAware(Trie.MergeResolver<T> mergeResolver,
                      Trie.MergeResolver<D> deletionResolver,
                      DeletionAwareMergeSource<T, D, D> c1,
                      DeletionAwareMergeSource<T, D, D> c2,
                      boolean deletionsAtFixedPoints)
        {
            super(mergeResolver, c1, c2);
            this.deletionResolver = deletionResolver;
            this.deletionsAtFixedPoints = deletionsAtFixedPoints;
        }

        @Override
        public T content()
        {
            T mc = atC2 ? c2.content() : null;
            T nc = atC1 ? c1.content() : null;
            if (mc == null)
                return nc;
            else if (nc == null)
                return mc;
            else
                return resolver.resolve(nc, mc);
        }

        @Override
        public long advance()
        {
            return maybeAddDeletionsBranch(super.advance());
        }

        @Override
        public long skipTo(long encodedSkipTransition)
        {
            return maybeAddDeletionsBranch(super.skipTo(encodedSkipTransition));
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return maybeAddDeletionsBranch(super.advanceMultiple(receiver));
        }

        long maybeAddDeletionsBranch(long encodedPosition)
        {
            if (Cursor.depth(encodedPosition) <= deletionBranchDepth)   // ascending above common deletions root
            {
                deletionBranchDepth = -1;
                assert !c1.hasDeletions();
                assert !c2.hasDeletions();
            }

            if (atC1 && atC2 && // otherwise even if there is deletion, the other cursor is ahead of it and can't be affected
                (!deletionsAtFixedPoints || deletionBranchDepth == -1)) // if we already found one, don't check the other source for branches below it
            {
                maybeAddDeletionsBranch(c1, c2);
                maybeAddDeletionsBranch(c2, c1);
            }
            return encodedPosition;
        }

        /// Attempts to add deletion branches from one source to another.
        /// This method implements the core deletion merging logic. When `deletionsAtFixedPoints`
        /// is true, it can skip expensive operations because we know deletion branches are
        /// mutually exclusive between sources.
        ///
        /// @param tgt target merge source that may receive deletions
        /// @param src source merge source that may provide deletions
        void maybeAddDeletionsBranch(DeletionAwareMergeSource<T, D, D> tgt,
                                     DeletionAwareMergeSource<T, D, D> src)
        {
            // If tgt already has deletions applied, no need to add more (we cannot have a deletion branch covering
            // another deletion branch).
            if (tgt.hasDeletions())
                return;

            RangeCursor<D> deletionsBranch = src.deletionBranchCursor(src.direction());
            if (deletionsBranch != null)
                tgt.addDeletions(deletionsBranch);  // apply all src deletions to tgt
        }


        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            int depth = Cursor.depth(encodedPosition());
            if (deletionBranchDepth != -1 && depth > deletionBranchDepth)
                return null;    // already covered by a deletion branch, if there is any here it will be reflected in that

            // if one of the two cursors is ahead, it can't affect this deletion branch
            if (!atC1)
                return maybeSetDeletionsDepth(c2.deletionBranchCursor(direction), depth);
            if (!atC2)
                return maybeSetDeletionsDepth(c1.deletionBranchCursor(direction), depth);

            // We are positioned at a common branch. If one has a deletion branch, we must combine it with the
            // deletion-tree branch of the other to make sure that we merge any higher-depth deletion branch with it.
            RangeCursor<D> b1 = c1.deletionBranchCursor(direction);
            RangeCursor<D> b2 = c2.deletionBranchCursor(direction);
            if (b1 == null && b2 == null)
                return null;

            deletionBranchDepth = depth;

            // OPTIMIZATION: When deletionsAtFixedPoints=true, we know that both sources would
            // have deletions at the same depth, i.e. if one source has a deletion
            // branch at this position, the other cannot have any deletion branches below this
            // point. We can thus avoid reproducing the data trie in the deletion branch.
            if (deletionsAtFixedPoints)
            {
                // With the optimization, we can directly return the existing deletion branch
                // without needing to create expensive DeletionsTrieCursor instances
                if (b1 != null && b2 != null)
                {
                    // Both have deletion branches - merge them directly
                    return new Range<>(deletionResolver, b1, b2);
                }
                else
                {
                    // Only one has a deletion branch - return it directly
                    // The optimization guarantees the other source has no conflicting deletions
                    return b1 != null ? b1 : b2;
                }
            }
            else
            {
                // Safe path: create DeletionsTrieCursor for missing deletion branches
                // This ensures we capture any deletion branches that might exist deeper
                // in the trie structure, but is expensive for large tries because we have
                // to list the whole data trie (minus content).
                if (b1 == null)
                    b1 = new DeletionAwareCursor.DeletionsTrieCursor<>(c1.data.tailCursor(direction));
                if (b2 == null)
                    b2 = new DeletionAwareCursor.DeletionsTrieCursor<>(c2.data.tailCursor(direction));

                return new Range<>(deletionResolver, b1, b2);
            }
        }

        private RangeCursor<D> maybeSetDeletionsDepth(RangeCursor<D> deletionBranchCursor, int depth)
        {
            if (deletionBranchCursor != null)
                deletionBranchDepth = depth;
            return deletionBranchCursor;
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            if (atC1 && atC2)
                return new DeletionAware<>(resolver,
                                           deletionResolver,
                                           c1.tailCursor(direction),
                                           c2.tailCursor(direction),
                                           deletionsAtFixedPoints);
            else if (atC1)
                return c1.tailCursor(direction);
            else if (atC2)
                return c2.tailCursor(direction);
            else
                throw new AssertionError();
        }
    }
}
