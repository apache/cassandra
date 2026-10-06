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
    final Direction direction;
    final Trie.MergeResolver<T> resolver;

    final C c1;
    final C c2;

    boolean atC1;
    boolean atC2;

    MergeCursor(Trie.MergeResolver<T> resolver, C c1, C c2)
    {
        assert c1.depth() == 0 : "The provided cursor has already been advanced.";
        assert c2.depth() == 0 : "The provided cursor has already been advanced.";
        this.direction = c1.direction();
        this.resolver = resolver;
        this.c1 = c1;
        this.c2 = c2;
        atC1 = atC2 = true;
    }

    @Override
    public int advance()
    {
        return checkOrder(atC1 ? c1.advance() : c1.depth(),
                          atC2 ? c2.advance() : c2.depth());
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        int c1depth = c1.depth();
        int c2depth = c2.depth();
        assert skipDepth <= c1depth + 1 || skipDepth <= c2depth + 1;
        if (atC1 || skipDepth < c1depth || skipDepth == c1depth && direction.gt(skipTransition, c1.incomingTransition()))
            c1depth = c1.skipTo(skipDepth, skipTransition);
        if (atC2 || skipDepth < c2depth || skipDepth == c2depth && direction.gt(skipTransition, c2.incomingTransition()))
            c2depth = c2.skipTo(skipDepth, skipTransition);

        return checkOrder(c1depth, c2depth);
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atC1 && atC2)
            return checkOrder(c1.advance(), c2.advance());

        // If we are in a branch that's only covered by one of the sources, we can use its advanceMultiple as it is
        // only different from advance if it takes multiple steps down, which does not change the order of the
        // cursors.
        // Since it might ascend, we still have to check the order after the call.
        if (atC1)
            return checkOrder(c1.advanceMultiple(receiver), c2.depth());
        else // atC2
            return checkOrder(c1.depth(), c2.advanceMultiple(receiver));
    }

    int checkOrder(int c1depth, int c2depth)
    {
        if (c1depth > c2depth)
        {
            atC1 = true;
            atC2 = false;
            return c1depth;
        }
        if (c1depth < c2depth)
        {
            atC1 = false;
            atC2 = true;
            return c2depth;
        }
        // c1depth == c2depth
        int c1trans = c1.incomingTransition();
        int c2trans = c2.incomingTransition();
        atC1 = direction.le(c1trans, c2trans);
        atC2 = direction.le(c2trans, c1trans);
        assert atC1 | atC2;
        return c1depth;
    }

    @Override
    public int depth()
    {
        return atC1 ? c1.depth() : c2.depth();
    }

    @Override
    public int incomingTransition()
    {
        return atC1 ? c1.incomingTransition() : c2.incomingTransition();
    }

    @Override
    public Direction direction()
    {
        return direction;
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
                    return state2;
                if (state2 == null)
                    return state1;
                state = resolver.resolve(state1, state2);
                stateCollected = true;
            }
            return state;
        }

        @Override
        public int advance()
        {
            stateCollected = false;
            return super.advance();
        }

        @Override
        public int skipTo(int depth, int incomingTransition)
        {
            stateCollected = false;
            return super.skipTo(depth, incomingTransition);
        }

        @Override
        public int advanceMultiple(Cursor.TransitionsReceiver receiver)
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
                return new Range<>(resolver, c1.tailCursor(direction), c2.precedingStateCursor(direction));
            else if (atC2)
                return new Range<>(resolver, c1.precedingStateCursor(direction), c2.tailCursor(direction));
            else
                throw new AssertionError();
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
            maybeAddDeletionsBranch(this.c1.depth());
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
        public int advance()
        {
            return maybeAddDeletionsBranch(super.advance());
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return maybeAddDeletionsBranch(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return maybeAddDeletionsBranch(super.advanceMultiple(receiver));
        }

        int maybeAddDeletionsBranch(int depth)
        {
            if (depth <= deletionBranchDepth)   // ascending above common deletions root
            {
                deletionBranchDepth = -1;
                assert !c1.hasDeletions();
                assert !c2.hasDeletions();
            }

            if (atC1 && atC2)
            {
                maybeAddDeletionsBranch(c1, c2);
                maybeAddDeletionsBranch(c2, c1);
            }   // otherwise even if there is deletion, the other cursor is ahead of it and can't be affected
            return depth;
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
            // Additionally, if `deletionsAtFixedPoints` is in force, we don't need to look for deletions below this
            // point when we already have applied tgt's deletions to src.
            if (deletionsAtFixedPoints && src.hasDeletions())
                return;

            RangeCursor<D> deletionsBranch = src.deletionBranchCursor(direction);
            if (deletionsBranch != null)
                tgt.addDeletions(deletionsBranch);  // apply all src deletions to tgt
        }


        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            int depth = depth();
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
                    b1 = new DeletionAwareCursor.DeletionsTrieCursor(c1.data.tailCursor(direction));
                if (b2 == null)
                    b2 = new DeletionAwareCursor.DeletionsTrieCursor(c2.data.tailCursor(direction));

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
