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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A merged view of multiple trie cursors.
///
/// This is accomplished by walking the cursors in parallel; the merged cursor takes the position and features of the
/// smallest and advances with it; when multiple cursors are equal, all of them are advanced. The ordered view of the
/// cursors is maintained using a custom binary min-heap, built for efficiently reforming the heap when the top elements
/// are advanced.
///
/// Crucial for the efficiency of this is the fact that when they are advanced like this, we can compare cursors'
/// positions by their `depth` descending and then `incomingTransition` ascending.
/// See [Trie.md](./Trie.md) for further details.
///
/// The merge cursor is a variation of the idea of a merge iterator with one key observation: because we advance
/// the source iterators together, we can compare them just by depth and incoming transition.
///
/// The most straightforward way to implement merging of iterators is to use a `PriorityQueue`,
/// `poll` it to find the next item to consume, then `add` the iterator back after advancing.
/// This is not very efficient as `poll` and `add` in all cases require at least
/// `log(size)` comparisons and swaps (usually more than `2*log(size)`) per consumed item, even
/// if the input is suitable for fast iteration.
///
/// The implementation below makes use of the fact that replacing the top element in a binary heap can be
/// done much more efficiently than separately removing it and placing it back, especially in the cases where
/// the top iterator is to be used again very soon (e.g. when there are large sections of the output where
/// only a limited number of input iterators overlap, which is normally the case in many practically useful
/// situations, e.g. levelled compaction).
///
/// The implementation builds and maintains a binary heap of sources (stored in an array), where we do not
/// add items after the initial construction. Instead we advance the smallest element (which is at the top
/// of the heap) and push it down to find its place for its new position. Should this source be exhausted,
/// we swap it with the last source in the heap and proceed by pushing that down in the heap.
///
/// In the case where we have multiple sources with matching positions, the merging algorithm
/// must be able to merge all equal values. To achieve this `content` walks the heap to
/// find all equal cursors without advancing them, and separately `advance` advances
/// all equal sources and restores the heap structure.
///
/// The latter is done equivalently to the process of initial construction of a min-heap using back-to-front
/// heapification as done in the classic heapsort algorithm. It only needs to heapify subheaps whose top item
/// is advanced (i.e. one whose position matches the current), and we can do that recursively from
/// bottom to top. Should a source be exhausted when advancing, it can be thrown away by swapping in the last
/// source in the heap (note: we must be careful to advance that source too if required).
///
/// To make it easier to advance efficienty in single-sourced branches of tries, we extract the current smallest
/// cursor (the head) separately, and start any advance with comparing that to the heap's first. When the smallest
/// cursor remains the same (e.g. in branches coming from a single source) this makes it possible to advance with
/// just one comparison instead of two at the expense of increasing the number by one in the general case.
///
/// Note: This is a simplification of the MergeIterator code from CASSANDRA-8915, without the leading ordered
/// section and equalParent flag since comparisons of cursor positions are cheap.
abstract class CollectionMergeCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    final Trie.CollectionMergeResolver<T> resolver;

    /// The smallest cursor, tracked separately to improve performance in single-source sections of the trie.
    C head;

    /// Binary heap of the remaining cursors. The smallest element is at position 0.
    /// Every element `i` is smaller than or equal to its two children, i.e.
    /// ```heap[i] <= heap[i*2 + 1] && heap[i] <= heap[i*2 + 2]```
    final C[] heap;

    /// A list used to collect contents during [#content()] calls.
    final List<T> contents;
    /// Whether content has already been collected for this position.
    boolean contentCollected;
    /// The collected content.
    T collectedContent;

    <I> CollectionMergeCursor(Trie.CollectionMergeResolver<T> resolver, Direction direction, Collection<I> inputs, IntFunction<C[]> cursorArrayConstructor, BiFunction<I, Direction, C> extractor)
    {
        this.resolver = resolver;
        int count = inputs.size();
        // Get cursors for all inputs. Put one of them in head and the rest in the heap.
        heap = cursorArrayConstructor.apply(count - 1);
        contents = new ArrayList<>(count);
        int i = -1;
        for (I src : inputs)
        {
            C cursor = extractor.apply(src, direction);
            cursor.assertFresh();
            if (i >= 0)
                heap[i] = cursor;
            else
                head = cursor;
            ++i;
        }

        // The cursors are all currently positioned on the root and thus in valid heap order.
    }

    /// Interface for internal operations that can be applied to selected top elements of the heap.
    interface HeapOp<T, C extends Cursor<T>>
    {
        void apply(CollectionMergeCursor<T, C> self, C cursor, int index);

        default boolean shouldContinueWithChild(C child, C head)
        {
            return equalCursor(child, head);
        }
    }

    /// Interface for non-interfering operations that can be applied to the source cursors.
    interface SourceOp<T, C extends Cursor<T>> extends HeapOp<T, C>
    {
        void apply(C cursor);

        default void apply(CollectionMergeCursor<T, C> self, C cursor, int index)
        {
            apply(cursor);
        }
    }

    /// Apply a non-interfering operation, i.e. one that does not change the cursor state, to the head and all inputs
    /// in the heap that satisfy the [SourceOp#shouldContinueWithChild] condition (by default, being equal to the head).
    /// For interfering operations like advancing the cursors, use [#advanceSelectedAndRestoreHeap(AdvancingHeapOp)].
    void applyToSelectedSources(SourceOp<T, C> action)
    {
        action.apply(head);
        applyToSelectedElementsInHeap(action, 0);
    }

    /// Apply a non-interfering operation, i.e. one that does not change the cursor state, to the head and all inputs
    /// in the heap.
    void applyToAllSources(SourceOp<T, C> action)
    {
        action.apply(head);
        for (int i = 0; i < heap.length; i++)
            action.apply(heap[i]);
    }

    /// Interface for internal advancing operations that can be applied to the heap cursors. This interface provides
    /// the code to restore the heap structure after advancing the cursors.
    interface AdvancingHeapOp<T, C extends Cursor<T>> extends HeapOp<T, C>
    {
        void apply(C cursor);

        default void apply(CollectionMergeCursor<T, C> self, C cursor, int index)
        {
            // Apply the operation, which should advance the position of the element.
            apply(cursor);

            // This method is called on the back path of the recursion. At this point the heaps at both children are
            // advanced and well-formed.
            // Place current node in its proper position.
            self.heapifyDown(cursor, index);
            // The heap rooted at index is now advanced and well-formed.
        }
    }


    /// Advance the state of all inputs in the heap that satisfy the [SourceOp#shouldContinueWithChild] condition
    /// (by default, being equal to the head) and restore the heap invariant.
    ///
    /// Note that this does not apply the operation to [#head].
    void advanceSelectedAndRestoreHeap(AdvancingHeapOp<T, C> action)
    {
        applyToSelectedElementsInHeap(action, 0);
    }

    /// Apply an operation to all elements on the heap that satisfy, recursively through the heap hierarchy, the
    /// `shouldContinueWithChild` condition (being equal to the head by default). Descends recursively in the
    /// heap structure to all selected children and applies the operation on the way back.
    ///
    /// This operation can be something that does not change the cursor state (see [#content]) or an operation
    /// that advances the cursor to a new state, wrapped in a [AdvancingHeapOp] ([#advance] or
    /// [#skipTo]). The latter interface takes care of pushing elements down in the heap after advancing
    /// and restores the subheap state on return from each level of the recursion.
    private void applyToSelectedElementsInHeap(HeapOp<T, C> action, int index)
    {
        if (index >= heap.length)
            return;
        C item = heap[index];
        if (!action.shouldContinueWithChild(item, head))
            return;

        // If the children are at the same position, they also need advancing and their subheap
        // invariant to be restored.
        applyToSelectedElementsInHeap(action, index * 2 + 1);
        applyToSelectedElementsInHeap(action, index * 2 + 2);

        // Apply the action. This is done on the reverse direction to give the action a chance to form proper
        // subheaps and combine them on processing the parent.
        action.apply(this, item, index);
    }

    /// Push the given state down in the heap from the given index until it finds its proper place among
    /// the subheap rooted at that position.
    private void heapifyDown(C item, int index)
    {
        while (true)
        {
            int next = index * 2 + 1;
            if (next >= heap.length)
                break;
            // Select the smaller of the two children to push down to.
            if (next + 1 < heap.length && greaterCursor(heap[next], heap[next + 1]))
                ++next;
            // If the child is greater or equal, the invariant has been restored.
            if (!greaterCursor(item, heap[next]))
                break;
            heap[index] = heap[next];
            index = next;
        }
        heap[index] = item;
    }

    /// Check if the head is greater than the top element in the heap, and if so, swap them and push down the new
    /// top until its proper place.
    ///
    /// @param headPosition the position of the head cursor (as returned by e.g. advance).
    /// @return the new head element's position
    private long maybeSwapHead(long headPosition)
    {
        long heap0Position = heap[0].encodedPosition();
        if (Cursor.compare(headPosition, heap0Position) <= 0)
            return headPosition;   // head is still smallest

        // otherwise we need to swap heap and heap[0]
        C newHeap0 = head;
        head = heap[0];
        heapifyDown(newHeap0, 0);
        return heap0Position;
    }

    boolean branchHasMultipleSources()
    {
        return equalCursor(heap[0], head);
    }

    boolean isExhausted()
    {
        return Cursor.isExhausted(head.encodedPosition());
    }

    @Override
    public long advance()
    {
        contentCollected = false;
        return doAdvance();
    }

    private long doAdvance()
    {
        advanceSelectedAndRestoreHeap(Cursor::advance);
        return maybeSwapHead(head.advance());
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        contentCollected = false;
        // If the current position is present in just one cursor, we can safely descend multiple levels within
        // its branch as no one of the other tries has content for it.
        if (branchHasMultipleSources())
            return doAdvance();   // More than one source at current position, do single-step advance.

        // If there are no children, i.e. the cursor ascends, we have to check if it's become larger than some
        // other candidate.
        return maybeSwapHead(head.advanceMultiple(receiver));
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        // We need to advance all cursors that stand before the requested position.
        // If a child cursor does not need to advance as it is greater than the skip position, neither of the ones
        // below it in the heap hierarchy do as they can't have an earlier position.
        class SkipTo implements AdvancingHeapOp<T, C>
        {
            @Override
            public boolean shouldContinueWithChild(C child, C head)
            {
                // When the requested position descends, the implicit prefix bytes are those of the head cursor,
                // and thus we need to check against that if it is a match.
                if (equalCursor(child, head))
                    return true;
                // Otherwise we can compare the child's position against a cursor advanced as requested, and need
                // to skip only if it would be before it.
                long childPosition = child.encodedPosition();
                return Cursor.compare(childPosition, encodedSkipPosition) < 0;
            }

            @Override
            public void apply(C cursor)
            {
                cursor.skipTo(encodedSkipPosition);
            }
        }

        contentCollected = false;
        applyToSelectedElementsInHeap(new SkipTo(), 0);
        return maybeSwapHead(head.skipTo(encodedSkipPosition));
    }

    @Override
    public long encodedPosition()
    {
        return head.encodedPosition();
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return head.byteComparableVersion();
    }

    T maybeCollectContent()
    {
        if (!contentCollected)
        {
            collectedContent = isExhausted() ? null : collectContent();
            contentCollected = true;
        }
        return collectedContent;
    }

    T collectContent()
    {
        applyToSelectedSources(this::collectContent);
        return resolveContent();
    }

    T resolveContent()
    {
        T toReturn;
        switch (contents.size())
        {
            case 0:
                toReturn = null;
                break;
            case 1:
                toReturn = contents.get(0);
                break;
            default:
                toReturn = resolver.resolve(contents);
                break;
        }
        contents.clear();
        return toReturn;
    }

    void collectContent(C item)
    {
        T itemContent = getContent(item);
        if (itemContent != null)
            contents.add(itemContent);
    }

    abstract T getContent(C item);

    /// Compare the positions of two cursors. One is before the other when
    /// - its depth is greater, or
    /// - its depth is equal, and the incoming transition is smaller.
    static <T> boolean greaterCursor(Cursor<T> c1, Cursor<T> c2)
    {
        return Cursor.compare(c1.encodedPosition(), c2.encodedPosition()) > 0;
    }

    static <T> boolean equalCursor(Cursor<T> c1, Cursor<T> c2)
    {
        return Cursor.compare(c1.encodedPosition(), c2.encodedPosition()) == 0;
    }

    static class Plain<T> extends CollectionMergeCursor<T, Cursor<T>> implements Cursor<T>
    {
        public <I> Plain(Trie.CollectionMergeResolver<T> resolver, Direction direction, Collection<I> inputs, BiFunction<I, Direction, Cursor<T>> extractor)
        {
            super(resolver, direction, inputs, Cursor[]::new, extractor);
        }

        @Override
        public T content()
        {
            return maybeCollectContent();
        }

        @Override
        T getContent(Cursor<T> item)
        {
            return item.content();
        }

        @Override
        public Cursor<T> tailCursor(Direction dir)
        {
            if (!branchHasMultipleSources())
                return head.tailCursor(dir);

            List<Cursor<T>> inputs = new ArrayList<>(heap.length + 1);
            applyToSelectedSources(inputs::add);

            return new Plain<>(resolver, dir, inputs, Cursor::tailCursor);
        }
    }

    static class Range<S extends RangeState<S>> extends CollectionMergeCursor<S, RangeCursor<S>> implements RangeCursor<S>
    {
        <I> Range(Trie.CollectionMergeResolver<S> resolver,
                  Direction direction,
                  Collection<I> inputs,
                  BiFunction<I, Direction, RangeCursor<S>> extractor)
        {
            super(resolver, direction, inputs, RangeCursor[]::new, extractor);
        }

        @Override
        public S state()
        {
            return maybeCollectContent();
        }

        @Override
        S collectContent()
        {
            // Unlike the parent method, we need to collect the state of all cursors on the heap
            // (state for equal cursors, and preceding state for the ones that have moved ahead).
            applyToAllSources(this::collectContent);
            return resolveContent();
        }

        @Override
        S getContent(RangeCursor<S> item)
        {
            return equalCursor(item, head) ? item.state() : item.precedingState();
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            List<RangeCursor<S>> inputs = new ArrayList<>(heap.length + 1);
            applyToAllSources(cursor ->
                             {
                                 if (equalCursor(head, cursor))
                                     inputs.add(cursor.tailCursor(direction));
                                 else if (cursor.precedingState() != null)
                                     inputs.add(cursor.precedingStateCursor(direction));
                             });

            if (inputs.size() == 1)
                return inputs.get(0);

            return new Range<>(resolver, direction, inputs, (x, dir) -> x);
        }
    }

    /// Collection merge cursor for deletion-aware tries.
    ///
    /// This cursor efficiently merges multiple deletion-aware tries by walking their cursors in parallel
    /// while properly handling both live data and deletion metadata. It extends the basic collection merge
    /// functionality with deletion-aware semantics, including proper deletion application and branch management.
    ///
    /// The implementation maintains a separate merge cursor for deletion branches (`relevantDeletions`) and
    /// coordinates between live data and deletions to ensure correct deletion application during iteration.
    static class DeletionAware<T, D extends RangeState<D>>
    extends CollectionMergeCursor<T, DeletionAwareCursor<T, D>> implements DeletionAwareCursor<T, D>
    {
        final BiFunction<D, T, T> deleter;
        final Trie.CollectionMergeResolver<D> deletionResolver;

        /// Store to avoid calling direction() repeatedly for deletion branch handling.
        Direction direction;

        /// Critical performance optimization flag. When true, guarantees that if one merge source
        /// has a deletion branch at some position, the other source cannot have deletion branches
        /// below or above that position. This allows us to skip walking the data trie to look for
        /// lower-level deletion branches when merging. If the flag is false, we cannot know where
        /// in the covered branch we may have a deletion, thus to be sure to find all we _must_
        /// walk the whole data subtrie. This can be terribly expensive.
        ///
        /// If we can guarantee that deletions always come at the same points in each path (e.g. at
        /// partition roots), we can use this optimization.
        final boolean deletionsAtFixedPoints;

        RangeCursor<D> relevantDeletions;
        int deletionBranchDepth = -1;

        enum DeletionState
        {
            NONE,
            MATCHING,
            AHEAD
        }
        DeletionState relevantDeletionsState = DeletionState.NONE;
        List<RangeCursor<D>> collectedDeletionBranches;
        List<DeletionAwareCursor<T, D>> sourcesWithNoDeletionBranch;

        /// Creates a deletion-aware collection merge cursor with configurable deletion optimization.
        ///
        /// @param liveResolver           resolver for merging live data content
        /// @param deletionResolver       resolver for merging deletion metadata
        /// @param deleter                function to apply deletions to live data
        /// @param deletionsAtFixedPoints optimization flag for deletion handling
        /// @param direction              iteration direction (forward or reverse)
        /// @param inputs                 collection of input sources to merge
        /// @param extractor              function to extract deletion-aware cursors from inputs
        <L> DeletionAware(Trie.CollectionMergeResolver<T> liveResolver,
                          Trie.CollectionMergeResolver<D> deletionResolver,
                          BiFunction<D, T, T> deleter,
                          boolean deletionsAtFixedPoints,
                          Direction direction,
                          Collection<L> inputs,
                          BiFunction<L, Direction, DeletionAwareCursor<T, D>> extractor)
        {
            super(liveResolver,
                  direction,
                  inputs,
                  DeletionAwareCursor[]::new,
                  extractor);
            this.direction = direction();

            // We will add deletion sources to the above as we find them.
            this.deletionResolver = deletionResolver;
            this.deleter = deleter;
            this.deletionsAtFixedPoints = deletionsAtFixedPoints;
            // Initialize deletion merger as null - we'll create it lazily when needed
            relevantDeletions = null;
            collectedDeletionBranches = new ArrayList<>(heap.length + 1);
            if (!deletionsAtFixedPoints)
                sourcesWithNoDeletionBranch = new ArrayList<>(heap.length + 1);
            else
                sourcesWithNoDeletionBranch = null;

            processRelevantDeletions(this.encodedPosition());
        }

        @Override
        public long advance()
        {
            return processRelevantDeletions(super.advance());
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            return processRelevantDeletions(super.skipTo(encodedSkipPosition));
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return (branchHasMultipleSources() || relevantDeletionsState == DeletionState.MATCHING)
                   ? advance()
                   : processRelevantDeletions(super.advanceMultiple(receiver));
        }

        /// Adjusts the deletion state based on the relative positions of deletion and content cursors.
        /// This determines how deletions should be applied to live data at the current position.
        void adjustDeletionState(long deletionPosition, long contentPosition)
        {
            if (Cursor.isExhausted(deletionPosition))
                relevantDeletionsState = DeletionState.NONE;
            else if (Cursor.compare(deletionPosition, contentPosition) > 0)
                relevantDeletionsState = DeletionState.AHEAD;
            else
                relevantDeletionsState = DeletionState.MATCHING;
        }

        /// Manages deletion branches during cursor advancement.
        /// This method coordinates between live data cursors and deletion cursors to ensure
        /// proper deletion application at each position.
        long processRelevantDeletions(long contentPosition)
        {
            if (deletionBranchDepth != -1)
            {
                if (Cursor.depth(contentPosition) > deletionBranchDepth)
                {
                    // We are still in the branch where the current relevantDeletions apply.
                    // Advance them to match the current state.
                    switch (relevantDeletionsState)
                    {
                        case MATCHING:
                        {
                            long deletionPosition = relevantDeletions.skipTo(contentPosition);
                            adjustDeletionState(deletionPosition, contentPosition);
                            break;
                        }
                        case AHEAD:
                        {
                            long deletionPosition = relevantDeletions.skipToWhenAhead(contentPosition);
                            adjustDeletionState(deletionPosition, contentPosition);
                            break;
                        }
                        // nothing to do for NONE (where relevantDeletions is exhausted, but we still haven't left its branch)
                    }
                    return contentPosition;
                }
                else
                {
                    // ascended above the common deletions root, we need to track and report deletion branches again.
                    deletionBranchDepth = -1;
                    relevantDeletions = null;
                    relevantDeletionsState = DeletionState.NONE;
                }
            }

            // If the branch is single-source, its deletions cannot affect the merge as they can't delete its own data.
            // (Note that covering deletions from other sources can still affect it though.)
            // Otherwise we need to get the deletions from all sources to track and apply them.
            if (branchHasMultipleSources())
            {
                RangeCursor<D> deletions = deletionsAtFixedPoints ? makeRelevantDeletionsFixedPoints()
                                                                  : makeRelevantDeletionsNoFixedPoints();
                if (deletions != null)
                {
                    deletionBranchDepth = Cursor.depth(contentPosition);
                    relevantDeletions = DepthAdjustedCursor.make(deletions, contentPosition);
                    relevantDeletionsState = DeletionState.MATCHING;
                }
            }

            return contentPosition;
        }

        private RangeCursor<D> makeRelevantDeletionsFixedPoints()
        {
            applyToSelectedSources(this::addDeletionTrieBranchFixedPoints);
            if (collectedDeletionBranches.isEmpty())
                return null;

            return cursorForCollectedDeletionBranches();
        }

        /// Adds a deletion trie branch for the given cursor. This version applies to the fixed point mode, where
        /// deletion branches must be presented at shared positions.
        void addDeletionTrieBranchFixedPoints(DeletionAwareCursor<T,D> cursor)
        {
            RangeCursor<D> deletionsBranch = cursor.deletionBranchCursor(direction);
            if (deletionsBranch != null)
                collectedDeletionBranches.add(deletionsBranch);
            // Otherwise there is no need to track the subtrie. If there are deletions, they must be presented here.
        }

        private RangeCursor<D> makeRelevantDeletionsNoFixedPoints()
        {
            applyToSelectedSources(this::addDeletionTrieBranchNoFixedPoints);
            if (collectedDeletionBranches.isEmpty())
            {
                sourcesWithNoDeletionBranch.clear();
                return null;
            }

            for (DeletionAwareCursor<T, D> cursor : sourcesWithNoDeletionBranch)
                collectedDeletionBranches.add(new DeletionsTrieCursor<>(cursor.tailCursor(direction)));
            sourcesWithNoDeletionBranch.clear();

            return cursorForCollectedDeletionBranches();
        }

        /// Adds a deletion trie branch for the given cursor. This means either the deletion branch that it presents,
        /// or, in the case where we accept non-aligned deletions, any deletion branch that may be present in its
        /// substructure.
        void addDeletionTrieBranchNoFixedPoints(DeletionAwareCursor<T,D> cursor)
        {
            RangeCursor<D> deletionsBranch = cursor.deletionBranchCursor(direction);
            if (deletionsBranch != null)
                collectedDeletionBranches.add(deletionsBranch);
            else
                sourcesWithNoDeletionBranch.add(cursor);
        }

        private RangeCursor<D> cursorForCollectedDeletionBranches()
        {
            RangeCursor<D> toReturn;
            if (collectedDeletionBranches.size() == 1)
                toReturn = collectedDeletionBranches.get(0);
            else
                toReturn = new Range<D>(deletionResolver, direction, collectedDeletionBranches, (c, d) -> c);
            collectedDeletionBranches.clear();
            return toReturn;
        }

        /// Resolves content by applying deletions to live data.
        /// This is the core method that implements deletion application during iteration.
        @Override
        T resolveContent()
        {
            T content = super.resolveContent();
            if (content == null)
                return null;

            D deletion;
            switch (relevantDeletionsState)
            {
                case MATCHING:
                    deletion = relevantDeletions.state();
                    break;
                case AHEAD:
                    deletion = relevantDeletions.precedingState();
                    break;
                default:
                    deletion = null;
            }
            if (deletion == null)
                return content;
            return deleter.apply(deletion, content);
        }

        /// Returns the deletion branch cursor for the current position.
        @Override
        public RangeCursor<D> deletionBranchCursor(Direction dir)
        {
            // If we aren't tracking relevant deletions yet, it may be because we are in a single-source branch.
            // If that is so, defer to that source's deletionBranchCursor.
            if (deletionBranchDepth == -1)
                return branchHasMultipleSources() ? null : head.deletionBranchCursor(dir);

            // Otherwise we are already tracking deletions. We only need to report them if they are introduced at this depth.
            if (deletionBranchDepth == Cursor.depth(encodedPosition()))
            {
                assert relevantDeletionsState == DeletionState.MATCHING;
                return relevantDeletions.tailCursor(dir);
            }

            return null;
        }

        @Override
        public T content()
        {
            return maybeCollectContent();
        }

        ///
        /// Gets content from a specific cursor (required by CollectionMergeCursor).
        ///
        @Override
        T getContent(DeletionAwareCursor<T, D> cursor)
        {
            return cursor.content();
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction dir)
        {
            if (deletionBranchDepth != -1 && Cursor.depth(encodedPosition()) > deletionBranchDepth)
            {
                // We are already inside the coverage of a deletion branch. In this case we don't report that branch,
                // but we make sure we apply its deletions to the data we report.
                RangeCursor<D> deletions = null;
                switch (relevantDeletionsState)
                {
                    case NONE:
                        break;
                    case MATCHING:
                        deletions = relevantDeletions.tailCursor(dir);
                        break;
                    case AHEAD:
                        deletions = relevantDeletions.precedingStateCursor(dir);
                        break;
                }

                if (deletions != null)
                {
                    // Because deletions branch is already active (and no new one can be introduced now), we treat the
                    // sources as plain tries.
                    Cursor<T> source;

                    if (!branchHasMultipleSources())
                        source = head.tailCursor(dir);
                    else
                    {
                        List<DeletionAwareCursor<T, D>> inputs = new ArrayList<>(heap.length + 1);
                        applyToSelectedSources(inputs::add);

                        source = new Plain<>(resolver, dir, inputs, DeletionAwareCursor::tailCursor);
                    }

                    return new RangeApplyCursor.DeletionAwareDataBranch<>(deleter, deletions, source);
                }
            }

            if (!branchHasMultipleSources())
                return head.tailCursor(dir);

            List<DeletionAwareCursor<T, D>> inputs = new ArrayList<>(heap.length + 1);
            applyToSelectedSources(inputs::add);

            return new DeletionAware<>(resolver, deletionResolver, deleter, deletionsAtFixedPoints, dir, inputs, DeletionAwareCursor::tailCursor);
        }
    }
}
