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
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

/// In-memory implementation of deletion-aware tries with concurrent access support.
///
/// This class provides a concrete implementation of [DeletionAwareTrie] that stores both live data
/// and deletion information in a unified in-memory structure. It extends [InMemoryBaseTrie] to
/// inherit the efficient cell-based memory management, concurrent access patterns, and performance
/// optimizations of the base trie implementation.
///
/// This class stores deletion branches in the "alternate branch" field of prefix nodes. All the
/// machinery to support this is already provided by [InMemoryBaseTrie]. This class implements the
/// relevant cursor and mutation methods.
///
/// See [InMemoryTrie] for information on the consistency model.
///
/// @param <T> The content type for live data stored in the trie
/// @param <D> The deletion marker type, must extend [RangeState] for range operations
public class InMemoryDeletionAwareTrie<T, D extends RangeState<D>>
extends InMemoryBaseTrie<T> implements DeletionAwareTrie<T, D>
{
    // constants for space calculations
    private static final long EMPTY_SIZE_ON_HEAP;
    private static final long EMPTY_SIZE_OFF_HEAP;
    static
    {
        // Measuring the empty size of long-lived tries, because these are the ones for which we want to track size.
        InMemoryBaseTrie<Object> empty = new InMemoryDeletionAwareTrie<>(ByteComparable.Version.OSS50, BufferType.ON_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryDeletionAwareTrie<>(ByteComparable.Version.OSS50, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    InMemoryDeletionAwareTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, bufferType, lifetime, opOrder);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder);
    }

    static class DeletionAwareInMemoryCursor<T, D extends RangeState<D>>
    extends InMemoryCursor<T> implements DeletionAwareCursor<T, D>
    {
        DeletionAwareInMemoryCursor(InMemoryDeletionAwareTrie<T, D> trie, Direction direction, int root, int depth, int incomingTransition)
        {
            super(trie, direction, root, depth, incomingTransition);
        }

        @Override
        public T content()
        {
            return content;
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            int alternateBranch = trie.getAlternateBranch(currentFullNode);
            return ((InMemoryDeletionAwareTrie<T, D>) trie).makeRangeCursor(direction, alternateBranch);
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new DeletionAwareInMemoryCursor<>((InMemoryDeletionAwareTrie<T, D>) trie,
                                                     direction,
                                                     currentFullNode,
                                                     0,
                                                     -1);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RangeCursor<D> makeRangeCursor(Direction direction, int alternateBranch) {
        return isNull(alternateBranch)
                ? null
                : new InMemoryRangeTrie.InMemoryRangeCursor<>((InMemoryReadTrie) this, direction, alternateBranch, 0, -1);
    }

    //noinspection ClassEscapesDefinedScope
    @Override
    public DeletionAwareInMemoryCursor<T, D> makeCursor(Direction direction)
    {
        return new DeletionAwareInMemoryCursor<>(this, direction, root, 0, -1);
    }

    protected long emptySizeOnHeap()
    {
        return bufferType == BufferType.ON_HEAP ? EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP;
    }

    /// Reused storage for the state of application of deletions (i.e. merging deletion branches into this trie).
    /// Mutations switch to using this state object (leaving the [#applyState] to store the state leading up to the
    /// deletion branch) whenever we switch to using [InMemoryRangeTrie] methods to apply deletions.
    ///
    /// See [#applyState] for additional information about the state arrays.
    @SuppressWarnings("unchecked")
    InMemoryTrie<D>.ApplyState deletionState = (InMemoryTrie<D>.ApplyState) new ApplyState();

    static class Mutation<T, D extends RangeState<D>, V, E extends RangeState<E>>
    extends InMemoryBaseTrie.Mutation<T, V, DeletionAwareMergeSource<V, E, D>>
    {
        final UpsertTransformerWithKeyProducer<D, E> deletionTransformer;
        final UpsertTransformerWithKeyProducer<T, E> deleter;
        final boolean deletionsAtFixedPoints;
        final InMemoryTrie<D>.ApplyState deletionState;

        Mutation(UpsertTransformerWithKeyProducer<T, V> dataTransformer,
                 UpsertTransformerWithKeyProducer<D, E> deletionTransformer,
                 UpsertTransformerWithKeyProducer<T, E> existingDeleter,
                 BiFunction<D, V, V> insertedDeleter,
                 Predicate<NodeFeatures<V>> needsForcedCopy,
                 boolean deletionsAtFixedPoints,
                 DeletionAwareCursor<V, E> mutationCursor,
                 InMemoryBaseTrie<T>.ApplyState state,
                 InMemoryBaseTrie<D>.ApplyState deletionState)
        {
            super(dataTransformer, needsForcedCopy, new DeletionAwareMergeSource<>(insertedDeleter, mutationCursor), state);
            this.deletionTransformer = deletionTransformer;
            this.deleter = existingDeleter;
            this.deletionsAtFixedPoints = deletionsAtFixedPoints;
            this.deletionState = deletionState;
        }

        @Override
        void apply() throws TrieSpaceExhaustedException
        {
            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                // Content must be applied before descending into the branch to make sure we call the transformers
                // in the right order.
                applyContent();

                int existingAlternateBranch = state.alternateBranch();
                RangeCursor<E> incomingAlternateBranch = mutationCursor.deletionBranchCursor(Direction.FORWARD);
                if (incomingAlternateBranch != null || existingAlternateBranch != NONE)
                {
                    int updatedAlternateBranch = existingAlternateBranch;
                    RangeCursor<D> ourDeletionBranch;
                    if (!deletionsAtFixedPoints && existingAlternateBranch == NONE && state.existingFullNode() != NONE)
                    {
                        // Move any covered deletion branches up to this depth so that we can correctly merge the
                        // incoming deletions.
                        updatedAlternateBranch = hoistOurDeletionBranches();
                    }
                    ourDeletionBranch = ((InMemoryDeletionAwareTrie<T, D>) state.trie()).makeRangeCursor(Direction.FORWARD, updatedAlternateBranch);

                    if (!deletionsAtFixedPoints && incomingAlternateBranch == null)
                    {
                        // The incoming cursor has no deletions here, but it may have some below this point.
                        // Switch to deletion branch to transform them to be rooted here.
                        // (Note: this will cause a lot of processing of unproductive branches.)
                        incomingAlternateBranch = new DeletionAwareCursor.DeletionsTrieCursor<>(mutationCursor.tailCursor(Direction.FORWARD));
                    }

                    if (incomingAlternateBranch != null)
                    {
                        // Duplicate cursor as we need it for both deletion and data branches.
                        RangeCursor<E> deletionBranch = incomingAlternateBranch.tailCursor(Direction.FORWARD);

                        // Delete data that is covered by the new deletions.
                        applyDeletions(incomingAlternateBranch);

                        // Merge the deletions into our deletion branch.
                        updatedAlternateBranch = mergeDeletionBranch(updatedAlternateBranch, deletionBranch);
                    }

                    // Continue processing to also insert the incoming data at this branch.
                    // Note that this will also apply the incoming content to this node and advance the mutation cursor
                    // to the position after this branch.
                    applyDataUnderDeletion(ourDeletionBranch);

                    // Ascend and apply alternate branch.
                    state.alternateBranchToAttach = updatedAlternateBranch;
                    if (state.currentDepth == 0)
                        break; // to be attached to root by complete()
                    state.attachAndMoveToParentState(forcedCopyDepth);
                    depth = mutationCursor.depth();
                }
                else
                    depth = mutationCursor.advance();

                if (!state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
        }

        private void applyDataUnderDeletion(RangeCursor<D> ourDeletionBranch) throws TrieSpaceExhaustedException
        {
            // Add our deletion to DeletionAwareMergeSource to apply them to incoming data.
            if (ourDeletionBranch != null)
                mutationCursor.addDeletions(ourDeletionBranch);
            int initialDepth = state.currentDepth;

            // The first forcedCopyDepth and applyContent are already called.
            int depth = mutationCursor.advance();

            // Below is the same as the main loop in `apply`, slightly rearranged and ignoring deletion branches.
            while (state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth, initialDepth))
            {
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";

                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                applyContent();
                depth = mutationCursor.advance();
            }
            assert state.currentDepth == initialDepth;
        }

        private void applyDeletions(RangeCursor<E> incomingAlternateBranch) throws TrieSpaceExhaustedException
        {
            // Apply the deletion branch to our data.
            @SuppressWarnings({"unchecked", "rawtypes"})
            InMemoryTrie.RangeMutation<T, E, RangeCursor<E>> deleteMutation = new InMemoryTrie.RangeMutation<>(
                    deleter,
                    (Predicate<NodeFeatures<E>>) (Predicate) needsForcedCopy,
                    incomingAlternateBranch,
                    state,
                    forcedCopyDepth);
            deleteMutation.apply();

            // Make sure the data pass that follows walks the updated branch.
            state.prepareToWalkBranchAgain();
        }

        private int mergeDeletionBranch(int existingAlternateBranch, RangeCursor<E> deletionBranch) throws TrieSpaceExhaustedException
        {
            // If forced copying is in force, apply it to the deletion branch too.
            int deletionForcedCopyDepth = forcedCopyDepth <= state.currentDepth ? 0 : Integer.MAX_VALUE;

            @SuppressWarnings({"unchecked", "rawtypes"})
            InMemoryRangeTrie.Mutation<D, E> rangeMutation = new InMemoryRangeTrie.Mutation<>(
                    deletionTransformer,
                    (Predicate<NodeFeatures<E>>) (Predicate) needsForcedCopy,
                    deletionBranch,
                    deletionState.start(existingAlternateBranch),
                    deletionForcedCopyDepth);
            rangeMutation.apply();
            return deletionState.completeBranch(deletionForcedCopyDepth);
        }

        private int hoistOurDeletionBranches() throws TrieSpaceExhaustedException
        {
            // Walk all of our data branch and build new branches corresponding to it. When we reach a deletion
            // branch, link it. If a branch is walked without finding a deletion branch, the returned NONEs should
            // propagate up.
            // We need to walk both the deletion-aware/data trie, as well as the deletion branch being built, so that
            // the existing deletion branch mappings can be removed.
            deletionState.start(NONE);
            int initialDepth = state.currentDepth;

            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                int existingAlternateBranch = state.alternateBranch();
                if (existingAlternateBranch != NONE)
                {
                    deletionState.attachBranchAndMoveToParentState(existingAlternateBranch, forcedCopyDepth);
                    // Drop the existing alternate branch from the main state and ascend.
                    // The normal applyContent() method uses alternate branch value of NONE.
                    state.attachAndMoveToParentState(forcedCopyDepth);
                }

                if (!state.advanceToNextExisting(forcedCopyDepth, initialDepth))
                    break;
                depth = state.currentDepth;
                deletionState.advanceTo(depth - initialDepth, state.incomingTransition(), forcedCopyDepth - initialDepth);
            }
            if (deletionState.currentDepth > 0)
                deletionState.advanceTo(-1, -1, forcedCopyDepth - initialDepth);

            // Make sure the walks over the data branch that follow use the updated branch.
            state.prepareToWalkBranchAgain();
            return deletionState.completeBranch(forcedCopyDepth - initialDepth);
        }
    }


    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// All of the deletions in the given mutation trie will be applied, removing any content and trie paths that become
    /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletion branches
    /// of the trie will be combined with the incoming deletions.
    ///
    /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
    /// different than the element type for this memtable trie.
    /// @param dataTransformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie. The transformer can return null
    /// if the entry should not be added or preserved.
    /// @param deletionTransformer a function applied to combine overlapping deletions into a consistent view. Called
    /// even if there is no pre-existing deletion to convert the marker type. The transformer can return null if
    /// deletions cancel out or should not be preserved.
    /// **Note: for code simplicity this transformer is provided only the path to the root of the deletion branch.**
    /// @param existingDeleter a function used to apply a deletion marker to potentially delete live data. This is
    /// only called if there is both content and deletion at a given covered point. It should return null if the entry
    /// is to be deleted.
    /// @param insertedDeleter a function used to filter incoming entries that are covered by existing deletions
    /// in this trie, called only if both an entry and a deletion apply to a given point. This function is not provided
    /// with a path to the modified data.
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions.
    /// @see DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints
    public <V, E extends RangeState<E>>
    void apply(DeletionAwareTrie<V, E> mutation,
               final UpsertTransformerWithKeyProducer<T, V> dataTransformer,
               final UpsertTransformerWithKeyProducer<D, E> deletionTransformer,
               final UpsertTransformerWithKeyProducer<T, E> existingDeleter,
               final BiFunction<D, V, V> insertedDeleter,
               boolean deletionsAtFixedPoints,
               Predicate<NodeFeatures<V>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        // TODO: track hasDeletions and do plain Trie merges if neither this nor mutation has deletions.
        try
        {
            Mutation<T, D, V, E> m = new Mutation<>(dataTransformer,
                    deletionTransformer,
                    existingDeleter,
                    insertedDeleter,
                    needsForcedCopy,
                    deletionsAtFixedPoints,
                    mutation.cursor(Direction.FORWARD),
                    applyState.start(),
                    deletionState);
            m.apply();
            m.complete();
            completeMutation();
        }
        catch (Throwable t)
        {
            abortMutation();
            throw t;
        }
    }

    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// All of the deletions in the given mutation trie will be applied, removing any content and trie paths that become
    /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletion branches
    /// of the trie will be combined with the incoming deletions.
    ///
    /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
    /// different than the element type for this memtable trie.
    /// @param dataTransformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie. The transformer can return null
    /// if the entry should not be added or preserved.
    /// @param deletionTransformer a function applied to combine overlapping deletions into a consistent view. Called
    /// even if there is no pre-existing deletion to convert the marker type. The transformer can return null if
    /// deletions cancel out or should not be preserved.
    /// @param existingDeleter a function used to apply a deletion marker to potentially delete live data. This is
    /// only called if there is both content and deletion at a given covered point. It should return null if the entry
    /// is to be deleted.
    /// @param insertedDeleter a function used to filter incoming entries that are covered by existing deletions
    /// in this trie, called only if both an entry and a deletion apply to a given point. This function is not provided
    /// with a path to the modified data.
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions.
    /// @see DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints
    public <V, E extends RangeState<E>>
    void apply(DeletionAwareTrie<V, E> mutation,
               final UpsertTransformer<T, V> dataTransformer,
               final UpsertTransformer<D, E> deletionTransformer,
               final UpsertTransformer<T, E> existingDeleter,
               final BiFunction<D, V, V> insertedDeleter,
               boolean deletionsAtFixedPoints,
               Predicate<NodeFeatures<V>> needsForcedCopy)
            throws TrieSpaceExhaustedException
    {
        apply(mutation,
              (UpsertTransformerWithKeyProducer<T, V>) dataTransformer,
              (UpsertTransformerWithKeyProducer<D, E>) deletionTransformer,
              (UpsertTransformerWithKeyProducer<T, E>) existingDeleter,
              insertedDeleter, deletionsAtFixedPoints, needsForcedCopy);
    }

    private class DumpCursor extends InMemoryReadTrie<T>.DumpCursor<DeletionAwareInMemoryCursor<T, D>> implements DeletionAwareCursor<String, D>
    {
        DumpCursor(DeletionAwareInMemoryCursor<T, D> source, Function<T, String> contentToString)
        {
            super(source, contentToString);
        }


        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return source.deletionBranchCursor(direction);
        }

        @Override
        public DumpCursor tailCursor(Direction direction)
        {
            // `DumpCursor` is only created by the dump method to be used immediately, and that use should never call
            // `tailCursor`.
            throw new AssertionError();
        }
    }

    public String dump(Function<T, String> contentToString)
    {
        return dump(contentToString, Object::toString);
    }

    /// Override of dump to provide more detailed printout that includes the type of each node in the trie.
    /// We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
    public String dump(Function<T, String> contentToString, Function<D, String> rangeToString)
    {
        return new DumpCursor(makeCursor(Direction.FORWARD), contentToString).process(new TrieDumper.DeletionAware<>(Function.identity(), rangeToString));
    }

    /// Dump the branch rooted at the given node. To be used for debugging only.
    @SuppressWarnings("unused")
    private String dumpBranch(int branchRoot)
    {
        return new DumpCursor(new DeletionAwareInMemoryCursor<>(this, Direction.FORWARD, branchRoot, 0, -1), Object::toString)
               .process(new TrieDumper.DeletionAware<>(Function.identity(), Object::toString));
    }
}
