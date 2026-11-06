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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
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
        InMemoryBaseTrie<Object> empty = new InMemoryDeletionAwareTrie<>(ByteComparable.Version.OSS50, null, BufferType.ON_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryDeletionAwareTrie<>(ByteComparable.Version.OSS50, null, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    InMemoryDeletionAwareTrie(ByteComparable.Version byteComparableVersion, Predicate<T> shouldPreserveContentWithoutChildren, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, true, shouldPreserveContentWithoutChildren, bufferType, lifetime, opOrder);
    }

    InMemoryDeletionAwareTrie(ByteComparable.Version version, BufferManager bufferManager, ContentManager<T> contentManager)
    {
        super(version, true, bufferManager, contentManager);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, null, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    /// Create a short-lived on-heap in-memory deletion-aware trie, where content that has no children and fails the
    /// `shouldPreserveContentWithoutChildren` predicate is removed.
    /// This is used to clean up dangling metadata that has no meaning when its branch is empty.
    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion, Predicate<T> shouldPreserveContentWithoutChildren)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, shouldPreserveContentWithoutChildren, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, null, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, null, bufferType, ExpectedLifetime.LONG, opOrder);
    }

    /// Create a long-lived in-memory deletion-aware trie, where data is stored in trie cells using the given
    /// `contentSerializer`.
    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder, ContentSerializer<T> contentSerializer)
    {
        BufferManagerMultibuf bufferManager = new BufferManagerMultibuf(bufferType, ExpectedLifetime.LONG, opOrder);
        ContentManager<T> contentManager = new ContentManagerBytes<>(contentSerializer, bufferManager);
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, bufferManager, contentManager);
    }

    static class DeletionAwareInMemoryCursor<T, D extends RangeState<D>>
    extends InMemoryCursor<T> implements DeletionAwareCursor<T, D>
    {
        DeletionAwareInMemoryCursor(InMemoryBaseTrie<T> trie, Direction direction, int root)
        {
            super(trie, direction, root);
        }

        @Override
        public T content()
        {
            return content;
        }

        @Override
        T processPrefix(int node, int depth, int transition)
        {
            T content = super.processPrefix(node, depth, transition);
            if (trie.getIntVolatile(node + PREFIX_ALTERNATE_OFFSET) != NONE)
                currentPosition |= MAY_HAVE_DELETION_BRANCH_BIT;
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
                                                     currentFullNode);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RangeCursor<D> makeRangeCursor(Direction direction, int alternateBranch) {
        return isNull(alternateBranch)
                ? null
                : new InMemoryRangeTrie.InMemoryRangeCursor<>((InMemoryReadTrie) this, direction, alternateBranch);
    }

    //noinspection ClassEscapesDefinedScope
    @Override
    public DeletionAwareInMemoryCursor<T, D> makeCursor(Direction direction)
    {
        return new DeletionAwareInMemoryCursor<>(this, direction, root);
    }

    protected long emptySizeOnHeap()
    {
        return bufferManager.bufferType() == BufferType.ON_HEAP ? EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP;
    }

    @Override
    public RangeTrie<D> deletionBranchAtRoot()
    {
        int db = getAlternateBranch(root);
        if (isNull(db))
            return dir -> RangeCursor.empty(dir, byteComparableVersion);
        else
            return dir -> new InMemoryRangeTrie.InMemoryRangeCursor<>((InMemoryReadTrie<D>) this, dir, db);
    }

    @Override
    public D deletionAtRoot()
    {
        // Both calls handle NONE gracefully.
        return (D) getNodeContent(getAlternateBranch(root));
    }

    static class ApplyState<T, D extends RangeState<D>> extends InMemoryBaseTrie.ApplyState<T>
    {
        int alternateBranchToAttach;

        ApplyState(InMemoryDeletionAwareTrie<T, D> trie)
        {
            super(trie);
        }

        @Override
        ApplyState<T, D> start(int root, Consumer<? super T> droppedContentCallback)
        {
            alternateBranchToAttach = NONE;
            return (ApplyState<T, D>) super.start(root, droppedContentCallback);
        }

        public int alternateBranch()
        {
            return trie.getAlternateBranch(existingFullNode());
        }

        @Override
        protected int applyContent(boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            if (alternateBranchToAttach != NONE)
            {
                int alternateBranch = alternateBranchToAttach;
                alternateBranchToAttach = NONE;
                return applyContentWithAlternateBranch(alternateBranch, forcedCopy);
            }
            else
                return super.applyContent(forcedCopy);
        }

        /// Apply the collected content and alternate branch to a node, when it is known that the node contains an
        /// alternate branch. This will create or update a prefix node to reflect the new alternate branch pointer.
        int applyContentWithAlternateBranch(int alternateBranch, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            int contentId = descentPathContentId();
            final int updatedPostContentNode = updatedPostContentNode();
            final int existingPreContentNode = existingFullNode();
            final int existingPostContentNode = existingPostContentNode();
            if (isNull(updatedPostContentNode) && !isNull(contentId) && !trie.shouldPreserveWithoutChildren(contentId))
            {
                trie.releaseContent(contentId);
                contentId = NONE;
            }

            // applyPrefixChange does not understand leaf nodes, handle upgrade from one explicitly.
            if (isLeaf(existingPreContentNode))
                return trie.createPrefixNode(contentId, alternateBranch, updatedPostContentNode, true);

            return applyPrefixChange(updatedPostContentNode,
                                     existingPreContentNode,
                                     existingPostContentNode,
                                     contentId,
                                     alternateBranch,
                                     forcedCopy);
        }
    }

    /// Reused storage for the state of application of mutations. This stores the backtracking path, including changes
    /// already applied (e.g. new version of a node that is not yet linked to the current trie) and some that are yet
    /// to be applied (e.g. updated content).
    ///
    /// This state is used for the data part of the trie (i.e. excluding deletion branches). This includes machinery to
    /// attach deletion branches to nodes in the data trie.
    ///
    /// Because in-memory tries are single-writer, we can reuse a single state array for all updates. The updates are
    /// serialized and thus no other thread can corrupt this state (note that this is not the factor enforcing the
    /// single writer policy, and since we are already bound to it there is cost involved in reusing this state array).
    final private ApplyState<T, D> applyState = new ApplyState<>(this);

    /// Reused storage for the state of application of deletions (i.e. merging deletion branches into this trie).
    /// Mutations switch to using this state object (leaving the [#applyState] to store the state leading up to the
    /// deletion branch) whenever we switch to using [InMemoryRangeTrie] methods to apply deletions.
    ///
    /// This treats this data buffers as a range trie and uses an unchecked cast to treat the deletion branches as
    /// containing only deletion states of type `D`.
    @SuppressWarnings("unchecked")
    final InMemoryRangeTrie.ApplyState<D> deletionState = new InMemoryRangeTrie.ApplyState<>((InMemoryBaseTrie<D>) this);

    /// Deletion-aware trie mutator, binding the trie with a merge configuration (i.e. transformers and predicates).
    /// Can be used to apply multiple modifications to the trie using [#apply(DeletionAwareTrie)].
    public class Mutator<V, E extends RangeState<E>>
    extends InMemoryBaseTrie.Mutator<T, V, DeletionAwareMergeSource<V, E, D>, ApplyState<T, D>>
    {
        final BiFunction<D, V, V> insertedDeleter;
        final boolean deletionsAtFixedPoints;
        final InMemoryRangeTrie.MutatorStatic<D, E> deletionMutator;
        final InMemoryTrie.RangeMutator<T, E> deleter;

        /// See [InMemoryDeletionAwareTrie#mutator(UpsertTransformer, UpsertTransformer, UpsertTransformer, BiFunction, boolean, Predicate, Predicate)]
        /// for the meaning of the parameters.
        Mutator(UpsertTransformer<T, V> dataTransformer,
                UpsertTransformer<D, E> deletionTransformer,
                UpsertTransformer<T, E> existingDeleter,
                BiFunction<D, V, V> insertedDeleter,
                Predicate<NodeFeatures<V>> needsForcedCopyInData,
                Predicate<NodeFeatures<E>> needsForcedCopyInDeletionBranch,
                Consumer<? super T> droppedContentCallback,
                boolean deletionsAtFixedPoints)
        {
            super(dataTransformer, needsForcedCopyInData, droppedContentCallback, applyState);
            this.deletionMutator = new InMemoryRangeTrie.MutatorStatic<>(deletionState,
                                                                         deletionTransformer,
                                                                         needsForcedCopyInDeletionBranch,
                                                                         (Consumer<D>) droppedContentCallback);
            this.deleter = new InMemoryTrie.RangeMutator<>(applyState, existingDeleter, needsForcedCopyInDeletionBranch, droppedContentCallback);
            this.insertedDeleter = insertedDeleter;
            this.deletionsAtFixedPoints = deletionsAtFixedPoints;
        }

        Mutator<V, E> start(DeletionAwareCursor<V, E> mutationCursor)
        {
            super.start(new DeletionAwareMergeSource<>(insertedDeleter, mutationCursor));
            return this;
        }

        @Override
        Mutator<V, E> apply() throws TrieSpaceExhaustedException
        {
            int depth = state.currentDepth;
            long position = mutationCursor.encodedPosition();
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                // Content must be applied before descending into the branch to make sure we call the transformers
                // in the right order.
                applyContent(Cursor.content(mutationCursor, position));

                int existingAlternateBranch = state.alternateBranch();
                RangeCursor<E> incomingAlternateBranch = DeletionAwareCursor.deletionBranchCursor(mutationCursor, position);
                if (incomingAlternateBranch != null || existingAlternateBranch != NONE)
                {
                    int updatedAlternateBranch = applyDeletionBranch(existingAlternateBranch, incomingAlternateBranch);

                    // Ascend and apply alternate branch.
                    state.alternateBranchToAttach = updatedAlternateBranch;
                    if (state.currentDepth == 0)
                        break; // to be attached to root by complete()
                    state.attachAndMoveToParentState(forcedCopyDepth);
                    position = mutationCursor.encodedPosition();
                }
                else
                    position = mutationCursor.advance();

                assert !Cursor.isOnReturnPath(position) : "Return path in forward direction can only be used in range tries.";
                depth = Cursor.depth(position);
                if (!state.advanceTo(depth, Cursor.incomingTransition(position), forcedCopyDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
            return this;
        }

        private int applyDeletionBranch(int existingAlternateBranch, RangeCursor<E> incomingAlternateBranch) throws TrieSpaceExhaustedException
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
            return updatedAlternateBranch;
        }

        private void applyDataUnderDeletion(RangeCursor<D> ourDeletionBranch) throws TrieSpaceExhaustedException
        {
            // Add our deletion to DeletionAwareMergeSource to apply them to incoming data.
            if (ourDeletionBranch != null)
                mutationCursor.addDeletions(ourDeletionBranch);
            int initialDepth = state.currentDepth;

            // The first forcedCopyDepth and applyContent are already called.
            long position = mutationCursor.advance();
            int depth = Cursor.depth(position);
            assert !Cursor.isOnReturnPath(position) : "Return path in forward direction can only be used in range tries.";

            // Below is the same as the main loop in `apply`, slightly rearranged and ignoring deletion branches.
            while (state.advanceTo(depth, Cursor.incomingTransition(position), forcedCopyDepth, initialDepth))
            {
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";

                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                applyContent((position & Cursor.MAY_HAVE_CONTENT_BIT) != 0 ? mutationCursor.content() : null);
                position = mutationCursor.advance();
                depth = Cursor.depth(position);
            }
            assert state.currentDepth == initialDepth;
        }

        private void applyDeletions(RangeCursor<E> incomingAlternateBranch) throws TrieSpaceExhaustedException
        {
            // Apply the deletion branch to our data.
            deleter.continueFromCurrentState(incomingAlternateBranch, forcedCopyDepth).apply();

            // Make sure the data pass that follows walks the updated branch.
            state.prepareToWalkBranchAgain();
        }

        private int mergeDeletionBranch(int existingAlternateBranch, RangeCursor<E> deletionBranch) throws TrieSpaceExhaustedException
        {
            // If forced copying is in force, apply it to the deletion branch too.
            int deletionForcedCopyDepth = forcedCopyDepth <= state.currentDepth ? 0 : Integer.MAX_VALUE;
            deletionMutator.start(existingAlternateBranch, deletionBranch, deletionForcedCopyDepth).apply();

            return deletionMutator.completeBranch();
        }

        private int hoistOurDeletionBranches() throws TrieSpaceExhaustedException
        {
            // Walk all of our data branch and build new branches corresponding to it. When we reach a deletion
            // branch, link it. If a branch is walked without finding a deletion branch, the returned NONEs should
            // propagate up.
            // We need to walk both the deletion-aware/data trie, as well as the deletion branch being built, so that
            // the existing deletion branch mappings can be removed.
            // The process must be atomic to make sure readers can't lose a deletion branch that has already been added,
            // thus we force all modifications to `state` to be done with force-copying, which will also result in
            // copying the leading node if anything was modified, ensuring the original is still available regardless
            // how the caller sets their force-copy depth.
            InMemoryRangeTrie.ApplyState<D> deletionState = deletionMutator.state;
            deletionState.start(NONE);
            final int initialDepth = state.currentDepth;
            final int targetForcedCopyDepth = Integer.MAX_VALUE; // never force-copy in the new branch, these are completely new nodes
            final int sourceForcedCopyDepth = initialDepth; // always force-copy in the source so that hoisting can be atomic
            while (true)
            {
                int existingAlternateBranch = state.alternateBranch();
                if (existingAlternateBranch != NONE)
                {
                    deletionState.attachBranchAndMoveToParentState(existingAlternateBranch, targetForcedCopyDepth);
                    // Drop the existing alternate branch from the main state and ascend.
                    // The normal applyContent() method uses alternate branch value of NONE.
                    state.attachAndMoveToParentState(sourceForcedCopyDepth);
                }

                if (!state.advanceToNextExisting(sourceForcedCopyDepth, initialDepth))
                    break;
                int depth = state.currentDepth;
                deletionState.advanceTo(depth - initialDepth, state.incomingTransition(), targetForcedCopyDepth);
            }
            if (deletionState.currentDepth > 0)
                deletionState.advanceTo(-1, -1, targetForcedCopyDepth);

            // Make sure the walks over the data branch that follow use the updated branch.
            state.prepareToWalkBranchAgain();
            return deletionState.applyContent(false);
        }

        /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
        /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
        /// All of the deletions in the given mutation trie will be applied, removing any content and trie paths that become
        /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletion branches
        /// of the trie will be combined with the incoming deletions.
        ///
        /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
        /// different than the element type for this memtable trie.
        /// @see DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints
        public
        void apply(DeletionAwareTrie<V, E> mutation)
        throws TrieSpaceExhaustedException
        {
            // TODO: track hasDeletions and do plain Trie merges if neither this nor mutation has deletions.
            try
            {
                start(mutation.cursor(Direction.FORWARD)).apply().complete();
                completeMutation();
            }
            catch (Throwable t)
            {
                abortMutation();
                throw t;
            }
        }

        /// Modify this trie to apply the given deletion branch. This has the same effect as applying a deletion-aware
        /// trie that contains only the given range trie as a deletion branch at its root.
        /// All deletions in the given mutation trie will be applied, removing any content and trie paths that become
        /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletion
        /// branches of the trie will be combined with the incoming deletions, hoisting the deletion branch to the root
        /// of the trie if necessary.
        ///
        /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
        /// different than the element type for this memtable trie.
        public
        void delete(RangeTrie<E> mutation)
        throws TrieSpaceExhaustedException
        {
            DeletionAwareCursor<V, E> mutationCursor = new SingletonCursor.DeletionBranch<>(Direction.FORWARD,
                                                                                            ByteSource.EMPTY,
                                                                                            byteComparableVersion,
                                                                                            mutation);

            try
            {
                start(mutationCursor).apply().complete();
                completeMutation();
            }
            catch (Throwable t)
            {
                abortMutation();
                throw t;
            }
        }

        /// Get the bytes of the key in the deletion branch. This will not include the portion of the trie path leading
        /// to the deletion branch.
        ///
        /// This method may be called by `deletionTransformer` to get information about the current state.
        public byte[] getDeletionBranchKeyBytes()
        {
            return deletionMutator.getCurrentKeyBytes();
        }

        /// Get the bytes of the key in the deletion branch from the given depth, which must be obtained using
        /// [#getDeletionBranchDepth()]. The returned array can be safely modified and/or stored.
        ///
        /// This method may be called by `deletionTransformer` to get information about the current state.
        public byte[] getDeletionBranchKeyBytes(int startDepth)
        {
            return deletionMutator.getCurrentKeyBytes(startDepth);
        }

        /// Return the depth of the currently processed node in the deletion branch.
        ///
        /// This method may be called by the upsert transformer to get information about the current state.
        public int getDeletionBranchDepth()
        {
            return deletionMutator.currentDepth();
        }
    }

    /// Creates a trie mutator that can be used to apply multiple modifications to the trie.
    ///
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
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See
    /// [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints].
    /// @param needsForcedCopyInData a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in data branches. See [NodeFeatures] for details.
    /// @param needsForcedCopyInDeletions a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in deletion branches. See [NodeFeatures] for details.
    /// @param droppedContentCallback function to call when childless content is dropped because
    /// [ContentManager#shouldPreserveWithoutChildren] returned false.
    public <V, E extends RangeState<E>>
    Mutator<V, E> mutator(final UpsertTransformer<T, V> dataTransformer,
                          final UpsertTransformer<D, E> deletionTransformer,
                          final UpsertTransformer<T, E> existingDeleter,
                          final BiFunction<D, V, V> insertedDeleter,
                          boolean deletionsAtFixedPoints,
                          Predicate<NodeFeatures<V>> needsForcedCopyInData,
                          Predicate<NodeFeatures<E>> needsForcedCopyInDeletions,
                          Consumer<? super T> droppedContentCallback)
    {
        return new Mutator<>(dataTransformer,
                             deletionTransformer,
                             existingDeleter,
                             insertedDeleter,
                             needsForcedCopyInData,
                             needsForcedCopyInDeletions,
                             droppedContentCallback,
                             deletionsAtFixedPoints);
    }


    /// Creates a trie mutator that can be used to apply multiple modifications to the trie.
    ///
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
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See
    /// [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints].
    /// @param needsForcedCopyInData a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in data branches. See [NodeFeatures] for details.
    /// @param needsForcedCopyInDeletions a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in deletion branches. See [NodeFeatures] for details.
    public <V, E extends RangeState<E>>
    Mutator<V, E> mutator(final UpsertTransformer<T, V> dataTransformer,
                          final UpsertTransformer<D, E> deletionTransformer,
                          final UpsertTransformer<T, E> existingDeleter,
                          final BiFunction<D, V, V> insertedDeleter,
                          boolean deletionsAtFixedPoints,
                          Predicate<NodeFeatures<V>> needsForcedCopyInData,
                          Predicate<NodeFeatures<E>> needsForcedCopyInDeletions)
    {
        return new Mutator<>(dataTransformer,
                             deletionTransformer,
                             existingDeleter,
                             insertedDeleter,
                             needsForcedCopyInData,
                             needsForcedCopyInDeletions,
                             null,
                             deletionsAtFixedPoints);
    }


    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// All of the deletions in the given mutation trie will be applied, removing any content and trie paths that become
    /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletion branches
    /// of the trie will be combined with the incoming deletions.
    ///
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
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See
    /// [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints].
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in both data and deletion branches. See [NodeFeatures] for details.
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <V, E extends RangeState<E>>
    Mutator<V, E> mutator(final UpsertTransformer<T, V> dataTransformer,
                          final UpsertTransformer<D, E> deletionTransformer,
                          final UpsertTransformer<T, E> existingDeleter,
                          final BiFunction<D, V, V> insertedDeleter,
                          boolean deletionsAtFixedPoints,
                          Predicate<NodeFeatures<?>> needsForcedCopy)
    {
        return mutator(dataTransformer,
                       deletionTransformer,
                       existingDeleter,
                       insertedDeleter,
                       deletionsAtFixedPoints,
                       (Predicate) needsForcedCopy,
                       (Predicate) needsForcedCopy);
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
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See
    /// [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints].
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in both data and deletion branches. See [NodeFeatures] for details.
    public <V, E extends RangeState<E>>
    void apply(DeletionAwareTrie<V, E> mutation,
               final UpsertTransformer<T, V> dataTransformer,
               final UpsertTransformer<D, E> deletionTransformer,
               final UpsertTransformer<T, E> existingDeleter,
               final BiFunction<D, V, V> insertedDeleter,
               boolean deletionsAtFixedPoints,
               Predicate<NodeFeatures<?>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        // TODO: track hasDeletions and do plain Trie merges if neither this nor mutation has deletions.
        mutator(dataTransformer,
                deletionTransformer,
                existingDeleter,
                insertedDeleter,
                deletionsAtFixedPoints,
                needsForcedCopy)
        .apply(mutation);
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
        return new DumpCursor(new DeletionAwareInMemoryCursor<>(this, Direction.FORWARD, branchRoot), Object::toString)
               .process(new TrieDumper.DeletionAware<>(Function.identity(), Object::toString));
    }
}
