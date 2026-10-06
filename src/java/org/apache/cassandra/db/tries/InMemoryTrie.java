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

import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.base.Predicates;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

/// In-memory trie built for fast modification and reads executing concurrently with writes from a single mutator thread.
///
/// The main method for performing writes is [#apply(Trie,UpsertTransformer,Predicate)] which takes a trie as
/// an argument and merges it into the current trie using the methods supplied by the given [UpsertTransformer],
/// force copying anything below the points where the third argument returns true.
///
///
/// The predicate can be used to implement several forms of atomicity and consistency guarantees:
///   -  if the predicate is `nf -> false`, neither atomicity nor sequential consistency is guaranteed - readers
///     can see any mixture of old and modified content
///   -  if the predicate is `nf -> true`, full sequential consistency will be provided, i.e. if a reader sees any
///     part of a modification, it will see all of it, and all the results of all previous modifications
///   -  if the predicate is `nf -> nf.isBranching()` the write will be atomic, i.e. either none or all of the
///     content of the merged trie will be visible by concurrent readers, but not sequentially consistent, i.e. there
///     may be writes that are not visible to a reader even when they precede writes that are visible.
///   -  if the predicate is `nf -> <some_test>(nf.content())` the write will be consistent below the identified
///     point (used e.g. by Memtable to ensure partition-level consistency)
///
///
///     Additionally, the class provides several simpler write methods for efficiency and convenience:
///   -  [#putRecursive(ByteComparable,Object,UpsertTransformer)] inserts a single value using a recursive walk.
///     It cannot provide consistency (single-path writes are always atomic). This is more efficient as it stores the
///     walk state in the stack rather than on the heap but can cause a `StackOverflowException`.
///   -  [#putSingleton(ByteComparable,Object,UpsertTransformer)] is a non-recursive version of the above, using
///     the `apply` machinery.
///   -  [#putSingleton(ByteComparable,Object,UpsertTransformer,boolean)] uses the fourth argument to choose
///     between the two methods above, where some external property can be used to decide if the keys are short enough
///     to permit recursive execution.
///
///     Because it uses 32-bit pointers in byte buffers, this trie has a fixed size limit of 2GB.
public class InMemoryTrie<T> extends InMemoryBaseTrie<T> implements Trie<T>
{
    // constants for space calculations
    private static final long EMPTY_SIZE_ON_HEAP;
    private static final long EMPTY_SIZE_OFF_HEAP;
    static
    {
        // Measuring the empty size of long-lived tries, because these are the ones for which we want to track size.
        InMemoryBaseTrie<Object> empty = new InMemoryTrie<>(ByteComparable.Version.OSS50, BufferType.ON_HEAP, ExpectedLifetime.LONG, null, true);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryTrie<>(ByteComparable.Version.OSS50, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null, true);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    InMemoryTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder, boolean presentContentOnDescentPath)
    {
        super(byteComparableVersion, presentContentOnDescentPath, bufferType, lifetime, opOrder);
    }

    InMemoryTrie(ByteComparable.Version byteComparableVersion,
                 BufferType bufferType,
                 ExpectedLifetime lifetime,
                 OpOrder opOrder,
                 boolean presentContentOnDescentPath,
                 Predicate<T> shouldPreserveWithoutChildren)
    {
        super(byteComparableVersion, presentContentOnDescentPath, shouldPreserveWithoutChildren, bufferType, lifetime, opOrder);
    }

    InMemoryTrie(ByteComparable.Version byteComparableVersion, boolean presentContentOnDescentPath, BufferManager bufferManager, ContentManager<T> contentManager)
    {
        super(byteComparableVersion, presentContentOnDescentPath, bufferManager, contentManager);
    }

    /// Short-lived tries do not try to recycle and reuse cells and content slots of the trie that are no longer in use
    /// and are expected to be recycled as a whole after the user is done with them.
    public static <T> InMemoryTrie<T> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return shortLived(byteComparableVersion, BufferType.ON_HEAP);
    }

    /// Short-lived tries do not try to recycle and reuse cells and content slots of the trie that are no longer in use
    /// and are expected to be recycled as a whole after the user is done with them.
    public static <T> InMemoryTrie<T> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null, true);
    }

    /// Long-lived tries are expected to stay around for a long time and will try to minimize the space wasted to data
    /// or structure that is no longer referenced. To do this they need a signal that lets them know if all readers
    /// started before a given point in time have completed work, given by the `opOrder` parameter.
    public static <T> InMemoryTrie<T> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    /// Long-lived tries are expected to stay around for a long time and will try to minimize the space wasted to data
    /// or structure that is no longer referenced. To do this they need a signal that lets them know if all readers
    /// started before a given point in time have completed work, given by the `opOrder` parameter.
    public static <T> InMemoryTrie<T> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder, true);
    }

    /// Long-lived tries are expected to stay around for a long time and will try to minimize the space wasted to data
    /// or structure that is no longer referenced. To do this they need a signal that lets them know if all readers
    /// started before a given point in time have completed work, given by the `opOrder` parameter.
    public static <T> InMemoryTrie<T> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder, ContentSerializer<T> contentSerializer)
    {
        BufferManagerMultibuf bufferManager = new BufferManagerMultibuf(bufferType, ExpectedLifetime.LONG, opOrder);
        ContentManager<T> contentManager = new ContentManagerBytes<>(contentSerializer, bufferManager);
        return new InMemoryTrie<>(byteComparableVersion, true, bufferManager, contentManager);
    }

    /// Creates a short-lived "ordered" in-memory trie, i.e. where reverse iteration presents content on the ascent
    /// path so that it can be correctly lexicographically ordered with any keys for which it is a prefix.
    ///
    /// Short-lived tries do not try to recycle and reuse cells and content slots of the trie that are no longer in use
    /// and are expected to be recycled as a whole after the user is done with them.
    public static <T> InMemoryTrie<T> shortLivedOrdered(ByteComparable.Version byteComparableVersion)
    {
        return shortLivedOrdered(byteComparableVersion, BufferType.ON_HEAP);
    }

    /// Creates a short-lived "ordered" in-memory trie, i.e. where reverse iteration presents content on the ascent
    /// path so that it can be correctly lexicographically ordered with any keys for which it is a prefix.
    ///
    /// Short-lived tries do not try to recycle and reuse cells and content slots of the trie that are no longer in use
    /// and are expected to be recycled as a whole after the user is done with them.
    public static <T> InMemoryTrie<T> shortLivedOrdered(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null, false);
    }

    /// Creates a long-lived "ordered" in-memory trie, i.e. where reverse iteration presents content on the ascent
    /// path so that it can be correctly lexicographically ordered with any keys for which it is a prefix.
    ///
    /// Long-lived tries are expected to stay around for a long time and will try to minimize the space wasted to data
    /// or structure that is no longer referenced. To do this they need a signal that lets them know if all readers
    /// started before a given point in time have completed work, given by the `opOrder` parameter.
    public static <T> InMemoryTrie<T> longLivedOrdered(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLivedOrdered(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    /// Creates a long-lived "ordered" in-memory trie, i.e. where reverse iteration presents content on the ascent
    /// path so that it can be correctly lexicographically ordered with any keys for which it is a prefix.
    ///
    /// Long-lived tries are expected to stay around for a long time and will try to minimize the space wasted to data
    /// or structure that is no longer referenced. To do this they need a signal that lets them know if all readers
    /// started before a given point in time have completed work, given by the `opOrder` parameter.
    public static <T> InMemoryTrie<T> longLivedOrdered(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder, false);
    }

    public InMemoryCursor<T> makeCursor(Direction direction)
    {
        return new InMemoryCursor<>(this, direction, root);
    }

    protected long emptySizeOnHeap()
    {
        return bufferManager.bufferType() == BufferType.ON_HEAP ? EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP;
    }

    /// Reused storage for the state of application of mutations. This stores the backtracking path, including changes
    /// already applied (e.g. new version of a node that is not yet linked to the current trie) and some that are yet
    /// to be applied (e.g. updated content).
    ///
    /// Because in-memory tries are single-writer, we can reuse a single state array for all updates. The updates are
    /// serialized and thus no other thread can corrupt this state (note that this is not the factor enforcing the
    /// single writer policy, and since we are already bound to it there is cost involved in reusing this state array).
    final private ApplyState<T> applyState = new ApplyState<>(this);

    /// Mutator for `InMemoryTrie`. Combines the target trie with the merge options (i.e. the transformers and
    /// predicates) and can be used repeatedly to apply modifications to the trie using [#apply(Trie)].
    public class Mutator<U> extends InMemoryBaseTrie.Mutator<T, U, Cursor<U>, ApplyState<T>>
    {
        /// See [InMemoryTrie#mutator(UpsertTransformer, Predicate)] for the meaning of the
        /// parameters.
        Mutator(UpsertTransformer<T, U> transformer, Predicate<NodeFeatures<U>> needsForcedCopy, Consumer<? super T> droppedContentCallback)
        {
            super(transformer, needsForcedCopy, droppedContentCallback, applyState);
        }

        /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
        /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
        /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
        /// different than the element type for this memtable trie.
        public void apply(Trie<U> mutation)
        throws TrieSpaceExhaustedException
        {
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

        /// Map-like put method, using the apply machinery above which cannot run into stack overflow. When the correct
        /// position in the trie has been reached, the value will be resolved with the given function before being placed in
        /// the trie (even if there's no pre-existing content in this trie).
        /// @param key the trie path/key for the given value.
        /// @param value the value being put in the memtable trie. Note that it can be of type different than the element
        /// type for this memtable trie. It's up to the `transformer` to return the final value that will stay in
        /// the memtable trie.
        public void putSingleton(ByteComparable key, U value) throws TrieSpaceExhaustedException
        {
            apply(Trie.singleton(key, byteComparableVersion, value));
        }
    }

    /// Creates a trie mutator that can be used to apply multiple modifications to the trie.
    ///
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    /// @param droppedContentCallback function to call when childless content is dropped because
    /// [ContentManager#shouldPreserveWithoutChildren] returned false.
    public <U> Mutator<U> mutator(UpsertTransformer<T, U> transformer,
                                  Predicate<NodeFeatures<U>> needsForcedCopy,
                                  Consumer<? super T> droppedContentCallback)
    {
        return new Mutator<>(transformer, needsForcedCopy, droppedContentCallback);
    }

    /// Creates a trie mutator that can be used to apply multiple modifications to the trie.
    ///
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <U> Mutator<U> mutator(UpsertTransformer<T, U> transformer,
                                  Predicate<NodeFeatures<U>> needsForcedCopy)
    {
        return new Mutator<>(transformer, needsForcedCopy, null);
    }

    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
    /// different than the element type for this memtable trie.
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <U> void apply(Trie<U> mutation,
                          final UpsertTransformer<T, U> transformer,
                          final Predicate<NodeFeatures<U>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        mutator(transformer, needsForcedCopy).apply(mutation);
    }

    /// Trie mutator that accepts a range trie (e.g. deletions) to apply over data in this trie using [#apply(RangeTrie)].
    public static class RangeMutator<T, S extends RangeState<S>>
    extends InMemoryBaseTrie.Mutator<T, S, RangeCursor<S>, ApplyState<T>>
    {
        int initialDepth;

        /// See [InMemoryTrie#mutator(UpsertTransformer, Predicate)] for the meaning of
        /// the parameters.
        RangeMutator(ApplyState<T> state,
                     UpsertTransformer<T, S> transformer,
                     Predicate<NodeFeatures<S>> needsForcedCopy,
                     Consumer<? super T> droppedContentCallback)
        {
            super(transformer, needsForcedCopy, droppedContentCallback, state);
        }

        RangeMutator<T, S> start(int root, RangeCursor<S> mutationCursor, int initialForcedCopyDepth)
        {
            initialDepth = 0;
            super.start(root, mutationCursor, initialForcedCopyDepth);
            return this;
        }

        /// A variation of `start` which starts the operation at some point in the trie rather than the root. Used for
        /// processing deletion branches in deletion-aware tries.
        RangeMutator<T, S> continueFromCurrentState(RangeCursor<S> mutationCursor, int initialForcedCopyDepth)
        {
            mutationCursor.assertFresh();
            this.mutationCursor = mutationCursor;
            this.initialDepth = state.currentDepth;
            this.forcedCopyDepth = initialForcedCopyDepth;
            return this;
        }

        @Override
        RangeMutator<T, S> apply() throws TrieSpaceExhaustedException
        {
            int depth = state.currentDepth;
            long position = mutationCursor.encodedPosition();
            assert !Cursor.isOnReturnPath(position) : "Cursor cannot start with position on return path.";
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                S content = mutationCursor.content();
                if (content != null)
                    applyDeletionRange(position);

                position = mutationCursor.advance();
                depth = Cursor.depth(position) + initialDepth;
                // Descend but do not modify anything yet. If the position is on the return path, we can still follow
                // it, `applyDeletionRange` will take care to not apply it to content or descendants.
                if (!state.advanceTo(depth, Cursor.incomingTransition(position), forcedCopyDepth, initialDepth))
                    break;
                assert depth == state.currentDepth : "Unexpected change to applyState. Concurrent trie modification?";
            }

            assert state.currentDepth == initialDepth;
            return this;
        }

        /// Walk all existing content covered under a deletion. Returns true if the caller needs to continue processing
        /// the mutation cursor, and false if the mutation has been exhausted (i.e. the range was open on the right
        /// and we have consumed all existing content).
        void applyDeletionRange(long position) throws TrieSpaceExhaustedException
        {
            S mutationCoveringState = null;
            boolean atMutation = true;
            int depth = Cursor.depth(position) + initialDepth;
            int transition = Cursor.incomingTransition(position);
            boolean onReturnPath = Cursor.isOnReturnPath(position);
            // We are walking both tries in parallel.
            while (true)
            {
                if (atMutation)
                {
                    if (state.currentDepth < forcedCopyDepth)
                        forcedCopyDepth = needsForcedCopy.test(this) ? state.currentDepth : Integer.MAX_VALUE;

                    S mutationContent = mutationCursor.content();

                    if (mutationContent != null)
                    {
                        if (!onReturnPath)
                            applyContent(mutationContent);
                        mutationCoveringState = mutationContent.succedingState(Direction.FORWARD);
                    }
                    else if (!onReturnPath)
                        applyContent(mutationCoveringState);

                    if (mutationCoveringState == null)
                        return;

                    position = mutationCursor.advance();
                    depth = Cursor.depth(position) + initialDepth;
                    transition = Cursor.incomingTransition(position);
                    onReturnPath = Cursor.isOnReturnPath(position);
                }
                else
                    applyContent(mutationCoveringState);

                atMutation = !state.advanceToNextExistingOr(depth, transition, onReturnPath, forcedCopyDepth, initialDepth);
            }
        }

        void applyContent(S content) throws TrieSpaceExhaustedException
        {
            T existingContent = state.getDescentPathContent();
            if (existingContent != null)
            {
                T combinedContent = transformer.apply(existingContent, content);
                if (combinedContent != existingContent)
                    state.setDescentPathContent(combinedContent, // can be null
                                                state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
            }
        }


        /// Apply the given range trie to this in-memory trie. Any existing content that falls under the ranges of the given
        /// trie will be modified by applying the transformer. This is usually used to delete covered content (by returning
        /// null from the transformer).
        /// @param rangeTrie the ranges to be applied, given in the form of a range trie.
        public void apply(RangeTrie<S> rangeTrie) throws TrieSpaceExhaustedException
        {
            apply(rangeTrie.cursor(Direction.FORWARD));
        }

        void apply(RangeCursor<S> cursor) throws TrieSpaceExhaustedException
        {
            try
            {
                start(cursor).apply().complete();
                state.trie.completeMutation();
            }
            catch (Throwable t)
            {
                state.trie.abortMutation();
                throw t;
            }
        }
    }

    /// A variation of range mutator to apply sets as deletions of data in the trie.
    public static class SetMutator<T> extends RangeMutator<T, TrieSetCursor.RangeState>
    {
        SetMutator(ApplyState<T> state, Predicate<NodeFeatures<TrieSetCursor.RangeState>> needsForcedCopy, Consumer<? super T> droppedContentCallback)
        {
            super(state, SetMutator::deleteEntry, needsForcedCopy, droppedContentCallback);
        }

        void apply(TrieSet set) throws TrieSpaceExhaustedException
        {
            apply(set.cursor(Direction.FORWARD));
        }

        private static <T> T deleteEntry(T entry, TrieSetCursor.RangeState state)
        {
            return state.applicableAfter ? null : entry;
        }

    }

    /// Creates a range mutator that can be used to apply multiple modifications/deletions to the trie.
    ///
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    /// @param droppedContentCallback function to call when childless content is dropped because
    /// [ContentManager#shouldPreserveWithoutChildren] returned false.
    public <S extends RangeState<S>> RangeMutator<T, S> rangeMutator(UpsertTransformer<T, S> transformer,
                                                                     Predicate<NodeFeatures<S>> needsForcedCopy,
                                                                     Consumer<? super T> droppedContentCallback)
    {
        return new RangeMutator<>(applyState, transformer, needsForcedCopy, droppedContentCallback);
    }

    /// Creates a range mutator that can be used to apply multiple modifications/deletions to the trie.
    ///
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <S extends RangeState<S>> RangeMutator<T, S> rangeMutator(UpsertTransformer<T, S> transformer,
                                                                     Predicate<NodeFeatures<S>> needsForcedCopy)
    {
        return new RangeMutator<>(applyState, transformer, needsForcedCopy, null);
    }

    /// Creates a set mutator that can be used to apply multiple deletions to the trie.
    public SetMutator<T> deleter(Consumer<? super T> droppedContentCallback)
    {
        return new SetMutator<>(applyState, NodeFeatures::isBranching, droppedContentCallback);
    }

    /// Delete all entries covered under the specified TrieSet
    public void delete(TrieSet set) throws TrieSpaceExhaustedException
    {
        deleter(null).apply(set);
    }

    /// Map-like put method, using the apply machinery above which cannot run into stack overflow. When the correct
    /// position in the trie has been reached, the value will be resolved with the given function before being placed in
    /// the trie (even if there's no pre-existing content in this trie).
    /// @param key the trie path/key for the given value.
    /// @param value the value being put in the memtable trie. Note that it can be of type different than the element
    /// type for this memtable trie. It's up to the `transformer` to return the final value that will stay in
    /// the memtable trie.
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
    /// even if there's no pre-existing value in the memtable trie.
    public <R> void putSingleton(ByteComparable key,
                                 R value,
                                 UpsertTransformer<T, ? super R> transformer) throws TrieSpaceExhaustedException
    {
        mutator(transformer, Predicates.alwaysFalse()).apply(Trie.singleton(key, byteComparableVersion, value));
    }

    /// A version of putSingleton which uses recursive put if the last argument is true.
    public <R> void putSingleton(ByteComparable key,
                                 R value,
                                 UpsertTransformer<T, ? super R> transformer,
                                 boolean useRecursive) throws TrieSpaceExhaustedException
    {
        if (useRecursive)
            putRecursive(key, value, transformer);
        else
            putSingleton(key, value, transformer);
    }
}
