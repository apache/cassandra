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
        InMemoryBaseTrie<Object> empty = new InMemoryTrie<>(ByteComparable.Version.OSS50, BufferType.ON_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryTrie<>(ByteComparable.Version.OSS50, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    InMemoryTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, bufferType, lifetime, opOrder);
    }

    public static <T> InMemoryTrie<T> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryTrie<>(byteComparableVersion, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <T> InMemoryTrie<T> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <T> InMemoryTrie<T> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <T> InMemoryTrie<T> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder);
    }

    public InMemoryCursor<T> makeCursor(Direction direction)
    {
        return new InMemoryCursor<>(this, direction, root, 0, -1);
    }

    protected long emptySizeOnHeap()
    {
        return bufferType == BufferType.ON_HEAP ? EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP;
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
                          final UpsertTransformerWithKeyProducer<T, U> transformer,
                          final Predicate<NodeFeatures<U>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        try
        {
            Mutation<T, U, Cursor<U>> m = new Mutation<>(transformer,
                                                         needsForcedCopy,
                                                         mutation.cursor(Direction.FORWARD),
                                                         applyState.start());
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
        apply(mutation, (UpsertTransformerWithKeyProducer<T, U>) transformer, needsForcedCopy);
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
        apply(Trie.singleton(key, byteComparableVersion, value), transformer, Predicates.alwaysFalse());
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

    /// Delete all entries covered under the specified TrieSet
    public void delete(TrieSet set) throws TrieSpaceExhaustedException
    {
        apply(set.cursor(Direction.FORWARD),
              (UpsertTransformer<T, TrieSetCursor.RangeState>) RangeMutation::deleteEntry,
              NodeFeatures::isBranching);
    }

    /// Apply the given range trie to this in-memory trie. Any existing content that falls under the ranges of the given
    /// trie will be modified by applying the transformer. This is usually used to delete covered content (by returning
    /// null from the transformer).
    /// @param rangeTrie the ranges to be applied, given in the form of a range trie.
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <S extends RangeState<S>>
    void apply(RangeTrie<S> rangeTrie,
               final UpsertTransformerWithKeyProducer<T, S> transformer,
               final Predicate<NodeFeatures<S>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        apply(rangeTrie.cursor(Direction.FORWARD), transformer, needsForcedCopy);
    }

    private <S extends RangeState<S>> void apply(RangeCursor<S> cursor,
                                                 UpsertTransformerWithKeyProducer<T, S> transformer,
                                                 Predicate<NodeFeatures<S>> needsForcedCopy) throws TrieSpaceExhaustedException
    {
        try
        {
            RangeMutation<T, S, RangeCursor<S>> m = new RangeMutation<>(transformer,
                                                                        needsForcedCopy,
                                                                        cursor,
                                                                        applyState.start());
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

    static class RangeMutation<T, S extends RangeState<S>, C extends RangeCursor<S>> extends Mutation<T, S, C>
    {
        final int initialDepth;

        RangeMutation(UpsertTransformerWithKeyProducer<T, S> transformer,
                      Predicate<NodeFeatures<S>> needsForcedCopy,
                      C mutationCursor,
                      InMemoryBaseTrie<T>.ApplyState state)
        {
            this(transformer, needsForcedCopy, mutationCursor, state, Integer.MAX_VALUE);
        }

        RangeMutation(UpsertTransformerWithKeyProducer<T, S> transformer,
                      Predicate<NodeFeatures<S>> needsForcedCopy,
                      C mutationCursor,
                      InMemoryBaseTrie<T>.ApplyState state,
                      int initialForcedCopyDepth)
        {
            super(transformer, needsForcedCopy, mutationCursor, state);
            initialDepth = state.currentDepth;
            forcedCopyDepth = initialForcedCopyDepth;
        }

        @Override
        void apply() throws TrieSpaceExhaustedException
        {
            // A TrieSet may start already in a deleted range. If so, pretend there's a START at the initial position.
            S content = mutationCursor.precedingState();
            if (coveringStateApplies(content))
                content = content.asBoundary(Direction.FORWARD);
            else
                content = mutationCursor.content();

            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                if (content != null)
                {
                    applyCoveringContent(content);
                    S mutationCoveringState = content.precedingState(Direction.REVERSE); // Use the right side of the deletion
                    if (coveringStateApplies(mutationCoveringState))
                    {
                        boolean done = !applyDeletionRange(mutationCoveringState);
                        if (done)
                            break;
                    }
                }

                depth = mutationCursor.advance() + initialDepth;
                // Descend but do not modify anything yet.
                if (!state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth, initialDepth))
                    break;

                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
                content = mutationCursor.content();
            }

            assert state.currentDepth == initialDepth;
        }

        /// Walk all existing content covered under a deletion. Returns true if the caller needs to continue processing
        /// the mutation cursor, and false if the mutation has been exhausted (i.e. the range was open on the right
        /// and we have consumed all existing content).
        boolean applyDeletionRange(S mutationCoveringState) throws TrieSpaceExhaustedException
        {
            boolean atMutation = true;
            int depth = mutationCursor.depth() + initialDepth;
            int transition = mutationCursor.incomingTransition();
            // We are walking both tries in parallel.
            while (true)
            {
                if (atMutation)
                {
                    depth = mutationCursor.advance() + initialDepth;
                    transition = mutationCursor.incomingTransition();
                    atMutation = false;
                }

                // Mutation can be open on the right (i.e. not have a closing marker).
                if (depth > 0)
                {
                    if (depth < forcedCopyDepth)
                        forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;
                    atMutation = !state.advanceToNextExistingOr(depth, transition, forcedCopyDepth);
                }
                else if (!state.advanceToNextExisting(forcedCopyDepth))
                    return false;

                T existingContent = state.getContent();
                S mutationContent = atMutation ? mutationCursor.content() : null;
                if (mutationContent != null)
                {
                    applyCoveringContent(mutationContent);
                    mutationCoveringState = mutationContent.precedingState(Direction.REVERSE);
                    if (!coveringStateApplies(mutationCoveringState))
                        return true; // mutation deletion range was closed, we can continue normal mutation cursor iteration
                }
                else if (existingContent != null)
                    applyCoveringContent(mutationCoveringState);
            }
        }

        private static <S extends RangeState<S>> boolean coveringStateApplies(S state)
        {
            // Sets return non-null state (START_END_PREFIX) for regions that they do not cover. Check that too.
            return state != null && state != TrieSetCursor.RangeState.START_END_PREFIX;
        }

        void applyCoveringContent(S content) throws TrieSpaceExhaustedException
        {
            if (content != null)
            {
                T existingContent = state.getContent();
                if (existingContent != null)
                {
                    T combinedContent = transformer.apply(existingContent, content, state);
                    state.setContent(combinedContent, // can be null
                                     state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
                }
            }
        }

        private static <T> T deleteEntry(T entry, TrieSetCursor.RangeState state)
        {
            return state.applicableBefore ? null : entry;
        }
    }
}
