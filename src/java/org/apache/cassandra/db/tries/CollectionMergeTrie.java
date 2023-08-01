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

import com.google.common.collect.Iterables;

/**
 * A merged view of multiple tries.
 *
 * This is accomplished by walking the cursors in parallel; the merged cursor takes the position and features of the
 * smallest and advances with it; when multiple cursors are equal, all of them are advanced. The ordered view of the
 * cursors is maintained using a custom binary min-heap, built for efficiently reforming the heap when the top elements
 * are advanced (see {@link CollectionMergeCursor}).
 *
 * Crucial for the efficiency of this is the fact that when they are advanced like this, we can compare cursors'
 * positions by their depth descending and then incomingTransition ascending.
 *
 * See Trie.md for further details.
 */
class CollectionMergeTrie<T> extends Trie<T>
{
    private final CollectionMergeResolver<T> resolver;  // only called on more than one input
    protected final Collection<? extends Trie<T>> inputs;

    CollectionMergeTrie(Collection<? extends Trie<T>> inputs, CollectionMergeResolver<T> resolver)
    {
        this.resolver = resolver;
        this.inputs = inputs;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new CollectionMergeCursor<>(resolver, inputs);
    }

    /**
     * Compare the positions of two cursors. One is before the other when
     * - its depth is greater, or
     * - its depth is equal, and the incoming transition is smaller.
     */
    static <T> boolean greaterCursor(Cursor<T> c1, Cursor<T> c2)
    {
        int c1depth = c1.depth();
        int c2depth = c2.depth();
        if (c1depth != c2depth)
            return c1depth < c2depth;
        return c1.incomingTransition() > c2.incomingTransition();
    }

    static <T> boolean equalCursor(Cursor<T> c1, Cursor<T> c2)
    {
        return c1.depth() == c2.depth() && c1.incomingTransition() == c2.incomingTransition();
    }

    /*
     * The merge cursor is a variation of the idea of a merge iterator with one key observation: because we advance
     * the source iterators together, we can compare them just by depth and incoming transition.
     *
     * The most straightforward way to implement merging of iterators is to use a {@code PriorityQueue},
     * {@code poll} it to find the next item to consume, then {@code add} the iterator back after advancing.
     * This is not very efficient as {@code poll} and {@code add} in all cases require at least
     * {@code log(size)} comparisons and swaps (usually more than {@code 2*log(size)}) per consumed item, even
     * if the input is suitable for fast iteration.
     *
     * The implementation below makes use of the fact that replacing the top element in a binary heap can be
     * done much more efficiently than separately removing it and placing it back, especially in the cases where
     * the top iterator is to be used again very soon (e.g. when there are large sections of the output where
     * only a limited number of input iterators overlap, which is normally the case in many practically useful
     * situations, e.g. levelled compaction).
     *
     * The implementation builds and maintains a binary heap of sources (stored in an array), where we do not
     * add items after the initial construction. Instead we advance the smallest element (which is at the top
     * of the heap) and push it down to find its place for its new position. Should this source be exhausted,
     * we swap it with the last source in the heap and proceed by pushing that down in the heap.
     *
     * In the case where we have multiple sources with matching positions, the merging algorithm
     * must be able to merge all equal values. To achieve this {@code content} walks the heap to
     * find all equal cursors without advancing them, and separately {@code advance} advances
     * all equal sources and restores the heap structure.
     *
     * The latter is done equivalently to the process of initial construction of a min-heap using back-to-front
     * heapification as done in the classic heapsort algorithm. It only needs to heapify subheaps whose top item
     * is advanced (i.e. one whose position matches the current), and we can do that recursively from
     * bottom to top. Should a source be exhausted when advancing, it can be thrown away by swapping in the last
     * source in the heap (note: we must be careful to advance that source too if required).
     *
     * To make it easier to advance efficienty in single-sourced branches of tries, we extract the current smallest
     * cursor (the head) separately, and start any advance with comparing that to the heap's first. When the smallest
     * cursor remains the same (e.g. in branches coming from a single source) this makes it possible to advance with
     * just one comparison instead of two at the expense of increasing the number by one in the general case.
     *
     * Note: This is a simplification of the MergeIterator code from CASSANDRA-8915, without the leading ordered
     * section and equalParent flag since comparisons of cursor positions are cheap.
     */
    static class CollectionMergeCursor<T> implements Cursor<T>
    {
        private final CollectionMergeResolver<T> resolver;

        /**
         * The smallest cursor, tracked separately to improve performance in single-source sections of the trie.
         */
        private Cursor<T> head;

        /**
         * Binary heap of the remaining cursors. The smallest element is at position 0.
         * Every element i is smaller than or equal to its two children, i.e.
         *     heap[i] <= heap[i*2 + 1] && heap[i] <= heap[i*2 + 2]
         */
        private final Cursor<T>[] heap;

        /**
         * A list used to collect contents during content() calls.
         */
        private final List<T> contents;

        public CollectionMergeCursor(CollectionMergeResolver<T> resolver, Collection<? extends Trie<T>> inputs)
        {
            this.resolver = resolver;
            int count = inputs.size();
            // Get cursors for all inputs. Put one of them in head and the rest in the heap.
            heap = new Cursor[count - 1];
            contents = new ArrayList<>(count);
            int i = -1;
            for (Trie<T> trie : inputs)
            {
                Cursor<T> cursor = trie.cursor();
                assert cursor.depth() == 0;
                if (i >= 0)
                    heap[i] = cursor;
                else
                    head = cursor;
                ++i;
            }
            // The cursors are all currently positioned on the root and thus in valid heap order.
        }

        /**
         * Interface for internal operations that can be applied to the equal top elements of the heap.
         */
        interface HeapOp<T>
        {
            void apply(CollectionMergeCursor<T> self, Cursor<T> cursor, int index);
        }

        /**
         * Apply a non-interfering operation, i.e. one that does not change the cursor state, to all inputs in the heap
         * that are on equal position to the head.
         * For interfering operations like advancing the cursors, use {@link #advanceEqualAndRestoreHeap(AdvancingHeapOp)}.
         */
        private void applyToEqualOnHeap(HeapOp<T> action)
        {
            applyToEqualElementsInHeap(action, 0);
        }

        /**
         * Interface for internal advancing operations that can be applied to the heap cursors. This interface provides
         * the code to restore the heap structure after advancing the cursors.
         */
        interface AdvancingHeapOp<T> extends HeapOp<T>
        {
            void apply(Cursor<T> cursor);

            default void apply(CollectionMergeCursor<T> self, Cursor<T> cursor, int index)
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


        /**
         * Advance the state of all inputs in the heap that are on equal position as the head and restore the heap
         * invariant.
         */
        private void advanceEqualAndRestoreHeap(AdvancingHeapOp<T> action)
        {
            applyToEqualElementsInHeap(action, 0);
        }

        /**
         * Apply an operation to all elements on the heap that are equal to the head. Descends recursively in the heap
         * structure to all equal children and applies the operation on the way back.
         *
         * This operation can be something that does not change the cursor state (see {@link #content}) or an operation
         * that advances the cursor to a new state, wrapped in a {@link AdvancingHeapOp} ({@link #advance} or
         * {@link #skipChildren}). The latter interface takes care of pushing elements down in the heap after advancing
         * and restores the subheap state on return from each level of the recursion.
         */
        private void applyToEqualElementsInHeap(HeapOp<T> action, int index)
        {
            if (index >= heap.length)
                return;
            Cursor<T> item = heap[index];
            if (!equalCursor(item, head))
                return;

            // If the children are at the same position, they also need advancing and their subheap
            // invariant to be restored.
            applyToEqualElementsInHeap(action, index * 2 + 1);
            applyToEqualElementsInHeap(action, index * 2 + 2);

            // Apply the action. This is done on the reverse direction to give the action a chance to form proper
            // subheaps and combine them on processing the parent.
            action.apply(this, item, index);
        }

        /**
         * Push the given state down in the heap from the given index until it finds its proper place among
         * the subheap rooted at that position.
         */
        private void heapifyDown(Cursor<T> item, int index)
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

        /**
         * Check if the head is greater than the top element in the heap, and if so, swap them and push down the new
         * top until its proper place.
         * @param headDepth the depth of the head cursor (as returned by e.g. advance).
         * @return the new head element's depth
         */
        private int maybeSwapHead(int headDepth)
        {
            int heap0Depth = heap[0].depth();
            if (headDepth > heap0Depth ||
                (headDepth == heap0Depth && head.incomingTransition() <= heap[0].incomingTransition()))
                return headDepth;   // head is still smallest

            // otherwise we need to swap heap and heap[0]
            Cursor<T> newHeap0 = head;
            head = heap[0];
            heapifyDown(newHeap0, 0);
            return heap0Depth;
        }

        @Override
        public int advance()
        {
            advanceEqualAndRestoreHeap(Cursor::advance);
            return maybeSwapHead(head.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            // If the current position is present in just one cursor, we can safely descend multiple levels within
            // its branch as no one of the other tries has content for it.
            if (equalCursor(heap[0], head))
                return advance();   // More than one source at current position, do single-step advance.

            // If there are no children, i.e. the cursor ascends, we have to check if it's become larger than some
            // other candidate.
            return maybeSwapHead(head.advanceMultiple(receiver));
        }

        @Override
        public int skipChildren()
        {
            advanceEqualAndRestoreHeap(Cursor::skipChildren);
            return maybeSwapHead(head.skipChildren());
        }

        @Override
        public int depth()
        {
            return head.depth();
        }

        @Override
        public int incomingTransition()
        {
            return head.incomingTransition();
        }

        @Override
        public T content()
        {
            applyToEqualOnHeap(CollectionMergeCursor::collectContent);
            collectContent(head, -1);

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

        private void collectContent(Cursor<T> item, int index)
        {
            T itemContent = item.content();
            if (itemContent != null)
                contents.add(itemContent);
        }
    }

    /**
     * Special instance for sources that are guaranteed distinct. The main difference is that we can form unordered
     * value list by concatenating sources.
     */
    static class Distinct<T> extends CollectionMergeTrie<T>
    {
        Distinct(Collection<? extends Trie<T>> inputs)
        {
            super(inputs, throwingResolver());
        }

        @Override
        public Iterable<T> valuesUnordered()
        {
            return Iterables.concat(Iterables.transform(inputs, Trie::valuesUnordered));
        }
    }
}
