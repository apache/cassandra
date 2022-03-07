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

package org.apache.cassandra.utils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A merger of input streams (e.g. iterators or cursors) that may consume multiple input values per output value.
 *
 * The most straightforward way to implement this is to use a {@code PriorityQueue} of iterators, {@code poll} it to
 * find the next item to consume, then {@code add} the iterator back after advancing. This is not very efficient as
 * {@code poll} and {@code add} in all cases require at least {@code log(size)} comparisons (usually more than
 * {@code 2*log(size)}) per consumed item, even if the input is suitable for fast iteration.
 *
 * The implementation below makes use of the fact that replacing the top element in a binary heap can be done much
 * more efficiently than separately removing it and placing it back, especially in the cases where the top iterator
 * is to be used again very soon (e.g. when there are large sections of the output where only a limited number of
 * input iterators overlap, which is normally the case in many practically useful situations, e.g. levelled
 * compaction). To further improve this particular scenario, we also use a short sorted section at the start of the
 * queue.
 *
 * The heap is laid out as this (for {@code SORTED_SECTION_SIZE == 2}):
 *                 0
 *                 |
 *                 1
 *                 |
 *                 2
 *               /   \
 *              3     4
 *             / \   / \
 *             5 6   7 8
 *            .. .. .. ..
 * Where each line is a <= relationship.
 *
 * In the sorted section we can advance with a single comparison per level, while advancing a level within the heap
 * requires two (so that we can find the lighter element to pop up).
 * The sorted section adds a constant overhead when data is uniformly distributed among the iterators, but may up
 * to halve the iteration time when one iterator is dominant over sections of the merged data (as is the case with
 * non-overlapping iterators).
 *
 * The iterator is further complicated by the need to avoid advancing the input iterators until an output is
 * actually requested. To achieve this {@code consume} walks the heap to find equal items without advancing the
 * iterators, and {@code advance} moves them and restores the heap structure before any items can be consumed.
 *
 * To avoid having to do additional comparisons in consume to identify the equal items, we keep track of equality
 * between children and their parents in the heap. More precisely, the lines in the diagram above define the
 * following relationship:
 *   parent <= child && (parent == child) == child.equalParent
 * We can track, make use of and update the equalParent field without any additional comparisons.
 *
 * For more formal definitions and proof of correctness, see CASSANDRA-8915.
 */
public class Merger<In, Source, Out> implements AutoCloseable
{
    /** The heap of candidates, each containing their current item and the source from which it was obtained. */
    protected final Candidate<In, Source>[] heap;

    /** Reducer, called for each input item to combine them into the output. */
    final Reducer<In, Out> reducer;

    /** Function called on each source to get the next item. Should return null if the source is exhausted. */
    final Function<Source, In> inputRetriever;

    /** Method to call on each source on close, may be null. */
    final Consumer<Source> onClose;

    /** Number of non-exhausted iterators. */
    int size;

    /**
     * Position of the deepest, right-most child that needs advancing before we can start consuming.
     * Because advancing changes the values of the items of each source, the parent-chain from any position
     * in this range that needs advancing is not in correct order. The trees rooted at any position that does
     * not need advancing, however, retain their prior-held binary heap property.
     */
    int needingAdvance;

    /**
     * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
     */
    static final int SORTED_SECTION_SIZE = 4;

    /**
     * @param sources        The input sources.
     * @param inputRetriever Function called on each source to get the next item. Should return null if the source is
     *                       exhausted.
     * @param onClose        Method to call on each source on close, may be null.
     * @param comparator     Comparator of input items.
     * @param reducer        Reducer, called for each input item to combine them into the output.
     */
    public Merger(List<Source> sources,
                  Function<Source, In> inputRetriever,
                  Consumer<Source> onClose,
                  Comparator<? super In> comparator,
                  Reducer<In, Out> reducer)
    {
        this.inputRetriever = inputRetriever;
        this.onClose = onClose;
        this.reducer = reducer;

        heap = new Candidate[sources.size()];
        size = 0;

        for (int i = 0; i < sources.size(); i++)
        {
            Candidate<In, Source> candidate = new Candidate<>(i, sources.get(i), comparator);
            heap[size++] = candidate;
        }
        needingAdvance = size;
    }

    public boolean hasNext()
    {
        advance();  // no-op if already advanced
        return size > 0;
    }

    public Out next()
    {
        advance();  // no-op if already advanced (e.g. hasNext() called)
        assert size > 0;
        return consume();
    }

    public void close()
    {
        if (onClose == null)
            return;

        Throwable t = null;
        for (Candidate<In, Source> c : heap)
        {
            try
            {
                onClose.accept(c.input);
            }
            catch (Throwable e)
            {
                t = Throwables.merge(t, e);
            }
        }
        Throwables.maybeFail(t);
    }


    /**
     * Advance all sources that need to be advanced and place them into suitable positions in the heap.
     *
     * By walking the sources backwards we know that everything after the point being processed already forms
     * correctly ordered subheaps, thus we can build a subheap rooted at the current position by only sinking down
     * the newly advanced source. Because all parents of a consumed source are also consumed there is no way
     * that we can process one consumed source but skip over its parent.
     *
     * The procedure is the same as the one used for the initial building of a heap in the heapsort algorithm and
     * has a maximum number of comparisons {@code (2 * log(size) + SORTED_SECTION_SIZE / 2)} multiplied by the
     * number of sources whose items were consumed at the previous step, but is also at most linear in the size of
     * the heap if the number of consumed elements is high (as it is in the initial heap construction). With non- or
     * lightly-overlapping sources the procedure finishes after just one (resp. a couple of) comparisons.
     */
    private void advance()
    {
        // Turn the set of candidates into a heap.
        for (int i = needingAdvance - 1; i >= 0; --i)
        {
            Candidate<In, Source> candidate = heap[i];
            /**
             *  needingAdvance runs to the maximum index (and deepest-right node) that may need advancing;
             *  since the equal items that were consumed at-once may occur in sub-heap "veins" of equality,
             *  not all items above this deepest-right position may have been consumed; these already form
             *  valid sub-heaps and can be skipped-over entirely
             */
            if (candidate.needsAdvance())
                replaceAndSink(candidate, !candidate.advance(inputRetriever), i);
        }
        needingAdvance = 0;
    }

    /**
     * Consume all items that sort like the current top of the heap. As we cannot advance the sources to let
     * equivalent items pop up, we walk the heap to find them and mark them as needing advance.
     *
     * This relies on the equalParent flag to avoid doing any comparisons.
     */
    private Out consume()
    {
        reducer.onKeyChange();
        assert !heap[0].equalParent;
        heap[0].consume(reducer);
        final int size = this.size;
        final int sortedSectionSize = Math.min(size, SORTED_SECTION_SIZE);
        int i;
        consume: {
            for (i = 1; i < sortedSectionSize; ++i)
            {
                if (!heap[i].equalParent)
                    break consume;
                heap[i].consume(reducer);
            }
            i = Math.max(i, consumeHeap(i) + 1);
        }
        needingAdvance = i;
        return reducer.getReduced();
    }

    /**
     * Recursively consume all items equal to equalItem in the binary subheap rooted at position idx.
     *
     * @return the largest equal index found in this search.
     */
    private int consumeHeap(int idx)
    {
        if (idx >= size || !heap[idx].equalParent)
            return -1;

        heap[idx].consume(reducer);
        int nextIdx = (idx << 1) - (SORTED_SECTION_SIZE - 1);
        return Math.max(idx, Math.max(consumeHeap(nextIdx), consumeHeap(nextIdx + 1)));
    }

    /**
     * Replace a source in the heap with the given position and move it down the heap until it finds its proper
     * position, pulling lighter elements up the heap.
     *
     * Whenever an equality is found between two elements that form a new parent-child relationship, the child's
     * equalParent flag is set to true if the elements are equal.
     */
    private void replaceAndSink(Candidate<In, Source> candidate, boolean candidateDone, int currIdx)
    {
        if (candidateDone)
        {
            // Drop source by swapping it with the last one in the heap.
            Candidate<In, Source> replacement = heap[--size];
            heap[size] = candidate;
            candidate = replacement;
        }
        // The new element will be top of its heap, at this point there is no parent to be equal to.
        candidate.equalParent = false;

        final int size = this.size;
        final int sortedSectionSize = Math.min(size - 1, SORTED_SECTION_SIZE);

        int nextIdx;

        // Advance within the sorted section, pulling up items lighter than candidate.
        while ((nextIdx = currIdx + 1) <= sortedSectionSize)
        {
            if (!heap[nextIdx].equalParent) // if we were greater then an (or were the) equal parent, we are >= the child
            {
                int cmp = candidate.compareTo(heap[nextIdx]);
                if (cmp <= 0)
                {
                    heap[nextIdx].equalParent = cmp == 0;
                    heap[currIdx] = candidate;
                    return;
                }
            }

            heap[currIdx] = heap[nextIdx];
            currIdx = nextIdx;
        }
        // If size <= SORTED_SECTION_SIZE, nextIdx below will be no less than size,
        // because currIdx == sortedSectionSize == size - 1 and nextIdx becomes
        // (size - 1) * 2) - (size - 1 - 1) == size.

        // Advance in the binary heap, pulling up the lighter element from the two at each level.
        while ((nextIdx = (currIdx * 2) - (sortedSectionSize - 1)) + 1 < size)
        {
            if (!heap[nextIdx].equalParent)
            {
                if (!heap[nextIdx + 1].equalParent)
                {
                    // pick the smallest of the two children
                    int siblingCmp = heap[nextIdx + 1].compareTo(heap[nextIdx]);
                    if (siblingCmp < 0)
                        ++nextIdx;

                    // if we're smaller than this, we are done, and must only restore the heap and equalParent properties
                    int cmp = candidate.compareTo(heap[nextIdx]);
                    if (cmp <= 0)
                    {
                        if (cmp == 0)
                        {
                            heap[nextIdx].equalParent = true;
                            if (siblingCmp == 0) // siblingCmp == 0 => nextIdx is the left child
                                heap[nextIdx + 1].equalParent = true;
                        }

                        heap[currIdx] = candidate;
                        return;
                    }

                    if (siblingCmp == 0)
                    {
                        // siblingCmp == 0 => nextIdx is still the left child
                        // if the two siblings were equal, and we are inserting something greater, we will
                        // pull up the left one; this means the right gets an equalParent
                        heap[nextIdx + 1].equalParent = true;
                    }
                }
                else
                    ++nextIdx;  // descend down the path where we found the equal child
            }

            heap[currIdx] = heap[nextIdx];
            currIdx = nextIdx;
        }

        // our loop guard ensures there are always two siblings to process; typically when we exit the loop we will
        // be well past the end of the heap and this next condition will match...
        if (nextIdx >= size)
        {
            heap[currIdx] = candidate;
            return;
        }

        // ... but sometimes we will have one last child to compare against, that has no siblings
        if (!heap[nextIdx].equalParent)
        {
            int cmp = candidate.compareTo(heap[nextIdx]);
            if (cmp <= 0)
            {
                heap[nextIdx].equalParent = cmp == 0;
                heap[currIdx] = candidate;
                return;
            }
        }

        heap[currIdx] = heap[nextIdx];
        heap[nextIdx] = candidate;
    }

    /**
     * Returns an iterable listing all the sources of this merger.
     */
    public Iterable<Source> allSources()
    {
        return () -> new Iterator<Source>()
        {
            int index = 0;

            public boolean hasNext()
            {
                return index < heap.length;
            }

            public Source next()
            {
                return heap[index++].input;
            }
        };
    }

    /**
     * Returns an iterable that lists all inputs that are positioned after the current position,
     * i.e. all inputs that are not equal to the current top.
     * Meant to be called inside getReduced.
     */
    public Iterable<In> allGreaterValues()
    {
        return () -> new AbstractIterator<In>()
        {
            int index = 1;  // skip first item, it's always equal

            protected In computeNext()
            {
                while (true)
                {
                    if (index >= size)
                        return endOfData();
                    Candidate<In, Source> candidate = heap[index++];
                    if (!candidate.needsAdvance())
                        return candidate.item;
                }
            }
        };
    }

    // Holds and is comparable by the head item of a source it owns
    protected static final class Candidate<In, Source> implements Comparable<Candidate<In, Source>>
    {
        private final Source input;
        private final Comparator<? super In> comp;
        private final int idx;
        private In item;
        boolean equalParent;

        public Candidate(int idx, Source input, Comparator<? super In> comp)
        {
            this.input = input;
            this.comp = comp;
            this.idx = idx;
        }

        /** Advance this source and returns true if it had an item, i.e. was not exhausted. */
        protected boolean advance(Function<Source, In> inputRetriever)
        {
            item = inputRetriever.apply(input);
            return item != null;
        }

        public int compareTo(Candidate<In, Source> that)
        {
            assert this.item != null && that.item != null;
            return comp.compare(this.item, that.item);
        }

        public void consume(Reducer reducer)
        {
            reducer.reduce(idx, item);
            item = null;
        }

        public boolean needsAdvance()
        {
            return item == null;
        }
    }
}
