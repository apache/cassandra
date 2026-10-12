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

package org.apache.cassandra.db.compaction;

/**
 * The order {@link org.apache.cassandra.utils.MergeIterator}'s many-source merge gives equal items
 * in, for sources a cursor merge reads.  The iterator path keeps the clustering of the last row and
 * of the last range tombstone marker it is given, and the first of two cells that tie on
 * everything.  Values can compare equal and still differ in bytes (decimal 1.0 and 1.00), so the
 * order decides which source's bytes the output holds.
 *
 * <p>This is {@code MergeIterator.ManyToOne} with each source's current item read through
 * {@link Sources} instead of stored: the same sorted section, binary heap, equal-parent flags and
 * removal of an exhausted source, so equal items come out in the same order.  The merge reads no
 * data itself: the caller moves the sources, and {@link Sources#advance} only reports whether a
 * source has a current item.
 *
 * <p>Allocates only when built.
 */
final class IteratorMergeOrder<T>
{
    /** What the order reads from the merge. */
    interface Sources<T>
    {
        /** Compares the current items of two sources. */
        int compare(T a, T b);

        /**
         * Called where the merge would move {@code source} to its next item, or to its first one.
         * The caller has already moved it.
         *
         * @return false when the source has no item left
         */
        boolean advance(T source);
    }

    /** {@code MergeIterator.ManyToOne.SORTED_SECTION_SIZE}. */
    private static final int SORTED_SECTION_SIZE = 4;

    private static final class Candidate<T>
    {
        T source;
        /** The current item was consumed, or there is none yet: the source needs to advance. */
        boolean needsAdvance;
        boolean equalParent;
    }

    private final Sources<T> sources;
    private final Candidate<T>[] heap;
    /** The candidates, for {@link #reset}; the heap moves them around. */
    private final Candidate<T>[] pool;
    private int size;
    private int needingAdvance;
    /** The sources of the items {@link #consume} took, in order; filled by each step. */
    private T[] out;
    private int outCount;

    /** An order for up to {@code capacity} sources, given by {@link #reset}. */
    @SuppressWarnings("unchecked")
    IteratorMergeOrder(int capacity, Sources<T> sources)
    {
        this.sources = sources;
        this.heap = new Candidate[capacity];
        this.pool = new Candidate[capacity];
        for (int i = 0; i < capacity; i++)
            pool[i] = new Candidate<>();
    }

    /** Starts a new merge of the first {@code count} of {@code inSourceOrder}, in that order. */
    void reset(T[] inSourceOrder, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Candidate<T> candidate = pool[i];
            candidate.source = inSourceOrder[i];
            candidate.needsAdvance = true;
            candidate.equalParent = false;
            heap[i] = candidate;
        }
        for (int i = count; i < heap.length; i++)
            heap[i] = null;
        this.size = count;
        this.needingAdvance = count;
    }

    /**
     * Advances the sources the last step consumed, then takes the next items.  Writes their sources
     * to {@code into}, in the order the merge gives them.
     *
     * @return how many sources it wrote; 0 once every source is exhausted
     */
    int next(T[] into)
    {
        advance();
        if (size == 0)
            return 0;
        this.out = into;
        outCount = 0;
        consume();
        this.out = null;
        return outCount;
    }

    /** {@code MergeIterator.ManyToOne.advance}. */
    private void advance()
    {
        for (int i = needingAdvance - 1; i >= 0; --i)
        {
            Candidate<T> candidate = heap[i];
            if (candidate.needsAdvance)
            {
                candidate.needsAdvance = false;
                replaceAndSink(sources.advance(candidate.source) ? candidate : null, i);
            }
        }
    }

    private int compare(Candidate<T> a, Candidate<T> b)
    {
        return sources.compare(a.source, b.source);
    }

    private void consume(Candidate<T> candidate)
    {
        candidate.needsAdvance = true;
        out[outCount++] = candidate.source;
    }

    /** {@code MergeIterator.ManyToOne.consume}. */
    private void consume()
    {
        consume(heap[0]);
        final int size = this.size;
        final int sortedSectionSize = Math.min(size, SORTED_SECTION_SIZE);
        int i;
        consume:
        {
            for (i = 1; i < sortedSectionSize; ++i)
            {
                if (!heap[i].equalParent)
                    break consume;
                consume(heap[i]);
            }
            i = Math.max(i, consumeHeap(i) + 1);
        }
        needingAdvance = i;
    }

    /** {@code MergeIterator.ManyToOne.consumeHeap}. */
    private int consumeHeap(int idx)
    {
        if (idx >= size || !heap[idx].equalParent)
            return -1;

        consume(heap[idx]);
        int nextIdx = (idx << 1) - (SORTED_SECTION_SIZE - 1);
        return Math.max(idx, Math.max(consumeHeap(nextIdx), consumeHeap(nextIdx + 1)));
    }

    /** {@code MergeIterator.ManyToOne.replaceAndSink}. */
    private void replaceAndSink(Candidate<T> candidate, int currIdx)
    {
        if (candidate == null)
        {
            // drop the source by replacing it with the last one in the heap
            candidate = heap[--size];
            heap[size] = null;
        }
        candidate.equalParent = false;

        final int size = this.size;
        final int sortedSectionSize = Math.min(size - 1, SORTED_SECTION_SIZE);

        int nextIdx;
        while ((nextIdx = currIdx + 1) <= sortedSectionSize)
        {
            if (!heap[nextIdx].equalParent)
            {
                int cmp = compare(candidate, heap[nextIdx]);
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
        nextIdx = (currIdx * 2) - (sortedSectionSize - 1);
        if (nextIdx >= size)
        {
            heap[currIdx] = candidate;
            return;
        }
        sinkInBinaryHeap(candidate, currIdx, size, sortedSectionSize);
    }

    /** {@code MergeIterator.ManyToOne.sinkInBinaryHeap}. */
    private void sinkInBinaryHeap(Candidate<T> candidate, int currIdx, final int size, final int sortedSectionSize)
    {
        int nextIdx;
        while ((nextIdx = (currIdx * 2) - (sortedSectionSize - 1)) + 1 < size)
        {
            if (!heap[nextIdx].equalParent)
            {
                if (!heap[nextIdx + 1].equalParent)
                {
                    int siblingCmp = compare(heap[nextIdx + 1], heap[nextIdx]);
                    if (siblingCmp < 0)
                        ++nextIdx;

                    int cmp = compare(candidate, heap[nextIdx]);
                    if (cmp <= 0)
                    {
                        if (cmp == 0)
                        {
                            heap[nextIdx].equalParent = true;
                            if (siblingCmp == 0)
                                heap[nextIdx + 1].equalParent = true;
                        }

                        heap[currIdx] = candidate;
                        return;
                    }

                    if (siblingCmp == 0)
                        heap[nextIdx + 1].equalParent = true;
                }
                else
                    ++nextIdx;
            }

            heap[currIdx] = heap[nextIdx];
            currIdx = nextIdx;
        }

        if (nextIdx >= size)
        {
            heap[currIdx] = candidate;
            return;
        }

        if (!heap[nextIdx].equalParent)
        {
            int cmp = compare(candidate, heap[nextIdx]);
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
}
