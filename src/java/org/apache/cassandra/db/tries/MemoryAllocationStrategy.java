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

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.IntArrayList;

import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Allocation strategy for buffers and arrays for InMemoryTrie's. Controls how space is allocated and reused.
 */
public interface MemoryAllocationStrategy
{
    /**
     * Get a free index. This is either a new index, allocated via the passed index producer functions, or one that
     * has been previously recycled.
     */
    int allocate() throws TrieSpaceExhaustedException;

    /**
     * Marks the given index for recycling.
     *
     * When the index is actually reused depends on the recycling strategy. In any case it cannot be before the current
     * mutation is complete (because it may still be walking cells that have been moved), and any concurrent readers
     * that have started before this cell has become unreachable must also have completed.
     */
    void recycle(int index);

    /**
     * To be called when a mutation completes. No new readers must be able to see recycled content at the time of this
     * call (the paths for reaching them must have been overwritten via a volatile write; additionally, if the buffer
     * has grown, the root variable (which is stored outside the buffer) must have accepted a volatile write).
     * No recycled indexes can be made available for reuse before this is called, and before any readers started before
     * this call have completed.
     */
    void completeMutation();

    /**
     * Called when a mutation is aborted because of an exception. This means that the indexes that were marked for
     * recycling are still going to be in use (unless this is called a later separate completeMutation call may release
     * and reuse them, causing corruption).
     *
     * Aborted mutations are not normal, and at this time we are not trying to ensure that a trie will behave at its
     * best if an abort has taken place (i.e. it may take more space, be slower etc.), but it should still operate
     * correctly.
     */
    void abortMutation();

    /**
     * Returns the number of indexes that have been claimed by the allocation strategy but are not currently in use
     * (either because they are in various stages of recycling, or have yet to see first use).
     */
    long indexCountInPipeline();

    /**
     * Constructs a list of all the indexes that are in the recycling pipeline.
     * Used to test available and unreachable indexes are the same thing.
     */
    @VisibleForTesting
    IntArrayList indexesInPipeline();

    interface Allocator
    {
        int allocate() throws TrieSpaceExhaustedException;

        default void allocate(int[] indexList) throws TrieSpaceExhaustedException
        {
            for (int i = indexList.length - 1; i >= 0; --i)
                indexList[i] = allocate();
        }
    }

    /**
     * Strategy for small short-lived tries, usually on-heap. This strategy does not reuse any indexes.
     */
    class NoReuseStrategy implements MemoryAllocationStrategy
    {
        final Allocator allocator;

        public NoReuseStrategy(Allocator allocator)
        {
            this.allocator = allocator;
        }

        public int allocate() throws TrieSpaceExhaustedException
        {
            return allocator.allocate();
        }

        public void recycle(int index)
        {
            // No reuse, do nothing
        }

        public void completeMutation()
        {
            // No reuse, nothing to do
        }

        public void abortMutation()
        {
            // No reuse, nothing to do
        }

        @Override
        public long indexCountInPipeline()
        {
            // No indexes recycled
            return 0;
        }

        @Override
        public IntArrayList indexesInPipeline()
        {
            return new IntArrayList();
        }
    }

    /**
     * Reuse strategy for large, long-lived tries. Recycles indexes when it knows that the mutation recycling
     * them has completed, and all reads started no later than this completion have also completed (signalled by an
     * OpOrder which the strategy assumes all readers subscribe to).
     *
     * The OpOrder recycling strategy holds queues of indexes available for recycling. The queues ar organized in blocks
     * of REUSE_BLOCK_SIZE entries. The blocks move through the following stages:
     * - Being filled with newly released indexes. In this stage they are at the head of the "justReleased" list. When
     *   a block becomes full, a new block is created and attached to the head of the list.
     * - Full, but the mutation that released one or more of the mutations in them has not yet completed. In this stage
     *   they are attached to the "justReleased" list as the second or further block. When a mutationComplete is
     *   received, all such blocks get issued a common OpOrder.Barrier and are attached to "awaitingBarrierTail" (which
     *   is the tail of the "free" list).
     * - Awaiting a barrier. In this stage they are in the "free" list after its head, closer to its
     *   "awaitingBarrierTail", identified by the fact that their barrier has not yet expired. Note that the blocks are
     *   put in the order in which their barriers are issued, thus if a block has an active barrier, all blocks that
     *   follow it in the list also do.
     * - Ready for use. In this stage they are still in the "free" list after its head, but their barrier has now
     *   expired. All the indexes in such blocks can now be reused, and will be when the head of the list is exhausted.
     * - Active free block at the head of the "free" list. This block is the one new allocations are served from. When
     *   it is exhausted, we check if the next block's barrier has expired. If so, the "free" pointer moves to it.
     *   If not, there's nothing to reuse as any blocks in the list still have an active barrier, thus we grab some new
     *   memory and refill the block.
     * - If a mutation is aborted by an error, we throw away all indexes in the "justReleased" list. This is done so
     *   that none of the indexes that were marked for release, but whose parent chain may have remained in place,
     *   making them reachable, are reused and corrupt the trie. This will leak some indexes (from earlier mutations in
     *   the block and/or ones whose parents have already been moved), but we prefer not to pay the cost of identifying
     *   the exact indexes that need to remain or be recycled.
     *   We assume that exceptions while mutating are not normal and should not happen, and thus a temporary leak (e.g.
     *   until the memtable is switched) is acceptable. Should this change (e.g. if a trie is used for the full lifetime
     *   of the process or longer and exceptions are expected as part of its function), we can implement a reachability
     *   walk to identify orphaned indexes and call it with some frequency after one or more exceptions have occured.
     */
    static class OpOrderReuseStrategy implements MemoryAllocationStrategy
    {
        /**
         * Cells list holding indexes that are just recycled. When full, new one is allocated and linked.
         *
         * On mutationComplete, any full (in justReleased.nextList) lists get issued a barrier and are moved to
         * awaitingBarrierTail.
         */
        IndexBlockList justReleased;

        /**
         * Tail of the "free and awaiting barrier" queue. This is reachable by following the links from free.
         *
         * Full lists are attached to this tail when their barrier is issued.
         * Lists are consumed from the head when free becomes empty if the list at the head has an expired barrier.
         */
        IndexBlockList awaitingBarrierTail;

        /**
         * Current free list, head of the "free and awaiting barrier" queue. Allocations are served from here.
         *
         * Starts full, and when it is exhausted we check the barrier at the next linked block.
         * If expired, update free to point to it (consuming one block from the queue).
         * If not, re-fill the block by allocating a new set of REUSE_BLOCK_SIZE indexes.
         */
        IndexBlockList free;

        /**
         * Called to allocate a new block of indexes to distribute.
         */
        final Allocator allocator;
        final OpOrder opOrder;

        public OpOrderReuseStrategy(Allocator allocator, OpOrder opOrder)
        {
            this.allocator = allocator;
            this.opOrder = opOrder;
            justReleased = new IndexBlockList(null);
            awaitingBarrierTail = free = new IndexBlockList(null);
            free.count = 0;
        }

        @Override
        public int allocate() throws TrieSpaceExhaustedException
        {
            if (free.count == 0)
            {
                IndexBlockList awaitingBarrierHead = free.nextList;
                if (awaitingBarrierHead != null &&
                    (awaitingBarrierHead.barrier == null || awaitingBarrierHead.barrier.allPriorOpsAreFinished()))
                {
                    // A block is ready for reuse. Switch to it.
                    free = awaitingBarrierHead;
                    // Index blocks only enter these lists when the justReleased block is filled. Sanity check that
                    // the block is still full.
                    assert free.count == free.indexes.length;
                    // We could recycle/pool the IndexBlockList object that free was pointing to before this.
                    // As the trie will create and drop many times more objects to end up filling one of these, the
                    // potential impact does not appear to justify the extra complexity.
                }
                else
                {
                    // Nothing available for reuse. Grab more memory.
                    allocator.allocate(free.indexes);
                    free.count = free.indexes.length;
                }
            }

            return free.indexes[--free.count];
        }

        @Override
        public void recycle(int index)
        {
            if (justReleased.count == REUSE_BLOCK_SIZE)
            {
                // Block is full, allocate and attach a new one.
                justReleased = new IndexBlockList(justReleased);
            }

            justReleased.indexes[justReleased.count++] = index;
        }

        @Override
        public void completeMutation()
        {
            IndexBlockList toProcess = justReleased.nextList;
            if (toProcess == null)
                return;

            // We have some completed blocks now, issue a barrier for them and move them to the
            // "free and awaiting barrier" queue.
            justReleased.nextList = null;

            OpOrder.Barrier barrier = null;
            if (opOrder != null)
            {
                barrier = opOrder.newBarrier();
                barrier.issue();
            }

            IndexBlockList last = null;
            for (IndexBlockList current = toProcess; current != null; current = current.nextList)
            {
                current.barrier = barrier;
                last = current;
            }

            assert awaitingBarrierTail.nextList == null;
            awaitingBarrierTail.nextList = toProcess;
            awaitingBarrierTail = last;
        }

        @Override
        public void abortMutation()
        {
            // Some of the releases in the justReleased queue may still be reachable cells.
            // We don't have a way of telling which, so we have to remove everything.
            justReleased.nextList = null;
            justReleased.count = 0;
        }

        /**
         * Returns the number of indexes that are somewhere in the recycling pipeline.
         */
        @Override
        public long indexCountInPipeline()
        {
            long count = 0;
            for (IndexBlockList list = justReleased; list != null; list = list.nextList)
                count += list.count;
            for (IndexBlockList list = free; list != null; list = list.nextList) // includes awaiting barrier
                count += list.count;
            return count;
        }

        @Override
        public IntArrayList indexesInPipeline()
        {
            IntArrayList res = new IntArrayList((int) indexCountInPipeline(), -1);
            for (IndexBlockList list = justReleased; list != null; list = list.nextList)
                res.addAll(new IntArrayList(list.indexes, list.count, -1));
            for (IndexBlockList list = free; list != null; list = list.nextList) // includes awaiting barrier
                res.addAll(new IntArrayList(list.indexes, list.count, -1));
            return res;
        }
    }


    static final int REUSE_BLOCK_SIZE = 252; // array fits into 1k bytes

    static class IndexBlockList
    {
        final int[] indexes;
        int count;
        OpOrder.Barrier barrier;
        IndexBlockList nextList;

        IndexBlockList(IndexBlockList next)
        {
            indexes = new int[REUSE_BLOCK_SIZE];
            nextList = next;
            count = 0;
        }
    }
}
