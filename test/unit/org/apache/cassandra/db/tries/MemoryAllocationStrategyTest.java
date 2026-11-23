/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.tries.MemoryAllocationStrategy.IndexBlockList;
import org.apache.cassandra.db.tries.MemoryAllocationStrategy.OpOrderReuseStrategy;
import org.apache.cassandra.db.tries.MemoryAllocationStrategy.OpOrderReuseWithClearingStrategy;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.MemoryAllocationStrategy.REUSE_BLOCK_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/// Tests of the recycling pipeline of the `OpOrder`-based [MemoryAllocationStrategy] implementations, in particular
/// that the strategies do not hold on to state they no longer need, the behaviour of
/// [MemoryAllocationStrategy#stopRecycling], and that [MemoryAllocationStrategy#indexCountInPipeline] stays correct.
public class MemoryAllocationStrategyTest
{
    /// Released indexes are numbered from here, so that they can be told apart from new ones.
    private static final int RELEASED_START = 1 << 20;

    private int nextNew = 0;
    private int nextReleased = RELEASED_START;

    private int allocateNew()
    {
        return nextNew++;
    }

    /// Releases indexes until the given number of blocks are full, and returns the indexes in the full blocks, i.e.
    /// the ones the next `completeMutation` will issue a barrier for. The index that does not fit in the last full
    /// block starts a new one, which stays in the `justReleased` stage.
    private IntArrayList releaseBlocks(OpOrderReuseStrategy strategy, int blocks)
    {
        while (fullBlocks(strategy) < blocks)
            strategy.recycle(nextReleased++);

        IntArrayList result = new IntArrayList();
        for (IndexBlockList list = strategy.justReleased.nextList; list != null; list = list.nextList)
            for (int i = 0; i < list.count; ++i)
                result.addInt(list.indexes[i]);
        return result;
    }

    private static int fullBlocks(OpOrderReuseStrategy strategy)
    {
        int count = 0;
        for (IndexBlockList list = strategy.justReleased.nextList; list != null; list = list.nextList)
            ++count;
        return count;
    }

    private static IntArrayList justReleased(OpOrderReuseStrategy strategy)
    {
        IndexBlockList list = strategy.justReleased;
        IntArrayList result = new IntArrayList();
        for (int i = 0; i < list.count; ++i)
            result.addInt(list.indexes[i]);
        return result;
    }

    private static Set<Integer> allocate(MemoryAllocationStrategy strategy, int count) throws TrieSpaceExhaustedException
    {
        Set<Integer> result = new HashSet<>();
        for (int i = 0; i < count; ++i)
            assertTrue(result.add(strategy.allocate()));
        return result;
    }

    private static void assertNoneReused(Set<Integer> allocated, IntArrayList released, String message)
    {
        for (int index : released)
            assertFalse(message + ": " + index, allocated.contains(index));
    }

    /// Checks the incrementally maintained count against the indexes found by walking the pipeline.
    private static void assertCountMatches(MemoryAllocationStrategy strategy)
    {
        assertEquals(strategy.indexesInPipeline().size(), strategy.indexCountInPipeline());
    }

    @Test
    public void testAdvanceDetachesExhaustedBlock() throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        OpOrderReuseStrategy strategy = new OpOrderReuseStrategy(this::allocateNew, order);

        // Two blocks released in separate mutations, so that they have separate barriers. No operations are running,
        // so both barriers expire immediately.
        IntArrayList first = releaseBlocks(strategy, 1);
        strategy.completeMutation();
        IntArrayList second = releaseBlocks(strategy, 1);
        strategy.completeMutation();

        IndexBlockList initial = strategy.free;
        IndexBlockList firstBlock = initial.nextList;
        assertNotNull(firstBlock.barrier);

        // The initial free block is empty, so the strategy switches to the first block straight away.
        assertEquals(new HashSet<>(first), allocate(strategy, REUSE_BLOCK_SIZE));
        assertSame(firstBlock, strategy.free);
        assertNull("exhausted block still links to the queue", initial.nextList);
        assertNull("expired barrier retained", firstBlock.barrier);
        assertNotNull(firstBlock.nextList);

        assertEquals(new HashSet<>(second), allocate(strategy, REUSE_BLOCK_SIZE));
        assertNull("exhausted block still links to the queue", firstBlock.nextList);
        assertNull("expired barrier retained", strategy.free.barrier);

        // Nothing left to reuse.
        assertTrue(strategy.allocate() < RELEASED_START);
    }

    @Test
    public void testStopRecyclingDropsPipeline() throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        OpOrderReuseStrategy strategy = new OpOrderReuseStrategy(this::allocateNew, order);

        IntArrayList expired = releaseBlocks(strategy, 1);
        strategy.completeMutation();
        OpOrder.Group reader = order.start();
        IntArrayList waiting = releaseBlocks(strategy, 1);
        strategy.completeMutation();

        strategy.stopRecycling();
        assertNull(strategy.free.nextList);
        assertSame(strategy.free, strategy.awaitingBarrierTail);
        assertNull(strategy.justReleased.nextList);
        // Only the indexes in the block being filled remain.
        assertEquals(strategy.justReleased.count + strategy.free.count, strategy.indexCountInPipeline());
        assertCountMatches(strategy);

        // The dropped indexes are never reused, even once their barriers have expired.
        reader.close();
        Set<Integer> allocated = allocate(strategy, 3 * REUSE_BLOCK_SIZE);
        assertNoneReused(allocated, expired, "index from dropped block with expired barrier reused");
        assertNoneReused(allocated, waiting, "index from dropped block awaiting barrier reused");
    }

    @Test
    public void testRecyclingAfterStopRecycling() throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        OpOrderReuseStrategy strategy = new OpOrderReuseStrategy(this::allocateNew, order);

        // Some indexes are released by the last mutation before stopRecycling, and a read that may still be using them
        // is in progress.
        OpOrder.Group reader = order.start();
        for (int i = 0; i < 10; ++i)
            strategy.recycle(nextReleased++);
        strategy.completeMutation();
        IntArrayList beforeStop = justReleased(strategy);
        strategy.stopRecycling();

        // Unexpected mutations after stopRecycling. The indexes released before it go through the pipeline with the
        // ones released after it, and must not be reused until the read completes.
        IntArrayList afterStop = releaseBlocks(strategy, 1);
        assertTrue(afterStop.containsAll(beforeStop));
        strategy.completeMutation();
        assertNoneReused(allocate(strategy, REUSE_BLOCK_SIZE), afterStop, "index reused while a read is in progress");

        reader.close();
        assertEquals(new HashSet<>(afterStop), allocate(strategy, REUSE_BLOCK_SIZE));
    }

    @Test
    public void testStopRecyclingClearsExpiredBlocks() throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        Set<Integer> cleared = new HashSet<>();
        OpOrderReuseWithClearingStrategy strategy = new OpOrderReuseWithClearingStrategy(this::allocateNew, cleared::add, order);

        OpOrder.Group firstReader = order.start();
        IntArrayList first = releaseBlocks(strategy, 1);
        strategy.completeMutation();
        OpOrder.Group secondReader = order.start();
        IntArrayList second = releaseBlocks(strategy, 1);
        strategy.completeMutation();
        assertTrue(cleared.isEmpty());

        // The barrier of the first block expires, but clearing only advances on mutations.
        firstReader.close();
        assertTrue(cleared.isEmpty());

        // Stopping recycling clears the blocks whose barrier has expired.
        strategy.stopRecycling();
        assertEquals(new HashSet<>(first), cleared);

        // The references in the other blocks are left in place, even after their barrier expires and later
        // (unexpected) mutations clear the blocks they release.
        secondReader.close();
        IntArrayList third = releaseBlocks(strategy, 1);
        strategy.completeMutation();
        Set<Integer> expected = new HashSet<>(first);
        expected.addAll(third);
        assertEquals(expected, cleared);
        for (int index : second)
            assertFalse(cleared.contains(index));
    }

    @Test
    public void testAbortMutationDropsJustReleased() throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        OpOrderReuseStrategy strategy = new OpOrderReuseStrategy(this::allocateNew, order);

        // A completed mutation leaves a block awaiting reuse, and an index in the block being filled.
        IntArrayList completed = releaseBlocks(strategy, 1);
        strategy.completeMutation();
        assertEquals(1, justReleased(strategy).size());

        // The aborted mutation fills two more blocks and part of a third.
        releaseBlocks(strategy, 2);
        for (int i = 0; i < 10; ++i)
            strategy.recycle(nextReleased++);
        strategy.abortMutation();

        // All indexes in the justReleased stage are dropped, including the one released by the completed mutation.
        assertEquals(completed.size(), strategy.indexCountInPipeline());
        assertCountMatches(strategy);

        // The block awaiting reuse is not affected.
        assertEquals(new HashSet<>(completed), allocate(strategy, REUSE_BLOCK_SIZE));
        assertEquals(0, strategy.indexCountInPipeline());
        assertCountMatches(strategy);
    }

    @Test
    public void testIndexCountInPipeline() throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        testIndexCountInPipeline(new OpOrderReuseStrategy(this::allocateNew, order), order);
    }

    @Test
    public void testIndexCountInPipelineWithClearing() throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        testIndexCountInPipeline(new OpOrderReuseWithClearingStrategy(this::allocateNew, index -> {}, order), order);
    }

    /// Checks that the incrementally maintained [MemoryAllocationStrategy#indexCountInPipeline] stays equal to the
    /// number of indexes in the pipeline through a random sequence of mutations (some of them aborted), reads that
    /// hold back the reuse of the indexes released while they are in progress, and calls to `stopRecycling`.
    private void testIndexCountInPipeline(OpOrderReuseStrategy strategy, OpOrder order) throws TrieSpaceExhaustedException
    {
        Random rand = new Random(1);
        IntArrayList live = new IntArrayList();
        ArrayDeque<OpOrder.Group> readers = new ArrayDeque<>();
        assertCountMatches(strategy);

        for (int step = 0; step < 1000; ++step)
        {
            // Vary the share of releases, so that the pipeline both grows and shrinks.
            double releaseShare = rand.nextDouble();
            int ops = rand.nextInt(2 * REUSE_BLOCK_SIZE);
            for (int i = 0; i < ops; ++i)
            {
                if (!live.isEmpty() && rand.nextDouble() < releaseShare)
                    strategy.recycle(live.fastUnorderedRemove(rand.nextInt(live.size())));
                else
                    live.addInt(strategy.allocate());
            }

            if (rand.nextInt(10) == 0)
                strategy.abortMutation();
            else
                strategy.completeMutation();
            assertCountMatches(strategy);

            if (rand.nextInt(3) == 0)
                readers.add(order.start());
            if (rand.nextInt(3) == 0)
                while (!readers.isEmpty() && rand.nextBoolean())
                    readers.remove().close();

            if (rand.nextInt(100) == 0)
            {
                strategy.stopRecycling();
                assertCountMatches(strategy);
            }
        }

        while (!readers.isEmpty())
            readers.remove().close();
        // Reuse everything that is available.
        for (int i = 0; i < 10 * REUSE_BLOCK_SIZE; ++i)
            strategy.allocate();
        assertCountMatches(strategy);
    }
}
