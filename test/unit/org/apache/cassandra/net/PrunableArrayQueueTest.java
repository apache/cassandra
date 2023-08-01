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
package org.apache.cassandra.net;

import java.util.Random;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class PrunableArrayQueueTest
{
    private static final Logger logger = LoggerFactory.getLogger(PrunableArrayQueueTest.class);

    private final PrunableArrayQueue<Integer> queue = new PrunableArrayQueue<>(8);

    @Test
    public void testIsEmptyWhenEmpty()
    {
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testIsEmptyWhenNotEmpty()
    {
        queue.offer(0);
        assertFalse(queue.isEmpty());
    }

    @Test
    public void testEmptyPeek()
    {
        assertNull(queue.peek());
    }

    @Test
    public void testNonEmptyPeek()
    {
        queue.offer(0);
        assertEquals((Integer) 0, queue.peek());
    }

    @Test
    public void testEmptyPoll()
    {
        assertNull(queue.poll());
    }

    @Test
    public void testNonEmptyPoll()
    {
        queue.offer(0);
        assertEquals((Integer) 0, queue.poll());
    }

    @Test
    public void testTransfersInCorrectOrder()
    {
        for (int i = 0; i < 1024; i++)
            queue.offer(i);

        for (int i = 0; i < 1024; i++)
            assertEquals((Integer) i, queue.poll());

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testTransfersInCorrectOrderWhenInterleaved()
    {
        for (int i = 0; i < 1024; i++)
        {
            queue.offer(i);
            assertEquals((Integer) i, queue.poll());
        }

        assertTrue(queue.isEmpty());
    }

    @Test
    public void testPrune()
    {
        for (int i = 0; i < 1024; i++)
            queue.offer(i);

        class Pruner implements PrunableArrayQueue.Pruner<Integer>
        {
            private int pruned, kept;

            public boolean shouldPrune(Integer val)
            {
                return val % 2 == 0;
            }

            public void onPruned(Integer val)
            {
                pruned++;
            }

            public void onKept(Integer val)
            {
                kept++;
            }
        }

        Pruner pruner = new Pruner();
        assertEquals(512, queue.prune(pruner));

        assertEquals(512, pruner.kept);
        assertEquals(512, pruner.pruned);
        assertEquals(512, queue.size());

        for (int i = 1; i < 1024; i += 2)
            assertEquals((Integer) i, queue.poll());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testUnreliablePruner()
    {
        long seed = System.currentTimeMillis();
        Random rand = new Random(seed);

        logger.info("Testing unreliable pruner with random seed {}...", seed);

        int iteratons = 100;
        int startingQueueSize = 1024;
        double pruneChance = 0.1;
        double errorOnKeptChance = 0.00005;
        double errorOnPruneChance = 0.00002;

        for (int i = 0; i < iteratons; i++)
        {
            int failureValue = rand.nextInt(startingQueueSize);

            PrunableArrayQueue<Integer> testQueue = new PrunableArrayQueue<>(startingQueueSize);

            for (int o = 0; o < startingQueueSize; o++)
                testQueue.offer(o);

            class UnreliablePruner implements PrunableArrayQueue.Pruner<Integer>
            {
                public boolean shouldPrune(Integer value)
                {
                    if (rand.nextDouble() < errorOnPruneChance)
                        throw new RuntimeException("Failed on pruning check for value: " + value);

                    return rand.nextDouble() < pruneChance;
                }

                public void onPruned(Integer value)
                {
                    if (value == failureValue)
                        throw new RuntimeException("Failed on pruned value: " + value);
                }

                public void onKept(Integer value)
                {
                    if (rand.nextDouble() < errorOnKeptChance)
                        throw new RuntimeException("Failed on retained value: " + value);
                }
            }

            assertEquals(startingQueueSize, testQueue.size());

            try
            {
                testQueue.prune(new UnreliablePruner());
            }
            catch (RuntimeException e)
            {
                logger.info("Expected pruning failure with seed {}", seed, e);
            }

            for (int p = 0, postPruneSize = testQueue.size(); p < postPruneSize; p++)
            {
                assertNotNull("Queue should contain no null elements after pruning. Seed: " + seed + ". Iteration: " + i, testQueue.poll());
            }

            assertEquals("Queue size should be zero after draining. Seed: " + seed + ". Iteration: " + i, 0, testQueue.size());
        }
    }
}
