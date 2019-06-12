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

import org.junit.Test;

import org.apache.cassandra.net.PrunableArrayQueue;

import static org.junit.Assert.*;

public class PrunableArrayQueueTest
{
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
}