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

import java.util.BitSet;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("ConstantConditions")
public class ManyToOneConcurrentLinkedQueueTest
{
    private final ManyToOneConcurrentLinkedQueue<Integer> queue = new ManyToOneConcurrentLinkedQueue<>();

    @Test
    public void testRelaxedIsEmptyWhenEmpty()
    {
        assertTrue(queue.relaxedIsEmpty());
    }

    @Test
    public void testRelaxedIsEmptyWhenNotEmpty()
    {
        queue.offer(0);
        assertFalse(queue.relaxedIsEmpty());
    }

    @Test
    public void testSizeWhenEmpty()
    {
        assertEquals(0, queue.size());
    }

    @Test
    public void testSizeWhenNotEmpty()
    {
        queue.offer(0);
        assertEquals(1, queue.size());

        for (int i = 1; i < 100; i++)
            queue.offer(i);
        assertEquals(100, queue.size());
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
        assertEquals(0, (int) queue.peek());
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
        assertEquals(0, (int) queue.poll());
    }

    @Test(expected = NoSuchElementException.class)
    public void testEmptyRemove()
    {
        queue.remove();
    }

    @Test
    public void testNonEmptyRemove()
    {
        queue.offer(0);
        assertEquals(0, (int) queue.remove());
    }

    @Test
    public void testOtherRemoveWhenEmpty()
    {
        assertFalse(queue.remove(0));
    }

    @Test
    public void testOtherRemoveSingleNode()
    {
        queue.offer(0);
        assertTrue(queue.remove(0));
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testOtherRemoveWhenFirst()
    {
        queue.offer(0);
        queue.offer(1);
        queue.offer(2);

        assertTrue(queue.remove(0));

        assertEquals(1, (int) queue.poll());
        assertEquals(2, (int) queue.poll());
        assertNull(queue.poll());
    }

    @Test
    public void testOtherRemoveFromMiddle()
    {
        queue.offer(0);
        queue.offer(1);
        queue.offer(2);

        assertTrue(queue.remove(1));

        assertEquals(0, (int) queue.poll());
        assertEquals(2, (int) queue.poll());
        assertNull(queue.poll());
    }

    @Test
    public void testOtherRemoveFromEnd()
    {
        queue.offer(0);
        queue.offer(1);
        queue.offer(2);

        assertTrue(queue.remove(2));

        assertEquals(0, (int) queue.poll());
        assertEquals(1, (int) queue.poll());
        assertNull(queue.poll());
    }

    @Test
    public void testOtherRemoveWhenDoesnNotExist()
    {
        queue.offer(0);
        queue.offer(1);
        queue.offer(2);

        assertFalse(queue.remove(3));

        assertEquals(0, (int) queue.poll());
        assertEquals(1, (int) queue.poll());
        assertEquals(2, (int) queue.poll());
    }

    @Test
    public void testTransfersInCorrectOrder()
    {
        for (int i = 0; i < 1024; i++)
            queue.offer(i);

        for (int i = 0; i < 1024; i++)
            assertEquals(i, (int) queue.poll());

        assertTrue(queue.relaxedIsEmpty());
    }

    @Test
    public void testTransfersInCorrectOrderWhenInterleaved()
    {
        for (int i = 0; i < 1024; i++)
        {
            queue.offer(i);
            assertEquals(i, (int) queue.poll());
        }

        assertTrue(queue.relaxedIsEmpty());
    }

    @Test
    public void testDrain()
    {
        for (int i = 0; i < 1024; i++)
            queue.offer(i);

        class Consumer
        {
            private int previous = -1;

            public void accept(int i)
            {
                assertEquals(++previous, i);
            }
        }

        Consumer consumer = new Consumer();
        queue.drain(consumer::accept);

        assertEquals(1023, consumer.previous);
        assertTrue(queue.relaxedIsEmpty());
    }

    @Test
    public void testPeekLastAndOffer()
    {
        assertNull(queue.relaxedPeekLastAndOffer(0));
        for (int i = 1; i < 1024; i++)
            assertEquals(i - 1, (int) queue.relaxedPeekLastAndOffer(i));

        for (int i = 0; i < 1024; i++)
            assertEquals(i, (int) queue.poll());

        assertTrue(queue.relaxedIsEmpty());
    }

    enum Strategy
    {
        PEEK_AND_REMOVE, POLL
    }

    @Test
    public void testConcurrentlyWithPoll()
    {
        testConcurrently(Strategy.POLL);
    }

    @Test
    public void testConcurrentlyWithPeekAndRemove()
    {
        testConcurrently(Strategy.PEEK_AND_REMOVE);
    }

    private void testConcurrently(Strategy strategy)
    {
        int numThreads = 4;
        int numItems = 1_000_000 * numThreads;

        class Producer implements Runnable
        {
            private final int start, step, limit;

            private Producer(int start, int step, int limit)
            {
                this.start = start;
                this.step = step;
                this.limit = limit;
            }

            public void run()
            {
                for (int i = start; i < limit; i += step)
                    queue.offer(i);
            }
        }

        Executor executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++)
            executor.execute(new Producer(i, numThreads, numItems));

        BitSet itemsPolled = new BitSet(numItems);
        for (int i = 0; i < numItems; i++)
        {
            Integer item;
            switch (strategy)
            {
                case PEEK_AND_REMOVE:
                    //noinspection StatementWithEmptyBody
                    while ((item = queue.peek()) == null) ;
                    assertFalse(queue.relaxedIsEmpty());
                    assertEquals(item, queue.remove());
                    itemsPolled.set(item);
                    break;
                case POLL:
                    //noinspection StatementWithEmptyBody
                    while ((item = queue.poll()) == null) ;
                    itemsPolled.set(item);
                    break;
            }
        }

        assertEquals(numItems, itemsPolled.cardinality());
        assertTrue(queue.relaxedIsEmpty());
    }
}
