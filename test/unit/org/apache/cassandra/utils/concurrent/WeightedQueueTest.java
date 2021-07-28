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

package org.apache.cassandra.utils.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;
import static org.apache.cassandra.utils.concurrent.WeightedQueue.NATURAL_WEIGHER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class WeightedQueueTest
{
    private static WeightedQueue<Object> queue()
    {
        return new WeightedQueue<>(10);
    }

    private WeightedQueue<Object> queue;

    @Before
    public void setUp()
    {
        queue = queue();
    }

    private static WeightedQueue.Weighable weighable(int weight)
    {
        return () -> weight;
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddUnsupported() throws Exception
    {
        queue.add(new Object());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveUnsupported() throws Exception
    {
        queue.remove();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testElementUnsupported() throws Exception
    {
        queue.element();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testPeekUnsupported() throws Exception
    {
        queue.peek();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemainingCapacityUnsupported() throws Exception
    {
        queue.remainingCapacity();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveElementUnsupported() throws Exception
    {
        queue.remove(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContainsAllUnsupported() throws Exception
    {
        queue.containsAll(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddAllUnsupported() throws Exception
    {
        queue.addAll(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveAllUnsupported() throws Exception
    {
        queue.removeAll(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRetainAllUnsupported() throws Exception
    {
        queue.retainAll(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testClearUnsupported() throws Exception
    {
        queue.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSizeUnsupported() throws Exception
    {
        queue.size();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIsEmptyUnsupported() throws Exception
    {
        queue.isEmpty();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContainsUnsupported() throws Exception
    {
        queue.contains(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIteratorUnsupported() throws Exception
    {
        queue.iterator();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToArrayUnsupported() throws Exception
    {
        queue.toArray();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToArray2Unsupported() throws Exception
    {
        queue.toArray( new Object[] {});
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testDrainToUnsupported() throws Exception
    {
        queue.drainTo(new ArrayList<>());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testTimedPollUnsupported() throws Exception
    {
        queue.poll(1, TimeUnit.MICROSECONDS);
    }

    @Test
    public void testDrainToWithLimit() throws Exception
    {
        queue.offer(new Object());
        queue.offer(new Object());
        queue.offer(new Object());
        ArrayList<Object> list = new ArrayList<>();
        queue.drainTo(list, 1);
        assertEquals(1, list.size());
        list.clear();
        queue.drainTo(list, 10);
        assertEquals(2, list.size());
    }

    @Test(expected = NullPointerException.class)
    public void offerNullThrows() throws Exception
    {
        queue.offer(null);
    }

    /**
     * This also tests that natural weight (weighable interface) is respected
     */
    @Test
    public void offerFullFails() throws Exception
    {
        assertTrue(queue.offer(weighable(10)));
        assertFalse(queue.offer(weighable(1)));
    }

    /**
     * Validate permits aren't leaked and return values are correct
     */
    @Test
    public void testOfferWrappedQueueRefuses() throws Exception
    {
        queue = new WeightedQueue<>(10, new BadQueue(true), WeightedQueue.NATURAL_WEIGHER);
        assertEquals(10, queue.availableWeight.permits());
        assertFalse(queue.offer(new Object()));
        assertEquals(10, queue.availableWeight.permits());
    }

    /**
     * Validate permits aren't leaked and return values are correct
     */
    @Test
    public void testOfferWrappedQueueThrows() throws Exception
    {
        queue = new WeightedQueue<>(10, new BadQueue(false), WeightedQueue.NATURAL_WEIGHER);
        assertEquals(10, queue.availableWeight.permits());
        try
        {
            assertFalse(queue.offer(new Object()));
            fail();
        }
        catch (UnsupportedOperationException e)
        {
            //expected and desired
        }
        assertEquals(10, queue.availableWeight.permits());
    }

    /**
     * If not weighable and not custom weigher the default weight is 1
     */
    @Test
    public void defaultWeightRespected() throws Exception
    {
        for (int ii = 0; ii < 10; ii++)
        {
            assertTrue(queue.offer(new Object()));
        }
        assertFalse(queue.offer(new Object()));
    }

    @Test
    public void testCustomWeigher() throws Exception
    {
        queue = new WeightedQueue<>(10, newBlockingQueue(), weighable -> 10 );
        assertTrue(queue.offer(new Object()));
        assertFalse(queue.offer(new Object()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCustomQueue() throws Exception
    {
        new WeightedQueue<>(10, new BadQueue(false), WeightedQueue.NATURAL_WEIGHER).offer(new Object());
    }

    @Test(expected = NullPointerException.class)
    public void timedOfferNullValueThrows() throws Exception
    {
        queue.offer(null, 1, TimeUnit.SECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void timedOfferNullTimeThrows() throws Exception
    {
        queue.offer(null, 1, null);
    }

    /**
     * This is how it seems to be handled in java.util.concurrent, it's the same as just try
     */
    @Test
    public void timedOfferNegativeTimeIgnored() throws Exception
    {
        queue.offer(weighable(10));
        queue.offer(new Object(), -1, TimeUnit.SECONDS);
    }

    /**
     * This also tests that natural weight (weighable interface) is respected
     */
    @Test
    public void timedOfferFullFails() throws Exception
    {
        assertTrue(queue.offer(weighable(10), 1, TimeUnit.MICROSECONDS));
        assertFalse(queue.offer(weighable(1), 1, TimeUnit.MICROSECONDS));
    }

    @Test
    public void timedOfferEventuallySucceeds() throws Exception
    {
        assertTrue(queue.offer(weighable(10), 1, TimeUnit.MICROSECONDS));
        Thread t = new Thread(() ->
          {
              try
              {
                  queue.offer(weighable(1), 1, TimeUnit.DAYS);
              }
              catch (InterruptedException e)
              {
                  e.printStackTrace();
              }
          });
        t.start();
        Thread.sleep(100);
        assertTrue(t.getState() != Thread.State.TERMINATED);
        queue.poll();
        t.join(60000);
        assertEquals(t.getState(), Thread.State.TERMINATED);
    }

    /**
     * Validate permits aren't leaked and return values are correct
     */
    @Test
    public void testTimedOfferWrappedQueueRefuses() throws Exception
    {
        queue = new WeightedQueue<>(10, new BadQueue(true), WeightedQueue.NATURAL_WEIGHER);
        assertEquals(10, queue.availableWeight.permits());
        assertFalse(queue.offer(new Object(), 1, TimeUnit.MICROSECONDS));
        assertEquals(10, queue.availableWeight.permits());
    }

    /**
     * Validate permits aren't leaked and return values are correct
     */
    @Test
    public void testTimedOfferWrappedQueueThrows() throws Exception
    {
        queue = new WeightedQueue<>(10, new BadQueue(false), WeightedQueue.NATURAL_WEIGHER);
        assertEquals(10, queue.availableWeight.permits());
        try
        {
            assertFalse(queue.offer(new Object(), 1, TimeUnit.MICROSECONDS));
            fail();
        }
        catch (UnsupportedOperationException e)
        {
            //expected and desired
        }
        assertEquals(10, queue.availableWeight.permits());
    }


    @Test
    public void testPoll() throws Exception
    {
        assertEquals(10, queue.availableWeight.permits());
        assertNull(queue.poll());
        assertEquals(10, queue.availableWeight.permits());
        Object o = new Object();
        assertTrue(queue.offer(o));
        assertEquals(9, queue.availableWeight.permits());
        WeightedQueue.Weighable weighable = weighable(9);
        assertTrue(queue.offer(weighable));
        assertEquals(0, queue.availableWeight.permits());
        assertEquals(o, queue.poll());
        assertEquals(1, queue.availableWeight.permits());
        assertEquals(weighable, queue.poll());
        assertEquals(10, queue.availableWeight.permits());
    }

    @Test(expected = NullPointerException.class)
    public void testPutNullThrows() throws Exception
    {
        queue.put(null);
    }

    @Test
    public void testPutFullBlocks() throws Exception
    {
        WeightedQueue.Weighable weighable = weighable(10);
        assertEquals(10, queue.availableWeight.permits());
        queue.put(weighable);
        assertEquals(0, queue.availableWeight.permits());
        Object o = new Object();
        Thread t = new Thread(() -> {
            try
            {
                queue.put(o);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        });
        t.start();
        Thread.sleep(100);
        assertTrue(t.getState() != Thread.State.TERMINATED);
        assertEquals(0, queue.availableWeight.permits());
        assertEquals(weighable, queue.poll());
        assertTrue(queue.availableWeight.permits() > 0);
        t.join();
        assertEquals(o, queue.poll());
        assertEquals(10, queue.availableWeight.permits());
    }

    @Test
    public void testPutWrappedQueueThrows() throws Exception
    {
        queue = new WeightedQueue<>(10, new BadQueue(false), WeightedQueue.NATURAL_WEIGHER);
        assertEquals(10, queue.availableWeight.permits());
        try
        {
            queue.put(new Object());
            fail();
        }
        catch (UnsupportedOperationException e)
        {
            //expected and desired
        }
        assertEquals(10, queue.availableWeight.permits());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTryAcquireWeightIllegalWeight()
    {
        queue.tryAcquireWeight(weighable(-1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAcquireWeightIllegalWeight() throws Exception
    {
        queue.acquireWeight(weighable(-1), 1, TimeUnit.DAYS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReleaseWeightIllegalWeight()
    {
        queue.releaseWeight(weighable(-1));
    }

    @Test
    public void testTake() throws Exception
    {
        Thread t = new Thread(() -> {
            try
            {
                queue.take();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        });
        t.start();
        Thread.sleep(500);
        assertTrue(t.getState() != Thread.State.TERMINATED);
        assertEquals(10, queue.availableWeight.permits());
        queue.offer(new Object());
        t.join(60 * 1000);
        assertEquals(t.getState(), Thread.State.TERMINATED);
        assertEquals(10, queue.availableWeight.permits());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorLTZeroWeightThrows() throws Exception
    {
        new WeightedQueue(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor2LTZeroWeightThrows() throws Exception
    {
        new WeightedQueue(0, newBlockingQueue(), NATURAL_WEIGHER);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullQueueThrows() throws Exception
    {
        new WeightedQueue(1, null, WeightedQueue.NATURAL_WEIGHER);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorNullWeigherThrows() throws Exception
    {
        new WeightedQueue(1, newBlockingQueue(), null);
    }

    /**
     * A blocking queue that throws or refuses on every method
     */
    private static class BadQueue implements BlockingQueue<Object>
    {
        /**
         * Refuse instead of throwing for some methods that have a boolean return value
         */
        private boolean refuse = false;

        private BadQueue(boolean refuse)
        {
            this.refuse = refuse;
        }

        @Override
        public boolean add(Object o)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean offer(Object o)
        {
            if (refuse)
            {
                return false;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public Object remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object poll()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object element()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object peek()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(Object o) throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException
        {
            if (refuse)
            {
                return false;
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public Object take() throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object poll(long timeout, TimeUnit unit) throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int remainingCapacity()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll(Collection c)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection c)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection c)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection c)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains(Object o)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator iterator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray(Object[] a)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int drainTo(Collection c)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int drainTo(Collection c, int maxElements)
        {
            throw new UnsupportedOperationException();
        }
    };

}
