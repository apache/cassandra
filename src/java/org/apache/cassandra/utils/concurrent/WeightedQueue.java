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

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.concurrent.BlockingQueues.newBlockingQueue;
import static org.apache.cassandra.utils.concurrent.Semaphore.newSemaphore;

/**
 * Weighted queue is a wrapper around any blocking queue that turns it into a blocking weighted queue. The queue
 * will weigh each element being added and removed. Adding to the queue is blocked if adding would violate
 * the weight bound.
 *
 * If an element weighs in at larger than the capacity of the queue then exactly one such element will be allowed
 * into the queue at a time.
 *
 * If the weight of an object changes after it is added you are going to have a bad time. Checking weight should be
 * cheap so memoize expensive to compute weights. If weight throws that can also result in leaked permits so it's
 * always a good idea to memoize weight so it doesn't throw.
 *
 * In the interests of not writing unit tests for methods no one uses there is a lot of UnsupportedOperationException.
 * If you need them then add them and add proper unit tests to WeightedQueueTest. "Good" tests. 100% coverage including
 * exception paths and resource leaks.
 **/
public class WeightedQueue<T> implements BlockingQueue<T>
{
    private static final Logger logger = LoggerFactory.getLogger(WeightedQueue.class);
    public static final Weigher NATURAL_WEIGHER = (Weigher<Object>) weighable ->
    {
        if (weighable instanceof Weighable)
        {
            return ((Weighable)weighable).weight();
        }
        return 1;
    };

    private final Weigher<T> weigher;
    private final BlockingQueue<T> queue;
    private final int maxWeight;
    final Semaphore availableWeight;

    public boolean add(T e)
    {
        throw new UnsupportedOperationException();
    }

    public boolean offer(T t)
    {
        Preconditions.checkNotNull(t);
        boolean acquired = tryAcquireWeight(t);
        if (acquired)
        {
            boolean offered = false;
            try
            {
                offered = queue.offer(t);
                return offered;
            }
            finally
            {
                if (!offered)
                {
                    releaseWeight(t);
                }
            }
        }
        return false;
    }

    public T remove()
    {
        throw new UnsupportedOperationException();
    }

    public T poll()
    {
        T retval = queue.poll();
        releaseWeight(retval);
        return retval;
    }

    public T element()
    {
        throw new UnsupportedOperationException();
    }

    public T peek()
    {
        throw new UnsupportedOperationException();
    }

    public void put(T t) throws InterruptedException
    {
        Preconditions.checkNotNull(t);
        acquireWeight(t, 0, null);
        boolean put = false;
        try
        {
            queue.put(t);
            put = true;
        }
        finally
        {
            if (!put)
            {
                releaseWeight(t);
            }
        }
    }

    public boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException
    {
        Preconditions.checkNotNull(t);
        Preconditions.checkNotNull(unit);
        boolean acquired = acquireWeight(t, timeout, unit);
        if (acquired)
        {
            boolean offered = false;
            try
            {
                offered = queue.offer(t, timeout, unit);
                return offered;
            }
            finally
            {
                if (!offered)
                {
                    releaseWeight(t);
                }
            }
        }
        return false;
    }

    public T take() throws InterruptedException
    {
        T retval = queue.take();
        releaseWeight(retval);
        return retval;
    }

    public T poll(long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    public int remainingCapacity()
    {
        throw new UnsupportedOperationException("Seems like a bad idea");
    }

    public boolean remove(Object o)
    {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(Collection<?> c)
    {
        throw new UnsupportedOperationException("Seems like a bad idea");
    }

    public boolean addAll(Collection<? extends T> c)
    {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> c)
    {
        throw new UnsupportedOperationException("Seems like a bad idea");
    }

    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException("Seems like a bad idea");
    }

    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    public int size()
    {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty()
    {
        throw new UnsupportedOperationException();
    }

    public boolean contains(Object o)
    {
        throw new UnsupportedOperationException("Seems like a bad idea");
    }

    public Iterator<T> iterator()
    {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray()
    {
        throw new UnsupportedOperationException();
    }

    public <T1> T1[] toArray(T1[] a)
    {
        throw new UnsupportedOperationException();
    }

    public int drainTo(Collection<? super T> c)
    {
        throw new UnsupportedOperationException();
    }

    public int drainTo(Collection<? super T> c, int maxElements)
    {
        int count = 0;
        T o;
        while(count < maxElements && (o = poll()) != null)
        {
            c.add(o);
            count++;
        }
        return count;
    }

    public interface Weigher<T>
    {
        int weigh(T weighable);
    }

    public interface Weighable
    {
        int weight();
    }

    public WeightedQueue(int maxWeight)
    {
        this(maxWeight, newBlockingQueue(), NATURAL_WEIGHER);
    }

    public WeightedQueue(int maxWeight, BlockingQueue<T> queue, Weigher<T> weigher)
    {
        Preconditions.checkNotNull(queue);
        Preconditions.checkNotNull(weigher);
        Preconditions.checkArgument(maxWeight > 0);
        this.maxWeight = maxWeight;
        this.queue = queue;
        this.weigher = weigher;
        availableWeight = newSemaphore(maxWeight);
    }

    boolean acquireWeight(T weighable, long timeout, TimeUnit unit) throws InterruptedException
    {
        int weight = weigher.weigh(weighable);
        if (weight < 1)
        {
            throw new IllegalArgumentException(String.format("Weighable: \"%s\" had illegal weight %d", Objects.toString(weighable), weight));
        }

        //Allow exactly one overweight element
        weight = Math.min(maxWeight, weight);

        if (unit != null)
        {
            return availableWeight.tryAcquire(weight, timeout, unit);
        }
        else
        {
            availableWeight.acquire(weight);
            return true;
        }
    }

    boolean tryAcquireWeight(T weighable)
    {
        int weight = weigher.weigh(weighable);
        if (weight < 1)
        {
            throw new IllegalArgumentException(String.format("Weighable: \"%s\" had illegal weight %d", Objects.toString(weighable), weight));
        }

        //Allow exactly one overweight element
        weight = Math.min(maxWeight, weight);

        return availableWeight.tryAcquire(weight);
    }

    void releaseWeight(T weighable)
    {
        if (weighable == null)
        {
            return;
        }

        int weight = weigher.weigh(weighable);
        if (weight < 1)
        {
            throw new IllegalArgumentException(String.format("Weighable: \"%s\" had illegal weight %d", Objects.toString(weighable), weight));
        }

        //Allow exactly one overweight element
        weight = Math.min(maxWeight, weight);

        availableWeight.release(weight);
    }
}
