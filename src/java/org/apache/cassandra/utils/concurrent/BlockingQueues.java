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
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue; // checkstyle: permit this import
import java.util.concurrent.SynchronousQueue; // checkstyle: permit this import
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Intercept;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.Awaitable.SyncAwaitable.waitUntil;

public class BlockingQueues
{
    @Intercept
    public static <T> BlockingQueue<T> newBlockingQueue()
    {
        return new LinkedBlockingQueue<>();
    }

    @Intercept
    public static <T> BlockingQueue<T> newBlockingQueue(int capacity)
    {
        return capacity == 0 ? new SynchronousQueue<>()
                             : new LinkedBlockingQueue<>(capacity);
    }

    public static class Sync<T> implements BlockingQueue<T>
    {
        final int capacity;
        final Queue<T> wrapped;
        public Sync(int capacity, Queue<T> wrapped)
        {
            this.capacity = capacity;
            this.wrapped = wrapped;
        }

        public synchronized boolean add(T t)
        {
            if (!wrapped.add(t))
                throw new IllegalStateException();
            notify();
            return true;
        }

        public synchronized boolean offer(T t)
        {
            if (wrapped.size() == capacity)
                return false;
            return add(t);
        }

        public synchronized T remove()
        {
            return poll();
        }

        public synchronized T poll()
        {
            if (wrapped.size() == capacity)
                notify();

            return wrapped.poll();
        }

        public synchronized T element()
        {
            return wrapped.element();
        }

        public synchronized T peek()
        {
            return wrapped.peek();
        }

        public synchronized void put(T t) throws InterruptedException
        {
            while (!offer(t))
                wait();
        }

        public synchronized boolean offer(T t, long timeout, TimeUnit unit) throws InterruptedException
        {
            if (offer(t))
                return true;

            long deadline = nanoTime() + unit.toNanos(timeout);
            while (true)
            {
                if (offer(t))
                    return true;

                if (!waitUntil(this, deadline))
                    return false;
            }
        }

        public synchronized T take() throws InterruptedException
        {
            T result;
            while (null == (result = poll()))
                wait();

            return result;
        }

        public synchronized T poll(long timeout, TimeUnit unit) throws InterruptedException
        {
            T result = poll();
            if (result != null)
                return result;

            long deadline = nanoTime() + unit.toNanos(timeout);
            while (null == (result = poll()))
            {
                if (!waitUntil(this, deadline))
                    return null;
            }
            return result;
        }

        public synchronized int remainingCapacity()
        {
            return capacity - wrapped.size();
        }

        public synchronized boolean remove(Object o)
        {
            if (!wrapped.remove(o))
                return false;
            if (wrapped.size() == capacity - 1)
                notify();
            return true;
        }

        public synchronized boolean containsAll(Collection<?> c)
        {
            return wrapped.containsAll(c);
        }

        public synchronized boolean addAll(Collection<? extends T> c)
        {
            c.forEach(this::add);
            return true;
        }

        public synchronized boolean removeAll(Collection<?> c)
        {
            boolean result = wrapped.removeAll(c);
            notifyAll();
            return result;
        }

        public synchronized boolean retainAll(Collection<?> c)
        {
            boolean result = wrapped.retainAll(c);
            notifyAll();
            return result;
        }

        public synchronized void clear()
        {
            wrapped.clear();
            notifyAll();
        }

        public synchronized int size()
        {
            return wrapped.size();
        }

        public synchronized boolean isEmpty()
        {
            return wrapped.isEmpty();
        }

        public synchronized boolean contains(Object o)
        {
            return wrapped.contains(o);
        }

        public synchronized Iterator<T> iterator()
        {
            Iterator<T> iter = wrapped.iterator();
            return new Iterator<T>()
            {
                public boolean hasNext()
                {
                    synchronized (Sync.this)
                    {
                        return iter.hasNext();
                    }
                }

                public T next()
                {
                    synchronized (Sync.this)
                    {
                        return iter.next();
                    }
                }
            };
        }

        public synchronized Object[] toArray()
        {
            return wrapped.toArray();
        }

        public synchronized <T1> T1[] toArray(T1[] a)
        {
            return wrapped.toArray(a);
        }

        public synchronized int drainTo(Collection<? super T> c)
        {
            return drainTo(c, Integer.MAX_VALUE);
        }

        public synchronized int drainTo(Collection<? super T> c, int maxElements)
        {
            int count = 0;
            while (count < maxElements && !isEmpty())
            {
                c.add(poll());
                ++count;
            }

            return count;
        }
    }
}
