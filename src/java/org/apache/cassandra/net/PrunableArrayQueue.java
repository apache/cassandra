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

import java.util.function.Predicate;

import org.apache.cassandra.utils.Throwables;

/**
 * A growing array-based queue that allows efficient bulk in-place removal.
 *
 * Can think of this queue as if it were an {@link java.util.ArrayDeque} with {@link #prune(Pruner)} method - an efficient
 * way to prune the queue in-place that is more expressive, and faster than {@link java.util.ArrayDeque#removeIf(Predicate)}.
 *
 * The latter has to perform O(n*n) shifts, whereas {@link #prune(Pruner)} only needs O(n) shifts at worst.
 */
final class PrunableArrayQueue<E>
{
    public interface Pruner<E>
    {
        /**
         * @return whether the element should be pruned
         *  if {@code true},  the element will be removed from the queue, and {@link #onPruned(Object)} will be invoked,
         *  if {@code false}, the element will be kept, and {@link #onKept(Object)} will be invoked.
         */
        boolean shouldPrune(E e);

        void onPruned(E e);
        void onKept(E e);
    }

    private int capacity;
    private E[] buffer;

    /*
     * mask = capacity - 1;
     * since capacity is a power of 2, value % capacity == value & (capacity - 1) == value & mask
     */
    private int mask;

    private int head = 0;
    private int tail = 0;

    @SuppressWarnings("unchecked")
    PrunableArrayQueue(int requestedCapacity)
    {
        capacity = Math.max(8, findNextPositivePowerOfTwo(requestedCapacity));
        mask = capacity - 1;
        buffer = (E[]) new Object[capacity];
    }

    @SuppressWarnings("UnusedReturnValue")
    boolean offer(E e)
    {
        buffer[tail] = e;
        if ((tail = (tail + 1) & mask) == head)
            doubleCapacity();
        return true;
    }

    E peek()
    {
        return buffer[head];
    }

    E poll()
    {
        E result = buffer[head];
        if (null == result)
            return null;

        buffer[head] = null;
        head = (head + 1) & mask;

        return result;
    }

    int size()
    {
        return (tail - head) & mask;
    }

    boolean isEmpty()
    {
        return head == tail;
    }

    /**
     * Prunes the queue using the specified {@link Pruner}
     *
     * @return count of removed elements.
     */
    @SuppressWarnings("ThrowFromFinallyBlock")
    int prune(Pruner<E> pruner)
    {
        E e;
        int removed = 0;
        Throwable error = null;

        try
        {
            int size = size();
            for (int i = 0; i < size; i++)
            {
                /*
                 * We start at the tail and work backwards to minimise the number of copies
                 * as we expect to primarily prune from the front.
                 */
                int k = (tail - 1 - i) & mask;
                e = buffer[k];

                boolean shouldPrune = false;

                // If any error has been thrown from the Pruner callbacks, don't bother asking the
                // pruner. Just move any elements that need to be moved, correct the head, and rethrow.
                if (error == null)
                {
                    try
                    {
                        shouldPrune = pruner.shouldPrune(e);
                    }
                    catch (Throwable t)
                    {
                        error = t;
                    }
                }

                if (shouldPrune)
                {
                    buffer[k] = null;
                    removed++;

                    try
                    {
                        pruner.onPruned(e);
                    }
                    catch (Throwable t)
                    {
                        error = t;
                    }
                }
                else
                {
                    if (removed > 0)
                    {
                        buffer[(k + removed) & mask] = e;
                        buffer[k] = null;
                    }

                    try
                    {
                        pruner.onKept(e);
                    }
                    catch (Throwable t)
                    {
                        if (error == null)
                        {
                            error = t;
                        }
                    }
                }
            }
        }
        finally
        {
            head = (head + removed) & mask;

            // Rethrow any error(s) from the Pruner callbacks, but only after the queue state is valid.
            if (error != null)
                throw Throwables.unchecked(error);
        }

        return removed;
    }

    @SuppressWarnings("unchecked")
    private void doubleCapacity()
    {
        assert head == tail;

        int newCapacity = capacity << 1;
        E[] newBuffer = (E[]) new Object[newCapacity];

        int headPortionLen = capacity - head;
        System.arraycopy(buffer, head, newBuffer, 0, headPortionLen);
        System.arraycopy(buffer, 0, newBuffer, headPortionLen, tail);

        head = 0;
        tail = capacity;

        capacity = newCapacity;
        mask = newCapacity - 1;
        buffer = newBuffer;
    }

    private static int findNextPositivePowerOfTwo(int value)
    {
        return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
    }
}
