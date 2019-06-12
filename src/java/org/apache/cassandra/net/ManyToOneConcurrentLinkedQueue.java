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

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

/**
 * A concurrent many-producers-to-single-consumer linked queue.
 *
 * Based roughly on {@link java.util.concurrent.ConcurrentLinkedQueue}, except with simpler/cheaper consumer-side
 * method implementations ({@link #poll()}, {@link #remove()}, {@link #drain(Consumer)}), and padding added
 * to prevent false sharing.
 *
 * {@link #offer(Object)} provides volatile visibility semantics. {@link #offer(Object)} is lock-free, {@link #poll()}
 * and all related consumer methods are wait-free.
 *
 * In addition to that, provides a {@link #relaxedPeekLastAndOffer(Object)} method that we use to avoid a CAS when
 * putting message handlers onto the wait queue.
 */
class ManyToOneConcurrentLinkedQueue<E> extends ManyToOneConcurrentLinkedQueueHead<E> implements Queue<E>
{
    @SuppressWarnings("unused") // pad two cache lines after the head to prevent false sharing
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;

    ManyToOneConcurrentLinkedQueue()
    {
        head = tail = new Node<>(null);
    }

    /**
     * See {@link #relaxedIsEmpty()}.
     */
    @Override
    public boolean isEmpty()
    {
        return relaxedIsEmpty();
    }

    /**
     * When invoked by the consumer thread, the answer will always be accurate.
     * When invoked by a non-consumer thread, it won't always be the case:
     *  - {@code true}  result indicates that the queue <em>IS</em> empty, no matter what;
     *  - {@code false} result indicates that the queue <em>MIGHT BE</em> non-empty - the value of {@code head} might
     *    not yet have been made externally visible by the consumer thread.
     */
    boolean relaxedIsEmpty()
    {
        return null == head.next;
    }

    @Override
    public int size()
    {
        int size = 0;
        Node<E> next = head;
        while (null != (next = next.next))
            size++;
        return size;
    }

    @Override
    public E peek()
    {
        Node<E> next = head.next;
        if (null == next)
            return null;
        return next.item;
    }

    @Override
    public E element()
    {
        E item = peek();
        if (null == item)
            throw new NoSuchElementException("Queue is empty");
        return item;
    }

    @Override
    public E poll()
    {
        Node<E> head = this.head;
        Node<E> next = head.next;

        if (null == next)
            return null;

        this.lazySetHead(next); // update head reference to next before making previous head node unreachable,
        head.lazySetNext(head); // to maintain the guarantee of tail being always reachable from head

        E item = next.item;
        next.item = null;
        return item;
    }

    @Override
    public E remove()
    {
        E item = poll();
        if (null == item)
            throw new NoSuchElementException("Queue is empty");
        return item;
    }

    @Override
    public boolean remove(Object o)
    {
        if (null == o)
            throw new NullPointerException();

        Node<E> prev = this.head;
        Node<E> next = prev.next;

        while (null != next)
        {
            if (o.equals(next.item))
            {
                prev.lazySetNext(next.next); // update prev reference to next before making removed node unreachable,
                next.lazySetNext(next);      // to maintain the guarantee of tail being always reachable from head

                next.item = null;
                return true;
            }

            prev = next;
            next = next.next;
        }

        return false;
    }

    /**
     * Consume the queue in its entirety and feed every item to the provided {@link Consumer}.
     *
     * Exists primarily for convenience, and essentially just wraps {@link #poll()} in a loop.
     * Yields no performance benefit over invoking {@link #poll()} manually - there just isn't
     * anything to meaningfully amortise on the consumer side of this queue.
     */
    void drain(Consumer<E> consumer)
    {
        E item;
        while ((item = poll()) != null)
            consumer.accept(item);
    }

    @Override
    public boolean add(E e)
    {
        return offer(e);
    }

    @Override
    public boolean offer(E e)
    {
        internalOffer(e); return true;
    }

    /**
     * Adds the element to the queue and returns the item of the previous tail node.
     * It's possible for the returned item to already have been consumed.
     *
     * @return previously last tail item in the queue, potentially stale
     */
    E relaxedPeekLastAndOffer(E e)
    {
        return internalOffer(e);
    }

    /**
     * internalOffer() is based on {@link java.util.concurrent.ConcurrentLinkedQueue#offer(Object)},
     * written by Doug Lea and Martin Buchholz with assistance from members of JCP JSR-166 Expert Group
     * and released to the public domain, as explained at http://creativecommons.org/publicdomain/zero/1.0/
     */
    private E internalOffer(E e)
    {
        if (null == e)
            throw new NullPointerException();

        final Node<E> node = new Node<>(e);

        for (Node<E> t = tail, p = t;;)
        {
            Node<E> q = p.next;
            if (q == null)
            {
                // p is last node
                if (p.casNext(null, node))
                {
                    // successful CAS is the linearization point for e to become an element of this queue and for node to become "live".
                    if (p != t) // hop two nodes at a time
                        casTail(t, node); // failure is ok
                    return p.item;
                }
                // lost CAS race to another thread; re-read next
            }
            else if (p == q)
            {
                /*
                 * We have fallen off list. If tail is unchanged, it will also be off-list, in which case we need to
                 * jump to head, from which all live nodes are always reachable. Else the new tail is a better bet.
                 */
                p = (t != (t = tail)) ? t : head;
            }
            else
            {
                // check for tail updates after two hops
                p = (p != t && t != (t = tail)) ? t : q;
            }
        }
    }

    @Override
    public boolean contains(Object o)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<E> iterator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
        throw new UnsupportedOperationException();
    }
}

class ManyToOneConcurrentLinkedQueueHead<E> extends ManyToOneConcurrentLinkedQueuePadding2<E>
{
    protected volatile ManyToOneConcurrentLinkedQueue.Node<E> head;

    private static final AtomicReferenceFieldUpdater<ManyToOneConcurrentLinkedQueueHead, Node> headUpdater =
        AtomicReferenceFieldUpdater.newUpdater(ManyToOneConcurrentLinkedQueueHead.class, Node.class, "head");

    @SuppressWarnings("WeakerAccess")
    protected void lazySetHead(Node<E> val)
    {
        headUpdater.lazySet(this, val);
    }
}

class ManyToOneConcurrentLinkedQueuePadding2<E> extends ManyToOneConcurrentLinkedQueueTail<E>
{
    @SuppressWarnings("unused") // pad two cache lines between tail and head to prevent false sharing
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

class ManyToOneConcurrentLinkedQueueTail<E> extends ManyToOneConcurrentLinkedQueuePadding1
{
    protected volatile ManyToOneConcurrentLinkedQueue.Node<E> tail;

    private static final AtomicReferenceFieldUpdater<ManyToOneConcurrentLinkedQueueTail, Node> tailUpdater =
        AtomicReferenceFieldUpdater.newUpdater(ManyToOneConcurrentLinkedQueueTail.class, Node.class, "tail");

    @SuppressWarnings({ "WeakerAccess", "UnusedReturnValue" })
    protected boolean casTail(Node<E> expect, Node<E> update)
    {
        return tailUpdater.compareAndSet(this, expect, update);
    }
}

class ManyToOneConcurrentLinkedQueuePadding1
{
    @SuppressWarnings("unused") // pad two cache lines before the tail to prevent false sharing
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;

    static final class Node<E>
    {
        E item;
        volatile Node<E> next;

        private static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");

        Node(E item)
        {
            this.item = item;
        }

        @SuppressWarnings("SameParameterValue")
        boolean casNext(Node<E> expect, Node<E> update)
        {
            return nextUpdater.compareAndSet(this, expect, update);
        }

        void lazySetNext(Node<E> val)
        {
            nextUpdater.lazySet(this, val);
        }
    }
}
