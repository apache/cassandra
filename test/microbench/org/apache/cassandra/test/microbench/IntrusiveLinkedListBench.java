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
package org.apache.cassandra.test.microbench;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import accord.utils.IntrusiveLinkedList;
import accord.utils.IntrusiveLinkedListNode;
import accord.utils.Invariants;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static java.util.Spliterators.spliteratorUnknownSize;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class IntrusiveLinkedListBench
{
    @Param({"1", "10", "100"})
    int listSize;

    CachingNotImplementedList cachingNotImplemented;
    CachingDisabledList cachingDisabled;
    CachingEnabledList cachingEnabled;

    @Setup
    public void setUp()
    {
        cachingNotImplemented = new CachingNotImplementedList();
        cachingDisabled = new CachingDisabledList();
        cachingEnabled = new CachingEnabledList();

        for (int i = 0; i < listSize; i++)
        {
            cachingNotImplemented.addLast(new CachingNotImplementedNode(i));
            cachingDisabled.addLast(new CachingDisabledNode(i));
            cachingEnabled.addLast(new CachingEnabledNode(i));
        }
    }

    @Benchmark
    public int iteratorCachingNotImplemented()
    {
        int sum = 0;
        Iterator<CachingNotImplementedNode> iter = cachingNotImplemented.iterator();
        while (iter.hasNext()) sum += iter.next().value;
        return sum;
    }

    @Benchmark
    public int iteratorCachingDisabled()
    {
        int sum = 0;
        Iterator<CachingDisabledNode> iter = cachingDisabled.iterator();
        while (iter.hasNext()) sum += iter.next().value;
        return sum;
    }

    @Benchmark
    public int iteratorCachingEnabled()
    {
        int sum = 0;
        Iterator<CachingEnabledNode> iter = cachingEnabled.iterator();
        while (iter.hasNext()) sum += iter.next().value;
        return sum;
    }

    private static class CachingNotImplementedNode extends IntrusiveLinkedListNode
    {
        final int value;

        CachingNotImplementedNode(int value)
        {
            this.value = value;
        }
    }

    private static class CachingNotImplementedList extends IntrusiveLinkedList<CachingNotImplementedNode>
    {
    }

    private static class CachingDisabledNode extends IteratorCachingIntrusiveLinkedListNode
    {
        final int value;

        CachingDisabledNode(int value)
        {
            this.value = value;
        }
    }

    private static class CachingDisabledList extends IteratorCachingIntrusiveLinkedList<CachingDisabledNode>
    {
        CachingDisabledList()
        {
            super(false);
        }
    }

    private static class CachingEnabledNode extends IteratorCachingIntrusiveLinkedListNode
    {
        final int value;

        CachingEnabledNode(int value)
        {
            this.value = value;
        }
    }

    private static class CachingEnabledList extends IteratorCachingIntrusiveLinkedList<CachingEnabledNode>
    {
        CachingEnabledList()
        {
            super(true);
        }
    }

    private static abstract class IteratorCachingIntrusiveLinkedListNode
    {
        IteratorCachingIntrusiveLinkedListNode prev;
        IteratorCachingIntrusiveLinkedListNode next;

        public IteratorCachingIntrusiveLinkedListNode()
        {
        }

        protected boolean isFree()
        {
            return this.next == null;
        }

        protected void remove()
        {
            if (this.next != null)
            {
                this.prev.next = this.next;
                this.next.prev = this.prev;
                this.next = null;
                this.prev = null;
            }
            Invariants.paranoid(this.prev == null);
        }
    }

    @SuppressWarnings("unchecked")
    private static class IteratorCachingIntrusiveLinkedList<O extends IteratorCachingIntrusiveLinkedListNode> extends IteratorCachingIntrusiveLinkedListNode implements Iterable<O>
    {
        private final boolean cacheIterator;

        public IteratorCachingIntrusiveLinkedList(boolean cacheIterator)
        {
            prev = next = this;
            this.cacheIterator = cacheIterator;
        }

        public void addFirst(O add)
        {
            if (add.next != null)
                throw new IllegalStateException();
            add(this, add, next);
        }

        public void addLast(O add)
        {
            if (add.next != null)
                throw new IllegalStateException();
            add(prev, add, this);
        }

        private void add(IteratorCachingIntrusiveLinkedListNode after, IteratorCachingIntrusiveLinkedListNode add, IteratorCachingIntrusiveLinkedListNode before)
        {
            add.next = before;
            add.prev = after;
            before.prev = add;
            after.next = add;
        }

        public O poll()
        {
            if (isEmpty())
                return null;

            IteratorCachingIntrusiveLinkedListNode next = this.next;
            next.remove();
            return (O) next;
        }

        public boolean isEmpty()
        {
            return next == this;
        }

        public Stream<O> stream()
        {
            return StreamSupport.stream(spliteratorUnknownSize(iterator(), Spliterator.IMMUTABLE), false);
        }

        @Override
        public Iterator<O> iterator()
        {
            if (!cacheIterator)
                return new ReusableIterator();
            ReusableIterator iterator = this.iterator;
            if (iterator == null)
                return this.iterator = new ReusableIterator();
            return iterator.reset();
        }

        private class ReusableIterator implements Iterator<O>
        {
            IteratorCachingIntrusiveLinkedListNode next = IteratorCachingIntrusiveLinkedList.this.next;

            @Override
            public boolean hasNext()
            {
                return next != IteratorCachingIntrusiveLinkedList.this;
            }

            @Override
            public O next()
            {
                O result = (O)next;
                if (result.next == null)
                    throw new NullPointerException();
                next = result.next;
                return result;
            }

            ReusableIterator reset()
            {
                next = IteratorCachingIntrusiveLinkedList.this.next;
                return this;
            }
        }
        private ReusableIterator iterator = null;
    }
}