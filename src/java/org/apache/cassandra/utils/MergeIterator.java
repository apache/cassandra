/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.io.IOException;
import java.io.IOError;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Ordering;

/** Merges sorted input iterators which individually contain unique items. */
public abstract class MergeIterator<In,Out> extends AbstractIterator<Out> implements CloseableIterator<Out>
{
    public final Comparator<In> comp;
    protected final List<? extends CloseableIterator<In>> iterators;
    // a queue for return: all candidates must be open and have at least one item
    protected final PriorityQueue<Candidate<In>> queue;

    protected MergeIterator(List<? extends CloseableIterator<In>> iters, Comparator<In> comp)
    {
        this.iterators = iters;
        this.comp = comp;
        this.queue = new PriorityQueue<Candidate<In>>(Math.max(1, iters.size()));
        for (CloseableIterator<In> iter : iters)
        {
            Candidate<In> candidate = new Candidate<In>(iter, comp);
            if (!candidate.advance())
                // was empty
                continue;
            this.queue.add(candidate);
        }
    }

    public static <E> MergeIterator<E,E> get(List<? extends CloseableIterator<E>> iters)
    {
        return get(iters, (Comparator<E>)Ordering.natural());
    }

    public static <E> MergeIterator<E,E> get(List<? extends CloseableIterator<E>> iters, Comparator<E> comp)
    {
        return new OneToOne<E>(iters, comp);
    }

    public static <In,Out> MergeIterator<In,Out> get(List<? extends CloseableIterator<In>> iters, Comparator<In> comp, Reducer<In,Out> reducer)
    {
        return new ManyToOne<In,Out>(iters, comp, reducer);
    }

    public Iterable<? extends CloseableIterator<In>> iterators()
    {
        return iterators;
    }

    /**
     * Consumes sorted items from the queue: should only remove items from the queue,
     * not add them.
     */
    protected abstract Out consume();

    /**
     * Returns consumed items to the queue.
     */
    protected abstract void advance();

    protected final Out computeNext()
    {
        advance();
        return consume();
    }

    public void close()
    {
        for (CloseableIterator<In> iterator : this.iterators)
        {
            try
            {
                iterator.close();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    /** A MergeIterator that returns a single value for each one consumed. */
    private static final class OneToOne<E> extends MergeIterator<E,E>
    {
        // the last returned candidate, so that we can lazily call 'advance()'
        protected Candidate<E> candidate;
        public OneToOne(List<? extends CloseableIterator<E>> iters, Comparator<E> comp)
        {
            super(iters, comp);
        }

        protected final E consume()
        {
            candidate = queue.poll();
            if (candidate == null)
                return endOfData();
            return candidate.item;
        }

        protected final void advance()
        {
            if (candidate != null && candidate.advance())
                // has more items
                queue.add(candidate);
        }
    }

    /** A MergeIterator that consumes multiple input values per output value. */
    private static final class ManyToOne<In,Out> extends MergeIterator<In,Out>
    {
        protected final Reducer<In,Out> reducer;
        // a stack of the last consumed candidates, so that we can lazily call 'advance()'
        // TODO: if we had our own PriorityQueue implementation we could stash items
        // at the end of its array, so we wouldn't need this storage
        protected final ArrayDeque<Candidate<In>> candidates;
        public ManyToOne(List<? extends CloseableIterator<In>> iters, Comparator<In> comp, Reducer<In,Out> reducer)
        {
            super(iters, comp);
            this.reducer = reducer;
            this.candidates = new ArrayDeque<Candidate<In>>(queue.size());
        }

        /** Consume values by sending them to the reducer while they are equal. */
        protected final Out consume()
        {
            reducer.onKeyChange();
            Candidate<In> candidate = queue.peek();
            if (candidate == null)
                return endOfData();
            do
            {
                candidate = queue.poll();
                candidates.push(candidate);
                reducer.reduce(candidate.item);
            }
            while (queue.peek() != null && queue.peek().compareTo(candidate) == 0);
            return reducer.getReduced();
        }

        /** Advance and re-enqueue all items we consumed in the last iteration. */
        protected final void advance()
        {
            Candidate<In> candidate;
            while ((candidate = candidates.pollFirst()) != null)
                if (candidate.advance())
                    queue.add(candidate);
        }
    }

    // Holds and is comparable by the head item of an iterator it owns
    protected static final class Candidate<In> implements Comparable<Candidate<In>>
    {
        private final CloseableIterator<In> iter;
        private final Comparator<In> comp;
        private In item;
        public Candidate(CloseableIterator<In> iter, Comparator<In> comp)
        {
            this.iter = iter;
            this.comp = comp;
        }

        public In item()
        {
            return item;
        }

        /** @return True if our iterator had an item, and it is now available */
        protected boolean advance()
        {
            if (!iter.hasNext())
                return false;
            item = iter.next();
            return true;
        }

        public int compareTo(Candidate<In> that)
        {
            return comp.compare(this.item, that.item);
        }
    }

    /** Accumulator that collects values of type A, and outputs a value of type B. */
    public static abstract class Reducer<In,Out>
    {
        /**
         * combine this object with the previous ones.
         * intermediate state is up to your implementation.
         */
        public abstract void reduce(In current);

        /** @return The last object computed by reduce */
        protected abstract Out getReduced();

        /**
         * Called at the begining of each new key, before any reduce is called.
         * To be overriden by implementing classes.
         */
        protected void onKeyChange() {}
    }
}
