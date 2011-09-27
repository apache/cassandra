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
import java.util.*;

import com.google.common.collect.AbstractIterator;

/** Merges sorted input iterators which individually contain unique items. */
public abstract class MergeIterator<In,Out> extends AbstractIterator<Out> implements IMergeIterator<In, Out>
{
    protected final Reducer<In,Out> reducer;
    protected final List<? extends CloseableIterator<In>> iterators;

    protected MergeIterator(List<? extends CloseableIterator<In>> iters, Reducer<In, Out> reducer)
    {
        this.iterators = iters;
        this.reducer = reducer;
    }

    public static <In, Out> IMergeIterator<In, Out> get(final List<? extends CloseableIterator<In>> sources,
                                                    Comparator<In> comparator,
                                                    final Reducer<In, Out> reducer)
    {
        if (sources.size() == 1)
            return reducer.trivialReduceIsTrivial()
                   ? new TrivialOneToOne<In, Out>(sources, reducer)
                   : new OneToOne<In, Out>(sources, reducer);
        return new ManyToOne<In, Out>(sources, comparator, reducer);
    }

    public Iterable<? extends CloseableIterator<In>> iterators()
    {
        return iterators;
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

    /** A MergeIterator that consumes multiple input values per output value. */
    private static final class ManyToOne<In,Out> extends MergeIterator<In,Out>
    {
        public final Comparator<In> comp;
        // a queue for return: all candidates must be open and have at least one item
        protected final PriorityQueue<Candidate<In>> queue;
        // a stack of the last consumed candidates, so that we can lazily call 'advance()'
        // TODO: if we had our own PriorityQueue implementation we could stash items
        // at the end of its array, so we wouldn't need this storage
        protected final ArrayDeque<Candidate<In>> candidates;
        public ManyToOne(List<? extends CloseableIterator<In>> iters, Comparator<In> comp, Reducer<In,Out> reducer)
        {
            super(iters, reducer);
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
            this.candidates = new ArrayDeque<Candidate<In>>(queue.size());
        }

        protected final Out computeNext()
        {
            advance();
            return consume();
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
         * @return true if Out is the same as In for the case of a single source iterator
         */
        public boolean trivialReduceIsTrivial()
        {
            return false;
        }

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

    private static class OneToOne<In, Out> extends MergeIterator<In, Out>
    {
        private final CloseableIterator<In> source;

        public OneToOne(List<? extends CloseableIterator<In>> sources, Reducer<In, Out> reducer)
        {
            super(sources, reducer);
            source = sources.get(0);
        }

        protected Out computeNext()
        {
            if (!source.hasNext())
                return endOfData();
            reducer.onKeyChange();
            reducer.reduce(source.next());
            return reducer.getReduced();
        }
    }

    private static class TrivialOneToOne<In, Out> extends MergeIterator<In, Out>
    {
        private final CloseableIterator<?> source;

        public TrivialOneToOne(List<? extends CloseableIterator<In>> sources, Reducer<In, Out> reducer)
        {
            super(sources, reducer);
            source = sources.get(0);
        }

        protected Out computeNext()
        {
            if (!source.hasNext())
                return endOfData();
            return (Out) source.next();
        }
    }
}
