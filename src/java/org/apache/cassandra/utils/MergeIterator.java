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
package org.apache.cassandra.utils;

import java.util.*;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

/** Merges sorted input iterators which individually contain unique items. */
public abstract class MergeIterator
{
    public static <In, Out> CloseableIterator<Out> getCloseable(List<? extends CloseableIterator<In>> sources,
                                                                Comparator<? super In> comparator,
                                                                Reducer<In, Out> reducer)
    {
        if (sources.size() == 1)
        {
            return reducer.singleSourceReduceIsTrivial()
                 ? (CloseableIterator<Out>) sources.get(0)
                 : new OneToOneCloseable<>(sources.get(0), reducer);
        }
        return new ManyToOneCloseable<>(sources, comparator, reducer);
    }

    public static <In, Out> Iterator<Out> get(List<Iterator<In>> sources,
                                              Comparator<? super In> comparator,
                                              Reducer<In, Out> reducer)
    {
        if (sources.size() == 1)
        {
            return reducer.singleSourceReduceIsTrivial()
                   ? (Iterator<Out>) sources.get(0)
                   : new OneToOne<>(sources.get(0), reducer);
        }
        return new ManyToOne<>(sources, comparator, reducer);
    }

    private static class ManyToOne<In, V extends Iterator<In>, Out> extends Merger<In, V, Out> implements Iterator<Out>
    {
        public ManyToOne(List<V> iters, Comparator<? super In> comp, Reducer<In, Out> reducer)
        {
            this(iters, null, comp, reducer);
        }

        ManyToOne(List<V> iters, Consumer<V> onClose, Comparator<? super In> comp, Reducer<In, Out> reducer)
        {
            super(iters, it -> it.hasNext() ? Preconditions.checkNotNull(it.next()) : null, onClose, comp, reducer);
        }
    }

    private static class ManyToOneCloseable<In, V extends CloseableIterator<In>, Out> extends ManyToOne<In, V, Out> implements CloseableIterator<Out>
    {
        public ManyToOneCloseable(List<V> iters, Comparator<? super In> comp, Reducer<In, Out> reducer)
        {
            super(iters, CloseableIterator::close, comp, reducer);
        }
    }

    private static class OneToOne<In, Out> implements Iterator<Out>
    {
        private final Iterator<In> source;
        private final Reducer<In, Out> reducer;

        public OneToOne(Iterator<In> source, Reducer<In, Out> reducer)
        {
            this.reducer = reducer;
            this.source = source;
        }

        public boolean hasNext()
        {
            return source.hasNext();
        }

        public Out next()
        {
            reducer.onKeyChange();
            reducer.reduce(0, source.next());
            return reducer.getReduced();
        }
    }

    private static class OneToOneCloseable<In, Out> implements CloseableIterator<Out>
    {
        private final CloseableIterator<In> source;
        private final Reducer<In, Out> reducer;

        public OneToOneCloseable(CloseableIterator<In> source, Reducer<In, Out> reducer)
        {
            this.reducer = reducer;
            this.source = source;
        }

        public boolean hasNext()
        {
            return source.hasNext();
        }

        public Out next()
        {
            reducer.onKeyChange();
            reducer.reduce(0, source.next());
            return reducer.getReduced();
        }

        public void close()
        {
            source.close();
        }
    }
}
