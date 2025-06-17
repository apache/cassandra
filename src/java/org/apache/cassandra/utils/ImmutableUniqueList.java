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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.RandomAccess;
import java.util.Set;

import org.agrona.collections.Object2IntHashMap;

/**
 * An immutable implementation of {@link UniqueList} that preserves insertion order.
 * <p>
 * This class provides:
 * - Immutability: Once created, the list cannot be modified
 * - Uniqueness: Each element can only appear once in the list
 * - Insertion order: Elements are stored and iterated in the order they were first added
 * <p>
 * Use the static factory methods or builder to create instances.
 *
 * @see #of(Object[])
 * @see #builder()
 * @see #builder(int)
 * @see #empty()
 * @param <T> the type of elements in this list
 */
public class ImmutableUniqueList<T> extends AbstractList<T> implements UniqueList<T>, RandomAccess
{
    private static final ImmutableUniqueList<Object> EMPTY = ImmutableUniqueList.builder().build();

    private final T[] values;
    private final Object2IntHashMap<T> indexLookup;

    private ImmutableUniqueList(Builder<T> builder)
    {
        //noinspection unchecked
        values = (T[]) builder.values.toArray(Object[]::new);
        indexLookup = new Object2IntHashMap<>(builder.indexLookup);
    }

    public static <T> ImmutableUniqueList<T> copyOf(Collection<T> collection)
    {
        if (collection instanceof ImmutableUniqueList) return (ImmutableUniqueList<T>) collection;
        return ImmutableUniqueList.<T>builder().addAll(collection).build();
    }

    public static <T> Builder<T> builder()
    {
        return new Builder<>();
    }

    public static <T> Builder<T> builder(int expectedSize)
    {
        return new Builder<>(expectedSize);
    }

    @SuppressWarnings("unchecked")
    public static <T> ImmutableUniqueList<T> empty()
    {
        return (ImmutableUniqueList<T>) EMPTY;
    }

    @SafeVarargs
    public static <T> ImmutableUniqueList<T> of(T... values)
    {
        Builder<T> builder = builder(values.length);
        for (T v : values)
            if (!builder.maybeAdd(v))
                throw new IllegalArgumentException("Unable to add " + v +  " as its a duplicate");
        return builder.build();
    }

    @Override
    public T get(int index)
    {
        return values[index];
    }

    @Override
    public int indexOf(Object o)
    {
        return indexLookup.getOrDefault(o, -1);
    }

    @Override
    public int lastIndexOf(Object o)
    {
        // values are unique...
        return indexOf(o);
    }

    @Override
    public boolean contains(Object o)
    {
        return indexLookup.containsKey(o);
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (o instanceof List)
            return super.equals(o);
        if (o instanceof Set)
        {
            Set<?> other = (Set<?>) o;
            if (other.size() != size())
                return false;
            return containsAll(other);
        }
        return false;
    }

    public static final class Builder<T>
    {
        private final List<T> values;
        private final Object2IntHashMap<T> indexLookup = new Object2IntHashMap<>(-1);
        private int idx;

        public Builder()
        {
            this.values = new ArrayList<>();
        }

        public Builder(int expectedSize)
        {
            this.values = new ArrayList<>(expectedSize);
        }

        public boolean maybeAdd(T t)
        {
            if (indexLookup.containsKey(t)) return false;
            int idx = this.idx++;
            indexLookup.put(t, idx);
            values.add(t);
            return true;
        }

        public Builder<T> add(T t)
        {
            maybeAdd(t);
            return this;
        }

        public Builder<T> addAll(Collection<? extends T> c)
        {
            c.forEach(this::add);
            return this;
        }

        public int indexOf(T t)
        {
            if (!indexLookup.containsKey(t)) return -1;
            return indexLookup.get(t);
        }

        public void clear()
        {
            values.clear();
            indexLookup.clear();
            idx = 0;
        }

        public ImmutableUniqueList<T> build()
        {
            return new ImmutableUniqueList<>(this);
        }

        public ImmutableUniqueList<T> buildAndClear()
        {
            ImmutableUniqueList<T> list = new ImmutableUniqueList<>(this);
            clear();
            return list;
        }
    }
}
