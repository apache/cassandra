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
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

import com.google.common.collect.Iterators;

import org.agrona.collections.Object2IntHashMap;

public class ImmutableUniqueList<T> extends AbstractList<T> implements RandomAccess
{
    private final T[] values;
    private final Object2IntHashMap<T> indexLookup;
    private transient AsSet asSet = null;
    private ImmutableUniqueList(Builder<T> builder)
    {
        values = (T[]) builder.values.toArray(Object[]::new);
        indexLookup = new Object2IntHashMap<>(builder.indexLookup);
    }

    public static <T> Builder<T> builder()
    {
        return new Builder<>();
    }

    public AsSet asSet()
    {
        if (asSet != null) return asSet;
        return asSet = new AsSet();
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

    public static final class Builder<T> extends AbstractSet<T>
    {
        private final List<T> values = new ArrayList<>();
        private final Object2IntHashMap<T> indexLookup = new Object2IntHashMap<>(-1);
        private int idx;

        public Builder<T> mayAddAll(Collection<? extends T> values)
        {
            addAll(values);
            return this;
        }

        @Override
        public boolean add(T t)
        {
            if (indexLookup.containsKey(t)) return false;
            int idx = this.idx++;
            indexLookup.put(t, idx);
            values.add(t);
            return true;
        }

        @Override
        public boolean remove(Object o)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear()
        {
            values.clear();
            indexLookup.clear();
            idx = 0;
        }

        @Override
        public boolean isEmpty()
        {
            return values.isEmpty();
        }

        @Override
        public boolean contains(Object o)
        {
            return indexLookup.containsKey(o);
        }

        @Override
        public Iterator<T> iterator()
        {
            return Iterators.unmodifiableIterator(values.iterator());
        }

        @Override
        public int size()
        {
            return values.size();
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

    public class AsSet extends AbstractSet<T>
    {
        @Override
        public Iterator<T> iterator()
        {
            return ImmutableUniqueList.this.iterator();
        }

        @Override
        public int size()
        {
            return values.length;
        }
    }
}
