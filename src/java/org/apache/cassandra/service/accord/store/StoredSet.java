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

package org.apache.cassandra.service.accord.store;

import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import accord.utils.DeterministicIdentitySet;
import org.apache.cassandra.service.accord.AccordState;
import org.apache.cassandra.utils.ObjectSizes;

public abstract class StoredSet<T, S extends Set<T>> extends AbstractStoredField
{
    private S set = null;
    private Set<T> view = null;
    private Set<T> additions = null;
    private Set<T> deletions = null;

    abstract S createDataSet();
    abstract Set<T> createMetaSet();
    abstract Set<T> createView(S data);
    abstract long emptySize();

    public StoredSet(AccordState.Kind kind)
    {
        super(kind);
    }

    @Override
    public boolean equals(Object o)
    {
        preGet();
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoredSet<?, ?> that = (StoredSet<?, ?>) o;
        return Objects.equals(set, that.set);
    }

    @Override
    public int hashCode()
    {
        preGet();
        return Objects.hash(set);
    }

    @Override
    public String valueString()
    {
        return view.stream()
                   .map(Object::toString)
                   .collect(Collectors.joining(", ", "{", "}"));
    }

    public void unload()
    {
        preUnload();
        set = null;
        view = null;
        additions = null;
        deletions = null;
    }

    void setInternal(S set)
    {
        this.set = set;
        this.view = createView(set);
    }

    public void load(S set)
    {
        preLoad();
        setInternal(set);
    }

    public Set<T> getView()
    {
        preGet();
        return view;
    }

    public void blindAdd(T item)
    {
        preBlindChange();
        if (hasValue())
            set.add(item);

        if (additions == null)
            additions = createMetaSet();

        additions.add(item);
        if (deletions != null)
            deletions.remove(item);
    }

    public void blindRemove(T item)
    {
        preBlindChange();
        if (hasValue())
            set.remove(item);

        if (!wasCleared())
        {
            if (deletions == null)
                deletions = createMetaSet();
            deletions.add(item);
        }
        if (additions != null)
            additions.remove(item);
    }

    public void clear()
    {
        clearModifiedFlag();
        preClear();
        setInternal(createDataSet());
    }

    @Override
    public void clearModifiedFlag()
    {
        super.clearModifiedFlag();
        if (additions != null) additions.clear();
        if (deletions != null) deletions.clear();
    }

    public boolean hasAdditions()
    {
        return additions != null && !additions.isEmpty();
    }

    public boolean hasDeletions()
    {
        return deletions != null && !deletions.isEmpty();
    }

    public void forEachAddition(Consumer<T> consumer)
    {
        if (additions != null)
            additions.forEach(consumer);
    }

    public void forEachDeletion(Consumer<T> consumer)
    {
        if (deletions != null)
            deletions.forEach(consumer);
    }

    public long estimatedSizeOnHeap(ToLongFunction<T> measure)
    {
        long size = emptySize();
        if (hasValue() && !set.isEmpty())
        {
            for (T val : set)
                size += measure.applyAsLong(val);
        }
        return size;
    }

    public static class Navigable<T extends Comparable<?>> extends StoredSet<T, NavigableSet<T>>
    {
        private static final long EMPTY_SIZE = ObjectSizes.measureDeep(new Navigable<>(AccordState.Kind.FULL));

        public Navigable(AccordState.Kind kind) { super(kind); }

        @Override
        NavigableSet<T> createDataSet()
        {
            return new TreeSet<>();
        }

        @Override
        NavigableSet<T> createMetaSet()
        {
            return new TreeSet<>();
        }

        @Override
        NavigableSet<T> createView(NavigableSet<T> data)
        {
            return Collections.unmodifiableNavigableSet(data);
        }

        @Override
        long emptySize()
        {
            return EMPTY_SIZE;
        }
    }

    public static class DeterministicIdentity<T> extends StoredSet<T, DeterministicIdentitySet<T>>
    {
        private static final long EMPTY_SIZE = ObjectSizes.measureDeep(new DeterministicIdentity<>(AccordState.Kind.FULL));

        public DeterministicIdentity(AccordState.Kind kind) { super(kind); }

        @Override
        DeterministicIdentitySet<T> createDataSet()
        {
            return new DeterministicIdentitySet<>();
        }

        @Override
        Set<T> createMetaSet()
        {
            return new HashSet<>();
        }

        @Override
        Set<T> createView(DeterministicIdentitySet<T> data)
        {
            return Collections.unmodifiableSet(data);
        }

        @Override
        long emptySize()
        {
            return EMPTY_SIZE;
        }
    }
}
