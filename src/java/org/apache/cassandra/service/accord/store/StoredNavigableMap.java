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
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import org.apache.cassandra.service.accord.AccordState;
import org.apache.cassandra.utils.ObjectSizes;

/**
 * Navigable Map, capable of blind add/remove
 */
public class StoredNavigableMap<K extends Comparable<?>, V> extends AbstractStoredField
{
    private static final long EMPTY_SIZE = ObjectSizes.measureDeep(new StoredNavigableMap<>(AccordState.ReadWrite.FULL));
    private NavigableMap<K, V> map = null;
    private NavigableMap<K, V> view = null;
    private NavigableMap<K, V> additions = null;
    private NavigableMap<K, V> deletions = null;

    public StoredNavigableMap(AccordState.ReadWrite readWrite)
    {
        super(readWrite);
    }

    @Override
    public boolean equals(Object o)
    {
        preGet();
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoredNavigableMap<?, ?> that = (StoredNavigableMap<?, ?>) o;
        return Objects.equals(map, that.map);
    }

    @Override
    public int hashCode()
    {
        preGet();
        return Objects.hash(map);
    }

    @Override
    public String valueString()
    {
        if (view == null)
            return "<not loaded>";
        return view.entrySet().stream()
                   .map(e -> e.getKey() + "=" + e.getValue())
                   .collect(Collectors.joining(", ", "{", "}"));
    }

    public void unload()
    {
        preUnload();
        map = null;
        view = null;
        additions = null;
        deletions = null;
    }

    void setInternal(NavigableMap<K, V> map)
    {
        this.map = map;
        this.view = Collections.unmodifiableNavigableMap(map);
    }

    public void load(NavigableMap<K, V> map)
    {
        preLoad();
        setInternal(map);
    }

    public NavigableMap<K, V> getView()
    {
        preGet();
        return view;
    }

    public void blindPut(K key, V val)
    {
        preBlindChange();
        if (hasValue())
            map.put(key, val);

        if (additions == null)
            additions = new TreeMap<>();

        additions.put(key, val);
        if (deletions != null)
            deletions.remove(key);
    }

    public void blindRemove(K key)
    {
        preBlindChange();
        if (hasValue())
            map.remove(key);

        if (!wasCleared())
        {
            if (deletions == null)
                deletions = new TreeMap<>();
            deletions.put(key, null);
        }
        if (additions != null)
            additions.remove(key);
    }

    // TODO: this is a kludge, but will suffice until we can more fully rework efficiency of waitingOn collections
    // this is semantically equivalent to blindRemove(key) but stores the value we believe was bound to key on removal
    // so that it can be used by forEachDeletion
    public void blindRemove(K key, V value)
    {
        preBlindChange();
        if (hasValue())
            map.remove(key);

        if (!wasCleared())
        {
            if (deletions == null)
                deletions = new TreeMap<>();
            deletions.put(key, value);
        }
        if (additions != null)
            additions.remove(key);
    }

    public void clear()
    {
        clearModifiedFlag();
        preClear();
        setInternal(new TreeMap<>());
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

    public int additionsSize()
    {
        return additions != null ? additions.size() : 0;
    }

    public boolean hasDeletions()
    {
        return deletions != null && !deletions.isEmpty();
    }

    public int deletionsSize()
    {
        return deletions != null ? deletions.size() : 0;
    }

    public int totalModifications()
    {
        return additionsSize() + deletionsSize();
    }

    public void forEachAddition(BiConsumer<K, V> consumer)
    {
        if (additions != null)
            additions.forEach(consumer);
    }

    public void forEachDeletion(Consumer<K> consumer)
    {
        if (deletions != null)
            deletions.keySet().forEach(consumer);
    }

    public void forEachDeletion(BiConsumer<K, V> consumer)
    {
        if (deletions != null)
            deletions.forEach(consumer);
    }

    public long estimatedSizeOnHeap(ToLongFunction<K> measureKey, ToLongFunction<V> measureVal)
    {
        long size = EMPTY_SIZE;
        if (hasValue() && ! map.isEmpty())
        {
            for (Map.Entry<K, V> entry : map.entrySet())
            {
                size += measureKey.applyAsLong(entry.getKey());
                size += measureVal.applyAsLong(entry.getValue());
            }
        }
        return size;
    }
}
