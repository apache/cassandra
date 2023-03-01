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

package org.apache.cassandra.utils.btree;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import static java.util.Comparator.naturalOrder;

public class BTreeMultimap<K, V> implements Multimap<K, V>
{
    private final BTreeMap<K, Collection<V>> map;
    private final Comparator<K> comparator;
    private final Comparator<V> valueComparator;
    private final int size;

    private BTreeMultimap(BTreeMap<K, Collection<V>> map, Comparator<K> comparator, Comparator<V> valueComparator, int size)
    {
        this.map = map;
        this.comparator = comparator;
        this.valueComparator = valueComparator;
        this.size = size;
    }

    public static <K extends Comparable<K>, V extends Comparable<V>> BTreeMultimap<K, V> empty()
    {
        return new BTreeMultimap<K, V>(BTreeMap.empty(), naturalOrder(), naturalOrder(), 0);
    }

    public BTreeMultimap<K, V> with(K key, V value)
    {
        if (map.containsKey(key))
        {
            BTreeSet<V> oldSet = (BTreeSet<V>) map.get(key);
            BTreeSet<V> newSet = oldSet.with(value);
            int newSize = size + newSet.size() - oldSet.size();
            return new BTreeMultimap<>(map.without(key).with(key, newSet), comparator, valueComparator, newSize);
        }
        else
        {
            BTreeSet<V> newSet = BTreeSet.of(valueComparator, value);
            return new BTreeMultimap<>(map.with(key, newSet), comparator, valueComparator, size + 1);
        }
    }

    public BTreeMultimap<K, V> without(K key)
    {
        Collection<V> oldSet = map.get(key);
        if (oldSet == null)
            return this;
        int newSize = size - oldSet.size();
        return new BTreeMultimap<>(map.without(key), comparator, valueComparator, newSize);
    }

    public BTreeMultimap<K, V> without(K key, V value)
    {
        BTreeSet<V> values = (BTreeSet<V>) map.get(key);
        if (values == null)
            return this;
        if (!values.contains(value))
            return this;
        BTreeSet<V> newValues = BTreeSet.wrap(BTreeRemoval.remove(values.tree, valueComparator, value), valueComparator);
        BTreeMap<K, Collection<V>> newMap = map.without(key);
        if (newValues.isEmpty())
            return new BTreeMultimap<>(newMap, comparator, valueComparator, size - 1);

        return new BTreeMultimap<>(newMap.with(key, newValues), comparator, valueComparator, size - 1);
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(@Nullable Object o)
    {
        if (o == null)
            return false;
        return map.containsKey(o);
    }

    @Override
    public boolean containsValue(@Nullable Object o)
    {
        if (o == null)
            return false;
        for (Map.Entry<K, Collection<V>> e : map.entrySet())
            if (e.getValue().contains(o))
                return true;
        return false;
    }

    @Override
    public boolean containsEntry(@Nullable Object key, @Nullable Object value)
    {
        if (key == null || value == null)
            throw new NullPointerException();
        return map.containsKey(key) && map.get(key).contains(value);
    }

    @Override
    public Collection<V> get(@Nullable K k)
    {
        if (k == null)
            return null;
        return map.get(k);
    }

    @Override
    public Set<K> keySet()
    {
        return map.keySet();
    }

    @Override
    public Multiset<K> keys()
    {
        ImmutableMultiset.Builder<K> keys = ImmutableMultiset.builder();
        keys.addAll(map.keySet());
        return keys.build();
    }

    @Override
    public Collection<V> values()
    {
        ImmutableList.Builder<V> builder = ImmutableList.builder();
        for (Map.Entry<K, Collection<V>> entry : map.entrySet())
            builder.addAll(entry.getValue());
        return builder.build();
    }

    @Override
    public Collection<Map.Entry<K, V>> entries()
    {
        Set<Map.Entry<K, V>> entries = new HashSet<>();
        for (Map.Entry<K, Collection<V>> entry : map.entrySet())
            for (V v : entry.getValue())
                entries.add(new AbstractBTreeMap.Entry<>(entry.getKey(), v));
        return entries;
    }

    @Override
    public Map<K, Collection<V>> asMap()
    {
        return map;
    }

    public boolean put(@Nullable K k, @Nullable V v) { throw new UnsupportedOperationException();}
    public boolean remove(@Nullable Object o, @Nullable Object o1) {throw new UnsupportedOperationException();}
    public boolean putAll(@Nullable K k, Iterable<? extends V> iterable) {throw new UnsupportedOperationException();}
    public boolean putAll(Multimap<? extends K, ? extends V> multimap) {throw new UnsupportedOperationException();}
    public Collection<V> replaceValues(@Nullable K k, Iterable<? extends V> iterable) {throw new UnsupportedOperationException();}
    public Collection<V> removeAll(@Nullable Object o) {throw new UnsupportedOperationException();}
    public void clear() { throw new UnsupportedOperationException(); }

    @Override
    public String toString()
    {
        return map.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof BTreeMultimap)) return false;
        BTreeMultimap<?, ?> that = (BTreeMultimap<?, ?>) o;
        return size == that.size &&
               Objects.equals(map, that.map) &&
               Objects.equals(comparator, that.comparator) &&
               Objects.equals(valueComparator, that.valueComparator);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(map, comparator, valueComparator, size);
    }
}
