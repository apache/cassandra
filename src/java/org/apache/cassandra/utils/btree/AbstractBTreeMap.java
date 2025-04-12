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

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterators;

public abstract class AbstractBTreeMap<K, V> extends AbstractMap<K, V>
{
    protected final Object[] tree;
    protected final KeyComparator<K, V> comparator;

    protected AbstractBTreeMap(Object[] tree, KeyComparator<K, V> comparator)
    {
        this.tree = tree;
        this.comparator = comparator;
    }

    /**
     * return a new map containing provided key and value
     *
     * @throws IllegalStateException if the key already exists in the map
     */
    public abstract AbstractBTreeMap<K, V> with(K key, V value);

    /**
     * returns a new map containing provided key and value - replaces existing key
     */
    public abstract AbstractBTreeMap<K, V> withForce(K key, V value);

    /**
     * returns a new map without key
     */
    public abstract AbstractBTreeMap<K, V> without(K key);

    @Override
    public int size()
    {
        return BTree.size(tree);
    }

    @Override
    public boolean isEmpty()
    {
        return BTree.isEmpty(tree);
    }

    @Override
    public boolean containsKey(Object key)
    {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value)
    {
        Iterator<Entry<K, V>> iter = BTree.iterator(tree);
        while (iter.hasNext())
        {
            Entry<K, V> entry = iter.next();
            if (entry.getValue().equals(value))
                return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public V get(Object key)
    {
        if (key == null)
            throw new NullPointerException();
        Entry<K, V> entry = BTree.find(tree, comparator, new Entry<>((K)key, null));
        if (entry != null)
            return entry.getValue();
        return null;
    }

    private Set<K> keySet = null;
    @Override
    public Set<K> keySet()
    {
        if (keySet == null)
            keySet = BTreeSet.wrap(BTree.transformAndFilter(tree, (entry) -> ((Map.Entry<K, V>)entry).getKey()), comparator.keyComparator);
        return keySet;
    }

    /**
     * This method, according to the contract of {@link Map#values()}, returns a collection backed by the map. It also
     * closely mirrors {@link AbstractMap#values()}, which returns an {@link AbstractCollection}.
     */
    @Override
    public Collection<V> values()
    {
        return new AbstractCollection<>()
        {
            @Override
            public Iterator<V> iterator()
            {
                return Iterators.transform(BTree.<Entry<K, V>>iterator(tree), Entry::getValue);
            }

            @Override
            public int size()
            {
                return AbstractBTreeMap.this.size();
            }
        };
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet()
    {
        return BTreeSet.wrap(tree, comparator);
    }

    public V put(K key, V value) { throw new UnsupportedOperationException(); }
    public V forcePut(K ignoredKey, V ignoredVal) { throw new UnsupportedOperationException(); }
    public V remove(Object key) { throw new UnsupportedOperationException();}
    public void putAll(Map<? extends K, ? extends V> m) { throw new UnsupportedOperationException(); }
    public void clear() { throw new UnsupportedOperationException(); }
    public Map.Entry<K, V> pollFirstEntry() { throw new UnsupportedOperationException(); }
    public Map.Entry<K, V> pollLastEntry() { throw new UnsupportedOperationException(); }

    protected static class KeyComparator<K, V> implements Comparator<Map.Entry<K, V>>
    {
        protected final Comparator<K> keyComparator;

        protected KeyComparator(Comparator<K> keyComparator)
        {
            this.keyComparator = keyComparator;
        }

        @Override
        public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
        {
            return keyComparator.compare(o1.getKey(), o2.getKey());
        }
    }

    static class Entry<K, V> extends AbstractMap.SimpleEntry<K, V>
    {
        public Entry(K key, V value)
        {
            super(key, value);
        }

        public V setValue(V value) { throw new UnsupportedOperationException(); }

    }
}
