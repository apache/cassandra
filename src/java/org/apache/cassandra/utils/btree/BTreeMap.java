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

import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;

import org.apache.cassandra.utils.BulkIterator;

import static java.util.Comparator.naturalOrder;


public class BTreeMap<K, V> extends AbstractBTreeMap<K, V> implements NavigableMap<K, V>
{
    protected static <K, V> BTreeMap<K, V> withComparator(Object[] tree, Comparator<K> comparator)
    {
        return new BTreeMap<>(tree, new KeyComparator<>(comparator), new AsymmetricKeyComparator<>(comparator));
    }

    protected BTreeMap(Object[] tree, KeyComparator<K, V> comparator, AsymmetricKeyComparator<K> asymmetricComparator)
    {
        super(tree, comparator, asymmetricComparator);
    }

    public static <K, V> BTreeMap<K, V> empty(Comparator<K> comparator)
    {
        return withComparator(BTree.empty(), comparator);
    }

    public static <K extends Comparable<K>, V> BTreeMap<K, V> empty()
    {
        return BTreeMap.<K, V>empty(naturalOrder());
    }

    @Override
    public BTreeMap<K, V> with(K key, V value)
    {
        if (key == null || value == null)
            throw new NullPointerException();

        AbstractBTreeMap.Entry<K, V> entry = new AbstractBTreeMap.Entry<>(key, value);
        AbstractBTreeMap.Entry<K, V> existing;
        if ((existing = BTree.find(tree, comparator, entry)) != null && !existing.equals(entry))
            throw new IllegalStateException("Map already contains " + key);
        return new BTreeMap<>(BTree.update(tree, BTree.singleton(entry), comparator, UpdateFunction.noOp()), comparator, asymmetricComparator);
    }

    public BTreeMap<K, V> withForce(K key, V value)
    {
        if (key == null || value == null)
            throw new NullPointerException();
        AbstractBTreeMap.Entry<K, V> entry = new AbstractBTreeMap.Entry<>(key, value);
        return new BTreeMap<>(BTree.update(tree, BTree.singleton(entry), comparator, UpdateFunction.Simple.of((a, b) -> b)), comparator, asymmetricComparator);
    }

    public BTreeMap<K, V> without(K key)
    {
        if (key == null)
            throw new NullPointerException();

        return new BTreeMap<>(BTreeRemoval.remove(tree, asymmetricComparator, key), comparator, asymmetricComparator);
    }

    @Override
    public Map.Entry<K, V> lowerEntry(K key)
    {
        //noinspection unchecked
        return (Map.Entry<K, V>) BTree.lower(tree, asymmetricComparator, key);
    }

    @Override
    public K lowerKey(K key)
    {
        Map.Entry<K, V> entry = lowerEntry(key);
        return entry == null ? null : entry.getKey();
    }

    @Override
    public Map.Entry<K, V> floorEntry(K key)
    {
        //noinspection unchecked
        return (Map.Entry<K, V>) BTree.floor(tree, asymmetricComparator, key);
    }

    @Override
    public K floorKey(K key)
    {
        Map.Entry<K, V> entry = floorEntry(key);
        return entry == null ? null : entry.getKey();
    }

    @Override
    public Map.Entry<K, V> ceilingEntry(K key)
    {
        //noinspection unchecked
        return (Map.Entry<K, V>) BTree.ceil(tree, asymmetricComparator, key);
    }

    @Override
    public K ceilingKey(K key)
    {
        Map.Entry<K, V> entry = ceilingEntry(key);
        return entry == null ? null : entry.getKey();
    }

    @Override
    public Map.Entry<K, V> higherEntry(K key)
    {
        //noinspection unchecked
        return (Map.Entry<K, V>) BTree.higher(tree, asymmetricComparator, key);
    }

    @Override
    public K higherKey(K key)
    {
        Map.Entry<K, V> entry = higherEntry(key);
        return entry == null ? null : entry.getKey();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map.Entry<K, V> firstEntry()
    {
        if (isEmpty())
            return null;
        return (AbstractBTreeMap.Entry<K, V>) BTree.iterator(tree).next();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map.Entry<K, V> lastEntry()
    {
        return getEntry(size() - 1);
    }

    @Override
    public NavigableMap<K, V> descendingMap()
    {
        Comparator<K> reversed = comparator.keyComparator.reversed();
        return new BTreeMap<>(BTree.build(BulkIterator.of(BTree.iterable(tree, BTree.Dir.DESC).iterator()), BTree.size(tree), UpdateFunction.noOp),
                              new KeyComparator<>(reversed), new AsymmetricKeyComparator<>(reversed));
    }

    @Override
    public NavigableSet<K> navigableKeySet()
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public NavigableSet<K> descendingKeySet()
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive)
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive)
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive)
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public Comparator<K> comparator()
    {
        return comparator.keyComparator;
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey)
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public SortedMap<K, V> headMap(K toKey)
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey)
    {
        throw new UnsupportedOperationException("todo");
    }

    @Override
    public K firstKey()
    {
        if (BTree.isEmpty(tree))
            return null;
        return BTree.<Map.Entry<K, V>>findByIndex(tree, 0).getKey();
    }

    @Override
    public K lastKey()
    {
        if (BTree.isEmpty(tree))
            return null;
        return getEntry(size() - 1).getKey();
    }

    private Map.Entry<K, V> getEntry(int idx)
    {
        return BTree.findByIndex(tree, idx);
    }
}
