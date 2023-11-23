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
import java.util.Set;

import com.google.common.collect.BiMap;

import static java.util.Comparator.naturalOrder;

public class BTreeBiMap<K, V> extends AbstractBTreeMap<K, V> implements BiMap<K, V>
{
    private final Object[] inverse;
    private final KeyComparator<V, K> valueComparator;

    protected static <K, V> BTreeBiMap<K, V> withComparators(Object[] tree, Object [] inverse, Comparator<K> comparator, Comparator<V> valueComparator)
    {
        return new BTreeBiMap<>(tree, inverse, new KeyComparator<>(comparator), new KeyComparator<>(valueComparator));
    }

    private BTreeBiMap(Object[] tree, Object [] inverse, KeyComparator<K, V> comparator, KeyComparator<V, K> valueComparator)
    {
        super(tree, comparator);
        this.valueComparator = valueComparator;
        this.inverse = inverse;
    }

    public static <K, V> BTreeBiMap<K, V> empty(Comparator<K> comparator, Comparator<V> valueComparator)
    {
        return withComparators(BTree.empty(), BTree.empty(), comparator, valueComparator);
    }

    public static <K extends Comparable<K>, V extends Comparable<V>> BTreeBiMap<K, V> empty()
    {
        return BTreeBiMap.<K, V>empty(naturalOrder(), naturalOrder());
    }

    @Override
    public BiMap<V, K> inverse()
    {
        return new BTreeBiMap<>(inverse, tree, valueComparator, comparator);
    }

    @Override
    public BTreeBiMap<K, V> with(K key, V value)
    {
        if (key == null || value == null)
            throw new NullPointerException();
        AbstractBTreeMap.Entry<K, V> entry = new AbstractBTreeMap.Entry<>(key, value);
        AbstractBTreeMap.Entry<V, K> inverseEntry = new AbstractBTreeMap.Entry<>(value, key);
        if (BTree.find(tree, comparator, entry) != null)
            throw new IllegalArgumentException("Key already exists in map: " + key);
        if (BTree.find(inverse, valueComparator, inverseEntry) != null)
            throw new IllegalArgumentException("Value already exists in map: " + value);

        return new BTreeBiMap<>(BTree.update(tree, new Object[]{ entry }, comparator, UpdateFunction.noOp()),
                                BTree.update(inverse, new Object[] { new AbstractBTreeMap.Entry<>(value, key) }, valueComparator, UpdateFunction.noOp()),
                                comparator,
                                valueComparator);
    }

    @Override
    public BTreeBiMap<K, V> withForce(K key, V value)
    {
        // todo: optimise
        return without(key).with(key, value);
    }

    public BTreeBiMap<K, V> without(K key)
    {
        AbstractBTreeMap.Entry<K, V> entry = new AbstractBTreeMap.Entry<>(key, null);
        AbstractBTreeMap.Entry<K, V> existingEntry = BTree.find(tree, comparator, entry);
        if (existingEntry == null)
            return this;

        Object[] newTree = BTreeRemoval.remove(tree, comparator, new AbstractBTreeMap.Entry<>(key, null));
        Object[] newInverse = BTreeRemoval.remove(inverse, valueComparator, new AbstractBTreeMap.Entry<>(existingEntry.getValue(), null));
        return new BTreeBiMap<>(newTree, newInverse, comparator, valueComparator);
    }

    public Set<V> values()
    {
        return inverse().keySet();
    }
}
