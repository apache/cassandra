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
package org.apache.cassandra.cache;

import java.util.Set;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weigher;

import com.googlecode.concurrentlinkedhashmap.Weighers;

/** Wrapper so CLHM can implement ICache interface.
 *  (this is what you get for making library classes final.) */
public class ConcurrentLinkedHashCache<K, V> implements ICache<K, V>
{
    public static final int DEFAULT_CONCURENCY_LEVEL = 64;
    private final ConcurrentLinkedHashMap<K, V> map;

    public ConcurrentLinkedHashCache(ConcurrentLinkedHashMap<K, V> map)
    {
        this.map = map;
    }

    /**
     * Initialize a cache with weigher = Weighers.singleton() and initial capacity 0
     *
     * @param capacity cache weighted capacity
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return initialized cache
     */
    public static <K, V> ConcurrentLinkedHashCache<K, V> create(long capacity)
    {
        return create(capacity, Weighers.<V>singleton());
    }

    /**
     * Initialize a cache with initial capacity set to 0
     *
     * @param weightedCapacity cache weighted capacity
     * @param weigher The weigher to use
     *
     * @param <K> key type
     * @param <V> value type
     *
     * @return initialized cache
     */
    public static <K, V> ConcurrentLinkedHashCache<K, V> create(long weightedCapacity, Weigher<V> weigher)
    {
        ConcurrentLinkedHashMap<K, V> map = new ConcurrentLinkedHashMap.Builder<K, V>()
                                            .weigher(weigher)
                                            .maximumWeightedCapacity(weightedCapacity)
                                            .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                                            .build();

        return new ConcurrentLinkedHashCache<K, V>(map);
    }

    public long capacity()
    {
        return map.capacity();
    }

    public void setCapacity(long capacity)
    {
        map.setCapacity(capacity);
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public int size()
    {
        return map.size();
    }

    public long weightedSize()
    {
        return map.weightedSize();
    }

    public void clear()
    {
        map.clear();
    }

    public V get(K key)
    {
        return map.get(key);
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public boolean putIfAbsent(K key, V value)
    {
        return map.putIfAbsent(key, value) == null;
    }

    public boolean replace(K key, V old, V value)
    {
        return map.replace(key, old, value);
    }

    public void remove(K key)
    {
        map.remove(key);
    }

    public Set<K> keySet()
    {
        return map.keySet();
    }

    public Set<K> hotKeySet(int n)
    {
        return map.descendingKeySetWithLimit(n);
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }

    public boolean isPutCopying()
    {
        return false;
    }
}
