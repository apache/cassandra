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

import java.util.Iterator;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EntryWeigher;

/** Wrapper so CLHM can implement ICache interface.
 *  (this is what you get for making library classes final.) */
public class ConcurrentLinkedHashCache<K extends IMeasurableMemory, V extends IMeasurableMemory> implements ICache<K, V>
{
    public static final int DEFAULT_CONCURENCY_LEVEL = 64;
    private final ConcurrentLinkedHashMap<K, V> map;

    private ConcurrentLinkedHashCache(ConcurrentLinkedHashMap<K, V> map)
    {
        this.map = map;
    }

    /**
     * Initialize a cache with initial capacity with weightedCapacity
     */
    public static <K extends IMeasurableMemory, V extends IMeasurableMemory> ConcurrentLinkedHashCache<K, V> create(long weightedCapacity, EntryWeigher<K, V> entryWeiger)
    {
        ConcurrentLinkedHashMap<K, V> map = new ConcurrentLinkedHashMap.Builder<K, V>()
                                            .weigher(entryWeiger)
                                            .maximumWeightedCapacity(weightedCapacity)
                                            .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                                            .build();

        return new ConcurrentLinkedHashCache<>(map);
    }

    public static <K extends IMeasurableMemory, V extends IMeasurableMemory> ConcurrentLinkedHashCache<K, V> create(long weightedCapacity)
    {
        return create(weightedCapacity, new EntryWeigher<K, V>()
        {
            public int weightOf(K key, V value)
            {
                long size = key.unsharedHeapSize() + value.unsharedHeapSize();
                assert size <= Integer.MAX_VALUE : "Serialized size cannot be more than 2GB/Integer.MAX_VALUE";
                return (int) size;
            }
        });
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

    public Iterator<K> keyIterator()
    {
        return map.keySet().iterator();
    }

    public Iterator<K> hotKeyIterator(int n)
    {
        return map.descendingKeySetWithLimit(n).iterator();
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }
}
