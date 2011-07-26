package org.apache.cassandra.cache;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
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

    public static <K, V> ConcurrentLinkedHashCache<K, V> create(int capacity, String tableName, String cfname)
    {
        ConcurrentLinkedHashMap<K, V> map = new ConcurrentLinkedHashMap.Builder<K, V>()
                                            .weigher(Weighers.<V>singleton())
                                            .initialCapacity(capacity)
                                            .maximumWeightedCapacity(capacity)
                                            .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                                            .build();
        return new ConcurrentLinkedHashCache<K, V>(map);
    }

    public int capacity()
    {
        return map.capacity();
    }

    public void setCapacity(int capacity)
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

    public boolean isPutCopying()
    {
        return false;
    }
}
