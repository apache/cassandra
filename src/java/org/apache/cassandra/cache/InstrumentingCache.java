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

import org.apache.cassandra.metrics.CacheMetrics;

/**
 * Wraps an ICache in requests + hits tracking.
 */
public class InstrumentingCache<K, V>
{
    private final ICache<K, V> map;
    private final String type;

    private CacheMetrics metrics;

    public InstrumentingCache(String type, ICache<K, V> map)
    {
        this.map = map;
        this.type = type;
        this.metrics = new CacheMetrics(type, map);
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public boolean putIfAbsent(K key, V value)
    {
        return map.putIfAbsent(key, value);
    }

    public boolean replace(K key, V old, V value)
    {
        return map.replace(key, old, value);
    }

    public V get(K key)
    {
        metrics.requests.mark();
        V v = map.get(key);
        if (v != null)
            metrics.hits.mark();
        else
            metrics.misses.mark();
        return v;
    }

    public V getInternal(K key)
    {
        return map.get(key);
    }

    public void remove(K key)
    {
        map.remove(key);
    }

    public long getCapacity()
    {
        return map.capacity();
    }

    public void setCapacity(long capacity)
    {
        map.setCapacity(capacity);
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

        // this does not clear metered metrics which are defined statically. for testing purposes, these can be
        // cleared by CacheMetrics.reset()
        metrics = new CacheMetrics(type, map);
    }

    public Iterator<K> keyIterator()
    {
        return map.keyIterator();
    }

    public Iterator<K> hotKeyIterator(int n)
    {
        return map.hotKeyIterator(n);
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }

    public CacheMetrics getMetrics()
    {
        return metrics;
    }
}
