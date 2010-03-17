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


import java.util.concurrent.atomic.AtomicLong;

import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

public class InstrumentedCache<K, V>
{
    private int capacity;
    private final ConcurrentLinkedHashMap<K, V> map;
    private final AtomicLong requests = new AtomicLong(0);
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong lastRequests = new AtomicLong(0);
    private final AtomicLong lastHits = new AtomicLong(0);

    public InstrumentedCache(int capacity)
    {
        this.capacity = capacity;
        map = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.SECOND_CHANCE, capacity);
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public V get(K key)
    {
        V v = map.get(key);
        requests.incrementAndGet();
        if (v != null)
            hits.incrementAndGet();
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

    public int getCapacity()
    {
        return capacity;
    }

    public void setCapacity(int capacity)
    {
        map.setCapacity(capacity);
        this.capacity = capacity;
     }

    public int getSize()
    {
        return map.size();
    }

    public long getHits()
    {
        return hits.get();
    }

    public long getRequests()
    {
        return requests.get();
    }

    public double getRecentHitRate()
    {
        long r = requests.get();
        long h = hits.get();
        try
        {
            return ((double)(h - lastHits.get())) / (r - lastRequests.get());
        }
        finally
        {
            lastRequests.set(r);
            lastHits.set(h);
        }
    }

    public void clear()
    {
        map.clear();
        requests.set(0);
        hits.set(0);
    }
}
