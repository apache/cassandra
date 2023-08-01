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

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy.Eviction;
import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.cassandra.concurrent.ImmediateExecutor;

/**
 * An adapter from a Caffeine cache to the ICache interface. This provides an on-heap cache using
 * the W-TinyLFU eviction policy (http://arxiv.org/pdf/1512.00727.pdf), which has a higher hit rate
 * than an LRU.
 */
public class CaffeineCache<K extends IMeasurableMemory, V extends IMeasurableMemory> implements ICache<K, V>
{
    private final Cache<K, V> cache;
    private final Eviction<K, V> policy;

    private CaffeineCache(Cache<K, V> cache)
    {
        this.cache = cache;
        this.policy = cache.policy().eviction().orElseThrow(() -> 
            new IllegalArgumentException("Expected a size bounded cache"));
        checkState(policy.isWeighted(), "Expected a weighted cache");
    }

    /**
     * Initialize a cache with initial capacity with weightedCapacity
     */
    public static <K extends IMeasurableMemory, V extends IMeasurableMemory> CaffeineCache<K, V> create(long weightedCapacity, Weigher<K, V> weigher)
    {
        Cache<K, V> cache = Caffeine.newBuilder()
                .maximumWeight(weightedCapacity)
                .weigher(weigher)
                .executor(ImmediateExecutor.INSTANCE)
                .build();
        return new CaffeineCache<>(cache);
    }

    public static <K extends IMeasurableMemory, V extends IMeasurableMemory> CaffeineCache<K, V> create(long weightedCapacity)
    {
        return create(weightedCapacity, (key, value) -> {
            long size = key.unsharedHeapSize() + value.unsharedHeapSize();
            if (size > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Serialized size cannot be more than 2GiB/Integer.MAX_VALUE");
            }
            return (int) size;
        });
    }

    public long capacity()
    {
        return policy.getMaximum();
    }

    public void setCapacity(long capacity)
    {
        policy.setMaximum(capacity);
    }

    public boolean isEmpty()
    {
        return cache.asMap().isEmpty();
    }

    public int size()
    {
        return cache.asMap().size();
    }

    public long weightedSize()
    {
        return policy.weightedSize().getAsLong();
    }

    public void clear()
    {
        cache.invalidateAll();
    }

    public V get(K key)
    {
        return cache.getIfPresent(key);
    }

    public void put(K key, V value)
    {
        cache.put(key, value);
    }

    public boolean putIfAbsent(K key, V value)
    {
        return cache.asMap().putIfAbsent(key, value) == null;
    }

    public boolean replace(K key, V old, V value)
    {
        return cache.asMap().replace(key, old, value);
    }

    public void remove(K key)
    {
        cache.invalidate(key);
    }

    public Iterator<K> keyIterator()
    {
        return cache.asMap().keySet().iterator();
    }

    public Iterator<K> hotKeyIterator(int n)
    {
        return policy.hottest(n).keySet().iterator();
    }

    public boolean containsKey(K key)
    {
        return cache.asMap().containsKey(key);
    }
}
