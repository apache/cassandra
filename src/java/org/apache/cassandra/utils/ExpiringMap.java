/**
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

package org.apache.cassandra.utils;

import java.util.*;

import com.google.common.base.Function;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class ExpiringMap<K, V>
{
    private static class CacheableObject<T>
    {
        private final T value;
        private final long age;
        private final long expiration;

        CacheableObject(T o, long e)
        {
            assert o != null;
            value = o;
            expiration = e;
            age = System.currentTimeMillis();
        }

        T getValue()
        {
            return value;
        }

        boolean isReadyToDie(long start)
        {
            return ((start - age) > expiration);
        }
    }

    private final NonBlockingHashMap<K, CacheableObject<V>> cache = new NonBlockingHashMap<K, CacheableObject<V>>();
    private final Timer timer;
    private static int counter = 0;
    private final long expiration;

    public ExpiringMap(long expiration)
    {
        this(expiration, null);
    }

    /**
     *
     * @param expiration the TTL for objects in the cache in milliseconds
     */
    public ExpiringMap(long expiration, final Function<Pair<K,V>, ?> postExpireHook)
    {
        this.expiration = expiration;

        if (expiration <= 0)
        {
            throw new IllegalArgumentException("Argument specified must be a positive number");
        }

        timer = new Timer("EXPIRING-MAP-TIMER-" + (++counter), true);
        TimerTask task = new TimerTask()
        {
            public void run()
            {
                long start = System.currentTimeMillis();
                for (Map.Entry<K, CacheableObject<V>> entry : cache.entrySet())
                {
                    if (entry.getValue().isReadyToDie(start))
                    {
                        cache.remove(entry.getKey());
                        if (postExpireHook != null)
                            postExpireHook.apply(new Pair<K, V>(entry.getKey(), entry.getValue().getValue()));
                    }
                }
            }
        };
        timer.schedule(task, expiration / 2, expiration / 2);
    }

    public void shutdown()
    {
        while (!cache.isEmpty())
        {
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
        }
        timer.cancel();
    }

    public void clear()
    {
        cache.clear();
    }

    public V put(K key, V value)
    {
        return put(key, value, this.expiration);
    }

    public V put(K key, V value, long timeout)
    {
        CacheableObject<V> previous = cache.put(key, new CacheableObject<V>(value, timeout));
        return (previous == null) ? null : previous.getValue();
    }

    public V get(K key)
    {
        CacheableObject<V> co = cache.get(key);
        return co == null ? null : co.getValue();
    }

    public V remove(K key)
    {
        CacheableObject<V> co = cache.remove(key);
        return co == null ? null : co.getValue();
    }

    public long getAge(K key)
    {
        CacheableObject<V> co = cache.get(key);
        return co == null ? 0 : co.age;
    }

    public int size()
    {
        return cache.size();
    }

    public boolean containsKey(K key)
    {
        return cache.containsKey(key);
    }

    public boolean isEmpty()
    {
        return cache.isEmpty();
    }

    public Set<K> keySet()
    {
        return cache.keySet();
    }
}
