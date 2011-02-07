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
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

public class ExpiringMap<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(ExpiringMap.class);
    private final Function<Pair<K,V>, ?> postExpireHook;

    private static class CacheableObject<T>
    {
        private final T value;
        private final long age;

        CacheableObject(T o)
        {
            value = o;
            age = System.currentTimeMillis();
        }

        T getValue()
        {
            return value;
        }

        boolean isReadyToDie(long expiration)
        {
            return ((System.currentTimeMillis() - age) > expiration);
        }
    }

    private class CacheMonitor extends TimerTask
    {
        private final long expiration;

        CacheMonitor(long expiration)
        {
            this.expiration = expiration;
        }

        @Override
        public void run()
        {
            for (Map.Entry<K, CacheableObject> entry : cache.entrySet())
            {
                if (entry.getValue().isReadyToDie(expiration))
                {
                    cache.remove(entry.getKey());
                    postExpireHook.apply(new Pair(entry.getKey(), entry.getValue().getValue()));
                }
            }
        }
    }

    private final NonBlockingHashMap<K, CacheableObject> cache = new NonBlockingHashMap<K, CacheableObject>();
    private final Timer timer;
    private static int counter = 0;

    public ExpiringMap(long expiration)
    {
        this(expiration, null);
    }

    /**
     *
     * @param expiration the TTL for objects in the cache in milliseconds
     */
    public ExpiringMap(long expiration, Function<Pair<K,V>, ?> postExpireHook)
    {
        this.postExpireHook = postExpireHook;
        if (expiration <= 0)
        {
            throw new IllegalArgumentException("Argument specified must be a positive number");
        }

        timer = new Timer("EXPIRING-MAP-TIMER-" + (++counter), true);
        timer.schedule(new CacheMonitor(expiration), expiration / 2, expiration / 2);
    }

    public void shutdown()
    {
        timer.cancel();
    }

    public V put(K key, V value)
    {
        CacheableObject<V> previous = cache.put(key, new CacheableObject<V>(value));
        return (previous == null) ? null : previous.getValue();
    }

    public V get(K key)
    {
        V result = null;
        CacheableObject<V> co = cache.get(key);
        if (co != null)
        {
            result = co.getValue();
        }
        return result;
    }

    public V remove(K key)
    {
        CacheableObject<V> co = cache.remove(key);
        V result = null;
        if (co != null)
        {
            result = co.getValue();
        }
        return result;
    }

    public long getAge(K key)
    {
        long age = 0;
        CacheableObject<V> co = cache.get(key);
        if (co != null)
        {
            age = co.age;
        }
        return age;
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
