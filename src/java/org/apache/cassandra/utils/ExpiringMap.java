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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpiringMap<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(ExpiringMap.class);

    private class CacheableObject
    {
        private V value_;
        private long age_;

        CacheableObject(V o)
        {
            value_ = o;
            age_ = System.currentTimeMillis();
        }

        @Override
        public boolean equals(Object o)
        {
            return value_.equals(o);
        }

        @Override
        public int hashCode()
        {
            return value_.hashCode();
        }

        V getValue()
        {
            return value_;
        }

        boolean isReadyToDie(long expiration)
        {
            return ((System.currentTimeMillis() - age_) > expiration);
        }
    }

    private class CacheMonitor extends TimerTask
    {
        private long expiration_;

        CacheMonitor(long expiration)
        {
            expiration_ = expiration;
        }

        @Override
        public void run()
        {
            synchronized (cache_)
            {
                Enumeration<K> e = cache_.keys();
                while (e.hasMoreElements())
                {
                    K key = e.nextElement();
                    CacheableObject co = cache_.get(key);
                    if (co != null && co.isReadyToDie(expiration_))
                    {
                        cache_.remove(key);
                    }
                }
            }
        }
    }

    private Hashtable<K, CacheableObject> cache_;
    private Timer timer_;
    private static int counter_ = 0;

    private void init(long expiration)
    {
        if (expiration <= 0)
        {
            throw new IllegalArgumentException("Argument specified must be a positive number");
        }

        cache_ = new Hashtable<K, CacheableObject>();
        timer_ = new Timer("CACHETABLE-TIMER-" + (++counter_), true);
        timer_.schedule(new CacheMonitor(expiration), expiration, expiration);
    }

    /*
    * Specify the TTL for objects in the cache
    * in milliseconds.
    */
    public ExpiringMap(long expiration)
    {
        init(expiration);
    }

    public void shutdown()
    {
        timer_.cancel();
    }

    public void put(K key, V value)
    {
        cache_.put(key, new CacheableObject(value));
    }

    public V get(K key)
    {
        V result = null;
        CacheableObject co = cache_.get(key);
        if (co != null)
        {
            result = co.getValue();
        }
        return result;
    }

    public V remove(K key)
    {
        CacheableObject co = cache_.remove(key);
        V result = null;
        if (co != null)
        {
            result = co.getValue();
        }
        return result;
    }

    public int size()
    {
        return cache_.size();
    }

    public boolean containsKey(K key)
    {
        return cache_.containsKey(key);
    }

    public boolean isEmpty()
    {
        return cache_.isEmpty();
    }

    public Set<K> keySet()
    {
        return cache_.keySet();
    }
}
