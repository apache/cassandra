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


public class JMXAggregatingCache implements JMXAggregatingCacheMBean
{
    private final Iterable<IAggregatableCacheProvider> cacheProviders;

    public JMXAggregatingCache(Iterable<IAggregatableCacheProvider> caches, String table, String name)
    {
        this.cacheProviders = caches;
        AbstractCache.registerMBean(this, table, name);
    }

    public int getCapacity()
    {
        int capacity = 0;
        for (IAggregatableCacheProvider cacheProvider : cacheProviders)
        {
            capacity += cacheProvider.getCache().getCapacity();
        }
        return capacity;
    }

    public void setCapacity(int capacity)
    {
        long totalObjects = 0;
        for (IAggregatableCacheProvider cacheProvider : cacheProviders)
        {
            totalObjects += cacheProvider.getObjectCount();
        }
        for (IAggregatableCacheProvider cacheProvider : cacheProviders)
        {
            double ratio = ((double)cacheProvider.getObjectCount()) / totalObjects;
            cacheProvider.getCache().setCapacity((int)(capacity * ratio));
        }
    }

    public int getSize()
    {
        int size = 0;
        for (IAggregatableCacheProvider cacheProvider : cacheProviders)
        {
            size += cacheProvider.getCache().getSize();
        }
        return size;
    }

    public long getRequests()
    {
        long requests = 0;
        for (IAggregatableCacheProvider cacheProvider : cacheProviders)
        {
            requests += cacheProvider.getCache().getRequests();
        }
        return requests;
    }

    public long getHits()
    {
        long hits = 0;
        for (IAggregatableCacheProvider cacheProvider : cacheProviders)
        {
            hits += cacheProvider.getCache().getHits();
        }
        return hits;
    }

    public double getRecentHitRate()
    {
        int n = 0;
        double rate = 0;
        for (IAggregatableCacheProvider cacheProvider : cacheProviders)
        {
            rate += cacheProvider.getCache().getRecentHitRate();
            n++;
        }
        return rate / n;
    }
}
