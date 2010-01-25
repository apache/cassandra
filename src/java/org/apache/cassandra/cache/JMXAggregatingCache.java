package org.apache.cassandra.cache;

import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

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

    public double getHitRate()
    {
        int n = 0;
        double rate = 0;
        for (IAggregatableCacheProvider cacheProvider : cacheProviders)
        {
            rate += cacheProvider.getCache().getHitRate();
            n++;
        }
        return rate / n;
    }
}
