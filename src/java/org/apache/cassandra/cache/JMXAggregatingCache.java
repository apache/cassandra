package org.apache.cassandra.cache;

public class JMXAggregatingCache implements JMXAggregatingCacheMBean
{
    private final Iterable<InstrumentedCache> caches;

    public JMXAggregatingCache(Iterable<InstrumentedCache> caches, String table, String name)
    {
        this.caches = caches;
        AbstractCache.registerMBean(this, table, name);
    }

    public int getCapacity()
    {
        int capacity = 0;
        for (InstrumentedCache cache : caches)
        {
            capacity += cache.getCapacity();
        }
        return capacity;
    }

    public int getSize()
    {
        int size = 0;
        for (InstrumentedCache cache : caches)
        {
            size += cache.getSize();
        }
        return size;
    }

    public double getHitRate()
    {
        int n = 0;
        double rate = 0;
        for (InstrumentedCache cache : caches)
        {
            rate += cache.getHitRate();
            n++;
        }
        return rate / n;
    }
}
