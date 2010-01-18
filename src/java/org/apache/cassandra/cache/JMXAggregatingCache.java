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

    public void setCapacity(int capacity)
    {
        double ratio = capacity / getCapacity();
        for (InstrumentedCache cache : caches)
        {
            cache.setCapacity(Math.max(1, (int)(cache.getCapacity() * ratio)));
        }
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
