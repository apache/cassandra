package org.apache.cassandra.cache;

public interface IAggregatableCacheProvider<K, V>
{
    public InstrumentedCache<K, V> getCache();
    public long getObjectCount();
}
