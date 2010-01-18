package org.apache.cassandra.cache;

public class JMXInstrumentedCache<K, V> extends InstrumentedCache<K, V> implements JMXInstrumentedCacheMBean
{
    public JMXInstrumentedCache(String table, String name, int capacity)
    {
        super(capacity);
        AbstractCache.registerMBean(this, table, name);
    }
}