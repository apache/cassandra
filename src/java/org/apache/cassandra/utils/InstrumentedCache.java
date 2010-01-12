package org.apache.cassandra.utils;

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

public class InstrumentedCache<K, V> implements InstrumentedCacheMBean
{
    private final int capacity;
    private final ConcurrentLinkedHashMap<K, V> map;
    private final TimedStatsDeque stats;

    public InstrumentedCache(String table, String name, int capacity)
    {
        this.capacity = capacity;
        map = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.SECOND_CHANCE, capacity);
        stats = new TimedStatsDeque(60000);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            String mbeanName = "org.apache.cassandra.db:type=Caches,keyspace=" + table + ",cache=" + name;
            mbs.registerMBean(this, new ObjectName(mbeanName));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public V get(K key)
    {
        V v = map.get(key);
        stats.add(v == null ? 0 : 1);
        return v;
    }

    public void remove(K key)
    {
        map.remove(key);
    }

    public int getCapacity()
    {
        return capacity;
    }

    public int getSize()
    {
        return map.size();
    }

    public double getHitRate()
    {
        return stats.mean();
    }
}
