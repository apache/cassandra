package org.apache.cassandra.cache;

import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;
import org.apache.cassandra.utils.TimedStatsDeque;

public class InstrumentedCache<K, V>
{
    private final int capacity;
    private final ConcurrentLinkedHashMap<K, V> map;
    private final TimedStatsDeque stats;

    public InstrumentedCache(int capacity)
    {
        this.capacity = capacity;
        map = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.SECOND_CHANCE, capacity);
        stats = new TimedStatsDeque(60000);
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
