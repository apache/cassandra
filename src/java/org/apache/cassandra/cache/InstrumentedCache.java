package org.apache.cassandra.cache;

import java.util.concurrent.atomic.AtomicLong;

import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

public class InstrumentedCache<K, V>
{
    private int capacity;
    private final ConcurrentLinkedHashMap<K, V> map;
    private final AtomicLong requests = new AtomicLong(0);
    private final AtomicLong hits = new AtomicLong(0);
    long lastRequests, lastHits;

    public InstrumentedCache(int capacity)
    {
        this.capacity = capacity;
        map = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.SECOND_CHANCE, capacity);
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public V get(K key)
    {
        V v = map.get(key);
        requests.incrementAndGet();
        if (v != null)
            hits.incrementAndGet();
        return v;
    }

    public V getInternal(K key)
    {
        return map.get(key);
    }

    public void remove(K key)
    {
        map.remove(key);
    }

    public int getCapacity()
    {
        return capacity;
    }

    public void setCapacity(int capacity)
    {
        map.setCapacity(capacity);
        this.capacity = capacity;
     }

    public int getSize()
    {
        return map.size();
    }

    public long getHits()
    {
        return hits.get();
    }

    public long getRequests()
    {
        return requests.get();
    }

    public double getRecentHitRate()
    {
        long r = requests.get();
        long h = hits.get();
        try
        {
            return ((double)(h - lastHits)) / (r - lastRequests);
        }
        finally
        {
            lastRequests = r;
            lastHits = h;
        }
    }
}
