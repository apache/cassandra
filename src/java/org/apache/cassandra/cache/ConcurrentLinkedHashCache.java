package org.apache.cassandra.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.Weighers;

/** Wrapper so CLHM can implement ICache interface.
 *  (this is what you get for making library classes final.) */
public class ConcurrentLinkedHashCache<K, V> implements ICache<K, V>
{
    public static final int DEFAULT_CONCURENCY_LEVEL = 64;
    private final ConcurrentLinkedHashMap<K, V> map;

    public ConcurrentLinkedHashCache(ConcurrentLinkedHashMap<K, V> map)
    {
        this.map = map;
    }

    public static <K, V> ConcurrentLinkedHashCache<K, V> create(int capacity)
    {
        ConcurrentLinkedHashMap<K, V> map = new ConcurrentLinkedHashMap.Builder<K, V>()
                                            .weigher(Weighers.<V>singleton())
                                            .initialCapacity(capacity)
                                            .maximumWeightedCapacity(capacity)
                                            .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                                            .build();
        return new ConcurrentLinkedHashCache<K, V>(map);
    }

    public void discard(K key)
    {
        remove(key);
    }

    public int capacity()
    {
        return map.capacity();
    }

    public void setCapacity(int capacity)
    {
        map.setCapacity(capacity);
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public int size()
    {
        return map.size();
    }

    public void clear()
    {
        map.clear();
    }

    public V get(K key)
    {
        return map.get(key);
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public void remove(K key)
    {
        map.remove(key);
    }

    public Set<K> keySet()
    {
        return map.keySet();
    }
}
