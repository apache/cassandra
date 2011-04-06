package org.apache.cassandra.cache;

import java.util.Set;

/**
 * This is similar to the Map interface, but requires maintaining a given capacity
 * and does not require put or remove to return values, which lets SerializingCache
 * be more efficient by avoiding deserialize except on get.
 */
public interface ICache<K, V>
{
    public int capacity();

    public void setCapacity(int capacity);

    public void put(K key, V value);

    public V get(K key);

    public void remove(K key);

    public int size();

    public void clear();

    public Set<K> keySet();

    /**
     * @return true if the cache implementation inherently copies the cached values; otherwise,
     * the caller should copy manually before caching shared values like Thrift ByteBuffers.
     */
    public boolean isPutCopying();
}
