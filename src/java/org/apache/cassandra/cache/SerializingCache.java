/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cache;

import java.io.IOException;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.MemoryInputStream;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.vint.EncodedDataInputStream;
import org.apache.cassandra.utils.vint.EncodedDataOutputStream;

/**
 * Serializes cache values off-heap.
 */
public class SerializingCache<K, V> implements ICache<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(SerializingCache.class);
    private static final TypeSizes ENCODED_TYPE_SIZES = TypeSizes.VINT;

    private static final int DEFAULT_CONCURENCY_LEVEL = 64;

    private final ConcurrentLinkedHashMap<K, FreeableMemory> map;
    private final ISerializer<V> serializer;

    private SerializingCache(long capacity, Weigher<FreeableMemory> weigher, ISerializer<V> serializer)
    {
        this.serializer = serializer;

        EvictionListener<K,FreeableMemory> listener = new EvictionListener<K, FreeableMemory>()
        {
            public void onEviction(K k, FreeableMemory mem)
            {
                mem.unreference();
            }
        };

        this.map = new ConcurrentLinkedHashMap.Builder<K, FreeableMemory>()
                   .weigher(weigher)
                   .maximumWeightedCapacity(capacity)
                   .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                   .listener(listener)
                   .build();
    }

    public static <K, V> SerializingCache<K, V> create(long weightedCapacity, Weigher<FreeableMemory> weigher, ISerializer<V> serializer)
    {
        return new SerializingCache<K, V>(weightedCapacity, weigher, serializer);
    }

    public static <K, V> SerializingCache<K, V> create(long weightedCapacity, ISerializer<V> serializer)
    {
        return create(weightedCapacity, new Weigher<FreeableMemory>()
        {
            public int weightOf(FreeableMemory value)
            {
                long size = value.size();
                assert size < Integer.MAX_VALUE : "Serialized size cannot be more than 2GB";
                return (int) size;
            }
        }, serializer);
    }

    private V deserialize(FreeableMemory mem)
    {
        try
        {
            return serializer.deserialize(new EncodedDataInputStream(new MemoryInputStream(mem)));
        }
        catch (IOException e)
        {
            logger.debug("Cannot fetch in memory data, we will failback to read from disk ", e);
            return null;
        }
    }

    private FreeableMemory serialize(V value)
    {
        long serializedSize = serializer.serializedSize(value, ENCODED_TYPE_SIZES);
        if (serializedSize > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Unable to allocate " + serializedSize + " bytes");

        FreeableMemory freeableMemory;
        try
        {
            freeableMemory = new FreeableMemory(serializedSize);
        }
        catch (OutOfMemoryError e)
        {
            return null;
        }

        try
        {
            serializer.serialize(value, new EncodedDataOutputStream(new MemoryOutputStream(freeableMemory)));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        return freeableMemory;
    }

    public long capacity()
    {
        return map.capacity();
    }

    public void setCapacity(long capacity)
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

    public long weightedSize()
    {
        return map.weightedSize();
    }

    public void clear()
    {
        map.clear();
    }

    public V get(Object key)
    {
        FreeableMemory mem = map.get(key);
        if (mem == null)
            return null;
        if (!mem.reference())
            return null;
        try
        {
            return deserialize(mem);
        }
        finally
        {
            mem.unreference();
        }
    }

    public void put(K key, V value)
    {
        FreeableMemory mem = serialize(value);
        if (mem == null)
            return; // out of memory.  never mind.

        FreeableMemory old = map.put(key, mem);
        if (old != null)
            old.unreference();
    }

    public boolean putIfAbsent(K key, V value)
    {
        FreeableMemory mem = serialize(value);
        if (mem == null)
            return false; // out of memory.  never mind.

        FreeableMemory old = map.putIfAbsent(key, mem);
        if (old != null)
            // the new value was not put, we've uselessly allocated some memory, free it
            mem.unreference();
        return old == null;
    }

    public boolean replace(K key, V oldToReplace, V value)
    {
        // if there is no old value in our map, we fail
        FreeableMemory old = map.get(key);
        if (old == null)
            return false;

        // see if the old value matches the one we want to replace
        FreeableMemory mem = serialize(value);
        if (mem == null)
            return false; // out of memory.  never mind.

        V oldValue;
        // reference old guy before de-serializing
        if (!old.reference())
            return false; // we have already freed hence noop.
        try
        {
             oldValue = deserialize(old);
        }
        finally
        {
            old.unreference();
        }
        boolean success = oldValue.equals(oldToReplace) && map.replace(key, old, mem);

        if (success)
            old.unreference(); // so it will be eventually be cleaned
        else
            mem.unreference();
        return success;
    }

    public void remove(K key)
    {
        FreeableMemory mem = map.remove(key);
        if (mem != null)
            mem.unreference();
    }

    public Set<K> keySet()
    {
        return map.keySet();
    }

    public Set<K> hotKeySet(int n)
    {
        return map.descendingKeySetWithLimit(n);
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }

    public boolean isPutCopying()
    {
        return true;
    }
}
