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
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weigher;

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.MemoryInputStream;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Serializes cache values off-heap.
 */
public class SerializingCache<K, V> implements ICache<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(SerializingCache.class);

    private static final int DEFAULT_CONCURENCY_LEVEL = 64;

    private final ConcurrentLinkedHashMap<K, RefCountedMemory> map;
    private final ISerializer<V> serializer;

    private SerializingCache(long capacity, Weigher<RefCountedMemory> weigher, ISerializer<V> serializer)
    {
        this.serializer = serializer;

        EvictionListener<K,RefCountedMemory> listener = new EvictionListener<K, RefCountedMemory>()
        {
            public void onEviction(K k, RefCountedMemory mem)
            {
                mem.unreference();
            }
        };

        this.map = new ConcurrentLinkedHashMap.Builder<K, RefCountedMemory>()
                   .weigher(weigher)
                   .maximumWeightedCapacity(capacity)
                   .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                   .listener(listener)
                   .build();
    }

    public static <K, V> SerializingCache<K, V> create(long weightedCapacity, Weigher<RefCountedMemory> weigher, ISerializer<V> serializer)
    {
        return new SerializingCache<>(weightedCapacity, weigher, serializer);
    }

    public static <K, V> SerializingCache<K, V> create(long weightedCapacity, ISerializer<V> serializer)
    {
        return create(weightedCapacity, new Weigher<RefCountedMemory>()
        {
            public int weightOf(RefCountedMemory value)
            {
                long size = value.size();
                assert size < Integer.MAX_VALUE : "Serialized size cannot be more than 2GB";
                return (int) size;
            }
        }, serializer);
    }

    private V deserialize(RefCountedMemory mem)
    {
        try
        {
            return serializer.deserialize(new MemoryInputStream(mem));
        }
        catch (IOException e)
        {
            logger.trace("Cannot fetch in memory data, we will fallback to read from disk ", e);
            return null;
        }
    }

    private RefCountedMemory serialize(V value)
    {
        long serializedSize = serializer.serializedSize(value);
        if (serializedSize > Integer.MAX_VALUE)
            throw new IllegalArgumentException(String.format("Unable to allocate %s", FBUtilities.prettyPrintMemory(serializedSize)));

        RefCountedMemory freeableMemory;
        try
        {
            freeableMemory = new RefCountedMemory(serializedSize);
        }
        catch (OutOfMemoryError e)
        {
            return null;
        }

        try
        {
            serializer.serialize(value, new WrappedDataOutputStreamPlus(new MemoryOutputStream(freeableMemory)));
        }
        catch (IOException e)
        {
            freeableMemory.unreference();
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

    @SuppressWarnings("resource")
    public V get(K key)
    {
        RefCountedMemory mem = map.get(key);
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

    @SuppressWarnings("resource")
    public void put(K key, V value)
    {
        RefCountedMemory mem = serialize(value);
        if (mem == null)
            return; // out of memory.  never mind.

        RefCountedMemory old;
        try
        {
            old = map.put(key, mem);
        }
        catch (Throwable t)
        {
            mem.unreference();
            throw t;
        }

        if (old != null)
            old.unreference();
    }

    @SuppressWarnings("resource")
    public boolean putIfAbsent(K key, V value)
    {
        RefCountedMemory mem = serialize(value);
        if (mem == null)
            return false; // out of memory.  never mind.

        RefCountedMemory old;
        try
        {
            old = map.putIfAbsent(key, mem);
        }
        catch (Throwable t)
        {
            mem.unreference();
            throw t;
        }

        if (old != null)
            // the new value was not put, we've uselessly allocated some memory, free it
            mem.unreference();
        return old == null;
    }

    @SuppressWarnings("resource")
    public boolean replace(K key, V oldToReplace, V value)
    {
        // if there is no old value in our map, we fail
        RefCountedMemory old = map.get(key);
        if (old == null)
            return false;

        V oldValue;
        // reference old guy before de-serializing
        if (!old.reference())
            return false; // we have already freed hence noop.

        oldValue = deserialize(old);
        old.unreference();

        if (!oldValue.equals(oldToReplace))
            return false;

        // see if the old value matches the one we want to replace
        RefCountedMemory mem = serialize(value);
        if (mem == null)
            return false; // out of memory.  never mind.

        boolean success;
        try
        {
            success = map.replace(key, old, mem);
        }
        catch (Throwable t)
        {
            mem.unreference();
            throw t;
        }

        if (success)
            old.unreference(); // so it will be eventually be cleaned
        else
            mem.unreference();
        return success;
    }

    public void remove(K key)
    {
        @SuppressWarnings("resource")
        RefCountedMemory mem = map.remove(key);
        if (mem != null)
            mem.unreference();
    }

    public Iterator<K> keyIterator()
    {
        return map.keySet().iterator();
    }

    public Iterator<K> hotKeyIterator(int n)
    {
        return map.descendingKeySetWithLimit(n).iterator();
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }
}
