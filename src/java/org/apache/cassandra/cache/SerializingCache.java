package org.apache.cassandra.cache;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataOutputStream;
import java.io.IOError;
import java.io.IOException;
import java.util.Set;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.googlecode.concurrentlinkedhashmap.EvictionListener;
import com.googlecode.concurrentlinkedhashmap.Weighers;

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.MemoryInputStream;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializes cache values off-heap.
 */
public class SerializingCache<K, V> implements ICache<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(SerializingCache.class);
    private static final int DEFAULT_CONCURENCY_LEVEL = 64;
    
    private final ConcurrentLinkedHashMap<K, FreeableMemory> map;
    private final ISerializer<V> serializer;

    public SerializingCache(int capacity, ISerializer<V> serializer, String tableName, String cfName)
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
                   .weigher(Weighers.<FreeableMemory>singleton())
                   .initialCapacity(capacity)
                   .maximumWeightedCapacity(capacity)
                   .concurrencyLevel(DEFAULT_CONCURENCY_LEVEL)
                   .listener(listener)
                   .build();
    }

	private V deserialize(FreeableMemory mem)
    {
        try
        {
            return serializer.deserialize(new MemoryInputStream(mem));
        }
        catch (IOException e)
        {
            logger.debug("Cannot fetch in memory data, we will failback to read from disk ", e);
            return null;
        }
    }

    private FreeableMemory serialize(V value)
    {
        long serializedSize = serializer.serializedSize(value);
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
            serializer.serialize(value, new DataOutputStream(new MemoryOutputStream(freeableMemory)));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return freeableMemory;
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

    public boolean isPutCopying()
    {
        return true;
    }
}
