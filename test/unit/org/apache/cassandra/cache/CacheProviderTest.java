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


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamily;

import com.googlecode.concurrentlinkedhashmap.Weighers;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.*;

public class CacheProviderTest extends SchemaLoader
{
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    String key4 = "key4";
    String key5 = "key5";
    private static final long CAPACITY = 4;
    private String tableName = "Keyspace1";
    private String cfName = "Standard1";

    private void simpleCase(ColumnFamily cf, ICache<String, IRowCacheEntry> cache)
    {
        cache.put(key1, cf);
        assert cache.get(key1) != null;

        assertDigests(cache.get(key1), cf);
        cache.put(key2, cf);
        cache.put(key3, cf);
        cache.put(key4, cf);
        cache.put(key5, cf);

        assertEquals(CAPACITY, cache.size());
    }

    private void assertDigests(IRowCacheEntry one, ColumnFamily two)
    {
        // CF does not implement .equals
        assert one instanceof ColumnFamily;
        assert ColumnFamily.digest((ColumnFamily)one).equals(ColumnFamily.digest(two));
    }

    // TODO this isn't terribly useful
    private void concurrentCase(final ColumnFamily cf, final ICache<String, IRowCacheEntry> cache) throws InterruptedException
    {
        Runnable runable = new Runnable()
        {
            public void run()
            {
                for (int j = 0; j < 10; j++)
                {
                    cache.put(key1, cf);
                    cache.put(key2, cf);
                    cache.put(key3, cf);
                    cache.put(key4, cf);
                    cache.put(key5, cf);
                }
            }
        };

        List<Thread> threads = new ArrayList<Thread>(100);
        for (int i = 0; i < 100; i++)
        {
            Thread thread = new Thread(runable);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads)
            thread.join();
    }

    private ColumnFamily createCF()
    {
        ColumnFamily cf = ColumnFamily.create(tableName, cfName);
        cf.addColumn(column("vijay", "great", 1));
        cf.addColumn(column("awesome", "vijay", 1));
        return cf;
    }

    @Test
    public void testHeapCache() throws InterruptedException
    {
        ICache<String, IRowCacheEntry> cache = ConcurrentLinkedHashCache.create(CAPACITY, Weighers.<String, IRowCacheEntry>entrySingleton());
        ColumnFamily cf = createCF();
        simpleCase(cf, cache);
        concurrentCase(cf, cache);
    }

    @Test
    public void testSerializingCache() throws InterruptedException
    {
        ICache<String, IRowCacheEntry> cache = SerializingCache.create(CAPACITY, Weighers.<FreeableMemory>singleton(), new SerializingCacheProvider.RowCacheSerializer());
        ColumnFamily cf = createCF();
        simpleCase(cf, cache);
        concurrentCase(cf, cache);
    }
    
    @Test
    public void testKeys()
    {
        UUID cfId = UUID.randomUUID();

        byte[] b1 = {1, 2, 3, 4};
        RowCacheKey key1 = new RowCacheKey(cfId, ByteBuffer.wrap(b1));
        byte[] b2 = {1, 2, 3, 4};
        RowCacheKey key2 = new RowCacheKey(cfId, ByteBuffer.wrap(b2));
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
        
        byte[] b3 = {1, 2, 3, 5};
        RowCacheKey key3 = new RowCacheKey(cfId, ByteBuffer.wrap(b3));
        assertNotSame(key1, key3);
        assertNotSame(key1.hashCode(), key3.hashCode());
    }
}
