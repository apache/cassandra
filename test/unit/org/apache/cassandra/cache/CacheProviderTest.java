/*
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
 */
package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;


import java.util.ArrayList;
import java.util.List;


import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;

import com.googlecode.concurrentlinkedhashmap.Weighers;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;

public class CacheProviderTest
{
    MeasureableString key1 = new MeasureableString("key1");
    MeasureableString key2 = new MeasureableString("key2");
    MeasureableString key3 = new MeasureableString("key3");
    MeasureableString key4 = new MeasureableString("key4");
    MeasureableString key5 = new MeasureableString("key5");
    private static final long CAPACITY = 4;
    private static final String KEYSPACE1 = "CacheProviderTest1";
    private static final String CF_STANDARD1 = "Standard1";

    private static CFMetaData cfm;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        cfm = CFMetaData.Builder.create(KEYSPACE1, CF_STANDARD1)
                                        .addPartitionKey("pKey", AsciiType.instance)
                                        .addRegularColumn("col1", AsciiType.instance)
                                        .build();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    cfm);
    }

    private CachedBTreePartition createPartition()
    {
        PartitionUpdate update = new RowUpdateBuilder(cfm, System.currentTimeMillis(), "key1")
                                 .add("col1", "val1")
                                 .buildUpdate();

        return CachedBTreePartition.create(update.unfilteredIterator(), FBUtilities.nowInSeconds());
    }

    private void simpleCase(CachedBTreePartition partition, ICache<MeasureableString, IRowCacheEntry> cache)
    {
        cache.put(key1, partition);
        assertNotNull(cache.get(key1));

        assertDigests(cache.get(key1), partition);
        cache.put(key2, partition);
        cache.put(key3, partition);
        cache.put(key4, partition);
        cache.put(key5, partition);

        assertEquals(CAPACITY, cache.size());
    }

    private void assertDigests(IRowCacheEntry one, CachedBTreePartition two)
    {
        assertTrue(one instanceof CachedBTreePartition);
        try
        {
            MessageDigest d1 = MessageDigest.getInstance("MD5");
            MessageDigest d2 = MessageDigest.getInstance("MD5");
            UnfilteredRowIterators.digest(null, ((CachedBTreePartition) one).unfilteredIterator(), d1, MessagingService.current_version);
            UnfilteredRowIterators.digest(null, ((CachedBTreePartition) two).unfilteredIterator(), d2, MessagingService.current_version);
            assertTrue(MessageDigest.isEqual(d1.digest(), d2.digest()));
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void concurrentCase(final CachedBTreePartition partition, final ICache<MeasureableString, IRowCacheEntry> cache) throws InterruptedException
    {
        final long startTime = System.currentTimeMillis() + 500;
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                while (System.currentTimeMillis() < startTime) {}
                for (int j = 0; j < 1000; j++)
                {
                    cache.put(key1, partition);
                    cache.put(key2, partition);
                    cache.put(key3, partition);
                    cache.put(key4, partition);
                    cache.put(key5, partition);
                }
            }
        };

        List<Thread> threads = new ArrayList<>(100);
        for (int i = 0; i < 100; i++)
        {
            Thread thread = new Thread(runnable);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads)
            thread.join();
    }

    @Test
    public void testSerializingCache() throws InterruptedException
    {
        ICache<MeasureableString, IRowCacheEntry> cache = SerializingCache.create(CAPACITY, Weighers.<RefCountedMemory>singleton(), new SerializingCacheProvider.RowCacheSerializer());
        CachedBTreePartition partition = createPartition();
        simpleCase(partition, cache);
        concurrentCase(partition, cache);
    }

    @Test
    public void testKeys()
    {
        Pair<String, String> ksAndCFName = Pair.create(KEYSPACE1, CF_STANDARD1);
        byte[] b1 = {1, 2, 3, 4};
        RowCacheKey key1 = new RowCacheKey(ksAndCFName, ByteBuffer.wrap(b1));
        byte[] b2 = {1, 2, 3, 4};
        RowCacheKey key2 = new RowCacheKey(ksAndCFName, ByteBuffer.wrap(b2));
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());

        byte[] b3 = {1, 2, 3, 5};
        RowCacheKey key3 = new RowCacheKey(ksAndCFName, ByteBuffer.wrap(b3));
        assertNotSame(key1, key3);
        assertNotSame(key1.hashCode(), key3.hashCode());
    }

    private class MeasureableString implements IMeasurableMemory
    {
        public final String string;

        public MeasureableString(String input)
        {
            this.string = input;
        }

        public long unsharedHeapSize()
        {
            return string.length();
        }
    }
}
