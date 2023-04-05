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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.github.benmanes.caffeine.cache.Weigher;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

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

    private static TableMetadata cfm;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        cfm =
            TableMetadata.builder(KEYSPACE1, CF_STANDARD1)
                         .addPartitionKeyColumn("pKey", AsciiType.instance)
                         .addRegularColumn("col1", AsciiType.instance)
                         .build();

        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), cfm);
    }

    private CachedBTreePartition createPartition()
    {
        PartitionUpdate update = new RowUpdateBuilder(cfm, currentTimeMillis(), "key1")
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
        Digest d1 = Digest.forReadResponse();
        Digest d2 = Digest.forReadResponse();
        UnfilteredRowIterators.digest(((CachedBTreePartition) one).unfilteredIterator(), d1, MessagingService.current_version);
        UnfilteredRowIterators.digest(((CachedBTreePartition) two).unfilteredIterator(), d2, MessagingService.current_version);
        assertArrayEquals(d1.digest(), d2.digest());
    }

    private void concurrentCase(final CachedBTreePartition partition, final ICache<MeasureableString, IRowCacheEntry> cache) throws InterruptedException
    {
        final long startTime = currentTimeMillis() + 500;
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                while (currentTimeMillis() < startTime) {}
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
            Thread thread = NamedThreadFactory.createAnonymousThread(runnable);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads)
            thread.join();
    }

    @Test
    public void testSerializingCache() throws InterruptedException
    {
        ICache<MeasureableString, IRowCacheEntry> cache = SerializingCache.create(CAPACITY,
            Weigher.singletonWeigher(), new SerializingCacheProvider.RowCacheSerializer());
        CachedBTreePartition partition = createPartition();
        simpleCase(partition, cache);
        concurrentCase(partition, cache);
    }

    @Test
    public void testKeys()
    {
        TableId id1 = TableId.generate();
        byte[] b1 = {1, 2, 3, 4};
        RowCacheKey key1 = new RowCacheKey(id1, null, ByteBuffer.wrap(b1));
        TableId id2 = TableId.fromString(id1.toString());
        byte[] b2 = {1, 2, 3, 4};
        RowCacheKey key2 = new RowCacheKey(id2, null, ByteBuffer.wrap(b2));
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());

        TableMetadata tm = TableMetadata.builder("ks", "tab", id1)
                                        .addPartitionKeyColumn("pk", UTF8Type.instance)
                                        .build();

        assertTrue(key1.sameTable(tm));

        byte[] b3 = {1, 2, 3, 5};
        RowCacheKey key3 = new RowCacheKey(id1, null, ByteBuffer.wrap(b3));
        assertNotSame(key1, key3);
        assertNotSame(key1.hashCode(), key3.hashCode());

        // with index name

        key1 = new RowCacheKey(id1, "indexFoo", ByteBuffer.wrap(b1));
        assertNotSame(key1, key2);
        assertNotSame(key1.hashCode(), key2.hashCode());

        key2 = new RowCacheKey(id2, "indexFoo", ByteBuffer.wrap(b2));
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());

        tm = TableMetadata.builder("ks", "tab.indexFoo", id1)
                          .kind(TableMetadata.Kind.INDEX)
                          .addPartitionKeyColumn("pk", UTF8Type.instance)
                          .indexes(Indexes.of(IndexMetadata.fromSchemaMetadata("indexFoo", IndexMetadata.Kind.KEYS, Collections.emptyMap())))
                          .build();
        assertTrue(key1.sameTable(tm));

        key3 = new RowCacheKey(id1, "indexFoo", ByteBuffer.wrap(b3));
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
