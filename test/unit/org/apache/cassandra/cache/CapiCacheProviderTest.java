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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cache.CapiRowCacheProvider.HashFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.partitions.CachedBTreePartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CapiCacheProviderTest
{

    static
    {
	//System.out.println("XXXXXXXXXXXXXXXXXXXX" + System.getProperty("java.library.path"));
        if (System.getProperty("cassandra.storagedir") == null)
            System.setProperty("cassandra.storagedir", "cassandra/data");
        if (System.getProperty("java.library.path") == null)
            System.setProperty("java.library.path", "cassandra/lib/sigar-bin/");
        if (System.getProperty("capi.devices") == null)
            System.setProperty("capi.devices", "/dev/sg7:0:1");  // Enabled this while using real capi flash
            //System.setProperty("capi.devices", "/dev/shm/capi.log:0:2");  // Enable this while using capi-sim
        if (System.getProperty("capi.capacity.blocks") == null)
            System.setProperty("capi.capacity.blocks", "524288");
        System.setProperty("capi.hash", TestHashFunction.class.getName());
    }

    public static class TestHashFunction implements HashFunction
    {

        @Override
        public int hashCode(byte[] bb)
        {
            String str = toString(bb);
            return Integer.parseInt(str.substring("key".length()));
        }

        @Override
        public String toString(byte[] bb)
       {
            return new String(bb);
        }

   }

    private static final String KEYSPACE1 = "CacheProviderTest1";
    private static final String CF_STANDARD1 = "Standard1";

    private static TableMetadata cfm;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        cfm = TableMetadata.builder(KEYSPACE1, CF_STANDARD1).addPartitionKeyColumn("pKey", AsciiType.instance)
                .addRegularColumn("col1", AsciiType.instance).build();

        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1), cfm);
    }

    long keyInfo = System.currentTimeMillis();

    private CachedBTreePartition createValue(String key, String value)
    {
        PartitionUpdate update = new RowUpdateBuilder(cfm, keyInfo, key).add("col1", value).buildUpdate();
        return CachedBTreePartition.create(update.unfilteredIterator(), FBUtilities.nowInSeconds());
    }

    @Before
    public void init()
    {
        CapiRowCacheProvider.cachePush.set(0L);
        CapiRowCacheProvider.cacheHit.set(0L);
        CapiRowCacheProvider.swapin.set(0L);
    }

    @Test
    public void testMemory() throws InterruptedException
    {
        int numOfTests = 1024;

        ICache<RowCacheKey, IRowCacheEntry> cache = new CapiRowCacheProvider().create();

        for (int i = 0; i < numOfTests; ++i)
        {
            String key = "key" + i;
            String value = "value" + i;
            RowCacheKey rowCacheKey = new RowCacheKey(cfm.id, "dummy", key.getBytes());
            cache.put(rowCacheKey, createValue(key, value));
        }

        for (int i = 0; i < numOfTests; ++i)
        {
            String key = "key" + i;
            String value = "value" + i;
            RowCacheKey rowCacheKey = new RowCacheKey(cfm.id, "dummy", key.getBytes());
            assertNotNull(cache.get(rowCacheKey));
            assertEquals(createValue(key, value).iterator().next(),
                    ((CachedBTreePartition) cache.get(rowCacheKey)).iterator().next());
        }

        assertEquals(numOfTests, CapiRowCacheProvider.cachePush.get());
        assertEquals(numOfTests * 2, CapiRowCacheProvider.cacheHit.get());
        assertEquals(0, CapiRowCacheProvider.swapin.get());
    }

    public static class MyKey extends Pair<String, String>
    {

        protected MyKey(String left, String right)
        {
            super(left, right);
        }

    }

    @Test
    public void testCapi() throws InterruptedException
    {
        long capacity = 1;
        int numOfTests = 1024 * 100;

        DatabaseDescriptor.setRowCacheSizeInMB(capacity);
        ICache<RowCacheKey, IRowCacheEntry> cache = new CapiRowCacheProvider().create();

        for (int i = 0; i < numOfTests; ++i)
        {
            String key = "key" + i;
            String value = "value" + i;
            RowCacheKey rowCacheKey = new RowCacheKey(cfm.id, "dummy", key.getBytes());
            cache.put(rowCacheKey, createValue(key, value));
        }

        ((CapiRowCacheProvider.CapiRowCache) cache).clearForTest();

        for (int i = 0; i < numOfTests; ++i)
        {
            String key = "key" + i;
            String value = "value" + i;
            RowCacheKey rowCacheKey = new RowCacheKey(cfm.id, "dummy", key.getBytes());
            assertNotNull(cache.get(rowCacheKey));
            assertEquals(createValue(key, value).iterator().next(),
                    ((CachedBTreePartition) cache.get(rowCacheKey)).iterator().next());
        }

        assertEquals(numOfTests, CapiRowCacheProvider.cachePush.get());
        assertEquals(numOfTests * 2, CapiRowCacheProvider.cacheHit.get());
        assertTrue(CapiRowCacheProvider.swapin.get() > 0);
    }
}
