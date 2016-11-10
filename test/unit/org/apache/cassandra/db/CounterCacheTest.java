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
package org.apache.cassandra.db;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CounterCacheTest
{
    private static final String KEYSPACE1 = "CounterCacheTest";
    private static final String CF = "Counter1";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF).defaultValidator(CounterColumnType.instance));
    }

    @AfterClass
    public static void cleanup()
    {
        SchemaLoader.cleanupSavedCaches();
    }

    @Test
    public void testReadWrite()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        CacheService.instance.invalidateCounterCache();

        assertEquals(0, CacheService.instance.counterCache.size());
        assertNull(cfs.getCachedCounter(bytes(1), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(1), cellname(2)));
        assertNull(cfs.getCachedCounter(bytes(2), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(2), cellname(2)));

        cfs.putCachedCounter(bytes(1), cellname(1), ClockAndCount.create(1L, 1L));
        cfs.putCachedCounter(bytes(1), cellname(2), ClockAndCount.create(1L, 2L));
        cfs.putCachedCounter(bytes(2), cellname(1), ClockAndCount.create(2L, 1L));
        cfs.putCachedCounter(bytes(2), cellname(2), ClockAndCount.create(2L, 2L));

        assertEquals(4, CacheService.instance.counterCache.size());
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(1), cellname(2)));
        assertEquals(ClockAndCount.create(2L, 1L), cfs.getCachedCounter(bytes(2), cellname(1)));
        assertEquals(ClockAndCount.create(2L, 2L), cfs.getCachedCounter(bytes(2), cellname(2)));
    }

    @Test
    public void testCounterCacheInvalidate()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        CacheService.instance.invalidateCounterCache();

        assertEquals(0, CacheService.instance.counterCache.size());
        assertNull(cfs.getCachedCounter(bytes(1), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(1), cellname(2)));
        assertNull(cfs.getCachedCounter(bytes(2), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(2), cellname(2)));
        assertNull(cfs.getCachedCounter(bytes(3), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(3), cellname(2)));

        cfs.putCachedCounter(bytes(1), cellname(1), ClockAndCount.create(1L, 1L));
        cfs.putCachedCounter(bytes(1), cellname(2), ClockAndCount.create(1L, 2L));
        cfs.putCachedCounter(bytes(2), cellname(1), ClockAndCount.create(2L, 1L));
        cfs.putCachedCounter(bytes(2), cellname(2), ClockAndCount.create(2L, 2L));
        cfs.putCachedCounter(bytes(3), cellname(1), ClockAndCount.create(3L, 1L));
        cfs.putCachedCounter(bytes(3), cellname(2), ClockAndCount.create(3L, 2L));

        assertEquals(6, CacheService.instance.counterCache.size());
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(1), cellname(2)));
        assertEquals(ClockAndCount.create(2L, 1L), cfs.getCachedCounter(bytes(2), cellname(1)));
        assertEquals(ClockAndCount.create(2L, 2L), cfs.getCachedCounter(bytes(2), cellname(2)));
        assertEquals(ClockAndCount.create(3L, 1L), cfs.getCachedCounter(bytes(3), cellname(1)));
        assertEquals(ClockAndCount.create(3L, 2L), cfs.getCachedCounter(bytes(3), cellname(2)));

        cfs.invalidateCounterCache(Collections.singleton(new Bounds<Token>(cfs.partitioner.decorateKey(bytes(1)).getToken(),
                                                                           cfs.partitioner.decorateKey(bytes(2)).getToken())));

        assertEquals(2, CacheService.instance.counterCache.size());
        assertNull(cfs.getCachedCounter(bytes(1), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(1), cellname(2)));
        assertNull(cfs.getCachedCounter(bytes(2), cellname(1)));
        assertNull(cfs.getCachedCounter(bytes(2), cellname(2)));
        assertEquals(ClockAndCount.create(3L, 1L), cfs.getCachedCounter(bytes(3), cellname(1)));
        assertEquals(ClockAndCount.create(3L, 2L), cfs.getCachedCounter(bytes(3), cellname(2)));
    }

    @Test
    public void testSaveLoad() throws ExecutionException, InterruptedException, WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        CacheService.instance.invalidateCounterCache();

        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, FBUtilities.timestampMicros()));
        cells.addColumn(new BufferCounterUpdateCell(cellname(2), 2L, FBUtilities.timestampMicros()));
        new CounterMutation(new Mutation(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        new CounterMutation(new Mutation(KEYSPACE1, bytes(2), cells), ConsistencyLevel.ONE).apply();

        // flush the counter cache and invalidate
        CacheService.instance.counterCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.invalidateCounterCache();
        assertEquals(0, CacheService.instance.counterCache.size());

        // load from cache and validate
        CacheService.instance.counterCache.loadSaved();
        assertEquals(4, CacheService.instance.counterCache.size());
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(1), cellname(2)));
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(2), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(2), cellname(2)));
    }

    @Test
    public void testDroppedSaveLoad() throws ExecutionException, InterruptedException, WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        CacheService.instance.invalidateCounterCache();

        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, FBUtilities.timestampMicros()));
        cells.addColumn(new BufferCounterUpdateCell(cellname(2), 2L, FBUtilities.timestampMicros()));
        new CounterMutation(new Mutation(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        new CounterMutation(new Mutation(KEYSPACE1, bytes(2), cells), ConsistencyLevel.ONE).apply();

        // flush the counter cache and invalidate
        CacheService.instance.counterCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.invalidateCounterCache();
        assertEquals(0, CacheService.instance.counterCache.size());

        Keyspace ks = Schema.instance.removeKeyspaceInstance(KEYSPACE1);

        try
        {
            // load from cache and validate
            CacheService.instance.counterCache.loadSaved();
            assertEquals(0, CacheService.instance.counterCache.size());
        }
        finally
        {
            Schema.instance.storeKeyspaceInstance(ks);
        }
    }

    @Test
    public void testDisabledSaveLoad() throws ExecutionException, InterruptedException, WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(CF);
        cfs.truncateBlocking();
        CacheService.instance.invalidateCounterCache();

        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, FBUtilities.timestampMicros()));
        cells.addColumn(new BufferCounterUpdateCell(cellname(2), 2L, FBUtilities.timestampMicros()));
        new CounterMutation(new Mutation(KEYSPACE1, bytes(1), cells), ConsistencyLevel.ONE).apply();
        new CounterMutation(new Mutation(KEYSPACE1, bytes(2), cells), ConsistencyLevel.ONE).apply();

        // flush the counter cache and invalidate
        CacheService.instance.counterCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.invalidateCounterCache();
        assertEquals(0, CacheService.instance.counterCache.size());


        CacheService.instance.setCounterCacheCapacityInMB(0);
        try
        {
            // load from cache and validate
            CacheService.instance.counterCache.loadSaved();
            assertEquals(0, CacheService.instance.counterCache.size());
        }
        finally
        {
            CacheService.instance.setCounterCacheCapacityInMB(1);
        }
    }

}
