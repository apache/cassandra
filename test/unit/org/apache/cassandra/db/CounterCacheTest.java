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

import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static org.apache.cassandra.Util.cellname;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class CounterCacheTest extends SchemaLoader
{
    private static final String KS = "CounterCacheSpace";
    private static final String CF = "Counter1";

    @AfterClass
    public static void cleanup()
    {
        cleanupSavedCaches();
    }

    @Test
    public void testReadWrite()
    {
        ColumnFamilyStore cfs = Keyspace.open(KS).getColumnFamilyStore(CF);
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
    public void testSaveLoad() throws ExecutionException, InterruptedException, WriteTimeoutException
    {
        ColumnFamilyStore cfs = Keyspace.open(KS).getColumnFamilyStore(CF);
        CacheService.instance.invalidateCounterCache();

        ColumnFamily cells = ArrayBackedSortedColumns.factory.create(cfs.metadata);
        cells.addColumn(new BufferCounterUpdateCell(cellname(1), 1L, FBUtilities.timestampMicros()));
        cells.addColumn(new BufferCounterUpdateCell(cellname(2), 2L, FBUtilities.timestampMicros()));
        new CounterMutation(new Mutation(KS, bytes(1), cells), ConsistencyLevel.ONE).apply();
        new CounterMutation(new Mutation(KS, bytes(2), cells), ConsistencyLevel.ONE).apply();

        // flush the counter cache and invalidate
        CacheService.instance.counterCache.submitWrite(Integer.MAX_VALUE).get();
        CacheService.instance.invalidateCounterCache();
        assertEquals(0, CacheService.instance.counterCache.size());

        // load from cache and validate
        CacheService.instance.counterCache.loadSaved(cfs);
        assertEquals(4, CacheService.instance.counterCache.size());
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(1), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(1), cellname(2)));
        assertEquals(ClockAndCount.create(1L, 1L), cfs.getCachedCounter(bytes(2), cellname(1)));
        assertEquals(ClockAndCount.create(1L, 2L), cfs.getCachedCounter(bytes(2), cellname(2)));
    }
}
