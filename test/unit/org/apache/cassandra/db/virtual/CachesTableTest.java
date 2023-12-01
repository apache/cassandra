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

package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.service.CacheService;

public class CachesTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private CachesTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
    }

    @Before
    public void config()
    {
        table = new TestingCachesTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    public void testCachesTable()
    {
        resetAllCaches();

        table.getChunkCacheMetrics().hits.mark(6);
        table.getChunkCacheMetrics().requests.mark(12);

        assertRows(execute(getSelectQuery()),
                   row("chunks", ChunkCache.instance.capacity(), 123, 6L, 0.5d, 12L, 0L, 0L, 0L),
                   row("counters", CacheService.instance.counterCache.getCapacity(), 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("keys", CacheService.instance.keyCache.getCapacity(), 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("rows", CacheService.instance.rowCache.getCapacity(), 0, 0L, Double.NaN, 0L, 0L, 0L, 0L));

        table.getChunkCacheMetrics().hits.mark(12);
        table.getChunkCacheMetrics().requests.mark(12);

        assertRows(execute(getSelectQuery()),
                   row("chunks", ChunkCache.instance.capacity(), 123, 18L, 0.75d, 24L, 0L, 0L, 0L),
                   row("counters", CacheService.instance.counterCache.getCapacity(), 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("keys", CacheService.instance.keyCache.getCapacity(), 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("rows", CacheService.instance.rowCache.getCapacity(), 0, 0L, Double.NaN, 0L, 0L, 0L, 0L));
    }

    private void resetAllCaches()
    {
        CacheService.instance.counterCache.clear();
        CacheService.instance.counterCache.getMetrics().reset();
        CacheService.instance.keyCache.clear();
        CacheService.instance.keyCache.getMetrics().reset();
        CacheService.instance.rowCache.clear();
        CacheService.instance.rowCache.getMetrics().reset();
    }

    private String getSelectQuery()
    {
        return String.format("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s.%s",
                             CachesTable.NAME_COLUMN,
                             CachesTable.CAPACITY_BYTES_COLUMN,
                             CachesTable.ENTRY_COUNT_COLUMN,
                             CachesTable.HIT_COUNT_COLUMN,
                             CachesTable.HIT_RATIO_COLUMN,
                             CachesTable.REQUEST_COUNT_COLUMN,
                             CachesTable.SIZE_BYTES_COLUMN,
                             CachesTable.RECENT_HIT_RATE_PER_SECOND_COLUMN,
                             CachesTable.RECENT_REQUEST_RATE_PER_SECOND_COLUMN,
                             KS_NAME,
                             table.name());
    }

    private static class TestingCachesTable extends CachesTable
    {
        private final CacheMetrics cacheMetrics = new CacheMetrics("chunks", new CacheSize()
        {
            @Override
            public long capacity()
            {
                return ChunkCache.cacheSize;
            }

            @Override
            public void setCapacity(long capacity)
            {

            }

            @Override
            public int size()
            {
                return 123;
            }

            @Override
            public long weightedSize()
            {
                return 0;
            }
        })
        {
        };

        TestingCachesTable(String keyspace)
        {
            super(keyspace);
        }

        @Override
        protected CacheMetrics getChunkCacheMetrics()
        {
            return cacheMetrics;
        }
    }
}
