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
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.CacheMetrics;
import org.apache.cassandra.metrics.CacheMetricsRegister;
import org.apache.cassandra.service.CacheService;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class CacheMetricsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String CACHE_NAME = "mycache";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
    }

    @Test
    public void testCachesTable()
    {
        CacheMetricsRegister.getInstance().reset();
        CacheMetricsRegister.getInstance().register(metrics);

        CacheMetricsTable table = new CacheMetricsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        metrics.hits.mark(6);
        metrics.requests.mark(12);

        assertRows(execute(getSelectQuery()), row(CACHE_NAME, 125L, 30, 6L, 0.5d, 12L, 65L, 0L, 0L));

        // unregistering yields no metrics returned
        CacheMetricsRegister.getInstance().unregister(metrics);
        assertRowCount(execute(getSelectQuery()), 0);

        metrics.hits.mark(12);
        metrics.requests.mark(12);

        CacheMetricsRegister.getInstance().register(metrics);

        assertRows(execute(getSelectQuery()), row(CACHE_NAME, 125L, 30, 18L, 0.75d, 24L, 65L, 0L, 0L));
    }

    @Test
    public void testDefaultTableQuerying()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(new CacheMetricsTable(KS_NAME))));
        assertEquals(4, execute(getSelectQuery()).size());

        // remove it from register hence from table
        CacheMetricsRegister.getInstance().unregisterCacheMetrics("KeyCache");
        assertEquals(3, execute(getSelectQuery()).size());

        // move it back
        CacheMetricsRegister.getInstance().register(CacheService.instance.keyCache.getMetrics());
        assertEquals(4, execute(getSelectQuery()).size());
    }


    private final CacheMetrics metrics = new CacheMetrics(CACHE_NAME, new CacheSize()
    {
        @Override
        public long capacity()
        {
            return 125;
        }

        @Override
        public void setCapacity(long capacity)
        {

        }

        @Override
        public int size()
        {
            return 30;
        }

        @Override
        public long weightedSize()
        {
            return 65;
        }
    });

    private String getSelectQuery()
    {
        return format("SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s.%s",
                      CacheMetricsTable.NAME_COLUMN,
                      CacheMetricsTable.CAPACITY_BYTES_COLUMN,
                      CacheMetricsTable.ENTRY_COUNT_COLUMN,
                      CacheMetricsTable.HIT_COUNT_COLUMN,
                      CacheMetricsTable.HIT_RATIO_COLUMN,
                      CacheMetricsTable.REQUEST_COUNT_COLUMN,
                      CacheMetricsTable.SIZE_BYTES_COLUMN,
                      CacheMetricsTable.RECENT_HIT_RATE_PER_SECOND_COLUMN,
                      CacheMetricsTable.RECENT_REQUEST_RATE_PER_SECOND_COLUMN,
                      KS_NAME,
                      CacheMetricsTable.TABLE_NAME);
    }
}
