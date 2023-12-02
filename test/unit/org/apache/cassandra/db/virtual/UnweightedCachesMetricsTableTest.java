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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.cache.UnweightedCacheSize;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.CacheMetricsRegister;
import org.apache.cassandra.metrics.UnweightedCacheMetrics;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class UnweightedCachesMetricsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String CACHE_NAME = "mycache";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
    }

    @Test
    public void testCacheTableUpdating()
    {
        CacheMetricsRegister.getInstance().reset();
        CacheMetricsRegister.getInstance().register(metrics);

        UnweightedCacheMetricsTable table = new UnweightedCacheMetricsTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        assertRows(execute(getSelectQuery()), row(CACHE_NAME, 123, 58, 0L, Double.NaN, 0L, 0L, 0L));

        metrics.hits.mark(10);
        metrics.requests.mark(40);

        assertRows(execute(getSelectQuery()), row(CACHE_NAME, 123, 58, 10L, 0.25d, 40L, 0L, 0L));

        // unregistering yields no metrics returned
        CacheMetricsRegister.getInstance().unregister(metrics);
        assertRowCount(execute(getSelectQuery()), 0);

        CacheMetricsRegister.getInstance().register(metrics);
        metrics.reset();

        assertRows(execute(getSelectQuery()), row(CACHE_NAME, 123, 58, 0L, Double.NaN, 0L, 0L, 0L));
    }

    @Test
    public void testDefaultTableQuerying()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(new UnweightedCacheMetricsTable(KS_NAME))));
        assertEquals(5, execute(getSelectQuery()).size());

        // remove it from register hence table
        CacheMetricsRegister.getInstance().unregisterUnweightedCacheMetrics(NetworkPermissionsCacheMBean.CACHE_NAME);
        assertEquals(4, execute(getSelectQuery()).size());

        // remove it from register hence from table
        CacheMetricsRegister.getInstance().unregisterUnweightedCacheMetrics(PermissionsCacheMBean.CACHE_NAME);
        assertEquals(3, execute(getSelectQuery()).size());

        // move it back
        CacheMetricsRegister.getInstance().register(AuthenticatedUser.permissionsCache.getMetrics());
        assertEquals(4, execute(getSelectQuery()).size());
    }

    private final UnweightedCacheMetrics metrics = new UnweightedCacheMetrics(CACHE_NAME, new UnweightedCacheSize()
    {
        @Override
        public int maxEntries()
        {
            return 123;
        }

        @Override
        public void setMaxEntries(int maxEntries)
        {

        }

        @Override
        public int entries()
        {
            return 58;
        }
    });

    private String getSelectQuery()
    {
        return format("SELECT %s, %s, %s, %s, %s, %s, %s, %s FROM %s.%s",
                      UnweightedCacheMetricsTable.NAME_COLUMN,
                      UnweightedCacheMetricsTable.CAPACITY_COLUMN,
                      UnweightedCacheMetricsTable.ENTRY_COUNT_COLUMN,
                      UnweightedCacheMetricsTable.HIT_COUNT_COLUMN,
                      UnweightedCacheMetricsTable.HIT_RATIO_COLUMN,
                      UnweightedCacheMetricsTable.REQUEST_COUNT_COLUMN,
                      UnweightedCacheMetricsTable.RECENT_REQUEST_RATE_PER_SECOND_COLUMN,
                      UnweightedCacheMetricsTable.RECENT_HIT_RATE_PER_SECOND_COLUMN,
                      KS_NAME,
                      UnweightedCacheMetricsTable.TABLE_NAME);
    }
}
