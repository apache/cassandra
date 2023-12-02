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

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cache.UnweightedCacheSize;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.UnweightedCacheMetrics;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;

public class UnweightedCachesTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
    private static final String CACHE_NAME = "mycache";

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
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

    @Test
    public void testCacheTableUpdating()
    {
        UnweightedCachesTable table = new UnweightedCachesTable(KS_NAME, Set.of(() -> Optional.of(Pair.create(CACHE_NAME, metrics))));
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

        assertRows(execute(getSelectQuery()), row(CACHE_NAME, 123, 58, 0L, Double.NaN, 0L, 0L, 0L));

        metrics.hits.mark(10);
        metrics.requests.mark(40);

        assertRows(execute(getSelectQuery()), row(CACHE_NAME, 123, 58, 10L, 0.25d, 40L, 0L, 0L));

        metrics.reset();

        assertRows(execute(getSelectQuery()), row(CACHE_NAME, 123, 58, 0L, Double.NaN, 0L, 0L, 0L));
    }

    @Test
    public void testDefaultTableQuerying()
    {
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(new UnweightedCachesTable(KS_NAME))));
        Assert.assertEquals(4, execute(getSelectQuery()).size());
    }

    private String getSelectQuery()
    {
        return format("SELECT %s, %s, %s, %s, %s, %s, %s, %s FROM %s.%s",
                      UnweightedCachesTable.NAME_COLUMN,
                      UnweightedCachesTable.CAPACITY_COLUMN,
                      UnweightedCachesTable.ENTRY_COUNT_COLUMN,
                      UnweightedCachesTable.HIT_COUNT_COLUMN,
                      UnweightedCachesTable.HIT_RATIO_COLUMN,
                      UnweightedCachesTable.REQUEST_COUNT_COLUMN,
                      UnweightedCachesTable.RECENT_REQUEST_RATE_PER_SECOND_COLUMN,
                      UnweightedCachesTable.RECENT_HIT_RATE_PER_SECOND_COLUMN,
                      KS_NAME,
                      UnweightedCachesTable.TABLE_NAME);
    }
}
