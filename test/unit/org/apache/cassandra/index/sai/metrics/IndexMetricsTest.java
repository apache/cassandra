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
package org.apache.cassandra.index.sai.metrics;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.utils.Throwables;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexMetricsTest extends AbstractMetricsTest
{
    private static final String TABLE = "table_name";
    private static final String INDEX = "table_name_index";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s." + TABLE + " (ID1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS " + INDEX + " ON %s." + TABLE + "(%s) USING 'StorageAttachedIndex'";

    @Test
    public void testSameIndexNameAcrossKeyspaces() throws Throwable
    {
        String keyspace1 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        String keyspace2 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace1));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace1, "v1"));

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace2));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace2, "v1"));

        execute("INSERT INTO " + keyspace1 + "." + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");

        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace1, TABLE, INDEX, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace2, TABLE, INDEX, "IndexMetrics")));

        execute("INSERT INTO " + keyspace2 + "." + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace2 + "." + TABLE + " (id1, v1, v2) VALUES ('1', 1, '1')");

        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace1, TABLE, INDEX, "IndexMetrics")));
        assertEquals(2L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace2, TABLE, INDEX, "IndexMetrics")));
    }

    @Test
    public void testMetricRelease() throws Throwable
    {
        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace, "v1"));

        execute("INSERT INTO " + keyspace + "." + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");
        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace, TABLE, INDEX, "IndexMetrics")));

        dropIndex(String.format("DROP INDEX %s." + INDEX, keyspace));

        // once the index is dropped, make sure MBeans are no longer accessible
        assertThatThrownBy(() -> getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace, TABLE, INDEX, "IndexMetrics")))
                .hasCauseInstanceOf(javax.management.InstanceNotFoundException.class);
    }

    @Test
    public void testMetricsThroughWriteLifecycle() throws Throwable
    {
        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace, "v1"));

        int rowCount = 10;
        for (int i = 0; i < rowCount; i++)
            execute("INSERT INTO " + keyspace + "." + TABLE + "(id1, v1, v2) VALUES (?, ?, '0')", Integer.toString(i), i);

        assertEquals(10L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertTrue((Long)getMetricValue(objectName("MemtableIndexBytes", keyspace, TABLE, INDEX, "IndexMetrics")) > 0);
        assertEquals(0L, getMetricValue(objectName("MemtableIndexFlushCount", keyspace, TABLE, INDEX, "IndexMetrics")));

        waitForAssert(() -> {
            try
            {
                assertEquals(10L, getMBeanAttribute(objectName("MemtableIndexWriteLatency", keyspace, TABLE, INDEX, "IndexMetrics"), "Count"));
            }
            catch (Throwable ex)
            {
                throw Throwables.unchecked(ex);
            }
        }, 60, TimeUnit.SECONDS);

        assertEquals(0L, getMetricValue(objectName("SSTableCellCount", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("DiskUsedBytes", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("CompactionCount", keyspace, TABLE, INDEX, "IndexMetrics")));

        waitForVerifyHistogram(objectName("MemtableIndexFlushCellsPerSecond", keyspace, TABLE, INDEX, "IndexMetrics"), 0);

        flush(keyspace, TABLE);

        assertEquals(0L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("MemtableIndexBytes", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertEquals(1L, getMetricValue(objectName("MemtableIndexFlushCount", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertEquals(10L, getMetricValue(objectName("SSTableCellCount", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertTrue((Long)getMetricValue(objectName("DiskUsedBytes", keyspace, TABLE, INDEX, "IndexMetrics")) > 0);
        assertEquals(0L, getMetricValue(objectName("CompactionCount", keyspace, TABLE, INDEX, "IndexMetrics")));

        waitForVerifyHistogram(objectName("MemtableIndexFlushCellsPerSecond", keyspace, TABLE, INDEX, "IndexMetrics"), 1);

        compact(keyspace, TABLE);

        waitForIndexCompaction(keyspace, TABLE, INDEX);

        waitForIndexQueryable(keyspace, TABLE);
        ResultSet rows = executeNet(String.format("SELECT id1 FROM %s.%s WHERE v1 >= 0", keyspace, TABLE));
        assertEquals(rowCount, rows.all().size());

        assertEquals(0L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertEquals(1L, getMetricValue(objectName("MemtableIndexFlushCount", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertEquals(10L, getMetricValue(objectName("SSTableCellCount", keyspace, TABLE, INDEX, "IndexMetrics")));
        assertTrue((Long)getMetricValue(objectName("DiskUsedBytes", keyspace, TABLE, INDEX, "IndexMetrics")) > 0);
        assertEquals(1L, getMetricValue(objectName("CompactionCount", keyspace, TABLE, INDEX, "IndexMetrics")));

        waitForVerifyHistogram(objectName("CompactionSegmentCellsPerSecond", keyspace, TABLE, INDEX, "IndexMetrics"), 1);
    }
}
