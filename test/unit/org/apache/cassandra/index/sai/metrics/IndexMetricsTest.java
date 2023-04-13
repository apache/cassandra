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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

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

        execute("INSERT INTO " + keyspace1 + '.' + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");

        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace1, TABLE, INDEX, "IndexMetrics")));
        assertEquals(0L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace2, TABLE, INDEX, "IndexMetrics")));

        execute("INSERT INTO " + keyspace2 + '.' + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace2 + '.' + TABLE + " (id1, v1, v2) VALUES ('1', 1, '1')");

        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace1, TABLE, INDEX, "IndexMetrics")));
        assertEquals(2L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace2, TABLE, INDEX, "IndexMetrics")));
    }

    @Test
    public void testMetricRelease() throws Throwable
    {
        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, keyspace, "v1"));

        execute("INSERT INTO " + keyspace + '.' + TABLE + " (id1, v1, v2) VALUES ('0', 0, '0')");
        assertEquals(1L, getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace, TABLE, INDEX, "IndexMetrics")));

        dropIndex(String.format("DROP INDEX %s." + INDEX, keyspace));

        // once the index is dropped, make sure MBeans are no longer accessible
        assertThatThrownBy(() -> getMetricValue(objectName("LiveMemtableIndexWriteCount", keyspace, TABLE, INDEX, "IndexMetrics")))
                .hasCauseInstanceOf(javax.management.InstanceNotFoundException.class);
    }
}
