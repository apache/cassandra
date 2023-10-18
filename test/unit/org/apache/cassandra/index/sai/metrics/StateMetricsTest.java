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

import javax.management.InstanceNotFoundException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.ResultSet;

import static org.apache.cassandra.index.sai.metrics.TableStateMetrics.TABLE_STATE_METRIC_TYPE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class StateMetricsTest extends AbstractMetricsTest
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s.%s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS %s ON %s.%s(%s) USING 'StorageAttachedIndex'";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testMetricRelease() throws Throwable
    {
        String table = "test_metric_release";
        String index = "test_metric_release_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        execute("INSERT INTO " + keyspace + '.' + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + '.' + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());
        assertEquals(1L, getTableStateMetrics(keyspace, table, "TotalIndexCount"));

        // If we drop the last index on the table, we should no longer see the table-level state metrics:
        dropIndex(String.format("DROP INDEX %s." + index, keyspace));
        assertThatThrownBy(() -> getTableStateMetrics(keyspace, table, "TotalIndexCount")).hasCauseInstanceOf(InstanceNotFoundException.class);
    }

    @Test
    public void testMetricCreation() throws Throwable
    {
        String table = "test_table";
        String index = "test_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index + "_v1", keyspace, table, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index + "_v2", keyspace, table, "v2"));

        execute("INSERT INTO " + keyspace + '.' + table + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace + '.' + table + " (id1, v1, v2) VALUES ('1', 1, '1')");
        execute("INSERT INTO " + keyspace + '.' + table + " (id1, v1, v2) VALUES ('2', 2, '2')");
        execute("INSERT INTO " + keyspace + '.' + table + " (id1, v1, v2) VALUES ('3', 3, '3')");

        ResultSet rows = executeNet("SELECT id1, v1, v2 FROM " + keyspace + '.' + table + " WHERE v1 >= 0");

        int actualRows = rows.all().size();
        assertEquals(4, actualRows);

        waitForEquals(objectNameNoIndex("TotalIndexCount", keyspace, table, TABLE_STATE_METRIC_TYPE), 2);
        waitForEquals(objectNameNoIndex("TotalQueryableIndexCount", keyspace, table, TABLE_STATE_METRIC_TYPE), 2);
    }

    private int getTableStateMetrics(String keyspace, String table, String metricsName)
    {
        return (int) getMetricValue(objectNameNoIndex(metricsName, keyspace, table, TABLE_STATE_METRIC_TYPE));
    }
}
