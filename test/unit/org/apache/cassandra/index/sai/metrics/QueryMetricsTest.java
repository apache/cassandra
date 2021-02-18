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

import java.util.concurrent.ThreadLocalRandom;
import javax.management.InstanceNotFoundException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.datastax.driver.core.ResultSet;

import static org.apache.cassandra.index.sai.metrics.TableQueryMetrics.TABLE_QUERY_METRIC_TYPE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryMetricsTest extends AbstractMetricsTest
{
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s.%s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
                                                        "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    private static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS %s ON %s.%s(%s) USING 'StorageAttachedIndex'";

    private static final String PER_QUERY_METRIC_TYPE = "PerQuery";
    private static final String GLOBAL_METRIC_TYPE = "ColumnQueryMetrics";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testSameIndexNameAcrossKeyspaces() throws Throwable
    {
        String table = "test_same_index_name_across_keyspaces";
        String index = "test_same_index_name_across_keyspaces_index";

        String keyspace1 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        String keyspace2 = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace1, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace1, table, "v1"));

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace2, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace2, table, "v1"));

        execute("INSERT INTO " + keyspace1 + "." + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace1 + "." + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        assertEquals(1L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(0L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));

        execute("INSERT INTO " + keyspace2 + "." + table + " (id1, v1, v2) VALUES ('0', 0, '0')");
        execute("INSERT INTO " + keyspace2 + "." + table + " (id1, v1, v2) VALUES ('1', 1, '1')");

        rows = executeNet("SELECT id1 FROM " + keyspace1 + "." + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        rows = executeNet("SELECT id1 FROM " + keyspace2 + "." + table + " WHERE v1 = 1");
        assertEquals(1, rows.all().size());

        assertEquals(2L, getTableQueryMetrics(keyspace1, table, "TotalQueriesCompleted"));
        assertEquals(1L, getTableQueryMetrics(keyspace2, table, "TotalQueriesCompleted"));
    }

    @Test
    public void testMetricRelease() throws Throwable
    {
        String table = "test_metric_release";
        String index = "test_metric_release_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));
        waitForIndexQueryable(keyspace, table);

        execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES ('0', 0, '0')");

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 = 0");
        assertEquals(1, rows.all().size());

        assertEquals(1L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // Even if we drop the last index on the table, table-level metrics should still be visible:
        dropIndex(String.format("DROP INDEX %s." + index, keyspace));
        assertEquals(1L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // When the whole table is dropped, we should finally fail to find table-level metrics:
        dropTable(String.format("DROP TABLE %s." + table, keyspace));
        assertThatThrownBy(() -> getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted")).hasCauseInstanceOf(InstanceNotFoundException.class);
    }

    @Test
    public void testKDTreeQueryMetricsWithSingleIndex() throws Throwable
    {
        String table = "test_metrics_through_write_lifecycle";
        String index = "test_metrics_through_write_lifecycle_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));

        int resultCounter = 0;
        int queryCounter = 0;

        int rowsWritten = 10;

        for (int i = 0; i < rowsWritten; i++)
        {
            execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES (?, ?, '0')", Integer.toString(i), i);
        }

        flush(keyspace, table);
        compact(keyspace, table);
        waitForIndexCompaction(keyspace, table, index);

        waitForIndexQueryable(keyspace, table);

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 0");

        int actualRows = rows.all().size();
        assertEquals(rowsWritten, actualRows);
        resultCounter += actualRows;
        queryCounter++;

        rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 5");

        actualRows = rows.all().size();
        assertEquals(5, actualRows);
        resultCounter += actualRows;
        queryCounter++;

        assertEquals(2L, getPerQueryMetrics(keyspace, table, "SSTableIndexesHit"));
        assertEquals(2L, getPerQueryMetrics(keyspace, table, "IndexSegmentsHit"));
        assertEquals(2L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // run several times to get buffer faults across the metrics
        for (int x = 0; x < 20; x++)
        {
            rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 5");

            actualRows = rows.all().size();
            assertEquals(5, actualRows);
            resultCounter += actualRows;
            queryCounter++;
        }

        // column metrics

        waitForGreaterThanZero(objectNameNoIndex("QueryLatency", keyspace, table, PER_QUERY_METRIC_TYPE));

        waitForEquals(objectNameNoIndex("TotalPartitionReads", keyspace, table, TableQueryMetrics.TABLE_QUERY_METRIC_TYPE), resultCounter);
        waitForEquals(objectName("KDTreeIntersectionLatency", keyspace, table, index, GLOBAL_METRIC_TYPE), queryCounter);
    }

    @Test
    public void testKDTreePostingsQueryMetricsWithSingleIndex() throws Throwable
    {
        String table = "test_kdtree_postings_metrics_through_write_lifecycle";
        String v1Index = "test_kdtree_postings_metrics_through_write_lifecycle_v1_index";
        String v2Index = "test_kdtree_postings_metrics_through_write_lifecycle_v2_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));

        createIndex(String.format(CREATE_INDEX_TEMPLATE + " WITH OPTIONS = {'bkd_postings_min_leaves' : 1}", v1Index, keyspace, table, "v1"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, v2Index, keyspace, table, "v2"));

        int rowsWritten = 50;


        for (int i = 0; i < rowsWritten; i++)
        {
            execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES (?, ?, ?)", Integer.toString(i), i, Integer.toString(i));
        }

        flush(keyspace, table);
        compact(keyspace, table);
        waitForIndexCompaction(keyspace, table, v1Index);

        waitForIndexQueryable(keyspace, table);

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 0");

        int actualRows = rows.all().size();
        assertEquals(rowsWritten, actualRows);

        assertTrue(((Number) getMetricValue(objectName("NumPostings", keyspace, table, v1Index, "KDTreePostings"))).longValue() > 0);

        waitForVerifyHistogram(objectNameNoIndex("KDTreePostingsNumPostings", keyspace, table, PER_QUERY_METRIC_TYPE), 1);

        // V2 index is very selective, so it should lead the union merge process, causing V1 index to skip/advance
        execute("SELECT id1 FROM " + keyspace + "." + table + " WHERE v1 >= 0 AND v1 <= 1000 AND v2 = '5' ALLOW FILTERING");

        waitForVerifyHistogram(objectNameNoIndex("KDTreePostingsSkips", keyspace, table, PER_QUERY_METRIC_TYPE), 2);
    }

    @Test
    public void testInvertedIndexQueryMetricsWithSingleIndex() throws Throwable
    {
        String table = "test_invertedindex_metrics_through_write_lifecycle";
        String index = "test_invertedindex_metrics_through_write_lifecycle_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);
        createTable(String.format(CREATE_TABLE_TEMPLATE, keyspace, table));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v2"));

        int resultCounter = 0;
        int queryCounter = 0;

        int rowsWritten = 10;

        for (int i = 0; i < rowsWritten; i++)
        {
            execute("INSERT INTO " + keyspace + "." + table + " (id1, v1, v2) VALUES (?, ?, ?)", Integer.toString(i), i, Integer.toString(i));
        }

        flush(keyspace, table);
        compact(keyspace, table);
        waitForIndexCompaction(keyspace, table, index);

        waitForIndexQueryable(keyspace, table);

        ResultSet rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v2 = '0'");


        int actualRows = rows.all().size();
        assertEquals(1, actualRows);
        resultCounter += actualRows;
        queryCounter++;

        rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v2 = '5'");

        actualRows = rows.all().size();
        assertEquals(1, actualRows);
        resultCounter += actualRows;
        queryCounter++;

        assertEquals(2L, getPerQueryMetrics(keyspace, table, "SSTableIndexesHit"));
        assertEquals(2L, getPerQueryMetrics(keyspace, table, "IndexSegmentsHit"));
        assertEquals(2L, getTableQueryMetrics(keyspace, table, "TotalQueriesCompleted"));

        // run several times to get buffer faults across the metrics
        for (int x = 0; x < 20; x++)
        {
            rows = executeNet("SELECT id1 FROM " + keyspace + "." + table + " WHERE v2 = '" + ThreadLocalRandom.current().nextInt(0, 9) + "'");

            actualRows = rows.all().size();
            assertEquals(1, actualRows);
            resultCounter += actualRows;
            queryCounter++;
        }

        waitForGreaterThanZero(objectName("TermsLookupLatency", keyspace, table, index, GLOBAL_METRIC_TYPE));

        waitForGreaterThanZero(objectNameNoIndex("QueryLatency", keyspace, table, PER_QUERY_METRIC_TYPE));

        waitForEquals(objectNameNoIndex("TotalPartitionReads", keyspace, table, TableQueryMetrics.TABLE_QUERY_METRIC_TYPE), resultCounter);
    }

    @Test
    public void testKDTreePartitionsReadAndRowsFiltered() throws Throwable
    {
        String table = "test_rows_filtered_large_partition";
        String index = "test_rows_filtered_large_partition_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format("CREATE TABLE %s.%s (pk int, ck int, v1 int, PRIMARY KEY (pk, ck)) " +
                                  "WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }", keyspace,  table));

        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));
        waitForIndexQueryable(keyspace, table);

        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (0, 0, 0)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (1, 1, 1)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (1, 2, 2)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (2, 1, 3)");

        flush(keyspace, table);

        ResultSet rows = executeNet("SELECT pk, ck FROM " + keyspace + "." + table + " WHERE v1 > 0");

        int actualRows = rows.all().size();
        assertEquals(3, actualRows);

        waitForEquals(objectNameNoIndex("TotalPartitionReads", keyspace, table, TABLE_QUERY_METRIC_TYPE), 2);
        waitForVerifyHistogram(objectNameNoIndex("RowsFiltered", keyspace, table, PER_QUERY_METRIC_TYPE), 1);
        waitForEquals(objectNameNoIndex("TotalRowsFiltered", keyspace, table, TABLE_QUERY_METRIC_TYPE), 3);
    }

    @Test
    public void testKDTreeQueryEarlyExit() throws Throwable
    {
        String table = "test_queries_exited_early";
        String index = "test_queries_exited_early_index";

        String keyspace = createKeyspace(CREATE_KEYSPACE_TEMPLATE);

        createTable(String.format("CREATE TABLE %s.%s (pk int, ck int, v1 int, PRIMARY KEY (pk, ck)) " +
                                  "WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }", keyspace, table));

        createIndex(String.format(CREATE_INDEX_TEMPLATE, index, keyspace, table, "v1"));
        waitForIndexQueryable(keyspace, table);

        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (0, 0, 0)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (1, 1, 1)");
        execute("INSERT INTO " + keyspace + "." + table + "(pk, ck, v1) VALUES (1, 2, 2)");

        flush(keyspace, table);

        ResultSet rows = executeNet("SELECT pk, ck FROM " + keyspace + "." + table + " WHERE v1 > 2");

        assertEquals(0, rows.all().size());

        rows = executeNet("SELECT pk, ck FROM " + keyspace + "." + table + " WHERE v1 < 0");
        assertEquals(0, rows.all().size());

        waitForEquals(objectName("KDTreeIntersectionLatency", keyspace, table, index, GLOBAL_METRIC_TYPE), 0L);
        waitForEquals(objectName("KDTreeIntersectionEarlyExits", keyspace, table, index, GLOBAL_METRIC_TYPE), 2L);

        rows = executeNet("SELECT pk, ck FROM " + keyspace + "." + table + " WHERE v1 > 0");
        assertEquals(2, rows.all().size());

        waitForEquals(objectName("KDTreeIntersectionLatency", keyspace, table, index, GLOBAL_METRIC_TYPE), 1L);
        waitForEquals(objectName("KDTreeIntersectionEarlyExits", keyspace, table, index, GLOBAL_METRIC_TYPE), 2L);
    }

    private long getPerQueryMetrics(String keyspace, String table, String metricsName) throws Exception
    {
        return (long) getMetricValue(objectNameNoIndex(metricsName, keyspace, table, PER_QUERY_METRIC_TYPE));
    }

    private long getTableQueryMetrics(String keyspace, String table, String metricsName) throws Exception
    {
        return (long) getMetricValue(objectNameNoIndex(metricsName, keyspace, table, TableQueryMetrics.TABLE_QUERY_METRIC_TYPE));
    }
}
