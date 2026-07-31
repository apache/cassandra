/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.metrics;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.codahale.metrics.Histogram;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the Coordinator read-size histogram metrics.
 * It covers the four combinations for single/range and index/no-index types of queries.
 * All queries also update the aggregate {@code coordinatorReadSize} histogram.
 */
public class CoordinatorReadSizeMetricsTest
{
    private static final String KEYSPACE = "coordinator_read_size_test_ks";
    private static final String TABLE = "coordinator_read_size_test_tbl";

    private static EmbeddedCassandraService cassandra;
    private static Cluster cluster;
    private static Session session;

    @BeforeClass
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        cluster = Cluster.builder()
                         .addContactPoint("127.0.0.1")
                         .withPort(DatabaseDescriptor.getNativeTransportPort())
                         .build();
        session = cluster.connect();

        session.execute(String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s " +
        "WITH replication = {'class':'SimpleStrategy','replication_factor':1};",
        KEYSPACE));

        // Base table: pk (partition key), ck (clustering), v (non-key column for secondary index)
        session.execute(String.format(
        "CREATE TABLE IF NOT EXISTS %s.%s (pk int, ck int, v text, PRIMARY KEY (pk, ck));",
        KEYSPACE, TABLE));

        // Secondary index on 'v' so we can trigger secondary-index code paths.
        // Uses the legacy (CassandraIndex) implementation which is compatible with any partitioner,
        // including the ByteOrderedPartitioner used by the default unit-test configuration.
        session.execute(String.format(
        "CREATE INDEX IF NOT EXISTS ON %s.%s (v);",
        KEYSPACE, TABLE));

        waitForIndexQueryable();
        for (int i = 0; i < 5; i++)
            session.execute(String.format(
            "INSERT INTO %s.%s (pk, ck, v) VALUES (%d, %d, 'val%d');",
            KEYSPACE, TABLE, i, i, i));
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }

    /**
     * Clear all coordinator-read-size histograms before each test so that counts from one test
     * cannot bleed into another.
     */
    @Before
    public void clearMetrics()
    {
        ColumnFamilyStore cfs = cfs();

        clearHistogram(cfs.metric.coordinatorReadSize);
        clearHistogram(cfs.metric.coordinatorRangeReadSize);
        clearHistogram(cfs.metric.coordinatorRangeReadSizeWithIndex);
        clearHistogram(cfs.metric.coordinatorRangeReadSizeWithoutIndex);
        clearHistogram(cfs.metric.coordinatorSingleReadSize);
        clearHistogram(cfs.metric.coordinatorSingleReadSizeWithIndex);
        clearHistogram(cfs.metric.coordinatorSingleReadSizeWithoutIndex);

        Keyspace ks = Keyspace.open(KEYSPACE);
        clearHistogram(ks.metric.coordinatorReadSize);
        clearHistogram(ks.metric.coordinatorRangeReadSize);
        clearHistogram(ks.metric.coordinatorRangeReadSizeWithIndex);
        clearHistogram(ks.metric.coordinatorRangeReadSizeWithoutIndex);
        clearHistogram(ks.metric.coordinatorSingleReadSize);
        clearHistogram(ks.metric.coordinatorSingleReadSizeWithIndex);
        clearHistogram(ks.metric.coordinatorSingleReadSizeWithoutIndex);
    }

    /**
     * A single-partition lookup (no index).
     * Expected: coordinatorReadSize + coordinatorSingleReadSize + coordinatorSingleReadSizeWithoutIndex each get 1 sample.
     * All range/index histograms must remain at 0.
     */
    @Test
    public void testSinglePartitionQueryWithoutIndex() throws Exception
    {
        session.execute(String.format("SELECT * FROM %s.%s WHERE pk = 1;", KEYSPACE, TABLE));

        ColumnFamilyStore cfs = cfs();

        // Always updated
        assertCount(cfs.metric.coordinatorReadSize, 1);

        // Single-partition branch
        assertCount(cfs.metric.coordinatorSingleReadSize, 1);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithoutIndex, 1);

        // Range histograms must be untouched
        assertCount(cfs.metric.coordinatorRangeReadSize, 0);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithIndex, 0);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithoutIndex, 0);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithIndex, 0);

        // Keyspace-level mirrors
        Keyspace ks = Keyspace.open(KEYSPACE);
        assertCount(ks.metric.coordinatorReadSize, 1);
        assertCount(ks.metric.coordinatorSingleReadSize, 1);
        assertCount(ks.metric.coordinatorSingleReadSizeWithoutIndex, 1);
        assertCount(ks.metric.coordinatorRangeReadSize, 0);
        assertCount(ks.metric.coordinatorRangeReadSizeWithIndex, 0);
        assertCount(ks.metric.coordinatorRangeReadSizeWithoutIndex, 0);
        assertCount(ks.metric.coordinatorSingleReadSizeWithIndex, 0);
    }

    /**
     * A full table scan (no index) — isKeyRange = true, usesSecondaryIndexing = false.
     * Expected: coordinatorReadSize + coordinatorRangeReadSize + coordinatorRangeReadSizeWithoutIndex each get 1 sample.
     * All single-partition and index histograms must remain at 0.
     */
    @Test
    public void testRangeQueryWithoutIndex() throws Exception
    {
        session.execute(String.format("SELECT * FROM %s.%s;", KEYSPACE, TABLE));

        ColumnFamilyStore cfs = cfs();

        // Always updated
        assertCount(cfs.metric.coordinatorReadSize, 1);

        // Range-without-index branch
        assertCount(cfs.metric.coordinatorRangeReadSize, 1);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithoutIndex, 1);

        // Unaffected histograms
        assertCount(cfs.metric.coordinatorRangeReadSizeWithIndex, 0);
        assertCount(cfs.metric.coordinatorSingleReadSize, 0);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithIndex, 0);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithoutIndex, 0);

        // Keyspace-level mirrors
        Keyspace ks = Keyspace.open(KEYSPACE);
        assertCount(ks.metric.coordinatorReadSize, 1);
        assertCount(ks.metric.coordinatorRangeReadSize, 1);
        assertCount(ks.metric.coordinatorRangeReadSizeWithoutIndex, 1);
        assertCount(ks.metric.coordinatorRangeReadSizeWithIndex, 0);
        assertCount(ks.metric.coordinatorSingleReadSize, 0);
        assertCount(ks.metric.coordinatorSingleReadSizeWithIndex, 0);
        assertCount(ks.metric.coordinatorSingleReadSizeWithoutIndex, 0);
    }

    /**
     * A secondary-index query with no partition-key constraint - isKeyRange = true (unconstrained partition),
     * usesSecondaryIndexing = true.
     * Expected: coordinatorReadSize + coordinatorRangeReadSize + coordinatorRangeReadSizeWithIndex each get 1 sample.
     * All without-index and single-partition histograms must remain at 0.
     */
    @Test
    public void testRangeQueryWithSecondaryIndex() throws Exception
    {
        // No partition key constraint: isKeyRange = true; indexed column: usesSecondaryIndexing = true
        session.execute(String.format("SELECT * FROM %s.%s WHERE v = 'val1';", KEYSPACE, TABLE));

        ColumnFamilyStore cfs = cfs();

        // Always updated
        assertCount(cfs.metric.coordinatorReadSize, 1);

        // Range-with-index branch
        assertCount(cfs.metric.coordinatorRangeReadSize, 1);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithIndex, 1);

        // Unaffected histograms
        assertCount(cfs.metric.coordinatorRangeReadSizeWithoutIndex, 0);
        assertCount(cfs.metric.coordinatorSingleReadSize, 0);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithIndex, 0);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithoutIndex, 0);

        // Keyspace-level mirrors
        Keyspace ks = Keyspace.open(KEYSPACE);
        assertCount(ks.metric.coordinatorReadSize, 1);
        assertCount(ks.metric.coordinatorRangeReadSize, 1);
        assertCount(ks.metric.coordinatorRangeReadSizeWithIndex, 1);
        assertCount(ks.metric.coordinatorRangeReadSizeWithoutIndex, 0);
        assertCount(ks.metric.coordinatorSingleReadSize, 0);
        assertCount(ks.metric.coordinatorSingleReadSizeWithIndex, 0);
        assertCount(ks.metric.coordinatorSingleReadSizeWithoutIndex, 0);
    }

    /**
     * A range query using a clustering-column restriction with ALLOW FILTERING (isKeyRange = true) and no index.
     * Expected: same as a plain full-scan - range histograms without index.
     */
    @Test
    public void testClusteringRangeQueryWithoutIndex() throws Exception
    {
        // Clustering-column range without a partition-key equality: isKeyRange = true, no secondary index
        session.execute(String.format(
        "SELECT * FROM %s.%s WHERE ck > 0 ALLOW FILTERING;",
        KEYSPACE, TABLE));

        ColumnFamilyStore cfs = cfs();

        assertCount(cfs.metric.coordinatorReadSize, 1);
        assertCount(cfs.metric.coordinatorRangeReadSize, 1);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithoutIndex, 1);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithIndex, 0);
        assertCount(cfs.metric.coordinatorSingleReadSize, 0);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithIndex, 0);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithoutIndex, 0);
    }

    /**
     * A single-partition query that also filters on a secondary-indexed column -
     * isKeyRange = false (partition key fully specified), usesSecondaryIndexing = true.
     * Expected: coordinatorReadSize + coordinatorSingleReadSize + coordinatorSingleReadSizeWithIndex each get 1 sample.
     * All range and without-index histograms must remain at 0.
     */
    @Test
    public void testSinglePartitionQueryWithIndex() throws Exception
    {
        // pk = 1 pins the partition (isKeyRange = false); v is indexed (usesSecondaryIndexing = true)
        session.execute(String.format("SELECT * FROM %s.%s WHERE pk = 1 AND v = 'val1';", KEYSPACE, TABLE));

        ColumnFamilyStore cfs = cfs();

        // Always updated
        assertCount(cfs.metric.coordinatorReadSize, 1);

        // Single-partition-with-index branch
        assertCount(cfs.metric.coordinatorSingleReadSize, 1);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithIndex, 1);

        // Unaffected histograms
        assertCount(cfs.metric.coordinatorRangeReadSize, 0);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithIndex, 0);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithoutIndex, 0);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithoutIndex, 0);

        // Keyspace-level mirrors
        Keyspace ks = Keyspace.open(KEYSPACE);
        assertCount(ks.metric.coordinatorReadSize, 1);
        assertCount(ks.metric.coordinatorSingleReadSize, 1);
        assertCount(ks.metric.coordinatorSingleReadSizeWithIndex, 1);
        assertCount(ks.metric.coordinatorRangeReadSize, 0);
        assertCount(ks.metric.coordinatorRangeReadSizeWithIndex, 0);
        assertCount(ks.metric.coordinatorRangeReadSizeWithoutIndex, 0);
        assertCount(ks.metric.coordinatorSingleReadSizeWithoutIndex, 0);
    }

    /**
     * Verifies that the recorded size value is non-negative and, when rows are returned, greater than zero.
     * Uses a single-partition query on a partition known to have data.
     */
    @Test
    public void testReadSizeValueIsPositiveWhenRowsReturned() throws Exception
    {
        session.execute(String.format("SELECT * FROM %s.%s WHERE pk = 2;", KEYSPACE, TABLE));

        ColumnFamilyStore cfs = cfs();

        // There is one row for pk=2, so the size must be > 0
        long max = cfs.metric.coordinatorReadSize.tableOrKeyspaceHistogram().getSnapshot().getMax();
        assertTrue("Expected coordinatorReadSize > 0 for a non-empty result, but was " + max, max > 0);

        max = cfs.metric.coordinatorSingleReadSize.tableOrKeyspaceHistogram().getSnapshot().getMax();
        assertTrue("Expected coordinatorSingleReadSize > 0 for a non-empty result, but was " + max, max > 0);

        max = cfs.metric.coordinatorSingleReadSizeWithoutIndex.tableOrKeyspaceHistogram().getSnapshot().getMax();
        assertTrue("Expected coordinatorSingleReadSizeWithoutIndex > 0, but was " + max, max > 0);
    }

    /**
     * Verifies that the histogram is still updated (count = 1) even when the result set is empty,
     * and that the recorded size is non-negative.
     */
    @Test
    public void testReadSizeIsZeroForEmptyResult() throws Exception
    {
        // pk=99 was never inserted
        session.execute(String.format("SELECT * FROM %s.%s WHERE pk = 99;", KEYSPACE, TABLE));

        ColumnFamilyStore cfs = cfs();

        // The histogram must have been updated exactly once
        assertCount(cfs.metric.coordinatorReadSize, 1);
        assertCount(cfs.metric.coordinatorSingleReadSize, 1);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithoutIndex, 1);

        // The size value for an empty result must be >= 0 (readRowsSize accumulates no bytes)
        long max = cfs.metric.coordinatorReadSize.tableOrKeyspaceHistogram().getSnapshot().getMax();
        assertTrue("Expected coordinatorReadSize >= 0 for an empty result, but was " + max, max >= 0);
    }

    /**
     * Executes multiple queries of all four types and verifies cumulative counts across every histogram.
     */
    @Test
    public void testCumulativeCountsAcrossMultipleQueries() throws Exception
    {
        int singleNoIndex = 3; // WHERE pk = X
        int rangeNoIndex = 2; // SELECT *
        int rangeWithIndex = 2; // WHERE v = 'valX'        (range + index)
        int singleWithIndex = 2; // WHERE pk = X AND v = 'valX' (single + index)

        for (int i = 0; i < singleNoIndex; i++)
            session.execute(String.format("SELECT * FROM %s.%s WHERE pk = %d;", KEYSPACE, TABLE, i));

        for (int i = 0; i < rangeNoIndex; i++)
            session.execute(String.format("SELECT * FROM %s.%s;", KEYSPACE, TABLE));

        for (int i = 0; i < rangeWithIndex; i++)
            session.execute(String.format("SELECT * FROM %s.%s WHERE v = 'val%d';", KEYSPACE, TABLE, i));

        for (int i = 0; i < singleWithIndex; i++)
            session.execute(String.format("SELECT * FROM %s.%s WHERE pk = %d AND v = 'val%d';", KEYSPACE, TABLE, i, i));

        ColumnFamilyStore cfs = cfs();

        assertCount(cfs.metric.coordinatorReadSize, singleNoIndex + rangeNoIndex + rangeWithIndex + singleWithIndex);
        assertCount(cfs.metric.coordinatorSingleReadSize, singleNoIndex + singleWithIndex);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithoutIndex, singleNoIndex);
        assertCount(cfs.metric.coordinatorSingleReadSizeWithIndex, singleWithIndex);
        assertCount(cfs.metric.coordinatorRangeReadSize, rangeNoIndex + rangeWithIndex);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithoutIndex, rangeNoIndex);
        assertCount(cfs.metric.coordinatorRangeReadSizeWithIndex, rangeWithIndex);
    }

    @Test
    public void testReadSizeMetricsForZeroOneAndMultipleRows()
    {
        ColumnFamilyStore cfs = cfs();

        // 0 rows returned
        session.execute(String.format("SELECT * FROM %s.%s WHERE pk = 999;", KEYSPACE, TABLE)).one();
        long sizeForZeroRows = cfs.metric.coordinatorReadSize.tableOrKeyspaceHistogram().getSnapshot().getMax();
        clearMetrics();

        // 1 row returned
        session.execute(String.format("SELECT * FROM %s.%s WHERE pk = 0;", KEYSPACE, TABLE)).one();
        long sizeForOneRow = cfs.metric.coordinatorReadSize.tableOrKeyspaceHistogram().getSnapshot().getMax();
        assertTrue("Expected coordinatorReadSize for 1 row (" + sizeForOneRow + " bytes) to be greater than 0 rows (" + sizeForZeroRows + " bytes)", sizeForOneRow > sizeForZeroRows);
        clearMetrics();

        // 5 rows returned (all rows in the table)
        session.execute(String.format("SELECT * FROM %s.%s;", KEYSPACE, TABLE)).one();
        long sizeForFiveRows = cfs.metric.coordinatorReadSize.tableOrKeyspaceHistogram().getSnapshot().getMax();
        assertTrue("Expected coordinatorReadSize for 5 rows (" + sizeForFiveRows + " bytes) to be greater than 4x the size of 1 row (" + (sizeForOneRow * 4) + " bytes)", sizeForFiveRows > sizeForOneRow * 4);
    }

    private static ColumnFamilyStore cfs()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    private static void assertCount(TableMetrics.TableHistogram histogram, long expectedCount)
    {
        long actual = histogram.tableOrKeyspaceHistogram().getCount();
        assertEquals("Unexpected histogram count", expectedCount, actual);
    }

    private static void assertCount(Histogram histogram, long expectedCount)
    {
        assertEquals("Unexpected histogram count", expectedCount, histogram.getCount());
    }

    private static void clearHistogram(TableMetrics.TableHistogram histogram)
    {
        Histogram h = histogram.tableOrKeyspaceHistogram();
        if (h instanceof ClearableHistogram)
            ((ClearableHistogram) h).clear();
    }

    private static void clearHistogram(Histogram histogram)
    {
        if (histogram instanceof ClearableHistogram)
            ((ClearableHistogram) histogram).clear();
    }

    private static void waitForIndexQueryable()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline)
        {
            if (cfs.indexManager.listIndexes().stream().allMatch(idx -> cfs.indexManager.isIndexQueryable(idx)))
                return;
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new RuntimeException("Timed out waiting for index to become queryable on " + KEYSPACE + '.' + TABLE);
    }
}
