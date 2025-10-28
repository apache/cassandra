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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.view.MVBackfillManager;
import org.apache.cassandra.db.view.MVBackfillSSTableStreamSink;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.QueryResult;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for MV backfill functionality tests with SSTable streaming.
 *
 * This base class provides common functionality for testing materialized view
 * backfill operations in a distributed environment, including shared helper
 * methods for schema creation, data population, and verification.
 */
public abstract class MVBackfillTestBase extends TestBaseImpl
{
    protected static final Logger logger = LoggerFactory.getLogger(MVBackfillTestBase.class);
    protected static final String KEYSPACE = "mv_backfill_test";
    protected static final String BASE_TABLE = "base_table";
    protected static final String MV_SAME_PK = "mv_same_partition_key";
    protected static final String MV_DIFF_PK = "mv_different_partition_key";

    protected void createSchema(Cluster cluster, int replicationFactor) throws IOException
    {
        cluster.schemaChange(String.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
        "{'class': 'SimpleStrategy', 'replication_factor': %d}", KEYSPACE, replicationFactor));

        cluster.schemaChange(String.format(
        "CREATE TABLE %s.%s (" +
        "  pk int," +
        "  ck int," +
        "  v1 text," +
        "  v2 int," +
        "  v3 double," +
        "  PRIMARY KEY (pk, ck)" +
        ")", KEYSPACE, BASE_TABLE));
    }

    protected void populateBaseTable(Cluster cluster, int numRows)
    {
        // Insert data across multiple partitions to ensure good distribution
        for (int i = 0; i < numRows; i++)
        {
            int pk = i % 1000; // Create 1000 partitions
            int ck = i;
            String v1 = "value_" + i;
            int v2 = i * 2;
            double v3 = i * 1.5;

            cluster.coordinator(1).execute(
                String.format("INSERT INTO %s.%s (pk, ck, v1, v2, v3) VALUES (?, ?, ?, ?, ?)",
                             KEYSPACE, BASE_TABLE),
                ConsistencyLevel.ALL, pk, ck, v1, v2, v3);
        }
    }

    protected void createMaterializedViews(Cluster cluster)
    {
        // MV with same partition key as base table
        cluster.schemaChange(String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS " +
            "SELECT * FROM %s.%s " +
            "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v1 IS NOT NULL " +
            "PRIMARY KEY (pk, v1, ck)",
            KEYSPACE, MV_SAME_PK, KEYSPACE, BASE_TABLE));

        // MV with different partition key (v2 becomes partition key)
        cluster.schemaChange(String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS " +
            "SELECT * FROM %s.%s " +
            "WHERE pk IS NOT NULL AND ck IS NOT NULL AND v2 IS NOT NULL " +
            "PRIMARY KEY (v2, pk, ck)",
            KEYSPACE, MV_DIFF_PK, KEYSPACE, BASE_TABLE));

        // Wait for MV creation to complete
        cluster.forEach(instance -> {
            instance.runOnInstance(() -> {
                try
                {
                    Thread.sleep(2000); // Allow MV creation to settle
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }
            });
        });

        // check that MV are empty at this point
        QueryResult mvSamePkResult = cluster.coordinator(1).executeWithResult(
        String.format("SELECT * FROM %s.%s", KEYSPACE, MV_SAME_PK),
        ConsistencyLevel.ALL);

        Set<QueryResultRow> mvSamePk = processResultSetForComparison(mvSamePkResult);

        Assert.assertEquals(0, mvSamePk.size());

        // Verify MV with different partition key
        QueryResult mvDiffPkResult = cluster.coordinator(1).executeWithResult(
        String.format("SELECT * FROM %s.%s", KEYSPACE, MV_DIFF_PK),
        ConsistencyLevel.ALL);
        Set<QueryResultRow> mvDiffPk = processResultSetForComparison(mvDiffPkResult);

        Assert.assertEquals(0, mvDiffPk.size());
    }

    protected void performBackfillWithStreamingSuccessful(Cluster cluster)
    {
        // Perform backfill on each node for both MVs
        cluster.forEach(instance -> {
            instance.runOnInstance(() -> {
                try
                {
                    MVBackfillManager.BackfillState state1 = performNodeBackfill(MV_SAME_PK, false);
                    Assert.assertNull(state1.failure);
                    Assert.assertTrue(state1.completed);
                    MVBackfillManager.BackfillState state2 = performNodeBackfill(MV_DIFF_PK, false);
                    Assert.assertNull(state2.failure);
                    Assert.assertTrue(state2.completed);
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Backfill failed", e);
                }
            });
        });
    }

    protected static MVBackfillManager.BackfillState performNodeBackfill(String viewName, boolean forceRestart)
    {
        try
        {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            ColumnFamilyStore baseCfs = keyspace.getColumnFamilyStore(BASE_TABLE);
            View view = keyspace.viewManager.getByName(viewName);
            ColumnFamilyStore viewCfs = keyspace.getColumnFamilyStore(viewName);

            if (view == null)
            {
                throw new RuntimeException("View not found: " + viewName);
            }

            // Get all token ranges for this node
            Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();

            // Create backfill sink with streaming
            MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges, 128);

            // Create backfill state
            MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();

            // Submit backfill task
            Future<?> backfillFuture = MVBackfillManager.instance.submitBackfill(
                baseCfs, view, ranges, sink, state, forceRestart);

            // Wait for completion
            backfillFuture.get(60, TimeUnit.SECONDS);
            return state;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Backfill failed for view: " + viewName, e);
        }
    }

    protected void verifyDataConsistency(Cluster cluster)
    {
        // make sure the backfill is finished
        try
        {
            Thread.sleep(1000); // wait for 1 second before checking anything
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        verifyBackfillCompleteEntireCluster(MV_SAME_PK, cluster, true);
        verifyBackfillCompleteEntireCluster(MV_DIFF_PK, cluster, true);

        // Get base table rows
        QueryResult baseResult = cluster.coordinator(1).executeWithResult(
            String.format("SELECT * FROM %s.%s", KEYSPACE, BASE_TABLE),
            ConsistencyLevel.ALL);

        Set<QueryResultRow> baseTable = processResultSetForComparison(baseResult);

        // Verify MV with same partition key
        QueryResult mvSamePkResult = cluster.coordinator(1).executeWithResult(
            String.format("SELECT * FROM %s.%s", KEYSPACE, MV_SAME_PK),
            ConsistencyLevel.ALL);

        Set<QueryResultRow> mvSamePk = processResultSetForComparison(mvSamePkResult);

        Assert.assertEquals(baseTable, mvSamePk);

        // Verify MV with different partition key
        QueryResult mvDiffPkResult = cluster.coordinator(1).executeWithResult(
            String.format("SELECT * FROM %s.%s", KEYSPACE, MV_DIFF_PK),
            ConsistencyLevel.ALL);
        Set<QueryResultRow> mvDiffPk = processResultSetForComparison(mvDiffPkResult);

        Assert.assertEquals(baseTable, mvDiffPk);
    }

    private class QueryResultRow
    {
        public final int pk;
        public final int ck;
        public final String v1;
        public final int v2;
        public final double v3;

        QueryResultRow(int pk, int ck, String v1, int v2, double v3)
        {
            this.pk = pk;
            this.ck = ck;
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof QueryResultRow)) return false;
            QueryResultRow that = (QueryResultRow) o;
            return pk == that.pk &&
                   ck == that.ck &&
                   v2 == that.v2 &&
                   Double.compare(that.v3, v3) == 0 &&
                   Objects.equals(v1, that.v1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pk, ck, v1, v2, v3);
        }

    }

    private Set<QueryResultRow> processResultSetForComparison(QueryResult result)
    {
        Set<QueryResultRow> resultSet = new HashSet<>();
        while (result.hasNext())
        {
            Row row = result.next();
            resultSet.add(new QueryResultRow(row.getInteger("pk"),
                                             row.getInteger("ck"),
                                             row.getString("v1"),
                                             row.getInteger("v2"),
                                             row.getDouble("v3")));
        }
        return resultSet;
    }

    protected void verifyMVBackfillFilesRemoved(Cluster cluster)
    {
        cluster.forEach(instance -> {
            instance.runOnInstance(() -> {
                try
                {
                    Keyspace keyspace = Keyspace.open(KEYSPACE);
                    ColumnFamilyStore viewCfs = keyspace.getColumnFamilyStore(MV_SAME_PK);
                    Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
                    MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges, 128);
                    Assert.assertTrue(sink.getBackfillDirectory().exists());
                    Assert.assertTrue(sink.getBackfillDirectory().isDirectory());
                    File[] files = sink.getBackfillDirectory().list();
                    Assert.assertTrue(files == null || files.length == 0);
                    viewCfs = keyspace.getColumnFamilyStore(MV_DIFF_PK);
                    sink = new MVBackfillSSTableStreamSink(viewCfs, ranges, 128);
                    Assert.assertTrue(sink.getBackfillDirectory().exists());
                    Assert.assertTrue(sink.getBackfillDirectory().isDirectory());
                    files = sink.getBackfillDirectory().list();
                    Assert.assertTrue(files == null || files.length == 0);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }

            });
        });
    }

    private static void verifyBackfillCompleteEntireCluster(String viewName, Cluster cluster, boolean completed)
    {
        NodeToolResult result =  cluster.get(1).nodetoolResult("ismvbackfillfinished", KEYSPACE + "." + viewName);
        result.asserts().success();
        result =  cluster.get(1).nodetoolResult("ismvbackfillfinished", KEYSPACE + "." + viewName);
        result.asserts().success();
        if (completed)
        {
            Assert.assertTrue("MV backfill should be finished", result.getStdout().contains("true"));
        }
        else
        {
            Assert.assertTrue("MV backfill should not be finished", result.getStdout().contains("false"));
        }
    }
}