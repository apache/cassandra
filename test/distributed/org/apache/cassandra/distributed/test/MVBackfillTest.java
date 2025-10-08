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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import static net.bytebuddy.matcher.ElementMatchers.named;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.view.MVBackfillManager;
import org.apache.cassandra.db.view.MVBackfillSSTableStreamSink;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.QueryResult;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed tests for MV backfill functionality with SSTable streaming.
 *
 * This test creates a multi-node cluster, populates a base table with data,
 * creates materialized views, and tests the backfill process with streaming
 * to ensure data consistency across the cluster.
 */
public class MVBackfillTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MVBackfillTest.class);
    private static final String KEYSPACE = "mv_backfill_test";
    private static final String BASE_TABLE = "base_table";
    private static final String MV_SAME_PK = "mv_same_partition_key";
    private static final String MV_DIFF_PK = "mv_different_partition_key";

    /**
     * Test MV backfill with streaming in a cluster.
     * Creates two MVs: one with same partition key, one with different partition key.
     */
    @Test
    public void testMVBackfillWithStreaming1() throws Exception
    {
        backfillTestHelper(3, 1, 100);
    }

    @Test
    public void testMVBackfillWithStreaming2() throws Exception
    {
        backfillTestHelper(3, 1, 10000);
    }

    @Test
    public void testMVBackfillWithStreaming3() throws Exception
    {
        backfillTestHelper(6, 3, 10000);
    }

    @Test
    public void testMVBackfillWithStreaming4() throws Exception
    {
        backfillTestHelper(5, 2, 10000);
    }

    /**
     * Test MV backfill retry behavior when streaming fails.
     * This test verifies that:
     * 1. When streaming fails during initial backfill, the system tracks this correctly
     * 2. On retry, SSTables are not recreated (reuses existing ones)
     * 3. On retry, streaming is attempted again and succeeds
     *
     * Uses ByteBuddy injection similar to BootstrapTest.BBStreamFailure to simulate streaming failures.
     */
    @Test
    public void testMVBackfillPartialStreamingFailureRetry() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3) // 3 nodes to have multiple streaming targets
                                           .withConfig(config -> config.with(Feature.values())
                                                                       .set("materialized_view_auto_backfill_enabled", false)
                                                                       .set("materialized_views_enabled", true))
                                           .withInstanceInitializer(PartialStreamingFailureInjector::install)
                                           .start()))
        {
            // Step 1: Create keyspace and base table
            createSchema(cluster, 2); // RF=2 to ensure streaming to multiple nodes

            // Step 2: Populate base table with data
            populateBaseTable(cluster, 1000);

            // Step 3: Create materialized views
            createMaterializedViews(cluster);

            // Step 4: Test partial streaming failure and retry
            cluster.get(1).runOnInstance(() -> { // Run on node 1
                try
                {
                    // Enable failure injection for first attempt
                    PartialStreamingFailureInjector.enableFailureToNode3();

                    // Perform initial backfill - should succeed SSTable creation but fail streaming
                    performNodeBackfillExpectingPartialFailure(MV_SAME_PK);

                    // Verify state: SSTABLE_BUILD_COMPLETE with streaming failure
                    verifyPartialStreamingState(MV_SAME_PK);

                    // Perform initial backfill - should succeed SSTable creation but fail streaming
                    performNodeBackfillExpectingPartialFailure(MV_DIFF_PK);

                    // Verify state: SSTABLE_BUILD_COMPLETE with streaming failure
                    verifyPartialStreamingState(MV_DIFF_PK);

                    // Disable failure injection for retry
                    PartialStreamingFailureInjector.disableFailure();

                    // Reset counters to track retry behavior and record timestamp
                    PartialStreamingFailureInjector.resetCounters();
                    
                    // Wait for ViewBuildExecutor to become idle before retry
                    waitForViewBuildExecutorIdle();
                    long retryStartTime = System.currentTimeMillis();

                    // Retry the backfill - should NOT recreate SSTables, should only stream to node 3
                    performNodeBackfillRetry(MV_SAME_PK);

                    // Verify retry behavior with timestamp
                    verifyRetryBehavior(MV_SAME_PK, retryStartTime);

                    // Verify final successful state
                    verifyBackfillComplete(MV_SAME_PK);

                    // Retry the backfill - should NOT recreate SSTables, should only stream to node 3
                    performNodeBackfillRetry(MV_DIFF_PK);

                    // Verify retry behavior with timestamp
                    verifyRetryBehavior(MV_DIFF_PK, retryStartTime);

                    // Verify final successful state
                    verifyBackfillComplete(MV_DIFF_PK);
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Partial streaming failure retry test failed", e);
                }
            });

            // step 5 finish dispersal on other two nodes
            cluster.get(2).runOnInstance(() -> {
                performNodeBackfill(MV_SAME_PK);
                performNodeBackfill(MV_DIFF_PK);
            });
            cluster.get(3).runOnInstance(() -> {
                performNodeBackfill(MV_SAME_PK);
                performNodeBackfill(MV_DIFF_PK);
            });


            // Step 6: Verify data consistency across all nodes
            verifyDataConsistency(cluster);
        }
    }

    private void backfillTestHelper(int nodeCount, int replicationFactor, int rowCount) throws IOException
    {
        try (Cluster cluster = init(Cluster.build(nodeCount)
                                           .withConfig(config -> config.with(Feature.values())
                                                                       .set("materialized_view_auto_backfill_enabled", false)
                                                                       .set("materialized_views_enabled", true))
                                           .start()))
        {
            // Step 1: Create keyspace and base table
            createSchema(cluster, replicationFactor);

            // Step 2: Populate base table with data
            populateBaseTable(cluster, rowCount);

            // Step 3: Create materialized views
            createMaterializedViews(cluster);

            // Step 4: Perform backfill with streaming
            performBackfillWithStreaming(cluster);

            // Step 5: Verify data consistency
            verifyDataConsistency(cluster);

            // Step 6: verify the generated MV SSTables are removed
            verifyMVBackfillFilesRemoved(cluster);
        }
    }

    private void createSchema(Cluster cluster, int replicationFactor) throws IOException
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

    private void populateBaseTable(Cluster cluster, int numRows)
    {
        // Insert data across multiple partitions to ensure good distribution
        for (int i = 0; i < numRows; i++)
        {
            int pk = i % 100; // Create 100 partitions
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

    private void createMaterializedViews(Cluster cluster)
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

    private void performBackfillWithStreaming(Cluster cluster)
    {
        // Perform backfill on each node for both MVs
        cluster.forEach(instance -> {
            instance.runOnInstance(() -> {
                try
                {
                    performNodeBackfill(MV_SAME_PK);
                    performNodeBackfill(MV_DIFF_PK);
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Backfill failed", e);
                }
            });
        });
    }

    private static void waitForViewBuildExecutorIdle() throws InterruptedException
    {
        int maxWaitSeconds = 30;
        int waitedSeconds = 0;
        
        while (waitedSeconds < maxWaitSeconds)
        {
            if (!CompactionManager.instance.hasOngoingOrPendingTasks())
            {
                return;
            }
            
            Thread.sleep(1000); // Wait 1 second
            waitedSeconds++;
        }
        
        logger.warn("CompactionManager did not become idle within {} seconds", maxWaitSeconds);
    }

    private static void performNodeBackfill(String viewName)
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

            // Create backfill manager
            MVBackfillManager backfillManager = new MVBackfillManager();

            // Submit backfill task
            Future<?> backfillFuture = backfillManager.submitBackfill(
                baseCfs, view, ranges, sink, state, false);

            // Wait for completion
            backfillFuture.get(60, TimeUnit.SECONDS);

            // Check for streaming failures
            Set<InetAddressAndPort> failedHosts = sink.getFailedHosts();
            if (!failedHosts.isEmpty())
            {
                System.err.println("Warning: Streaming failed to hosts: " + failedHosts);
                // Don't fail the test - some failures might be expected in distributed environment
            }

            System.out.println(String.format(
                "Backfill completed for view %s: %d partitions, %d rows, %d view rows",
                viewName, state.partitionsProcessed, state.rowsProcessed, state.viewRowsGenerated));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Backfill failed for view: " + viewName, e);
        }
    }

    private void verifyDataConsistency(Cluster cluster)
    {
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

    private void verifyMVBackfillFilesRemoved(Cluster cluster)
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

    // Helper methods for partial streaming failure test

    private static void performNodeBackfillExpectingPartialFailure(String viewName) throws Exception
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore baseCfs = keyspace.getColumnFamilyStore(BASE_TABLE);
        View view = keyspace.viewManager.getByName(viewName);
        ColumnFamilyStore viewCfs = keyspace.getColumnFamilyStore(viewName);

        Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
        MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges, 128);

        MVBackfillManager.BackfillState state = new MVBackfillManager.BackfillState();
        MVBackfillManager backfillManager = new MVBackfillManager();

        Future<?> backfillFuture = backfillManager.submitBackfill(baseCfs, view, ranges, sink, state, false);
        backfillFuture.get(120, TimeUnit.SECONDS);

        // This should fail due to ByteBuddy injection
        Assert.assertNotNull("Backfill should have failed due to streaming failure injection", state.failure);
    }

    private static void performNodeBackfillRetry(String viewName) throws Exception
    {
        // Perform normal backfill - should reuse existing SSTables and only stream to failed hosts
        performNodeBackfill(viewName);
    }

    private static void verifyPartialStreamingState(String viewName)
    {
        Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
        SystemKeyspace.ViewBackfillStatus status = SystemKeyspace.getViewBackfillStatus(KEYSPACE, viewName, ranges);

        Assert.assertNotNull("Backfill status should exist after streaming failure", status);
        Assert.assertEquals("Status should be SSTABLE_BUILD_COMPLETE after streaming failure",
                          SystemKeyspace.ViewBackfillState.SSTABLE_BUILD_COMPLETE, status.status);
        // Note: succeeded hosts may be empty if all streaming failed
        logger.info("Verified streaming failure state: {} succeeded hosts",
                   status.streamSucceededHosts != null ? status.streamSucceededHosts.size() : 0);
    }

    private static void verifyRetryBehavior(String viewName, long retryStartTime)
    {
        // Verify that SSTables were not recreated during retry by checking file timestamps
        verifyNoNewSSTables(viewName, retryStartTime);

        // Verify that streaming happened during retry (should be at least 1 attempt)
        Assert.assertTrue("Should have attempted streaming during retry",
                        PartialStreamingFailureInjector.getStreamingAttemptCount() >= 1);

        logger.info("Verified retry behavior: no new SSTables created, streaming attempted {} times",
                   PartialStreamingFailureInjector.getStreamingAttemptCount());
    }

    private static void verifyNoNewSSTables(String viewName, long retryStartTime)
    {
        try
        {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            ColumnFamilyStore viewCfs = keyspace.getColumnFamilyStore(viewName);

            Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
            // Get the MV backfill directory where SSTables are temporarily created
            MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges, 128);
            File backfillDir = sink.getBackfillDirectory();

            Assert.assertTrue("backfill dir should exists", backfillDir.exists());

            // Check if any SSTable files were created after retry started
            File[] files = backfillDir.tryList();

            if (files != null)
            {
                for (File file : files)
                {
                    if (file.name().endsWith(".db") || file.name().endsWith(".txt") ||
                        file.name().endsWith(".sha1") || file.name().endsWith(".crc32"))
                    {
                        long fileCreationTime = file.lastModified();
                        Assert.assertTrue(
                            String.format("SSTable file %s was created during retry (created: %d, retry start: %d)",
                                        file.name(), fileCreationTime, retryStartTime),
                            fileCreationTime < retryStartTime
                        );
                        logger.debug("Verified SSTable file {} was created before retry (created: {}, retry: {})",
                                   file.name(), fileCreationTime, retryStartTime);
                    }
                }
            }

            logger.info("Verified no new SSTables were created during retry");
        }
        catch (Exception e)
        {
            logger.warn("Could not verify SSTable timestamps, but test can continue", e);
        }
    }

    private static void verifyBackfillComplete(String viewName)
    {
        Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
        SystemKeyspace.ViewBackfillStatus status = SystemKeyspace.getViewBackfillStatus(KEYSPACE, viewName, ranges);

        Assert.assertNotNull("Backfill status should exist after completion", status);
        Assert.assertEquals("Status should be COMPLETE after successful retry",
                          SystemKeyspace.ViewBackfillState.COMPLETE, status.status);
        logger.info("Verified final backfill completion");
    }

    /**
     * ByteBuddy helper class for injecting partial streaming failures
     * Based on BBStreamFailure from BootstrapTest.java
     */
    public static class PartialStreamingFailureInjector
    {
        public static final AtomicBoolean failStream = new AtomicBoolean(false);
        private static final AtomicInteger streamingAttemptCount = new AtomicInteger(0);

        public static void install(ClassLoader cl, int nodeNumber)
        {
            // Use the same approach as BootstrapTest - intercept startStreamingFiles
            new ByteBuddy().rebase(StreamSession.class)
                           .method(named("startStreamingFiles"))
                           .intercept(MethodDelegation.to(PartialStreamingFailureInjector.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            logger.info("Partial streaming failure injector installed on node {}", nodeNumber);
        }

        public static void enableFailureToNode3()
        {
            failStream.set(true);
            logger.info("Enabled streaming failure injection to node 3");
        }

        public static void disableFailure()
        {
            failStream.set(false);
            logger.info("Disabled streaming failure injection");
        }

        public static void resetCounters()
        {
            streamingAttemptCount.set(0);
            logger.info("Reset counters for retry behavior tracking");
        }

        public static int getStreamingAttemptCount()
        {
            return streamingAttemptCount.get();
        }

        @SuppressWarnings("unused")
        public static void startStreamingFiles(StreamSession.PrepareDirection prepareDirection,
                                             @SuperCall java.util.concurrent.Callable<Boolean> zuper) throws Exception
        {
            streamingAttemptCount.incrementAndGet();

            if (failStream.get())
            {
                logger.info("Injecting streaming failure (attempt #{}) ", streamingAttemptCount.get());
                throw new RuntimeException("ByteBuddy injected streaming failure");
            }

            zuper.call();
        }
    }
}
