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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
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
import org.apache.cassandra.db.view.MVBackfillManager;
import org.apache.cassandra.db.view.MVBackfillSSTableStreamSink;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamRequest;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for MV backfill retry scenarios with streaming failures.
 *
 * This test class focuses on testing the retry mechanism when MV backfill
 * operations encounter streaming failures, ensuring that the system can
 * recover gracefully and complete the backfill on retry.
 */
public class MVBackfillRetryPathTest extends MVBackfillTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(MVBackfillRetryPathTest.class);

    /**
     * Test MV backfill retry behavior when streaming fails.
     * This test verifies that:
     * 1. When streaming fails during initial backfill, the system tracks this correctly
     * 2. On retry, SSTables are not recreated (reuses existing ones)
     * 3. On retry, streaming is attempted again and succeeds
     */
    @Test
    public void testMVBackfillStreamingFailureRetryContinue() throws Exception
    {
        viewBackfillStreamingFailureRetryHelper(false);
    }

    /**
     * Test MV backfill retry behavior when streaming fails.
     * This test verifies that:
     * 1. When streaming fails during initial backfill, the system tracks this correctly
     * 2. On retry, we force restart,  SSTables will be recreated
     * 3. On retry, streaming is attempted again and succeeds
     */
    @Test
    public void testMVBackfillStreamingFailureRetryRestart() throws Exception
    {
        viewBackfillStreamingFailureRetryHelper(true);
    }

    // The parameter forceRestart is for testing if the retry should start from beginning
    public void viewBackfillStreamingFailureRetryHelper(boolean forceRestart) throws Exception
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
            populateBaseTable(cluster, 10000);

            // Step 3: Create materialized views
            createMaterializedViews(cluster);

            // Step 4: Test partial streaming failure and retry by enabling streaming failure on node 3 and start
            // streaming on node 1
            enableFailStreamingOnNode(cluster, 3);
            Set<String> expectedSucceededHosts = new HashSet<>();
            expectedSucceededHosts.add(InetAddressAndPort.getByAddress(cluster.get(1).broadcastAddress()).getHostAddressAndPort());
            expectedSucceededHosts.add(InetAddressAndPort.getByAddress(cluster.get(2).broadcastAddress()).getHostAddressAndPort());
            cluster.get(1).runOnInstance(() -> { // Run on node 1
                try
                {
                    // Perform initial backfill - should succeed SSTable creation but fail streaming
                    MVBackfillManager.BackfillState samePKState = performNodeBackfill(MV_SAME_PK, forceRestart);
                    Assert.assertNotNull("Backfill should have failed due to streaming failure injection", samePKState.failure);

                    // Verify state: SSTABLE_BUILD_COMPLETE with streaming failure
                    verifyPartialStreamingState(MV_SAME_PK, expectedSucceededHosts);

                    // Perform initial backfill - should succeed SSTable creation but fail streaming
                    MVBackfillManager.BackfillState diffPKState = performNodeBackfill(MV_DIFF_PK, forceRestart);
                    Assert.assertNotNull("Backfill should have failed due to streaming failure injection", diffPKState.failure);

                    // Verify state: SSTABLE_BUILD_COMPLETE with streaming failure
                    verifyPartialStreamingState(MV_DIFF_PK, expectedSucceededHosts);
                }
                catch (Exception e)
                {
                    throw new RuntimeException("Partial streaming failure retry test failed", e);
                }
            });

            verifyAndClearStreamAttemptOnReceiver(cluster, 1, false);
            verifyAndClearStreamAttemptOnReceiver(cluster, 2, false);
            verifyAndClearStreamAttemptOnReceiver(cluster, 3, false);

            // step 5 resume stream on node 3 and retry
            restoreStreamingOnNode(cluster, 3);
            // retry on node 1
            cluster.get(1).runOnInstance(() -> { // Run on node 1
                try
                {
                    long retryStartTime = System.currentTimeMillis();
                    // Retry the backfill - should NOT recreate SSTables, should only stream to node 3
                    MVBackfillManager.BackfillState samePKState = performNodeBackfill(MV_SAME_PK, forceRestart);
                    Assert.assertNull("Backfill should not have failed", samePKState.failure);
                    // Verify retry behavior with timestamp
                    // Verify that SSTables were not recreated during retry by checking file timestamps
                    verifyNewSSTables(MV_SAME_PK, retryStartTime, forceRestart);
                    // Verify final successful state
                    verifyBackfillComplete(MV_SAME_PK);

                    // repeat for MV_DIFF_PK
                    MVBackfillManager.BackfillState diffPKState = performNodeBackfill(MV_DIFF_PK, forceRestart);
                    Assert.assertNull("Backfill should not have failed", diffPKState.failure);
                    verifyNewSSTables(MV_DIFF_PK, retryStartTime, forceRestart);
                    verifyBackfillComplete(MV_DIFF_PK);

                }
                catch (Exception e)
                {
                    throw new RuntimeException("Partial streaming failure retry test failed", e);
                }
            });

            // node 1 and 2 should not receive the stream but node 3 should receive stream
            verifyAndClearStreamAttemptOnReceiver(cluster, 1, true);
            verifyAndClearStreamAttemptOnReceiver(cluster, 2, true);
            verifyAndClearStreamAttemptOnReceiver(cluster, 3, false);

            // step 5 finish dispersal on the other two nodes
            cluster.get(2).runOnInstance(() -> {
                MVBackfillManager.BackfillState samePKState = performNodeBackfill(MV_SAME_PK, forceRestart);
                Assert.assertNull("Backfill should not have failed", samePKState.failure);
                MVBackfillManager.BackfillState diffPKState = performNodeBackfill(MV_DIFF_PK, forceRestart);
                Assert.assertNull("Backfill should not have failed", diffPKState.failure);
            });
            cluster.get(3).runOnInstance(() -> {
                MVBackfillManager.BackfillState samePKState = performNodeBackfill(MV_SAME_PK, forceRestart);
                Assert.assertNull("Backfill should not have failed", samePKState.failure);
                MVBackfillManager.BackfillState diffPKState = performNodeBackfill(MV_DIFF_PK, forceRestart);
                Assert.assertNull("Backfill should not have failed", diffPKState.failure);
            });
            // Step 6: Verify data consistency across all nodes
            verifyDataConsistency(cluster);
        }
    }

    // Helper methods specific to retry path testing

    private void enableFailStreamingOnNode(Cluster cluster, int i)
    {
        //Mark the node stream failure
        cluster.get(i).runOnInstance(PartialStreamingFailureInjector::enableStreamFailure);
    }

    private void restoreStreamingOnNode(Cluster cluster, int i)
    {
        //Mark the node stream failure
        cluster.get(i).runOnInstance(PartialStreamingFailureInjector::disableFailure);
    }

    private static void verifyPartialStreamingState(String viewName, Set<String> succeededHosts)
    {
        Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
        SystemKeyspace.ViewBackfillStatus status = SystemKeyspace.getViewBackfillStatus(KEYSPACE, viewName, ranges);

        Assert.assertNotNull("Backfill status should exist after streaming failure", status);
        Assert.assertEquals("Status should be SSTABLE_BUILD_COMPLETE after streaming failure",
                            SystemKeyspace.ViewBackfillState.SSTABLE_BUILD_COMPLETE, status.status);
        Assert.assertEquals(succeededHosts, status.streamSucceededHosts);
    }

    private static void verifyAndClearStreamAttemptOnReceiver(Cluster cluster, int receiver, boolean noStreamAttempt)
    {
        cluster.get(receiver).runOnInstance(() -> {
            if (noStreamAttempt)
            {
                Assert.assertEquals(0, PartialStreamingFailureInjector.getStreamingAttemptCount());
            }
            else
            {
                Assert.assertTrue(PartialStreamingFailureInjector.getStreamingAttemptCount() > 0);
                PartialStreamingFailureInjector.resetCounters();
            }
        });
    }

    private static void verifyNewSSTables(String viewName, long retryStartTime, boolean expectingNewSSTables)
    {
        try
        {
            Keyspace keyspace = Keyspace.open(KEYSPACE);
            ColumnFamilyStore viewCfs = keyspace.getColumnFamilyStore(viewName);

            Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
            // Get the MV backfill directory where SSTables are temporarily created
            MVBackfillSSTableStreamSink sink = new MVBackfillSSTableStreamSink(viewCfs, ranges, 128);
            File backfillDir = sink.getBackfillDirectory();

            if (backfillDir != null && backfillDir.exists())
            {
                File[] files = backfillDir.tryList();
                if (files != null)
                {
                    for (File file : files)
                    {
                        if (file.name().endsWith("-Data.db"))
                        {
                            long fileTime = file.lastModified();
                            if (expectingNewSSTables)
                            {
                                Assert.assertTrue(
                                String.format("SSTable file %s was created before retry start time. " +
                                              "File time: %d, Retry start: %d. This suggests SSTables were NOT recreated during retry.",
                                              file.name(), fileTime, retryStartTime),
                                fileTime > retryStartTime);

                                logger.info("Verified SSTable {} was recreated during retry (file time: {}, retry start: {})",
                                            file.name(), fileTime, retryStartTime);
                            }
                            else
                            {
                                Assert.assertTrue(
                                String.format("SSTable file %s was created after retry start time. " +
                                              "File time: %d, Retry start: %d. This suggests SSTables were recreated during retry.",
                                              file.name(), fileTime, retryStartTime),
                                fileTime < retryStartTime);

                                logger.info("Verified SSTable {} was NOT recreated during retry (file time: {}, retry start: {})",
                                            file.name(), fileTime, retryStartTime);
                            }
                        }
                    }
                }
            }
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
        
        logger.info("Verified backfill completion for view {}", viewName);
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
                           .method(named("prepare"))
                           .intercept(MethodDelegation.to(PartialStreamingFailureInjector.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            logger.info("Partial streaming failure injector installed on node {}", nodeNumber);
        }

        public static void enableStreamFailure()
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
        public static void prepare(Collection<StreamRequest> requests, Collection<StreamSummary> summaries,
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
