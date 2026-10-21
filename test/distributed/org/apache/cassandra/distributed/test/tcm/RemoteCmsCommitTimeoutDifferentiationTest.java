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

package org.apache.cassandra.distributed.test.tcm;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.CMSOperations;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.sequences.InProgressSequences;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for CMS commit timeout differentiation across admin, schema, and default commit paths.
 * Uses a shared cluster to reduce setup time. Each test resets all hot-modifiable timeout
 * properties to defaults and clears message filters before running.
 */
public class RemoteCmsCommitTimeoutDifferentiationTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    // Config defaults (matching Config.java field initializers)
    private static final long DEFAULT_CMS_AWAIT_TIMEOUT_MS = 120_000L;
    private static final long DEFAULT_CMS_COMMIT_TIMEOUT_MS = 3_600_000L;
    private static final long DEFAULT_CMS_COMMIT_RETRY_INITIAL_DELAY_MS = 5_000L;
    private static final long DEFAULT_CMS_COMMIT_RETRY_MAX_DELAY_MS = 60_000L;

    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        TestBaseImpl.beforeClass();
        CLUSTER = init(Cluster.build(2)
                                .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP))
                                .start());
    }

    @AfterClass
    public static void afterClass()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void setUp()
    {
        // Clear all message filters
        CLUSTER.filters().reset();

        // Reset all hot-modifiable timeout properties to defaults on ALL instances
        CLUSTER.forEach(i -> i.runOnInstance(() -> {
            CMSOperations.instance.setCmsAwaitTimeoutMillis(DEFAULT_CMS_AWAIT_TIMEOUT_MS);
            CMSOperations.instance.setCmsCommitTimeoutMillis(DEFAULT_CMS_COMMIT_TIMEOUT_MS);
            CMSOperations.instance.setCmsCommitRetryInitialDelayMillis(DEFAULT_CMS_COMMIT_RETRY_INITIAL_DELAY_MS);
            CMSOperations.instance.setCmsCommitRetryMaxDelayMillis(DEFAULT_CMS_COMMIT_RETRY_MAX_DELAY_MS);
        }));

        // Verify all instances are up to date on TCM log
        ClusterUtils.waitForCMSToQuiesce(CLUSTER, CLUSTER.get(1));

        // Verify no pending topology change operations
        CLUSTER.forEach(i -> i.runOnInstance(() -> {
            InProgressSequences seqs = ClusterMetadata.current().inProgressSequences;
            assertTrue("Expected no in-progress sequences on instance, but found some", seqs.isEmpty());
        }));
    }

    // ==================== Admin timeout tests ====================

    /**
     * Move with dropped TCM_COMMIT_REQ fails near cms_commit_timeout (3s), not cms_await_timeout (120s).
     * After failure, restores filter and verifies move can be cancelled.
     */
    @Test
    public void moveUsesAdminTimeout()
    {
        setOnAllInstances(3_000L, 200L, 1_000L);

        CLUSTER.filters().verbs(Verb.TCM_COMMIT_REQ.id).from(2).to(1).drop();

        long start = nanoTime();
        CLUSTER.get(2).nodetoolResult("move", "1234").asserts().failure();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

        assertTrue("Move should fail near cms_commit_timeout (3s), but took " + elapsedMs + "ms",
                   elapsedMs < 30_000);
        assertMoveFailed(CLUSTER.get(2));

        // Restore filter and cancel the failed move
        CLUSTER.filters().reset();
        cancelInProgressSequencesOnNode(CLUSTER.get(2));
    }

    /**
     * Schema DDL is NOT routed through admin timeout — it should fail near cms_await_timeout
     * (set to 3s), well under the admin timeout (60s).
     */
    @Test
    public void schemaNotRoutedThroughAdminTimeout()
    {
        // Short cms_await_timeout, long admin timeout
        CLUSTER.forEach(i -> i.runOnInstance(() -> {
            CMSOperations.instance.setCmsAwaitTimeoutMillis(3_000L);
            CMSOperations.instance.setCmsCommitTimeoutMillis(60_000L);
        }));

        CLUSTER.filters().verbs(Verb.TCM_COMMIT_REQ.id).from(2).to(1).drop();

        long start = nanoTime();
        try
        {
            CLUSTER.coordinator(2).execute(
                "CREATE KEYSPACE IF NOT EXISTS schema_not_admin_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                ConsistencyLevel.ONE);
        }
        catch (Exception e)
        {
            // Expected timeout
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

        assertTrue("Schema DDL should fail near cms_await_timeout (3s), NOT admin timeout (60s), but took " + elapsedMs + "ms",
                   elapsedMs < 30_000);
    }

    /**
     * JMX runtime modification of cms_commit_timeout is a hot property and takes effect immediately.
     */
    @Test
    public void cmsCommitTimeoutIsHotProperty()
    {
        // Verify initial default
        long initial = CLUSTER.get(2).callOnInstance(() -> CMSOperations.instance.getCmsCommitTimeoutMillis());
        assertEquals("Initial cms_commit_timeout should be default (1h)", DEFAULT_CMS_COMMIT_TIMEOUT_MS, initial);

        // Update via JMX on the node that will attempt the move
        setOnAllInstances(3_000L, 200L, 1_000L);

        long updated = CLUSTER.get(2).callOnInstance(() -> CMSOperations.instance.getCmsCommitTimeoutMillis());
        assertEquals("Updated cms_commit_timeout should be 3s", 3_000L, updated);

        // Drop commits and verify the move fails with the new shorter timeout
        CLUSTER.filters().verbs(Verb.TCM_COMMIT_REQ.id).from(2).to(1).drop();

        long start = nanoTime();
        CLUSTER.get(2).nodetoolResult("move", "2345").asserts().failure();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(nanoTime() - start);

        assertTrue("Move should fail near updated 3s timeout, but took " + elapsedMs + "ms",
                   elapsedMs < 30_000);

        // Restore filter and cancel
        CLUSTER.filters().reset();
        cancelInProgressSequencesOnNode(CLUSTER.get(2));
    }

    /**
     * cms_await_timeout is settable at runtime and takes effect.
     */
    @Test
    public void cmsAwaitTimeoutIsHotProperty()
    {
        long initial = CLUSTER.get(1).callOnInstance(() -> CMSOperations.instance.getCmsAwaitTimeoutMillis());
        assertEquals("Initial cms_await_timeout should be 120s", DEFAULT_CMS_AWAIT_TIMEOUT_MS, initial);

        CLUSTER.forEach(i -> i.runOnInstance(() -> CMSOperations.instance.setCmsAwaitTimeoutMillis(5_000L)));
        long updated = CLUSTER.get(1).callOnInstance(() -> CMSOperations.instance.getCmsAwaitTimeoutMillis());
        assertEquals("cms_await_timeout should be 5s", 5_000L, updated);
    }

    // ==================== Helpers ====================

    /**
     * Set admin timeout properties on ALL instances.
     */
    private void setOnAllInstances(long adminTimeoutMs, long retryInitialDelayMs, long retryMaxDelayMs)
    {
        CLUSTER.forEach(i -> i.runOnInstance(() -> {
            CMSOperations.instance.setCmsCommitTimeoutMillis(adminTimeoutMs);
            CMSOperations.instance.setCmsCommitRetryInitialDelayMillis(retryInitialDelayMs);
            CMSOperations.instance.setCmsCommitRetryMaxDelayMillis(retryMaxDelayMs);
        }));
    }

    private static void assertMoveFailed(IInvokableInstance i)
    {
        String mode = i.callOnInstance(() -> StorageService.instance.operationMode().toString());
        assertEquals(StorageService.Mode.MOVE_FAILED.toString(), mode);
    }

    /**
     * Cancel any in-progress topology sequences for the given node and verify cleanup.
     */
    private static void cancelInProgressSequencesOnNode(IInvokableInstance instance)
    {
        instance.runOnInstance(() -> {
            StorageService.cancelInProgressSequences(ClusterMetadata.current().myNodeId());
        });

        // Wait briefly for cancellation to propagate
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}

        // Verify the cancellation succeeded
        instance.runOnInstance(() -> {
            InProgressSequences seqs = ClusterMetadata.current().inProgressSequences;
            assertTrue("Expected no in-progress sequences after cancellation", seqs.isEmpty());
        });
    }
}
