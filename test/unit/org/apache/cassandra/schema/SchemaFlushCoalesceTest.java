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
package org.apache.cassandra.schema;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.TCMMetrics;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Exercises {@link SchemaKeyspace#scheduleFlush()} directly, independent of the
 * {@code cassandra.test.flush_local_schema_changes} gate at the {@link SchemaKeyspace#applyChanges} call site
 * (build.xml sets that property to false for the unit test suite, so ordinary CQLTester-driven DDL never reaches
 * scheduleFlush() in this test run; scheduleFlush() itself has no such gate, so calling it directly still
 * exercises the real coalescing/sync behaviour under test).
 */
public class SchemaFlushCoalesceTest extends CQLTester
{
    @After
    public void resetCoalesceWindow()
    {
        // restore the documented default
        DatabaseDescriptor.setSchemaFlushCoalescingWindow(new DurationSpec.IntMillisecondsBound("1000ms"));
    }

    /**
     * window > 0: scheduleFlush() must not block the calling thread, and repeated rapid calls (as would
     * happen for a burst of DDL statements) must be coalesced into a single scheduled flush, not one per call.
     */
    @Test
    public void testAsynchronousCoalescedFlush() throws Throwable
    {
        DatabaseDescriptor.setSchemaFlushCoalescingWindow(new DurationSpec.IntMillisecondsBound("50ms"));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        ColumnFamilyStore tablesCfs = schemaCfs(SchemaKeyspaceTables.TABLES);
        long switchesBeforeSchedule = tablesCfs.metric.memtableSwitchCount.getCount();

        // Simulate a burst of DDL: several rapid calls should coalesce into a single flush task rather than
        // stacking up N flushes.
        SchemaKeyspace.scheduleFlush();
        SchemaKeyspace.scheduleFlush();
        SchemaKeyspace.scheduleFlush();

        // Must be asynchronous: the flush has not necessarily completed synchronously on this thread. We can't
        // assert "definitely not flushed yet" without being flaky against scheduler timing, so instead assert
        // only the eventual outcome, with a timeout well beyond the configured coalesce window.
        Util.spinAssert("system_schema tables flush was scheduled and completed",
                         greaterThan(switchesBeforeSchedule),
                         () -> tablesCfs.metric.memtableSwitchCount.getCount(),
                         5, TimeUnit.SECONDS);

        // A second burst of calls, once the previous flush has already run and reset flushScheduled, must
        // schedule (and complete) another flush rather than being silently dropped.
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        long switchesBeforeSecondSchedule = tablesCfs.metric.memtableSwitchCount.getCount();
        SchemaKeyspace.scheduleFlush();
        Util.spinAssert("a second round of scheduleFlush() also completes a flush",
                         greaterThan(switchesBeforeSecondSchedule),
                         () -> tablesCfs.metric.memtableSwitchCount.getCount(),
                         5, TimeUnit.SECONDS);
    }

    /**
     * window == 0ms: legacy behaviour. scheduleFlush() must flush synchronously, so the flush is visible
     * immediately after the call returns, with no polling required.
     */
    @Test
    public void testZeroWindowIsSynchronousFlush() throws Throwable
    {
        DatabaseDescriptor.setSchemaFlushCoalescingWindow(new DurationSpec.IntMillisecondsBound("0ms"));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        ColumnFamilyStore tablesCfs = schemaCfs(SchemaKeyspaceTables.TABLES);
        long switchesBefore = tablesCfs.metric.memtableSwitchCount.getCount();

        SchemaKeyspace.scheduleFlush();

        // Synchronous: the flush must already be visible the instant the call returns, no polling needed.
        assertEquals(switchesBefore + 1, tablesCfs.metric.memtableSwitchCount.getCount());
    }

    /**
     * Tests normal flush scheduling and flag management during successful operation. Verifies that the flag
     * transitions correctly and that subsequent flushes work after the scheduled task completes.
     */
    @Test
    public void testFlushScheduleStateManagement() throws Throwable
    {
        DatabaseDescriptor.setSchemaFlushCoalescingWindow(new DurationSpec.IntMillisecondsBound("50ms"));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        ColumnFamilyStore tablesCfs = schemaCfs(SchemaKeyspaceTables.TABLES);
        long failuresBefore = TCMMetrics.instance.schemaFlushScheduleFailures.getCount();

        // Initially, no flush should be scheduled
        assertFalse("No flush should be scheduled initially", SchemaKeyspace.isFlushScheduled());

        // Schedule a flush - the flag should be set transiently
        SchemaKeyspace.scheduleFlush();

        // The flag should be set immediately after scheduling (before the task runs)
        assertTrue("Flag should be set immediately after scheduling", SchemaKeyspace.isFlushScheduled());

        // Wait for the flush to complete (memtable switch proves flushBlocking() ran)
        long switchesBefore = tablesCfs.metric.memtableSwitchCount.getCount();
        Util.spinAssert("system_schema tables flush completes",
                         greaterThan(switchesBefore),
                         () -> tablesCfs.metric.memtableSwitchCount.getCount(),
                         5, TimeUnit.SECONDS);

        // Flag must be reset (the task resets it before calling flushBlocking, so by the time
        // the switch count increased, the flag was already reset)
        assertFalse("Flag must be reset after flush", SchemaKeyspace.isFlushScheduled());

        // Verify no scheduling failures occurred during normal operation
        assertEquals("No scheduling failures should occur during normal operation",
                     failuresBefore, TCMMetrics.instance.schemaFlushScheduleFailures.getCount());

        // Verify a subsequent flush can be scheduled successfully (flag was properly reset)
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        long switchesBeforeSecond = tablesCfs.metric.memtableSwitchCount.getCount();
        SchemaKeyspace.scheduleFlush();

        Util.spinAssert("subsequent flush after reset also completes",
                         greaterThan(switchesBeforeSecond),
                         () -> tablesCfs.metric.memtableSwitchCount.getCount(),
                         5, TimeUnit.SECONDS);
    }

    /**
     * Tests that scheduleFlush() correctly resets the flag when scheduling throws RejectedExecutionException.
     * This simulates the dangerous operational path: executor is shut down but the node is still serving DDL.
     * Without the fix, the flag gets stuck and all subsequent scheduleFlush() calls fail silently.
     */
    @Test
    public void testScheduleRejectedExecutionException() throws Throwable
    {
        DatabaseDescriptor.setSchemaFlushCoalescingWindow(new DurationSpec.IntMillisecondsBound("50ms"));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        ColumnFamilyStore tablesCfs = schemaCfs(SchemaKeyspaceTables.TABLES);
        SchemaKeyspace.FlushScheduler realScheduler = SchemaKeyspace.flushScheduler;
        long failuresBefore = TCMMetrics.instance.schemaFlushScheduleFailures.getCount();

        try
        {
            // Stub that throws RejectedExecutionException
            SchemaKeyspace.flushScheduler = (task, delay, unit) -> {
                throw new RejectedExecutionException("Executor is shut down");
            };

            // Attempt to schedule - should catch the exception and reset the flag
            SchemaKeyspace.scheduleFlush();

            // The flag must be reset (not stuck)
            assertFalse("Flag must be reset after RejectedExecutionException", SchemaKeyspace.isFlushScheduled());

            // Counter must have incremented
            assertEquals("Counter must increment on scheduling failure",
                         failuresBefore + 1, TCMMetrics.instance.schemaFlushScheduleFailures.getCount());
        }
        finally
        {
            // Restore real scheduler
            SchemaKeyspace.flushScheduler = realScheduler;
        }

        // Verify the feature recovers: subsequent flush with real scheduler should work
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        long switchesBefore = tablesCfs.metric.memtableSwitchCount.getCount();
        SchemaKeyspace.scheduleFlush();

        Util.spinAssert("flush scheduling recovers after exception",
                         greaterThan(switchesBefore),
                         () -> tablesCfs.metric.memtableSwitchCount.getCount(),
                         5, TimeUnit.SECONDS);
    }

    /**
     * Tests that scheduleFlush() correctly resets the flag when schedule() returns a cancelled future.
     * This simulates the silent-failure path where the executor's rejectedExecutionHandler calls cancel(false)
     * on the task rather than throwing. Without the fix, the flag gets stuck.
     */
    @Test
    public void testScheduleCancelledFuture() throws Throwable
    {
        DatabaseDescriptor.setSchemaFlushCoalescingWindow(new DurationSpec.IntMillisecondsBound("50ms"));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        ColumnFamilyStore tablesCfs = schemaCfs(SchemaKeyspaceTables.TABLES);
        SchemaKeyspace.FlushScheduler realScheduler = SchemaKeyspace.flushScheduler;
        long failuresBefore = TCMMetrics.instance.schemaFlushScheduleFailures.getCount();

        try
        {
            // Stub that returns a cancelled future (simulates rejectedExecutionHandler calling cancel())
            SchemaKeyspace.flushScheduler = (task, delay, unit) -> {
                // Schedule a real task and immediately cancel it to get a genuinely cancelled ScheduledFuture
                ScheduledFuture<?> future = ScheduledExecutors.nonPeriodicTasks.schedule(() -> {}, 1000, TimeUnit.MILLISECONDS);
                future.cancel(false);
                return future;
            };

            // Attempt to schedule - should detect cancelled future and reset the flag
            SchemaKeyspace.scheduleFlush();

            // The flag must be reset (not stuck)
            assertFalse("Flag must be reset when schedule returns cancelled future", SchemaKeyspace.isFlushScheduled());

            // Counter must have incremented
            assertEquals("Counter must increment on cancelled future",
                         failuresBefore + 1, TCMMetrics.instance.schemaFlushScheduleFailures.getCount());
        }
        finally
        {
            // Restore real scheduler
            SchemaKeyspace.flushScheduler = realScheduler;
        }

        // Verify the feature recovers: subsequent flush with real scheduler should work
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        long switchesBefore = tablesCfs.metric.memtableSwitchCount.getCount();
        SchemaKeyspace.scheduleFlush();

        Util.spinAssert("flush scheduling recovers after cancelled future",
                         greaterThan(switchesBefore),
                         () -> tablesCfs.metric.memtableSwitchCount.getCount(),
                         5, TimeUnit.SECONDS);
    }

    private static ColumnFamilyStore schemaCfs(String tableName)
    {
        return Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME).getColumnFamilyStore(tableName);
    }
}
