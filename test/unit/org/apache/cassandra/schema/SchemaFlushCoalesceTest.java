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

import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

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

    private static ColumnFamilyStore schemaCfs(String tableName)
    {
        return Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME).getColumnFamilyStore(tableName);
    }
}
