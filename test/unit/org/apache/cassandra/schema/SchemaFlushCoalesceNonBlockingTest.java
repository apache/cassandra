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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The coalesced flush task runs on {@link ScheduledExecutors#nonPeriodicTasks}, a single-threaded executor shared
 * with sstable deletions, the {@code LifecycleTransaction.waitForDeletions()} barrier that dropping a table waits on,
 * and TCM commit retries. The task must therefore only start the {@code system_schema} flushes, never wait for them:
 * if it blocks, everything else queued on that thread stalls behind the flush, which hung in-jvm dtests in CI.
 */
@RunWith(BMUnitRunner.class)
public class SchemaFlushCoalesceNonBlockingTest extends CQLTester
{
    static volatile boolean holdFlushes = false;
    static final CountDownLatch releaseFlushes = new CountDownLatch(1);

    @SuppressWarnings("unused") // called from the Byteman rule
    public static void awaitRelease() throws InterruptedException
    {
        releaseFlushes.await(30, TimeUnit.SECONDS);
    }

    @After
    public void restore()
    {
        holdFlushes = false;
        releaseFlushes.countDown();
        DatabaseDescriptor.setSchemaFlushCoalescingWindow(new DurationSpec.IntMillisecondsBound("1000ms"));
        SchemaKeyspace.restartCoalescedFlush();
    }

    @Test
    @BMRule(name = "Hold memtable flushes while the test asks for it",
            targetClass = "org.apache.cassandra.db.memtable.Flushing$FlushRunnable",
            targetMethod = "call",
            condition = "org.apache.cassandra.schema.SchemaFlushCoalesceNonBlockingTest.holdFlushes",
            action = "org.apache.cassandra.schema.SchemaFlushCoalesceNonBlockingTest.awaitRelease();")
    public void testCoalescedFlushTaskDoesNotWaitForFlushCompletion() throws Throwable
    {
        DatabaseDescriptor.setSchemaFlushCoalescingWindow(new DurationSpec.IntMillisecondsBound("50ms"));
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore tablesCfs = Keyspace.open(SchemaConstants.SCHEMA_KEYSPACE_NAME)
                                              .getColumnFamilyStore(SchemaKeyspaceTables.TABLES);
        long switchesBefore = tablesCfs.metric.memtableSwitchCount.getCount();

        // Capture the coalesced flush task instead of letting nonPeriodicTasks run it, so the test controls its thread.
        AtomicReference<Runnable> task = new AtomicReference<>();
        SchemaKeyspace.FlushScheduler realScheduler = SchemaKeyspace.flushScheduler;
        SchemaKeyspace.flushScheduler = (t, delay, unit) -> {
            task.set(t);
            return ScheduledExecutors.nonPeriodicTasks.schedule(() -> {}, 1, TimeUnit.HOURS);
        };
        try
        {
            SchemaKeyspace.scheduleFlush();
            assertNotNull("scheduleFlush() did not schedule a coalesced flush task", task.get());

            holdFlushes = true;
            Thread runner = new Thread(task.get(), "coalesced-flush-task");
            runner.start();
            runner.join(TimeUnit.SECONDS.toMillis(5));
            assertTrue("the coalesced flush task blocked on flush completion; it must only start the flushes",
                       !runner.isAlive());
        }
        finally
        {
            SchemaKeyspace.flushScheduler = realScheduler;
            holdFlushes = false;
            releaseFlushes.countDown();
        }

        // The task did start the flush (the memtable switch happens before the flush is written).
        Util.spinAssert("the coalesced flush task started a system_schema flush",
                         greaterThan(switchesBefore),
                         () -> tablesCfs.metric.memtableSwitchCount.getCount(),
                         10, TimeUnit.SECONDS);
    }
}
