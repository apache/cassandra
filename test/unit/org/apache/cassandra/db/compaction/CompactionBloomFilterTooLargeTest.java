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

package org.apache.cassandra.db.compaction;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A background compaction that fails because the bloom filter of its output would exceed 16 GB must not be
 * re-requested for the same table over and over: the failed compaction leaves the sstables unchanged, so the next
 * check would select the same task and fail the same way.
 * <p>
 * The failure is injected where {@code OffHeapBitSet} throws it, and only for compactions of the test table. A retry
 * is detected by counting injected failures, not by timing.
 * <p>
 * This is the end-to-end check that the exception thrown by the real writer reaches the runner with
 * {@code BloomFilterTooLargeException} as its cause. {@code BackgroundCompactionRunnerTest} covers the runner's
 * handling of it deterministically, including when the compaction executor is full, with mocked tasks and executors.
 */
@RunWith(BMUnitRunner.class)
@BMRules(rules = {
@BMRule(name = "Remember which table this compaction thread is writing",
        targetClass = "CompactionAwareWriter",
        targetMethod = "switchCompactionWriter",
        action = "org.apache.cassandra.db.compaction.CompactionBloomFilterTooLargeTest.compactingTable.set($0.realm.getTableName())"),
@BMRule(name = "Throw BloomFilterTooLargeException on every compaction attempt of the test table",
        targetClass = "OffHeapBitSet",
        targetMethod = "<init>(long, org.apache.cassandra.utils.obs.MemoryLimiter, boolean)",
        condition = "org.apache.cassandra.db.compaction.CompactionBloomFilterTooLargeTest.failureInjectionEnabled " +
                    "&& org.apache.cassandra.db.compaction.CompactionBloomFilterTooLargeTest.targetTable.equals(" +
                    "org.apache.cassandra.db.compaction.CompactionBloomFilterTooLargeTest.compactingTable.get())",
        action = "org.apache.cassandra.db.compaction.CompactionBloomFilterTooLargeTest.injectedFailureCount.incrementAndGet(); " +
                 "org.apache.cassandra.db.compaction.CompactionBloomFilterTooLargeTest.compactingTable.remove(); " +
                 "throw new org.apache.cassandra.exceptions.BloomFilterTooLargeException(20L * 1024 * 1024 * 1024)")
})
public class CompactionBloomFilterTooLargeTest extends CQLTester
{
    public static final AtomicInteger injectedFailureCount = new AtomicInteger();
    // Only inject while the test has compaction enabled, so other compactions are not failed or counted.
    public static volatile boolean failureInjectionEnabled;
    // Name of the table created by the test; the injected failure applies only to it.
    public static volatile String targetTable;
    // Table being written by a compaction on the current thread; set by the first Byteman rule.
    public static final ThreadLocal<String> compactingTable = new ThreadLocal<>();

    private static final long POLL_INTERVAL_MS = 10;
    private static final long IDLE_TIMEOUT_MS = 5_000;
    // Consecutive idle polls (~50ms) required; one is not enough as a worker taking a task is briefly not "active".
    private static final int REQUIRED_CONSECUTIVE_IDLE_POLLS = 5;
    private static final long PRE_TEST_IDLE_TIMEOUT_MS = 10_000;
    private static final int PRE_TEST_REQUIRED_IDLE_POLLS = 2;

    @Test
    public void testNoResubmitAfterBloomFilterTooLarge() throws Throwable
    {
        createTable("CREATE TABLE %s (k INT PRIMARY KEY, v INT) " +
                    "WITH compaction = {'class':'SizeTieredCompactionStrategy'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // 4 sstables, enough for STCS to produce a compaction task.
        for (int i = 0; i < 4; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        // Drain unrelated compactions (e.g. system tables) so only this table can raise the count.
        injectedFailureCount.set(0);
        assertTrue("Compaction executor still busy before the test armed the failure",
                   awaitCompactionExecutorIdle(PRE_TEST_IDLE_TIMEOUT_MS, PRE_TEST_REQUIRED_IDLE_POLLS));
        targetTable = currentTable();
        failureInjectionEnabled = true;
        try
        {
            try
            {
                cfs.enableAutoCompaction(true);
            }
            catch (Throwable expected)
            {
                // Waits for the first request and rethrows its failure, which is the injected one.
            }

            assertTrue("Rule never fired, no compaction attempt happened", injectedFailureCount.get() >= 1);

            // Run the wait first so that the failure count in the message is read after it, not before.
            boolean idle = awaitCompactionExecutorIdle(IDLE_TIMEOUT_MS, REQUIRED_CONSECUTIVE_IDLE_POLLS);
            assertTrue(String.format("Executor not idle within %d ms: slow environment or stuck task, not a retry loop. " +
                                     "injectedFailureCount=%d", IDLE_TIMEOUT_MS, injectedFailureCount.get()),
                       idle);

            assertEquals("BloomFilterTooLargeException must be thrown exactly once", 1, injectedFailureCount.get());
            assertEquals("Background compaction still marked as running",
                         0, CompactionManager.instance.getOngoingBackgroundCompactionsCount());
            assertTrue("Table still has sstables marked as compacting", cfs.getTracker().getCompacting().isEmpty());
        }
        finally
        {
            failureInjectionEnabled = false;
            targetTable = null;
            cfs.disableAutoCompaction(); // stops a runaway retry loop if one exists
        }
    }

    /** Fails at once if a retry is seen; returns false if the executor is not stably idle before the deadline. */
    private static boolean awaitCompactionExecutorIdle(long timeoutMs, int requiredConsecutiveIdlePolls) throws InterruptedException
    {
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        int consecutiveIdlePolls = 0;
        while (System.nanoTime() - deadlineNanos < 0)
        {
            int failures = injectedFailureCount.get();
            if (failures > 1)
                fail(String.format("RETRY DETECTED: BloomFilterTooLargeException thrown %d times", failures));

            consecutiveIdlePolls = CompactionManager.instance.hasOngoingOrPendingTasks() ? 0 : consecutiveIdlePolls + 1;
            if (consecutiveIdlePolls >= requiredConsecutiveIdlePolls)
                return true;
            Thread.sleep(POLL_INTERVAL_MS);
        }
        return false;
    }
}
