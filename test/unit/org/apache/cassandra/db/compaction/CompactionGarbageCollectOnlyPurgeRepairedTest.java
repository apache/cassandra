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
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.awaitility.Awaitility;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.utils.concurrent.Ref;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for garbage collection with only_purge_repaired_tombstones enabled.
 *
 * Specifically tests the fix for the ConcurrentModificationException that occurred
 * when calling nodetool garbagecollect on a table with mixed repaired/unrepaired
 * SSTables and only_purge_repaired_tombstones=true.
 *
 * The bug was in CompactionManager.performGarbageCollection() where the code
 * iterated directly over transaction.originals() while calling transaction.cancel()
 * inside the loop, causing the underlying collection to be modified during iteration.
 */
public class CompactionGarbageCollectOnlyPurgeRepairedTest extends CQLTester
{
    /**
     * Tests that garbage collection completes
     * when there are mixed repaired and unrepaired SSTables with only_purge_repaired_tombstones=true
     *
     * Before the fix, this would fail with:
     * java.util.ConcurrentModificationException
     *     at java.util.HashMap$HashIterator.nextNode(HashMap.java:1597)
     *     at org.apache.cassandra.db.compaction.CompactionManager$6.filterSSTables(CompactionManager.java:691)
     */
    @Test
    public void testOnlyPurgeRepaired() throws Throwable
    {
        // Create table with UnifiedCompactionStrategy and only_purge_repaired_tombstones=true
        createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                    "WITH gc_grace_seconds=0 " +
                    "AND compaction = {'class':'UnifiedCompactionStrategy', 'only_purge_repaired_tombstones':true}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Create multiple SSTables
        for (int i = 0; i < 5; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "data" + i);
            Util.flush(cfs);
        }

        Set<SSTableReader> sstables = cfs.getLiveSSTables();
        assertEquals("Should have 5 SSTables", 5, sstables.size());

        // Mark some SSTables as repaired (alternate pattern to ensure mix)
        int count = 0;
        for (SSTableReader sstable : sstables)
        {
            if (count % 2 == 0)
            {
                repair(cfs, sstable);
            }
            count++;
        }

        // Verify we have a mix of repaired and unrepaired
        long repairedCount = cfs.getLiveSSTables().stream().filter(SSTableReader::isRepaired).count();
        long unrepairedCount = cfs.getLiveSSTables().stream().filter(s -> !s.isRepaired()).count();
        assertTrue("Should have at least 1 repaired SSTable", repairedCount >= 1);
        assertTrue("Should have at least 1 unrepaired SSTable", unrepairedCount >= 1);

        // This should NOT throw ConcurrentModificationException
        // Before the fix, this would fail when iterating over originals() while cancel() modifies it
        try
        {
            CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.CELL, 1);
        }
        catch (ConcurrentModificationException e)
        {
            fail("Garbage collection should not throw ConcurrentModificationException. " +
                 "This indicates the bug in filterSSTables() where transaction.originals() is " +
                 "iterated while transaction.cancel() modifies the underlying collection.");
        }
    }

    /**
     * Tests that garbage collection works when ALL SSTables are unrepaired.
     * In this case, all SSTables should be cancelled (not processed for GC)
     * since only_purge_repaired_tombstones is true.
     */
    @Test
    public void testGarbageCollectAllUnrepaired() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                    "WITH gc_grace_seconds=0 " +
                    "AND compaction = {'class':'UnifiedCompactionStrategy', 'only_purge_repaired_tombstones':true}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Create SSTables (all unrepaired by default)
        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "data" + i);
            Util.flush(cfs);
        }

        // Verify all are unrepaired
        long unrepairedCount = cfs.getLiveSSTables().stream().filter(s -> !s.isRepaired()).count();
        assertEquals("All 3 SSTables should be unrepaired", 3, unrepairedCount);

        // Should complete without error - all SSTables will be cancelled
        CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.CELL, 1);
    }

    /**
     * Tests that garbage collection works when ALL SSTables are repaired.
     * In this case, no cancellation happens, so no risk of ConcurrentModificationException.
     */
    @Test
    public void testAllRepaired() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                    "WITH gc_grace_seconds=0 " +
                    "AND compaction = {'class':'UnifiedCompactionStrategy', 'only_purge_repaired_tombstones':true}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Create and repair all SSTables
        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "data" + i);
            Util.flush(cfs);
        }

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            repair(cfs, sstable);
        }

        // Verify all are repaired
        long repairedCount = cfs.getLiveSSTables().stream().filter(SSTableReader::isRepaired).count();
        assertEquals("All 3 SSTables should be repaired", 3, repairedCount);

        // Should complete without error
        CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.CELL, 1);
    }

    /**
     * Tests garbage collection without only_purge_repaired_tombstones (baseline test).
     * This code path doesn't involve the problematic iteration with cancel().
     */
    @Test
    public void testWithoutOnlyPurgeRepaired() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                    "WITH gc_grace_seconds=0 " +
                    "AND compaction = {'class':'UnifiedCompactionStrategy'}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        execute("INSERT INTO %s (id, data) VALUES (1, 'data1')");
        Util.flush(cfs);

        // Should complete without error
        CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.CELL, 1);
    }

    /**
     * Tests with SizeTieredCompactionStrategy to ensure the fix doesn't break
     * the original behavior reported in CASSANDRA-14204.
     */
    @Test
    public void testSTCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                    "WITH gc_grace_seconds=0 " +
                    "AND compaction = {'class':'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones':true}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Create multiple SSTables
        for (int i = 0; i < 4; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "data" + i);
            Util.flush(cfs);
        }

        // Mark half as repaired
        Iterator<SSTableReader> iter = cfs.getLiveSSTables().iterator();
        if (iter.hasNext()) repair(cfs, iter.next());
        if (iter.hasNext()) repair(cfs, iter.next());

        // Should complete without ConcurrentModificationException
        CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.CELL, 1);
    }

    /**
     * Tests with LeveledCompactionStrategy to ensure the fix works across
     * all compaction strategies.
     */
    @Test
    public void testLCS() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                    "WITH gc_grace_seconds=0 " +
                    "AND compaction = {'class':'LeveledCompactionStrategy', 'only_purge_repaired_tombstones':true}");

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // Create multiple SSTables
        for (int i = 0; i < 4; i++)
        {
            execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "data" + i);
            Util.flush(cfs);
        }

        // Mark half as repaired
        Iterator<SSTableReader> iter = cfs.getLiveSSTables().iterator();
        if (iter.hasNext()) repair(cfs, iter.next());
        if (iter.hasNext()) repair(cfs, iter.next());

        // Should complete without ConcurrentModificationException
        CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.CELL, 1);
    }

    /**
     * Tests that reference leaks are detected when ConcurrentModificationException
     * prevents proper transaction cleanup.
     *
     * This test uses Cassandra's Ref.OnLeak callback to detect when SSTable references
     * are not properly released. The bug in performGarbageCollection() causes a
     * ConcurrentModificationException which prevents the transaction from completing
     * properly, leaving SSTable references unreleased (leaked).
     *
     * The leak would show in logs as:
     * ERROR [Reference-Reaper] - LEAK DETECTED: a reference to ...
     */
    @Test
    public void testLeakDetectionWithMixedRepairedUnrepaired() throws Throwable
    {
        // Track detected leaks
        CopyOnWriteArrayList<Object> detectedLeaks = new CopyOnWriteArrayList<>();

        try
        {
            // Install leak detector callback
            Ref.setOnLeak(state -> {
                detectedLeaks.add(state);
                LOGGER.error("LEAK DETECTED in test: " + state);
            });

            createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                        "WITH gc_grace_seconds=0 " +
                        "AND compaction = {'class':'UnifiedCompactionStrategy', 'only_purge_repaired_tombstones':true}");

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            // Create multiple SSTables
            for (int i = 0; i < 5; i++)
            {
                execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "data" + i);
                Util.flush(cfs);
            }

            // Mark some as repaired to create mixed state
            int count = 0;
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                if (count % 2 == 0)
                {
                    repair(cfs, sstable);
                }
                count++;
            }

            // Attempt garbage collection - this may throw ConcurrentModificationException
            // if the bug is present, which would prevent proper reference cleanup
            boolean exceptionThrown = false;
            try
            {
                CompactionManager.instance.performGarbageCollection(cfs, CompactionParams.TombstoneOption.CELL, 1);
            }
            catch (ConcurrentModificationException e)
            {
                exceptionThrown = true;
                LOGGER.error("ConcurrentModificationException caught (bug is present): " + e.getMessage());
            }

            // Force garbage collection to trigger leak detection
            // The Ref cleanup happens via PhantomReference when objects are GC'd
            for (int i = 0; i < 5; i++)
            {
                System.gc();
                Thread.sleep(100);
            }

            // Give the Reference-Reaper thread time to process
            Thread.sleep(500);

            // If exception was thrown, we expect leaks (bug is present)
            // If no exception, we should have no leaks (fix is working)
            if (exceptionThrown)
            {
                // Bug is present - we may see leaks due to improper cleanup
                LOGGER.error("Bug detected: ConcurrentModificationException was thrown.");
                LOGGER.error("Leaks detected: " + detectedLeaks.size());
                fail("ConcurrentModificationException was thrown, indicating the bug is present. " +
                     "This can cause reference leaks.");
            }
            else
            {
                // Fix is working - verify no leaks were detected
                assertTrue("No leaks should be detected when garbage collection completes successfully. " +
                           "Detected leaks: " + detectedLeaks,
                           detectedLeaks.isEmpty());
            }
        }
        finally
        {
            // Clear leak handler
            Ref.setOnLeak(null);
        }
    }

    /**
     * Tests that reference leaks are reliably detected using Awaitility to wait
     * for the asynchronous leak detection mechanism.
     *
     * This test directly simulates what happens when a LifecycleTransaction is not
     * properly closed - references to SSTables are acquired but never released,
     * causing the Reference-Reaper to detect and report the leak.
     *
     * The leak detection is asynchronous because it relies on:
     * 1. JVM garbage collection to collect unreleased Ref objects
     * 2. PhantomReference processing by the Reference-Reaper thread
     * 3. The OnLeak callback being invoked
     *
     * Uses Awaitility to poll and wait for leak detection with proper timeouts.
     */
    @Test
    public void testLeakDetectionWithAwaitility() throws Throwable
    {
        AtomicBoolean leakDetected = new AtomicBoolean(false);
        CopyOnWriteArrayList<String> leakDetails = new CopyOnWriteArrayList<>();

        try
        {
            // Install leak detector callback
            Ref.setOnLeak(state -> {
                leakDetected.set(true);
                String detail = "LEAK DETECTED: " + state;
                leakDetails.add(detail);
                LOGGER.error(detail);
            });

            createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                        "WITH gc_grace_seconds=0 " +
                        "AND compaction = {'class':'UnifiedCompactionStrategy', 'only_purge_repaired_tombstones':true}");

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            // Create SSTables
            for (int i = 0; i < 3; i++)
            {
                execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "data" + i);
                Util.flush(cfs);
            }

            // Simulate what happens when ConcurrentModificationException prevents cleanup:
            // Acquire references to SSTables but deliberately don't release them
            // This simulates the leak that occurs when transaction.close() is not called
            simulateLeakedReferences(cfs);

            // Wait for leak detection with Awaitility
            Awaitility.await()
                      .atMost(Duration.ofSeconds(30))
                      .pollDelay(Duration.ofSeconds(1))
                      .pollInterval(Duration.ofSeconds(1))
                      .untilAsserted(() -> {
                          // Force GC to trigger leak detection
                          System.gc();
                          System.gc();
                          System.gc();
                          assertThat(leakDetected.get())
                              .as("Leak should be detected for unreleased SSTable references")
                              .isTrue();
                      });

            // Report findings
            LOGGER.error("Leak detection confirmed!");
            LOGGER.error("Leaks detected: " + leakDetails.size());
            for (String detail : leakDetails)
            {
                LOGGER.error("  - " + detail);
            }

            assertThat(leakDetails.size())
                .as("At least one leak should be detected")
                .isGreaterThan(0);
        }
        finally
        {
            // Clear leak handler
            Ref.setOnLeak(null);
        }
    }

    /**
     * Tests that NO reference leaks occur when references are properly managed.
     * This is the counterpart to testLeakDetectionWithAwaitility - it demonstrates
     * that when code properly releases references (like the fix does), no leaks are detected.
     *
     * The fix pattern is: acquire reference -> do work -> release reference
     * This mimics the correct behavior in performGarbageCollection() after the fix.
     */
    @Test
    public void testNoLeakDetectionWithAwaitility() throws Throwable
    {
        AtomicBoolean leakDetected = new AtomicBoolean(false);
        CopyOnWriteArrayList<String> leakDetails = new CopyOnWriteArrayList<>();

        try
        {
            // Install leak detector callback
            Ref.setOnLeak(state -> {
                leakDetected.set(true);
                String detail = "LEAK DETECTED: " + state;
                leakDetails.add(detail);
                LOGGER.error(detail);
            });

            createTable("CREATE TABLE %s (id int PRIMARY KEY, data text) " +
                        "WITH gc_grace_seconds=0 " +
                        "AND compaction = {'class':'UnifiedCompactionStrategy', 'only_purge_repaired_tombstones':true}");

            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();

            // Create SSTables
            for (int i = 0; i < 3; i++)
            {
                execute("INSERT INTO %s (id, data) VALUES (?, ?)", i, "data" + i);
                Util.flush(cfs);
            }

            // Simulate proper reference handling (like the fixed code does):
            // Acquire references AND release them properly
            simulateNoLeakedReferences(cfs);

            // Wait a reasonable time to confirm NO leaks are detected
            // We poll for a bit to give the Reference-Reaper time to process any potential leaks
            Awaitility.await()
                      .atMost(Duration.ofSeconds(10))
                      .pollDelay(Duration.ofSeconds(1))
                      .pollInterval(Duration.ofSeconds(1))
                      .untilAsserted(() -> {
                          // Force GC to give leak detection a chance to run
                          System.gc();
                          System.gc();
                          System.gc();
                          // With proper reference management, no leaks should be detected
                          assertThat(leakDetected.get())
                              .as("No leak should be detected when references are properly released")
                              .isFalse();
                      });

            // Final confirmation
            LOGGER.error("No leak detection confirmed - references were properly managed!");
            LOGGER.error("Leaks detected: " + leakDetails.size());

            assertThat(leakDetails.size())
                .as("No leaks should be detected when references are properly released")
                .isEqualTo(0);
        }
        finally
        {
            // Clear leak handler
            Ref.setOnLeak(null);
        }
    }

    /**
     * Simulates leaked references by acquiring SSTable refs without releasing them.
     * This is done in a separate method so local variables go out of scope and become GC-eligible.
     */
    @SuppressWarnings("unused")
    private void simulateLeakedReferences(ColumnFamilyStore cfs)
    {
        // Acquire references to SSTables - these will be "leaked" when we don't release them
        // This simulates what happens when ConcurrentModificationException prevents proper cleanup
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            // Acquire a reference but deliberately don't release it
            // This is equivalent to what happens when a transaction is interrupted mid-operation
            Ref<SSTableReader> leakedRef = sstable.tryRef();
            // leakedRef goes out of scope without release() being called - THIS IS THE LEAK
            // When leakedRef is garbage collected, the Reference-Reaper will detect the leak
        }
    }

    /**
     * Simulates proper reference handling: acquire refs AND release them properly.
     * This is the counterpart to simulateLeakedReferences which acquires but doesn't release.
     *
     * This demonstrates that when references are properly managed (acquired and released),
     * no leaks are detected by the Reference-Reaper.
     */
    private void simulateNoLeakedReferences(ColumnFamilyStore cfs)
    {
        // Simulate proper reference handling: acquire refs AND release them properly
        // This is the counterpart to simulateLeakedReferences which acquires but doesn't release
        for (SSTableReader sstable : new ArrayList<>(cfs.getLiveSSTables()))
        {
            Ref<SSTableReader> ref = sstable.tryRef();
            if (ref != null)
                ref.release();
        }
    }

    /**
     * Helper method to mark an SSTable as repaired.
     * Follows the pattern from RepairedDataTombstonesTest.
     */
    private static void repair(ColumnFamilyStore cfs, SSTableReader sstable) throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor, System.currentTimeMillis(), null, false);
        sstable.reloadSSTableMetadata();
        cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
    }
}
