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

package org.apache.cassandra.db.compaction.differential;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ThreadStats;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Regression gate for cursor compaction's GARBAGE-FREE property: steady-state heap allocation
 * must not grow with the number of rows or cells compacted.
 *
 * The measurement takes thread-allocated bytes (ThreadStats) around CompactionTask.execute for
 * a SMALL table and a 10x BIG table of the same row shape. Prior iterations warm the path, and
 * the minimum over several measured iterations suppresses transient noise. The gate asserts
 * that the difference stays under a fixed ceiling.
 *
 * The ceiling is NOT zero. JFR decomposition of the measured baseline delta (~450KB at these
 * sizes) attributes all of it outside cursor-owned code:
 * - Ref$Debug stack captures, which scale with buffer-chunk Ref churn. The ant test JVM sets
 *   -Dcassandra.debugrefcount=true (build.xml); the production default is false.
 * - Chunk-cache machinery, which scales with data volume.
 * - Per-key metadata (bloom filter, index summary).
 * The cursor reader, writer and compactor hot loops contribute ZERO scaling allocation.
 * Kind.fromOrdinal() (CASSANDRA-21528) took ClusteringPrefix.Kind.values()'s per-call array
 * allocation off the cursor's hot read and write paths.
 *
 * Ceiling 512KB = measured 450KB + margin. Run-to-run variance of the min-of-3 measurement is
 * a few hundred bytes, so the gate trips at a regression of ~+60KB, about +6 bytes per row.
 * JMH gc.alloc.rate.norm stays the precision instrument; this gate is the always-on tripwire.
 *
 * The differential harness cannot catch an allocation regression, because the output bytes are
 * identical whether or not the path allocates. This gate covers that hole.
 */
public class CursorCompactionAllocationGateTest extends DifferentialCompactionTester
{
    private static final int SMALL_ROWS_PER_PARTITION = 100;
    private static final int SMALL_PARTITIONS = 6;
    private static final int SCALE = 10;
    private static final int WARMUP_ITERATIONS = 4;
    private static final int MEASURED_ITERATIONS = 3;
    private static final long CEILING_BYTES = 512 * 1024;

    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    /**
     * Disables preemptive open for the duration of {@code body}, so the gate sees a stable
     * sstable set, then restores it and cursorCompactionEnabled to their original values.
     * {@code body} sets cursorCompactionEnabled itself, because some callers measure both the
     * cursor path and the iterator path in one call.
     */
    private void withMeasurementEnv(ThrowingRunnable body) throws Exception
    {
        int originalPreemptiveOpen = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB();
        DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(-1);
        boolean originalCursorEnabled = DatabaseDescriptor.cursorCompactionEnabled();
        try
        {
            body.run();
        }
        finally
        {
            DatabaseDescriptor.setSSTablePreemptiveOpenIntervalInMiB(originalPreemptiveOpen);
            DatabaseDescriptor.setCursorCompactionEnabled(originalCursorEnabled);
        }
    }

    /** Runs warmup + measured compactions, returning the minimum allocated over the measured tail. */
    private long measureBest(ColumnFamilyStore cfs, long gcBefore, int warmup, int measured) throws Exception
    {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < warmup + measured; i++)
        {
            long allocated = compactOnceMeasured(cfs, gcBefore);
            if (i >= warmup)
                best = Math.min(best, allocated);
        }
        return best;
    }

    /** Records lastInputBytes as the total on-disk length of cfs's current live sstables. */
    private void captureLastInputBytes(ColumnFamilyStore cfs)
    {
        lastInputBytes = 0;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            lastInputBytes += sstable.onDiskLength();
    }

    private void dumpAllocationProfile(java.nio.file.Path dest, int iterations,
                                       ColumnFamilyStore cfs, long gcBefore) throws Exception
    {
        dumpAllocationProfile(dest, WARMUP_ITERATIONS, iterations, cfs, gcBefore);
    }

    /** Warms up, then records a JFR allocation profile with stacks over {@code iterations} cursor
     *  compactions of {@code cfs}, and writes it to {@code dest} for offline attribution. */
    private void dumpAllocationProfile(java.nio.file.Path dest, int warmup, int iterations,
                                       ColumnFamilyStore cfs, long gcBefore) throws Exception
    {
        for (int i = 0; i < warmup; i++)
            compactOnceMeasured(cfs, gcBefore);

        try (jdk.jfr.Recording recording = new jdk.jfr.Recording())
        {
            recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
            recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
            recording.start();
            for (int i = 0; i < iterations; i++)
                compactOnceMeasured(cfs, gcBefore);
            recording.stop();
            recording.dump(dest);
        }
    }

    @Test
    public void allocationDoesNotScaleWithRows() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            long smallAlloc = measureSteadyStateAllocation(SMALL_PARTITIONS, true);
            long bigAlloc = measureSteadyStateAllocation(SMALL_PARTITIONS * SCALE, true);
            long delta = bigAlloc - smallAlloc;

            // iterator-path numbers measured purely for context in the log: the iterator
            // allocates per row/cell BY DESIGN and is not gated
            long smallIter = measureSteadyStateAllocation(SMALL_PARTITIONS, false);
            long bigIter = measureSteadyStateAllocation(SMALL_PARTITIONS * SCALE, false);

            logger.info("cursor compaction allocation: small={}B big={}B delta={}B ceiling={}B " +
                        "(iterator path for context: small={}B big={}B delta={}B)",
                        smallAlloc, bigAlloc, delta, CEILING_BYTES,
                        smallIter, bigIter, bigIter - smallIter);
            assertTrue(String.format("cursor compaction allocation scales with data: " +
                                     "%,dB (small) -> %,dB (big), delta %,dB exceeds ceiling %,dB. " +
                                     "A per-row/cell allocation has been introduced on the cursor hot path.",
                                     smallAlloc, bigAlloc, delta, CEILING_BYTES),
                       delta <= CEILING_BYTES);
        });
    }

    private long measureSteadyStateAllocation(int partitions, boolean cursor) throws Exception
    {
        return measureSteadyStateAllocation(partitions, cursor, 2, "val", WARMUP_ITERATIONS, MEASURED_ITERATIONS);
    }

    private long measureSteadyStateAllocation(int partitions, boolean cursor,
                                              int rounds, String valuePadding, int warmup, int measured) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        for (int round = 0; round < rounds; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                    execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, valuePadding + ck);
            flush();
        }

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        // guard against vacuous measurement: the gate is meaningless if the cursor run
        // silently fell back to the iterator pipeline
        if (cursor)
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

        captureLastInputBytes(cfs);
        return measureBest(cfs, gcBefore, warmup, measured);
    }

    /** Total on-disk input bytes recorded by the most recent {@link #captureLastInputBytes} call. */
    private long lastInputBytes;

    /**
     * Measurement at realistic file sizes: 4 input sstables of ~10MB each, uncompressed, from
     * 192 partitions x 100 rows of ~520B, against a 10x-smaller run. The scaling assertion is
     * per input BYTE, not an absolute delta, because the residual here grows with the workload
     * instead of staying constant, so the small-file gate's fixed ceiling does not transfer.
     */
    @Test
    public void allocationAtLargeFileSizes() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        String padding = "v".repeat(500);
        withMeasurementEnv(() -> {
            // each round flushes one input sstable
            long smallAlloc = measureSteadyStateAllocation(19, true, 4, padding, 2, 2);
            long smallBytes = lastInputBytes;
            long bigAlloc = measureSteadyStateAllocation(192, true, 4, padding, 2, 2);
            long bigBytes = lastInputBytes;
            long delta = bigAlloc - smallAlloc;
            long extraBytes = bigBytes - smallBytes;
            double perInputByte = (double) delta / extraBytes;
            long smallIter = measureSteadyStateAllocation(19, false, 4, padding, 2, 2);
            long bigIter = measureSteadyStateAllocation(192, false, 4, padding, 2, 2);

            logger.info("LARGE-FILE cursor compaction allocation (4 files, ~10MB each big): " +
                        "cursor small={}B big={}B delta={}B over {}B extra input = {}B/B; " +
                        "iterator small={}B big={}B delta={}B",
                        smallAlloc, bigAlloc, delta, extraBytes, String.format("%.3f", perInputByte),
                        smallIter, bigIter, bigIter - smallIter);
            // The residual scales with data VOLUME, not row count. JFR decomposition at this
            // scale: 62% Ref$Debug stack captures (test env only,
            // -Dcassandra.debugrefcount=true), then chunk-cache machinery and per-compaction
            // constants. ZERO cursor-owned sites. Measured ~0.27 B allocated per extra input
            // byte in the test env. Ceiling 0.5 B/B trips on any real per-element regression
            // and absorbs the volume-proportional test-env noise.
            assertTrue(String.format("cursor allocation per input byte too high: %.3f B/B (delta %,dB over %,dB)",
                                     perInputByte, delta, extraBytes),
                       perInputByte <= 0.5);
        });
    }

    /** Compacts all live sstables on the configured path, measuring ONLY execute(); restores inputs. */
    private long compactOnceMeasured(ColumnFamilyStore cfs, long gcBefore) throws Exception
    {
        Set<SSTableReader> inputs = new HashSet<>(cfs.getLiveSSTables());
        Set<Descriptor> liveBeforeDescs = new HashSet<>();
        List<Descriptor> inputDescriptors = new ArrayList<>();
        for (SSTableReader in : inputs)
        {
            liveBeforeDescs.add(in.descriptor);
            inputDescriptors.add(in.descriptor);
        }

        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting", txn);
        CompactionTask task = new CompactionTask(cfs, txn, gcBefore, true /* keepOriginals */);

        long before = ThreadStats.getCurrentThreadAllocatedBytes();
        task.execute(ActiveCompactionsTracker.NOOP);
        long after = ThreadStats.getCurrentThreadAllocatedBytes();
        // The JVM returns -1 while thread allocation measurement is DISABLED, and
        // isThreadAllocatedMemorySupported(), the condition every Assume in this class checks,
        // still reports supported. Two -1 readings subtract to a delta of 0, and 0 satisfies
        // every scaling assertion here, so the gate would report green having measured nothing.
        assertTrue("thread allocation measurement returned no reading (before=" + before +
                   " after=" + after + "); the allocation gate cannot measure and must not report a pass",
                   before >= 0 && after >= 0);
        long allocated = after - before;

        List<SSTableReader> retainedInputClones = new ArrayList<>();
        List<SSTableReader> outputs = identifyOutputs(cfs, liveBeforeDescs, liveBeforeDescs, retainedInputClones);
        restoreAfterCompaction(cfs, outputs, retainedInputClones, inputDescriptors, inputs.size());
        return allocated;
    }

    /**
     * Sparse rows: every other row omits a column, so the row carries a column-subset encoding
     * instead of the all-columns flag. This reaches the per-row subset path in
     * SSTableCursorReader (UnfilteredDescriptor.loadRow -> Columns.deserializeSubset ->
     * CellCursor.init identity-cache miss), which the full-row scenario cannot reach.
     */
    @Test
    public void allocationDoesNotScaleWithSparseRows() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            long smallAlloc = measureSparse(SMALL_PARTITIONS);
            long bigAlloc = measureSparse(SMALL_PARTITIONS * SCALE);
            long delta = bigAlloc - smallAlloc;
            logger.info("sparse-row cursor compaction allocation: small={}B big={}B delta={}B ceiling={}B",
                        smallAlloc, bigAlloc, delta, CEILING_BYTES);
            assertTrue(String.format("sparse-row cursor compaction allocation scales with data: " +
                                     "%,dB -> %,dB, delta %,dB exceeds ceiling %,dB",
                                     smallAlloc, bigAlloc, delta, CEILING_BYTES),
                       delta <= CEILING_BYTES);
        });
    }

    private long measureSparse(int partitions) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                {
                    if (ck % 2 == 0)
                        execute("INSERT INTO %s (pk, ck, v1) VALUES (?, ?, ?)", pk, ck, ck);
                    else
                        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "val" + ck);
                }
            flush();
        }
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
        return measureBest(cfs, gcBefore, WARMUP_ITERATIONS, MEASURED_ITERATIONS);
    }

    /**
     * Sparse rows in a >= 64-column superset. The column subset then uses the LARGE-subset wire
     * format, which encodes index vints in present-mode or missing-mode. This gate guards a
     * regression that decode once carried: a fresh Columns materialized per row, cascading
     * through CellCursor.init's identity cache into a per-row toArray, an AbstractType[]
     * rebuild, and O(columns) getType lookups.
     *
     * The small-superset gate, allocationDoesNotScaleWithSparseRows, cannot reach this format:
     * its mask fast path only covers fewer than 64 columns.
     *
     * The schema declares 70 columns, but only 69 ever carry a cell. The present-mode window's
     * base is always even, so c69 is never written, and the sstable header's superset, the union
     * of the columns actually written, is 69. That stays over the 64-column boundary. Rows
     * alternate present-mode (3 of 69 set) and missing-mode (67 of 69 set), so both wire modes
     * run.
     */
    @Test
    public void allocationDoesNotScaleWithWideSchemaSparseRows() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            long smallAlloc = measureWideSparse(SMALL_PARTITIONS);
            long smallBytes = lastInputBytes;
            long bigAlloc = measureWideSparse(SMALL_PARTITIONS * SCALE);
            long bigBytes = lastInputBytes;
            long delta = bigAlloc - smallAlloc;
            long extraBytes = bigBytes - smallBytes;
            double perInputByte = (double) delta / extraBytes;
            logger.info("wide-schema sparse-row cursor compaction allocation: small={}B big={}B delta={}B " +
                        "over {}B extra input = {} B/B",
                        smallAlloc, bigAlloc, delta, extraBytes, String.format("%.3f", perInputByte));
            // Calibrated per INPUT BYTE: the mixed 3-of-69 and 67-of-69 rows make multi-MB
            // inputs whose volume-proportional test-env residual (Ref$Debug, chunk cache)
            // dwarfs any fixed ceiling. Measured ~0.37 B/B on the BIG run. The per-row Columns
            // cascade this gate guards measured ~3.8 B/B. One small object leaked per row costs
            // about +0.2 B/B at this row size, lands at ~0.57 B/B, and still passes. The gate
            // catches a whole-pipeline regression, not a single re-introduced per-row object.
            assertTrue(String.format("wide-schema (>=64 col) sparse-row cursor allocation per input byte too high: " +
                                     "%.3f B/B (delta %,dB over %,dB extra input)",
                                     perInputByte, delta, extraBytes),
                       perInputByte <= 0.6);
        });
    }

    private long measureWideSparse(int partitions) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        int cols = 70;
        StringBuilder schema = new StringBuilder("CREATE TABLE %s (pk bigint, ck bigint");
        for (int c = 0; c < cols; c++)
            schema.append(", c").append(c).append(" bigint");
        schema.append(", PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        createTable(schema.toString());
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        // missing-mode insert: 67 of the 69-column superset (2 missing -> missing-index encoding)
        StringBuilder wide = new StringBuilder("INSERT INTO %s (pk, ck");
        StringBuilder marks = new StringBuilder("?, ?");
        for (int c = 0; c < cols - 3; c++)
        {
            wide.append(", c").append(c);
            marks.append(", ?");
        }
        String wideInsert = wide.append(") VALUES (").append(marks).append(")").toString();
        Object[] wideArgs = new Object[2 + cols - 3];
        for (int c = 0; c < cols - 3; c++)
            wideArgs[2 + c] = (long) c;

        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                {
                    if (ck % 2 == 0)
                    {
                        // present-mode: rotating 3-column window
                        int base = (int) ((ck * 3) % (cols - 2));
                        execute("INSERT INTO %s (pk, ck, c" + base + ", c" + (base + 1) + ", c" + (base + 2) +
                                ") VALUES (?, ?, ?, ?, ?)", pk, ck, ck, ck + 1, ck + 2);
                    }
                    else
                    {
                        wideArgs[0] = pk;
                        wideArgs[1] = ck;
                        execute(wideInsert, wideArgs);
                    }
                }
            flush();
        }
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
        captureLastInputBytes(cfs);
        return measureBest(cfs, gcBefore, WARMUP_ITERATIONS, MEASURED_ITERATIONS);
    }

    /**
     * Garbage-free property for RANGE-TOMBSTONE-dense workloads. The marker read, merge and
     * write path runs on the ReusableDeletionTime pool and on open-marker tracking, which the
     * row-centric gates barely touch. Each partition carries 300 bounded range tombstones per
     * round, and the second round shifts its bounds by one, so every marker pair overlaps
     * across sstables and forces real deletion reconciliation.
     *
     * Asserted per INPUT BYTE, not as a fixed delta: at marker-dense (sub-MB) scales the
     * test-env residual, Ref$Debug stack captures and chunk-cache machinery, exceeds any fixed
     * ceiling. JFR attribution of recordRangeTombstoneAllocationProfile finds ZERO cursor-owned
     * sites; the profile is Ref$Debug, buffer pool and per-compaction constants. Markers are
     * ~25-35B on disk, so one small object leaked per marker costs >1.5 B/B and trips the
     * 1.0 B/B ceiling with wide margin.
     */
    @Test
    public void allocationDoesNotScaleWithRangeTombstones() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            long smallAlloc = measureRangeTombstones(12);
            long smallBytes = lastInputBytes;
            long bigAlloc = measureRangeTombstones(96);
            long bigBytes = lastInputBytes;
            long delta = bigAlloc - smallAlloc;
            long extraBytes = bigBytes - smallBytes;
            double perInputByte = (double) delta / extraBytes;
            logger.info("RT-dense cursor compaction allocation: small={}B big={}B delta={}B " +
                        "over {}B extra input = {} B/B (ceiling {} B/B)",
                        smallAlloc, bigAlloc, delta, extraBytes,
                        String.format("%.3f", perInputByte), rtPerInputByteCeiling());
            assertTrue(String.format("RT-dense cursor compaction allocation scales with markers: " +
                                     "%,dB -> %,dB, delta %,dB over %,dB extra input = %.3f B/B exceeds " +
                                     "ceiling %.2f B/B. A per-marker allocation has been introduced on " +
                                     "the cursor hot path.",
                                     smallAlloc, bigAlloc, delta, extraBytes, perInputByte,
                                     rtPerInputByteCeiling()),
                       perInputByte <= rtPerInputByteCeiling());
        });
    }

    /** Calibrated from measured 0.684 B/B — all test-env residual by JFR attribution
     *  (Ref$Debug, buffer pool; zero cursor frames). */
    protected double rtPerInputByteCeiling()
    {
        return 1.0;
    }

    private long measureRangeTombstones(int partitions) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
            {
                // a few surviving rows well outside the tombstoned ck range
                for (long r = 0; r < 5; r++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 100_000L + r, "v" + r);
                // 300 bounded RTs; round 1 shifts bounds by 1 so markers overlap across rounds
                for (long t = 0; t < 300; t++)
                    execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?",
                            pk, t * 4 + round, t * 4 + round + 2);
            }
            flush();
        }
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
        captureLastInputBytes(cfs);
        return measureBest(cfs, gcBefore, WARMUP_ITERATIONS, MEASURED_ITERATIONS);
    }

    /** Ceiling 0.5 B/B, the calibration {@link #allocationAtLargeFileSizes} uses at this input
     *  scale. BTI needs a higher one: its trie and key snapshot cost ~2KB per partition. */
    protected double complexPerInputByteCeiling()
    {
        return 0.5;
    }

    /**
     * Garbage-free property for multi-cell (complex) columns: allocation must not grow per row.
     *
     * The whole cursor path runs — the read, the merge of each column and each cell path, and
     * the write of the markers and the row. Every row holds a map and a set, and the INSERT of
     * a collection literal adds a complex deletion. Several rounds of writes give the merge
     * real work to do on cell paths.
     */
    @Test
    public void allocationDoesNotScaleWithComplexColumns() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            long smallAlloc = measureComplex(19);
            long smallBytes = lastInputBytes;
            long bigAlloc = measureComplex(192);
            long bigBytes = lastInputBytes;
            long delta = bigAlloc - smallAlloc;
            long extraBytes = bigBytes - smallBytes;
            double perInputByte = (double) delta / extraBytes;
            logger.info("complex-column cursor compaction allocation: small={}B big={}B delta={}B " +
                        "over {}B extra input = {} B/B",
                        smallAlloc, bigAlloc, delta, extraBytes, String.format("%.3f", perInputByte));
            // Same input scale as allocationAtLargeFileSizes, several megabytes, so the same
            // ceiling applies. Below one megabyte the test environment's own allocation is above
            // 1 B/B for every scenario, the simple ones included.
            //
            // A JFR profile of 30 warmed complex compactions (recordComplexAllocationProfile)
            // shows no cursor method allocating in proportion to the input. Its only fixed cost
            // is the histogram spool in maybeSwitchWriter, once per compaction, which the delta
            // above subtracts out.
            assertTrue(String.format("complex-column cursor allocation per input byte too high: " +
                                     "%.3f B/B (delta %,dB over %,dB extra input, ceiling %.2f)",
                                     perInputByte, delta, extraBytes, complexPerInputByteCeiling()),
                       perInputByte <= complexPerInputByteCeiling());
        });
    }

    /**
     * Creates the table for the complex-column tests and fills it.
     *
     * It writes {@code rounds} rounds of rows that hold a map, a set and a text column, and
     * flushes after each round. Every fourth row also takes a single-element map update, so the
     * merge sees rows whose map holds a path the collection literal did not write.
     * {@code valuePrefix} sets the row width.
     */
    private ColumnFamilyStore populateComplexTable(int partitions, int rounds, String valuePrefix) throws Exception
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, m map<text, bigint>, s set<int>, v text, " +
                    "PRIMARY KEY (pk, ck)) WITH compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int round = 0; round < rounds; round++)
        {
            for (long pk = 0; pk < partitions; pk++)
                for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                {
                    execute("INSERT INTO %s (pk, ck, m, s, v) VALUES (?, ?, ?, ?, ?)",
                            pk, ck, map("k" + ck, ck, "r" + round, (long) round), set((int) ck, round), valuePrefix + ck);
                    if (ck % 4 == 0)
                        execute("UPDATE %s SET m[?] = ? WHERE pk = ? AND ck = ?", "extra", ck, pk, ck);
                }
            flush();
        }
        return cfs;
    }

    private long measureComplex(int partitions) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(true);
        // multi-MB inputs, the scale complexPerInputByteCeiling is calibrated at
        ColumnFamilyStore cfs = populateComplexTable(partitions, 4, "x".repeat(180));
        captureLastInputBytes(cfs);
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
        return measureBest(cfs, gcBefore, 2, 2);
    }

    /**
     * Diagnostic, not a gate: records JFR allocation events (with stacks) over many warmed
     * cursor compactions of the big table and dumps to /tmp/cursor-alloc.jfr for offline
     * attribution of the scaling allocation (jfr print + aggregation). Always passes.
     */
    @Test
    public void recordAllocationProfile() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = SMALL_PARTITIONS * SCALE;
            for (int round = 0; round < 2; round++)
            {
                for (long pk = 0; pk < partitions; pk++)
                    for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, "val" + ck);
                flush();
            }
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

            dumpAllocationProfile(java.nio.file.Path.of("/tmp/cursor-alloc.jfr"), 30, cfs, gcBefore);
            logger.info("allocation profile dumped to /tmp/cursor-alloc.jfr");
        });
    }

    /** Diagnostic, not a gate: JFR allocation profile over warmed cursor compactions of the
     *  big RANGE-TOMBSTONE-dense table; dumps /tmp/cursor-alloc-rt.jfr for attribution. */
    @Test
    public void recordRangeTombstoneAllocationProfile() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'} AND gc_grace_seconds = 864000");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            int partitions = SMALL_PARTITIONS * SCALE;
            for (int round = 0; round < 2; round++)
            {
                for (long pk = 0; pk < partitions; pk++)
                {
                    for (long r = 0; r < 5; r++)
                        execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 100_000L + r, "v" + r);
                    for (long t = 0; t < 150; t++)
                        execute("DELETE FROM %s WHERE pk = ? AND ck >= ? AND ck < ?",
                                pk, t * 4 + round, t * 4 + round + 2);
                }
                flush();
            }
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

            dumpAllocationProfile(java.nio.file.Path.of("/tmp/cursor-alloc-rt.jfr"), 30, cfs, gcBefore);
            logger.info("allocation profile dumped to /tmp/cursor-alloc-rt.jfr");
        });
    }

    /** Diagnostic, not a gate: JFR allocation profile over warmed cursor compactions of the big
     *  multi-cell table; dumps /tmp/cursor-alloc-complex.jfr for attribution. */
    @Test
    public void recordComplexAllocationProfile() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());

        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            ColumnFamilyStore cfs = populateComplexTable(SMALL_PARTITIONS * SCALE, 2, "v");
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

            dumpAllocationProfile(java.nio.file.Path.of("/tmp/cursor-alloc-complex.jfr"), 30, cfs, gcBefore);
            logger.info("allocation profile dumped to /tmp/cursor-alloc-complex.jfr");
        });
    }

    /** Diagnostic: JFR profile at large file sizes; dumps /tmp/cursor-alloc-large.jfr. */
    @Test
    public void recordLargeFileAllocationProfile() throws Exception
    {
        Assume.assumeTrue("thread allocation measurement unsupported on this JVM",
                          ThreadStats.isThreadAllocatedMemorySupported());
        String padding = "v".repeat(500);
        withMeasurementEnv(() -> {
            DatabaseDescriptor.setCursorCompactionEnabled(true);
            createTable("CREATE TABLE %s (pk bigint, ck bigint, v1 bigint, v2 text, PRIMARY KEY (pk, ck)) " +
                        "WITH compression = {'enabled': 'false'}");
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            cfs.disableAutoCompaction();
            for (int round = 0; round < 4; round++)
            {
                for (long pk = 0; pk < 192; pk++)
                    for (long ck = 0; ck < SMALL_ROWS_PER_PARTITION; ck++)
                        execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ck, ck, padding + ck);
                flush();
            }
            long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

            dumpAllocationProfile(java.nio.file.Path.of("/tmp/cursor-alloc-large.jfr"), 2, 8, cfs, gcBefore);
        });
    }
}
