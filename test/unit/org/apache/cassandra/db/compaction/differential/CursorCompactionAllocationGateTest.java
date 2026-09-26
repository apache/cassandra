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

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ThreadStats;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Regression gate: cursor compaction's steady-state heap allocation must not grow with the
 *  number of rows or cells compacted. */
public class CursorCompactionAllocationGateTest extends DifferentialCompactionTester
{
    private static final int SMALL_ROWS_PER_PARTITION = 100;
    private static final int SMALL_PARTITIONS = 6;
    private static final int SCALE = 10;
    private static final int WARMUP_ITERATIONS = 4;
    private static final int MEASURED_ITERATIONS = 3;
    private static final long CEILING_BYTES = 512 * 1024;

    private SSTableFormat<?, ?> originalFormat;

    /** The sstable format under test. */
    protected String formatName()
    {
        return "big";
    }

    @Before
    public void selectFormat()
    {
        originalFormat = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(formatName());
    }

    @After
    public void restoreFormat()
    {
        DatabaseDescriptor.setSelectedSSTableFormat(originalFormat);
    }

    /** The allocation ceiling for this format. */
    protected long ceilingBytes()
    {
        return CEILING_BYTES;
    }

    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    /** Runs {@code body} with preemptive open disabled, then restores it and cursorCompactionEnabled. */
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

    /** Warms up, then records a JFR allocation profile over {@code iterations} cursor compactions
     *  of {@code cfs} to {@code dest}. */
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

            // iterator path numbers logged for context only; not gated
            long smallIter = measureSteadyStateAllocation(SMALL_PARTITIONS, false);
            long bigIter = measureSteadyStateAllocation(SMALL_PARTITIONS * SCALE, false);

            logger.info("cursor compaction allocation: small={}B big={}B delta={}B ceiling={}B " +
                        "(iterator path for context: small={}B big={}B delta={}B)",
                        smallAlloc, bigAlloc, delta, ceilingBytes(),
                        smallIter, bigIter, bigIter - smallIter);
            assertTrue(String.format("cursor compaction allocation scales with data: " +
                                     "%,dB (small) -> %,dB (big), delta %,dB exceeds ceiling %,dB. " +
                                     "A per-row/cell allocation has been introduced on the cursor hot path.",
                                     smallAlloc, bigAlloc, delta, ceilingBytes()),
                       delta <= ceilingBytes());
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
        // fail if the cursor run silently fell back to the iterator path
        if (cursor)
            assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);

        captureLastInputBytes(cfs);
        return measureBest(cfs, gcBefore, warmup, measured);
    }

    /** Total on-disk input bytes recorded by the most recent {@link #captureLastInputBytes} call. */
    private long lastInputBytes;

    /** Allocation must not scale with data at realistic file sizes; asserted per input byte. */
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
                        "cursor small={}B big={}B delta={}B over {}B extra input = {}B/B " +
                        "(ceiling {} B/B); iterator small={}B big={}B delta={}B",
                        smallAlloc, bigAlloc, delta, extraBytes, String.format("%.3f", perInputByte),
                        largeFilePerInputByteCeiling(), smallIter, bigIter, bigIter - smallIter);
            assertTrue(String.format("cursor allocation per input byte too high: %.3f B/B (delta %,dB over %,dB, " +
                                     "ceiling %.2f B/B)",
                                     perInputByte, delta, extraBytes, largeFilePerInputByteCeiling()),
                       perInputByte <= largeFilePerInputByteCeiling());
        });
    }

    /** Ceiling for {@link #allocationAtLargeFileSizes}. A format subclass raises it. */
    protected double largeFilePerInputByteCeiling()
    {
        return 0.5;
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
        // -1 means the measurement is disabled; reject it so the gate cannot pass having measured nothing
        assertTrue("thread allocation measurement returned no reading (before=" + before +
                   " after=" + after + "); the allocation gate cannot measure and must not report a pass",
                   before >= 0 && after >= 0);
        long allocated = after - before;

        List<SSTableReader> retainedInputClones = new ArrayList<>();
        List<SSTableReader> outputs = identifyOutputs(cfs, liveBeforeDescs, liveBeforeDescs, retainedInputClones);
        restoreAfterCompaction(cfs, outputs, retainedInputClones, inputDescriptors, inputs.size());
        return allocated;
    }

    /** Allocation must not scale with sparse rows, which carry a column-subset encoding. */
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
                        smallAlloc, bigAlloc, delta, ceilingBytes());
            assertTrue(String.format("sparse-row cursor compaction allocation scales with data: " +
                                     "%,dB -> %,dB, delta %,dB exceeds ceiling %,dB",
                                     smallAlloc, bigAlloc, delta, ceilingBytes()),
                       delta <= ceilingBytes());
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

    /** Allocation must not scale with sparse rows in a >= 64-column superset, which uses the
     *  large-subset wire format. */
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
                        "over {}B extra input = {} B/B (ceiling {} B/B)",
                        smallAlloc, bigAlloc, delta, extraBytes, String.format("%.3f", perInputByte),
                        wideSchemaPerInputByteCeiling());
            assertTrue(String.format("wide-schema (>=64 col) sparse-row cursor allocation per input byte too high: " +
                                     "%.3f B/B (delta %,dB over %,dB extra input, ceiling %.2f B/B)",
                                     perInputByte, delta, extraBytes, wideSchemaPerInputByteCeiling()),
                       perInputByte <= wideSchemaPerInputByteCeiling());
        });
    }

    /** Ceiling for {@link #allocationDoesNotScaleWithWideSchemaSparseRows}. A format subclass raises it. */
    protected double wideSchemaPerInputByteCeiling()
    {
        return 0.6;
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

        // missing-mode insert: 67 of the 69-column superset
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

    /** Allocation must not scale with range-tombstone-dense workloads; asserted per input byte. */
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

    /** Ceiling for {@link #allocationDoesNotScaleWithRangeTombstones}. */
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
                // 300 bounded range tombstones; round 1 shifts bounds so markers overlap across rounds
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

    /** Ceiling for {@link #allocationDoesNotScaleWithComplexColumns}. A format subclass raises it. */
    protected double complexPerInputByteCeiling()
    {
        return 0.5;
    }

    /** Allocation must not grow per row for multi-cell (complex) columns. */
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
            assertTrue(String.format("complex-column cursor allocation per input byte too high: " +
                                     "%.3f B/B (delta %,dB over %,dB extra input, ceiling %.2f)",
                                     perInputByte, delta, extraBytes, complexPerInputByteCeiling()),
                       perInputByte <= complexPerInputByteCeiling());
        });
    }

    /** Creates and fills the complex-column table: {@code rounds} rounds of rows holding a map,
     *  a set and a text column. */
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
        ColumnFamilyStore cfs = populateComplexTable(partitions, 4, "x".repeat(180));
        captureLastInputBytes(cfs);
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        assertCursorPathWillRun(cfs, cfs.getLiveSSTables(), gcBefore);
        return measureBest(cfs, gcBefore, 2, 2);
    }

    /** Diagnostic, not a gate: dumps a JFR allocation profile of the big table to
     *  /tmp/cursor-alloc.jfr. Always passes. */
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

    /** Diagnostic, not a gate: dumps a JFR allocation profile of the range-tombstone-dense table
     *  to /tmp/cursor-alloc-rt.jfr. */
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

    /** Diagnostic, not a gate: dumps a JFR allocation profile of the multi-cell table to
     *  /tmp/cursor-alloc-complex.jfr. */
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
