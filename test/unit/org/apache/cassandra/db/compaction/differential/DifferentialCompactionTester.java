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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

import com.google.common.io.ByteStreams;

import org.apache.commons.io.FileUtils;
import org.junit.Assume;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.JsonTransformer;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Runs the same input sstables through both the iterator and cursor compaction paths and asserts
 * the outputs are byte-identical and logically identical.
 */
public abstract class DifferentialCompactionTester extends DifferentialCorpusDriver
{
    /** Fixed "now" used for JSON dumps so rendering cannot depend on wall clock. */
    private static final long DUMP_NOW_SEC = 0;

    /** Keeps a failed comparison's captured sstables under scratch for local post-mortem. */
    private static final boolean KEEP_SCRATCH_ON_FAILURE =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_KEEP_SCRATCH_ON_FAILURE.getBoolean();

    /** Whether {@link #capture} reads every captured output back through single-row slices. */
    private static final boolean SLICE_READBACK =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_SLICE_READBACK.getBoolean();

    /** Ceiling on how many rows of one partition {@link #assertEveryRowReadableThroughASlice} probes. */
    private static final int SLICE_READBACK_MAX_ROWS_PER_PARTITION =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_SLICE_READBACK_MAX_ROWS.getInt();

    // Matches the "expired" flag in a JSON dump, which every capture normalizes away.
    private static final Pattern EXPIRED_FLAG =
        Pattern.compile("\"expired\"\\s*:\\s*(true|false)");

    /** The rendered form of a cell tombstone, for absolute assertions over {@link #allJson}. */
    protected static final String CELL_TOMBSTONE = "\"deletion_info\":{\"local_delete_time\"";

    /**
     * Scale mode for very large scenarios: the logical dump is streamed into a SHA-256 digest
     * instead of being retained as a String, so capture memory stays flat regardless of row count.
     */
    protected boolean scaleCapture()
    {
        return false;
    }

    public static final class CapturedSSTable
    {
        final Path dir;                 // copied component files, named by component
        final String json;              // logical dump, or a SHA-256 digest string in scale mode
        final String statsSummary;
        final long totalRows;           // rows the output carries, for non-triviality guards (e.g. purge)
        final SortedMap<String, Long> componentSizes = new TreeMap<>();

        CapturedSSTable(Path dir, String json, String statsSummary, long totalRows)
        {
            this.dir = dir;
            this.json = json;
            this.statsSummary = statsSummary;
            this.totalRows = totalRows;
        }
    }

    public static final class CapturedOutput
    {
        final List<CapturedSSTable> sstables = new ArrayList<>();
    }

    /** Concatenates the logical dump (or digest string in scale mode) of every captured output sstable. */
    protected static String allJson(CapturedOutput out)
    {
        StringBuilder sb = new StringBuilder();
        for (CapturedSSTable s : out.sstables)
            sb.append(s.json);
        return sb.toString();
    }

    /** Sum of the per-sstable row counts across a captured output. */
    protected static long outputRows(CapturedOutput out)
    {
        long rows = 0;
        for (CapturedSSTable s : out.sstables)
            rows += s.totalRows;
        return rows;
    }

    /** The rendered form of a text-typed cell holding {@code text}, for absolute assertions over {@link #allJson}. */
    protected static String cellValue(String text)
    {
        return "\"value\":\"" + text + '"';
    }

    /** Counts non-overlapping occurrences of {@code needle} in {@code haystack}. */
    protected static int countOccurrences(String haystack, String needle)
    {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length()))
            count++;
        return count;
    }

    /**
     * The latest local deletion time among the given sstables, for placing a {@code gcBefore} relative
     * to their tombstones. Fails if the scenario produced no deletion.
     */
    protected static long maxTombstoneLocalDeletionTime(Iterable<SSTableReader> sstables)
    {
        long max = Long.MIN_VALUE;
        for (SSTableReader sstable : sstables)
        {
            long ldt = sstable.getSSTableMetadata().maxLocalDeletionTime;
            if (ldt != Cell.NO_DELETION_TIME)
                max = Math.max(max, ldt);
        }
        assertTrue("scenario produced no tombstone deletion times", max > 0 && max < Long.MAX_VALUE);
        return max;
    }

    /** Creates the CompactionTask for one differential run. Must honor keepOriginals=true. */
    public interface TaskFactory
    {
        CompactionTask create(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore);
    }

    public static final TaskFactory DEFAULT_TASK = (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, true);

    /** A TaskFactory that pins the TTL-expiry "now" to a fixed value instead of the wall clock. */
    public static TaskFactory taskWithFixedNow(long nowInSeconds)
    {
        return (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, true).setNowInSecondsSupplier(() -> nowInSeconds);
    }

    /** The earliest local deletion time across the live set: below this, gcBefore purges nothing. */
    protected static long minLocalDeletionTime(ColumnFamilyStore cfs)
    {
        long min = Long.MAX_VALUE;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            min = Math.min(min, sstable.getSSTableMetadata().minLocalDeletionTime);
        return min;
    }

    /** Asserts at least one cell in the current live set has expired relative to nowInSeconds. */
    protected void assertSomethingExpiredAt(ColumnFamilyStore cfs, long nowInSeconds)
    {
        long minLocalDeletionTime = minLocalDeletionTime(cfs);
        assertTrue("no cell in the live set has expired relative to the pinned now " + nowInSeconds +
                   "; the earliest local deletion time on disk is " + minLocalDeletionTime,
                   minLocalDeletionTime <= nowInSeconds);
    }

    /** Future skew for a purge shape's pinned "now", so its short-TTL cells and gc_grace=0 tombstones are past it. */
    private static final long PURGE_NOW_SKEW_SECONDS = 3600;

    /**
     * Runs the differential for one corpus shape. Most shapes run once at the default gcBefore. A shape that
     * {@link DifferentialSchema#expectsPurge() expects purge} runs twice with the SAME pinned future "now",
     * varying only gcBefore: a retain run keeps every expired cell and tombstone, a purge run drops them.
     * Both paths agree in each run, so each run is a valid cursor-vs-iterator comparison. The purged output
     * must then carry strictly fewer rows than the retained one; the merge dedup is identical across the two
     * runs, so the row difference isolates purge, and a shape that purges nothing fails here rather than
     * passing on a trivially equal comparison.
     */
    protected void assertCursorMatchesIteratorForShape(ColumnFamilyStore cfs, DifferentialSchema schema) throws Exception
    {
        if (!schema.expectsPurge())
        {
            assertShapeLeftMergedOutput(schema, assertCursorMatchesIterator(cfs));
            return;
        }

        long now = FBUtilities.nowInSeconds() + PURGE_NOW_SKEW_SECONDS;
        // Anchor the retain baseline to the data on disk, not the wall clock. One below the earliest local
        // deletion time is below every deletion time by construction, so the retain run purges nothing no
        // matter how long the writes and guards took. A wall-clock baseline drifts past short-TTL cells at
        // burn scale and makes the retain run purge them too, which would falsely fail the guard below.
        long retainGcBefore = minLocalDeletionTime(cfs) - 1;

        CapturedOutput retained = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskWithFixedNow(now), retainGcBefore);
        CapturedOutput purged = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskWithFixedNow(now), now);
        assertShapeLeftMergedOutput(schema, retained);
        assertTrue("purge shape '" + schema.name() + "' dropped no rows: the merge kept " + outputRows(retained) +
                   " rows with purge disabled and " + outputRows(purged) + " with purge at now=" + now +
                   "; the scenario purged nothing, so it does not exercise purge",
                   outputRows(purged) < outputRows(retained));
    }

    /**
     * Every corpus shape must leave at least one merged sstable to compare. An equal-but-empty comparison
     * proves nothing, so this guards the corpus path itself rather than any single test.
     */
    private static void assertShapeLeftMergedOutput(DifferentialSchema schema, CapturedOutput output)
    {
        assertFalse("corpus shape '" + schema.name() + "' produced no output sstable: an equal-but-empty " +
                    "comparison proves nothing; every shape must leave at least one merged sstable",
                    output.sstables.isEmpty());
    }

    /**
     * Runs both compaction paths over the current live sstables of the table and asserts
     * byte + logical equivalence of every output component.
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs) throws Exception
    {
        return assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), DEFAULT_TASK);
    }

    /**
     * Variant for partial-set compactions (inputs is a subset of the live sstables) and for custom
     * CompactionTask shapes. Returns the iterator-path capture for structural assertions.
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs,
                                                         Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory) throws Exception
    {
        return assertCursorMatchesIterator(cfs, inputs, taskFactory,
                                           cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
    }

    /** Variant with an explicit gcBefore, so scenarios can place purge decisions exactly at the boundary. */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs,
                                                         Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory,
                                                         long gcBefore) throws Exception
    {
        Path scratch = Files.createTempDirectory("differential-compaction");

        // scratch holds byte-for-byte copies of every captured output sstable, for both paths.
        boolean passed = false;
        try
        {
            CapturedOutput iterator = compactPath(cfs, inputs, false, gcBefore, scratch.resolve("iterator"), taskFactory);
            // input instances were replaced during restore; re-resolve the subset by descriptor
            Set<Descriptor> inputDescs = new HashSet<>();
            for (SSTableReader in : inputs)
                inputDescs.add(in.descriptor);
            Set<SSTableReader> reResolved = new HashSet<>();
            for (SSTableReader live : cfs.getLiveSSTables())
                if (inputDescs.contains(live.descriptor))
                    reResolved.add(live);
            assertEquals("input subset lost across restore", inputs.size(), reResolved.size());
            CapturedOutput cursor = compactPath(cfs, reResolved, true, gcBefore, scratch.resolve("cursor"), taskFactory);
            assertEquivalentOutputs(iterator, cursor);
            passed = true;
            return iterator;
        }
        finally
        {
            if (passed || !KEEP_SCRATCH_ON_FAILURE)
            {
                // never rethrown: an IOException here would replace the AssertionError carrying the
                // divergence report with an unrelated one
                try
                {
                    FileUtils.deleteDirectory(scratch.toFile());
                }
                catch (IOException e)
                {
                    logger.warn("could not delete differential scratch directory {}", scratch, e);
                }
            }
            else
            {
                logger.error("differential comparison failed; both paths' captured sstables kept at {}", scratch);
            }
        }
    }

    /**
     * Runs the differential at two generations: once over the flushed inputs (gen 1), then again over
     * the cursor-produced outputs (gen 2). Returns the gen-1 iterator capture for structural assertions.
     */
    protected CapturedOutput assertCursorMatchesIteratorAcrossGenerations(ColumnFamilyStore cfs) throws Exception
    {
        return assertCursorMatchesIteratorAcrossGenerations(cfs, FBUtilities::nowInSeconds);
    }

    /** As above, but pins the TTL-expiry "now" to nowInSecondsSupplier for both generations. */
    protected CapturedOutput assertCursorMatchesIteratorAcrossGenerations(ColumnFamilyStore cfs,
                                                                          LongSupplier nowInSecondsSupplier) throws Exception
    {
        TaskFactory taskFactory = (c, txn, gcBefore) -> new CompactionTask(c, txn, gcBefore, true).setNowInSecondsSupplier(nowInSecondsSupplier);
        CapturedOutput gen1 = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskFactory);

        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        commitCompaction(cfs, cfs.getLiveSSTables(), true, gcBefore, nowInSecondsSupplier);
        if (cfs.getLiveSSTables().isEmpty())
            return gen1; // gen 1 purged everything; there are no gen-2 inputs

        assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), taskFactory);
        return gen1;
    }

    /**
     * Commits one compaction over the whole live set without restore: the live set becomes the outputs.
     * The factory must build its writer with keepOriginals false.
     */
    protected void commitThroughFactory(ColumnFamilyStore cfs, boolean cursor, TaskFactory taskFactory) throws Exception
    {
        commitThroughFactory(cfs, cursor, taskFactory, ActiveCompactionsTracker.NOOP);
    }

    /** As above, with a tracker of the caller's choosing. */
    protected void commitThroughFactory(ColumnFamilyStore cfs, boolean cursor, TaskFactory taskFactory,
                                        ActiveCompactionsTracker tracker) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        long gcBefore = cfs.getDefaultGcBefore(FBUtilities.nowInSeconds());
        Set<SSTableReader> inputs = cfs.getLiveSSTables();
        assertFalse("scenario produced no input sstables", inputs.isEmpty());
        if (cursor)
            assertCursorPathWillRun(cfs, inputs, gcBefore);
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting for commit", txn);
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        taskFactory.create(cfs, txn, gcBefore).execute(tracker);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);
    }

    /** Commits one compaction over the given inputs without restore: the live set becomes the outputs. */
    protected void commitCompaction(ColumnFamilyStore cfs, Set<SSTableReader> inputs, boolean cursor, long gcBefore) throws Exception
    {
        commitCompaction(cfs, inputs, cursor, gcBefore, FBUtilities::nowInSeconds);
    }

    /** As above, but pins the TTL-expiry "now" to nowInSecondsSupplier. */
    protected void commitCompaction(ColumnFamilyStore cfs, Set<SSTableReader> inputs, boolean cursor, long gcBefore,
                                    LongSupplier nowInSecondsSupplier) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        if (cursor)
            assertCursorPathWillRun(cfs, inputs, gcBefore);
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting for commit", txn);
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        new CompactionTask(cfs, txn, gcBefore, false).setNowInSecondsSupplier(nowInSecondsSupplier).execute(ActiveCompactionsTracker.NOOP);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);
    }

    /**
     * Runs one compaction path over the given input subset, captures the outputs, and restores the
     * live set so the other path sees identical bytes.
     */
    protected CapturedOutput compactPath(ColumnFamilyStore cfs,
                                         Set<SSTableReader> inputs,
                                         boolean cursor,
                                         long gcBefore,
                                         Path scratch,
                                         TaskFactory taskFactory) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);

        assertFalse("scenario produced no input sstables", inputs.isEmpty());
        Set<Descriptor> liveBeforeDescs = new HashSet<>();
        int liveBeforeCount = 0;
        for (SSTableReader live : cfs.getLiveSSTables())
        {
            liveBeforeDescs.add(live.descriptor);
            liveBeforeCount++;
        }
        List<Descriptor> inputDescriptors = new ArrayList<>();
        for (SSTableReader in : inputs)
        {
            assertTrue("input is not live", liveBeforeDescs.contains(in.descriptor));
            inputDescriptors.add(in.descriptor);
        }
        Set<Descriptor> inputDescs = new HashSet<>(inputDescriptors);

        if (cursor)
            assertCursorPathWillRun(cfs, inputs, gcBefore);

        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting", txn);
        // assert the requested pipeline actually ran, so a silent fallback cannot compare a path with itself
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        taskFactory.create(cfs, txn, gcBefore).execute(ActiveCompactionsTracker.NOOP);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);

        // outputs are identified by descriptor diff against the pre-compaction live set
        List<SSTableReader> retainedInputClones = new ArrayList<>();
        List<SSTableReader> outputs = identifyOutputs(cfs, liveBeforeDescs, inputDescs, retainedInputClones);

        CapturedOutput captured = new CapturedOutput();
        int seq = 0;
        for (SSTableReader out : outputs)
            captured.sstables.add(capture(cfs, out, scratch.resolve("sstable-" + seq++)));

        restoreAfterCompaction(cfs, outputs, retainedInputClones, inputDescriptors, liveBeforeCount);

        return captured;
    }

    /**
     * Delists and releases outputs and retained input clones, deletes output files, then reopens every
     * input fresh from its descriptor so a subsequent run sees identical pristine readers.
     */
    protected void restoreAfterCompaction(ColumnFamilyStore cfs,
                                          List<SSTableReader> outputs,
                                          List<SSTableReader> retainedInputClones,
                                          List<Descriptor> inputDescriptors,
                                          int liveBeforeCount) throws Exception
    {
        List<Path> outputFiles = new ArrayList<>();
        for (SSTableReader out : outputs)
            for (Component c : out.descriptor.discoverComponents())
                outputFiles.add(out.descriptor.fileFor(c).toPath());

        Set<SSTableReader> toRemove = new HashSet<>(outputs);
        toRemove.addAll(retainedInputClones);
        cfs.getTracker().removeUnsafe(toRemove);
        for (SSTableReader reader : toRemove)
            reader.selfRef().release();
        for (Path f : outputFiles)
            Files.deleteIfExists(f);

        List<SSTableReader> reopened = new ArrayList<>();
        for (Descriptor desc : inputDescriptors)
        {
            if (!desc.fileFor(org.apache.cassandra.io.sstable.format.SSTableFormat.Components.DATA).exists())
                fail("input sstable lost during compaction (keepOriginals violated?): " + desc +
                     "\ndata dir contents:\n" + listDataDir(desc));
            reopened.add(SSTableReader.open(cfs, desc));
        }
        cfs.getTracker().addInitialSSTables(reopened);
        assertEquals("restore failed: live sstable count", liveBeforeCount, cfs.getLiveSSTables().size());
    }

    /** Identifies outputs by before/after descriptor diff. */
    protected static List<SSTableReader> identifyOutputs(ColumnFamilyStore cfs,
                                                         Set<Descriptor> liveBeforeDescs,
                                                         Set<Descriptor> inputDescs,
                                                         List<SSTableReader> retainedInputClonesOut)
    {
        List<SSTableReader> outputs = new ArrayList<>();
        for (SSTableReader reader : cfs.getLiveSSTables())
        {
            if (!liveBeforeDescs.contains(reader.descriptor))
                outputs.add(reader);
            else if (inputDescs.contains(reader.descriptor))
                retainedInputClonesOut.add(reader);
        }
        outputs.sort(Comparator.comparing(SSTableReader::getFirst));
        return outputs;
    }

    /** Asserts the cursor path would actually run for this scenario, so it cannot compare iterator vs iterator. */
    protected void assertCursorPathWillRun(ColumnFamilyStore cfs, Set<SSTableReader> inputs, long gcBefore) throws Exception
    {
        // skip an unsupported format rather than fail the assertion below
        Assume.assumeTrue("cursor compaction cannot write the selected sstable format; selected=" +
                          DatabaseDescriptor.getSelectedSSTableFormat().name(),
                          DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction());
        try (CompactionController controller = new CompactionController(cfs, inputs, gcBefore);
             AbstractCompactionStrategy.ScannerList scanners =
                 cfs.getCompactionStrategyManager().getScanners(new ArrayList<>(inputs), null))
        {
            assertTrue("scenario is not supported by cursor compaction; this harness run would " +
                       "silently compare iterator vs iterator. If unsupported-ness is intended, " +
                       "assert it explicitly instead.",
                       CursorCompactor.isSupported(scanners, controller));
        }
    }

    /** Skips a scenario unless the selected sstable format supports cursor compaction. */
    protected static void assumeCursorSupportedFormatSelected()
    {
        Assume.assumeTrue("cursor compaction does not support the selected sstable format; selected=" +
                          DatabaseDescriptor.getSelectedSSTableFormat().name(),
                          DatabaseDescriptor.getSelectedSSTableFormat().supportsCursorCompaction());
    }

    private static String listDataDir(Descriptor desc)
    {
        try (java.util.stream.Stream<Path> files = Files.list(desc.directory.toPath()))
        {
            StringBuilder sb = new StringBuilder();
            files.sorted().forEach(p -> sb.append("  ").append(p.getFileName()).append('\n'));
            return sb.toString();
        }
        catch (IOException e)
        {
            return "  <failed to list: " + e + ">";
        }
    }

    /** Asserts this compaction output was written in the sstable format the JVM currently has selected. */
    protected static void assertOutputFormatIsSelected(SSTableReader sstable)
    {
        SSTableFormat<?, ?> selected = DatabaseDescriptor.getSelectedSSTableFormat();
        SSTableFormat<?, ?> written = sstable.descriptor.getFormat();
        assertEquals("compaction output " + sstable.descriptor + " was written in the '" + written.name() +
                     "' format while '" + selected.name() + "' is selected: this scenario is not testing the " +
                     "format it claims, and the byte comparison below would compare that other format's " +
                     "components and pass",
                     selected.name(), written.name());
    }

    /**
     * Opens a single-row slice for every row of every partition and asserts the row that comes back is
     * the one a plain sequential walk returns, so the index routes seeks correctly.
     *
     * @return how many partitions carried a promoted row index
     */
    protected int assertEveryRowReadableThroughASlice(SSTableReader sstable)
    {
        TableMetadata metadata = sstable.metadata();
        // no clustering columns: one row per partition, never indexable, no seek to route
        if (metadata.comparator.size() == 0 || SLICE_READBACK_MAX_ROWS_PER_PARTITION <= 0)
            return 0;

        ColumnFilter fetchAll = ColumnFilter.all(metadata);
        // cells are only comparable when the probe's filter fetches exactly what the sequential read deserializes
        boolean cellsComparable = sstable.header.columns().equals(metadata.regularAndStaticColumns());
        int headCap = (SLICE_READBACK_MAX_ROWS_PER_PARTITION + 1) / 2;
        int tailCap = SLICE_READBACK_MAX_ROWS_PER_PARTITION / 2;
        int granularity = DatabaseDescriptor.getColumnIndexSize(-1);
        int indexedPartitions = 0;

        PendingPartition pending = new PendingPartition();

        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                List<Row> probes = new ArrayList<>();
                int unfiltereds;
                DecoratedKey key;
                DeletionTime partitionDeletion;
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    key = partition.partitionKey();
                    partitionDeletion = partition.partitionLevelDeletion();
                    unfiltereds = collectProbeRows(partition, probes, headCap, tailCap);
                }

                AbstractRowIndexEntry entry = sstable.getRowIndexEntry(key, SSTableReader.Operator.EQ);
                if (entry == null)
                    throw new AssertionError("a partition the sequential walk returned has no index entry: " +
                                             key + " in " + sstable.descriptor);
                if (entry.blockCount() > 1)
                    indexedPartitions++;

                pending.advance(sstable, key, entry, unfiltereds, granularity);

                assertPartitionDeletionReadableFromIndexEntry(sstable, key, entry, partitionDeletion);
                assertEveryProbeReturnsExactly(sstable, metadata, fetchAll, key, probes, cellsComparable);
            }
        }
        pending.finish(sstable, granularity);
        return indexedPartitions;
    }

    /** Checks each partition's block-count bound one iteration late, once the next partition's start is known. */
    private static final class PendingPartition
    {
        private DecoratedKey key;
        private long position = -1;
        private int blockCount;
        private int unfiltereds;

        /** Bounds the partition held here against the next one's start, then holds the next one. */
        void advance(SSTableReader sstable, DecoratedKey nextKey, AbstractRowIndexEntry nextEntry,
                     int nextUnfiltereds, int granularity)
        {
            if (key != null && nextEntry.position > position)
                assertBlockCountWithinBounds(sstable, key, blockCount, unfiltereds,
                                             nextEntry.position - position, granularity);
            key = nextKey;
            position = nextEntry.position;
            blockCount = nextEntry.blockCount();
            unfiltereds = nextUnfiltereds;
        }

        /** The last partition's bound, measured against the end of the file. */
        void finish(SSTableReader sstable, int granularity)
        {
            if (key != null && sstable.uncompressedLength() > position)
                assertBlockCountWithinBounds(sstable, key, blockCount, unfiltereds,
                                             sstable.uncompressedLength() - position, granularity);
        }
    }

    /** Every probe row of one partition, seeked in both directions. */
    private static void assertEveryProbeReturnsExactly(SSTableReader sstable, TableMetadata metadata,
                                                       ColumnFilter fetchAll, DecoratedKey key,
                                                       List<Row> probes, boolean cellsComparable)
    {
        for (Row expected : probes)
        {
            assertSliceReturnsExactly(sstable, metadata, fetchAll, key, expected, false, cellsComparable);
            assertSliceReturnsExactly(sstable, metadata, fetchAll, key, expected, true, cellsComparable);
        }
    }

    /**
     * Collects the rows one partition is probed with: the first {@code headCap}, then the last {@code tailCap}.
     *
     * @return how many unfiltereds the partition held, markers included
     */
    private static int collectProbeRows(UnfilteredRowIterator partition, List<Row> probes,
                                        int headCap, int tailCap)
    {
        ArrayDeque<Row> tail = new ArrayDeque<>();
        int unfiltereds = 0;
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            unfiltereds++;
            if (!unfiltered.isRow())
                continue;
            Row row = (Row) unfiltered;
            if (probes.size() < headCap)
                probes.add(row);
            else if (tailCap > 0)
            {
                tail.addLast(row);
                if (tail.size() > tailCap)
                    tail.removeFirst();
            }
        }
        probes.addAll(tail);
        return unfiltereds;
    }

    /**
     * Asserts one single-row slice, in one direction, returns exactly one row and that it is
     * {@code expected}. Range tombstone markers are skipped.
     */
    private static void assertSliceReturnsExactly(SSTableReader sstable,
                                                  TableMetadata metadata,
                                                  ColumnFilter fetchAll,
                                                  DecoratedKey key,
                                                  Row expected,
                                                  boolean reversed,
                                                  boolean cellsComparable)
    {
        Slices slices = Slices.with(metadata.comparator, Slice.make(expected.clustering()));
        try (UnfilteredRowIterator probe = sstable.rowIterator(key, slices, fetchAll, reversed,
                                                               SSTableReadsListener.NOOP_LISTENER))
        {
            Row found = null;
            int rows = 0;
            while (probe.hasNext())
            {
                Unfiltered unfiltered = probe.next();
                if (!unfiltered.isRow())
                    continue;
                rows++;
                found = (Row) unfiltered;
            }

            if (rows == 1 && (cellsComparable ? expected.equals(found) : sameRowIdentity(expected, found)))
                return;

            String where = (reversed ? "reverse" : "forward") + " slice of " +
                           expected.clustering().toString(metadata) + " in partition " + key +
                           " of " + sstable.descriptor;
            if (rows != 1)
                fail("the index routed a " + where + " to " + rows + " rows; a single-clustering slice " +
                     "must return exactly one");
            fail("the index routed a " + where + " to the wrong row" +
                 (cellsComparable ? "" : " (this sstable's header carries columns the schema does not, " +
                                         "so only clustering, liveness and row deletion are compared)") +
                 "\n  sequential walk: " + expected.toString(metadata, true) +
                 "\n  slice returned:  " + found.toString(metadata, true));
        }
    }

    /** Clustering, primary key liveness and row deletion — everything a misrouted seek would change. */
    private static boolean sameRowIdentity(Row expected, Row found)
    {
        return expected.clustering().equals(found.clustering())
               && expected.primaryKeyLivenessInfo().equals(found.primaryKeyLivenessInfo())
               && expected.deletion().equals(found.deletion());
    }

    /** Asserts the partition-level deletion the index entry carries matches the one in the data file. */
    private static void assertPartitionDeletionReadableFromIndexEntry(SSTableReader sstable,
                                                                      DecoratedKey key,
                                                                      AbstractRowIndexEntry entry,
                                                                      DeletionTime fromDataFile)
    {
        if (entry.blockCount() <= 1)
            return;
        try (UnfilteredRowIterator probe = sstable.rowIterator(key, Slices.ALL, ColumnFilter.NONE, false,
                                                               SSTableReadsListener.NOOP_LISTENER))
        {
            assertEquals("the index entry's partition-level deletion differs from the data file's for " +
                         key + " in " + sstable.descriptor,
                         fromDataFile, probe.partitionLevelDeletion());
        }
    }

    /** Bounds {@code blockCount()} from above by what the partition's own block structure implies. */
    private static void assertBlockCountWithinBounds(SSTableReader sstable,
                                                     DecoratedKey key,
                                                     int blockCount,
                                                     int unfiltereds,
                                                     long partitionLength,
                                                     int granularity)
    {
        // granularity is -1 when column_index_size is unset, in which case the length bound cannot be stated
        boolean lengthBoundHolds = granularity <= 0 || blockCount < 2
                                   || partitionLength >= (long) (blockCount - 1) * granularity;
        if (blockCount != 1 && blockCount <= unfiltereds && lengthBoundHolds)
            return;

        String where = " for partition " + key + " in " + sstable.descriptor;
        assertFalse("a promoted row index of exactly one block cannot be written: the writer drops it" + where,
                    blockCount == 1);
        assertTrue("row index claims " + blockCount + " blocks but the partition holds only " + unfiltereds +
                   " unfiltereds, and every block begins at its own unfiltered" + where,
                   blockCount <= unfiltereds);
        assertTrue("row index claims " + blockCount + " blocks, so at least " + (blockCount - 1) +
                   " of them were cut at the " + granularity + "-byte column_index_size, but the " +
                   "partition is only " + partitionLength + " bytes long" + where,
                   lengthBoundHolds);
    }

    private CapturedSSTable capture(ColumnFamilyStore cfs, SSTableReader sstable, Path dir) throws IOException
    {
        // 1. the output really is in the format this scenario selected
        assertOutputFormatIsSelected(sstable);

        // 2. structural verification of the output; the debug stream is silenced to avoid OOMing the fork
        OutputHandler verifyOutput = new OutputHandler.LogOutput() { @Override public void debug(String msg) {} };
        try (IVerifier verifier = sstable.getVerifier(cfs, verifyOutput, false,
                                                      IVerifier.options().invokeDiskFailurePolicy(true)
                                                                         .extendedVerification(true).build()))
        {
            verifier.verify();
        }

        // 3. every row is retrievable through a real slice, i.e. the index routes seeks correctly; skipped in scale mode
        if (SLICE_READBACK && !scaleCapture())
            assertEveryRowReadableThroughASlice(sstable);

        // 4. canonical logical dump, with the wall-clock-derived "expired" flag normalized out
        String json;
        if (scaleCapture())
        {
            // stream into a digest: capture memory stays flat at millions of rows
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
                NormalizingDigestOutputStream out = new NormalizingDigestOutputStream(digest);
                JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, false,
                                            sstable.metadata(), DUMP_NOW_SEC, out);
                out.flushTail();
                json = "sha256:" + org.apache.cassandra.utils.Hex.bytesToHex(digest.digest()) +
                       " (" + out.bytesSeen + " bytes)";
            }
            catch (java.security.NoSuchAlgorithmException e)
            {
                throw new AssertionError(e);
            }
        }
        else
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                JsonTransformer.toJsonLines(scanner, Util.iterToStream(scanner), true, false,
                                            sstable.metadata(), DUMP_NOW_SEC, baos);
            }
            json = EXPIRED_FLAG.matcher(baos.toString(StandardCharsets.UTF_8))
                               .replaceAll("\"expired\":\"normalized\"");
        }

        // 5. stats spot-check summary
        StatsMetadata stats = sstable.getSSTableMetadata();
        String statsSummary = "minTimestamp=" + stats.minTimestamp +
                              " maxTimestamp=" + stats.maxTimestamp +
                              " minLocalDeletionTime=" + stats.minLocalDeletionTime +
                              " maxLocalDeletionTime=" + stats.maxLocalDeletionTime +
                              " estimatedKeys=" + sstable.estimatedKeys() +
                              " totalRows=" + stats.totalRows +
                              " totalColumnsSet=" + stats.totalColumnsSet +
                              " encodingStats=" + sstable.header.stats() +
                              " metaEncodingStats=" + stats.encodingStats.minTimestamp + "/" + stats.encodingStats.minLocalDeletionTime + "/" + stats.encodingStats.minTTL +
                              " tombstoneHist=" + tombstoneHistogram(stats) +
                              " cellsPerPartition=" + stats.estimatedCellPerPartitionCount.mean() + "/" + stats.estimatedCellPerPartitionCount.count() +
                              " partitionSize=" + stats.estimatedPartitionSize.mean() + "/" + stats.estimatedPartitionSize.count() +
                              " sstableLevel=" + stats.sstableLevel +
                              " coveredClustering=" + stats.coveredClustering.toString(sstable.metadata().comparator) +
                              " tokenSpaceCoverage=" + stats.tokenSpaceCoverage +
                              " minTTL=" + stats.minTTL + " maxTTL=" + stats.maxTTL +
                              " hasPartitionLevelDeletions=" + stats.hasPartitionLevelDeletions;

        // 6. copy components for byte comparison
        Files.createDirectories(dir);
        CapturedSSTable captured = new CapturedSSTable(dir, json, statsSummary, stats.totalRows);
        for (Component c : sstable.descriptor.discoverComponents())
        {
            Path source = sstable.descriptor.fileFor(c).toPath();
            Path target = dir.resolve(c.name());
            Files.copy(source, target);
            captured.componentSizes.put(c.name(), Files.size(target));
        }
        return captured;
    }

    /** Renders the tombstone histogram's content, since TombstoneHistogram has no stable toString. */
    private static String tombstoneHistogram(StatsMetadata stats)
    {
        return "size=" + stats.estimatedTombstoneDropTime.size() +
               ",sum=" + stats.estimatedTombstoneDropTime.sum(Integer.MAX_VALUE);
    }

    protected void assertEquivalentOutputs(CapturedOutput iterator, CapturedOutput cursor)
    {
        assertEquals("output sstable count differs between paths", iterator.sstables.size(), cursor.sstables.size());
        for (int i = 0; i < iterator.sstables.size(); i++)
            assertEquivalentSSTable(i, iterator.sstables.get(i), cursor.sstables.get(i));
    }

    /** One output sstable of each path: logical dump, stats summary, then every component's bytes. */
    private void assertEquivalentSSTable(int i, CapturedSSTable it, CapturedSSTable cu)
    {
        // logical first: a row-level diff is more debuggable. In scale mode the dump is a digest, deferred below.
        boolean digestMode = it.json.startsWith("sha256:");
        if (!digestMode && !it.json.equals(cu.json))
            fail("LOGICAL divergence in output sstable " + i + " (iterator vs cursor):\n" + firstJsonDiff(it.json, cu.json) +
                 "\niterator stats: " + it.statsSummary + "\ncursor stats:   " + cu.statsSummary);

        assertEquals("stats summary divergence in output sstable " + i +
                     "\n  iterator: " + it.statsSummary + "\n  cursor:   " + cu.statsSummary,
                     it.statsSummary, cu.statsSummary);

        List<String> divergences = componentDivergences(it, cu);
        if (!divergences.isEmpty())
            fail("BYTE divergence in output sstable " + i + " (iterator vs cursor):\n" + String.join("\n", divergences) +
                 "\nNothing is allowed to diverge: every divergence found to date has been a bug in one of the paths");

        if (digestMode)
            assertEquals("logical dump digest divergence in output sstable " + i +
                         " (scale mode; rerun a reduced scenario without scale mode for a row-level diff)",
                         it.json, cu.json);
    }

    /** One description per component whose bytes differ, or that only one path wrote. */
    private static List<String> componentDivergences(CapturedSSTable it, CapturedSSTable cu)
    {
        SortedSet<String> components = new TreeSet<>();
        components.addAll(it.componentSizes.keySet());
        components.addAll(cu.componentSizes.keySet());
        List<String> divergences = new ArrayList<>();
        for (String comp : components)
        {
            Path a = it.dir.resolve(comp);
            Path b = cu.dir.resolve(comp);
            boolean hasA = Files.exists(a);
            boolean hasB = Files.exists(b);
            if (hasA != hasB)
            {
                divergences.add(String.format("  %s: present only in %s path", comp, hasA ? "iterator" : "cursor"));
                continue;
            }
            if (!hasA)
                continue;
            long firstDiff = firstFileDifference(a, b);
            if (firstDiff < 0)
                continue;
            divergences.add(describeFileDiff(comp, a, b, firstDiff));
        }
        return divergences;
    }

    /** Streaming comparison: -1 if byte-identical, else the offset of the first difference
     *  (the shorter length when one file is a prefix of the other). */
    private static long firstFileDifference(Path a, Path b)
    {
        try (java.io.InputStream ia = new java.io.BufferedInputStream(Files.newInputStream(a), 1 << 16);
             java.io.InputStream ib = new java.io.BufferedInputStream(Files.newInputStream(b), 1 << 16))
        {
            byte[] bufA = new byte[1 << 16];
            byte[] bufB = new byte[1 << 16];
            long offset = 0;
            while (true)
            {
                int readA = ia.readNBytes(bufA, 0, bufA.length);
                int readB = ib.readNBytes(bufB, 0, bufB.length);
                int common = Math.min(readA, readB);
                int mismatch = java.util.Arrays.mismatch(bufA, 0, common, bufB, 0, common);
                if (mismatch >= 0)
                    return offset + mismatch;
                if (readA != readB)
                    return offset + common; // same prefix, different length
                if (readA == 0)
                    return -1;
                offset += readA;
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static String describeFileDiff(String component, Path a, Path b, long firstDiff)
    {
        try
        {
            return String.format("  %s: lengths %d vs %d, first divergence at offset %d%n    iterator: %s%n    cursor:   %s",
                                 component, Files.size(a), Files.size(b), firstDiff,
                                 hexContext(a, firstDiff), hexContext(b, firstDiff));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    private static String hexContext(Path file, long offset) throws IOException
    {
        long size = Files.size(file);
        long from = Math.max(0, offset - 8);
        int len = (int) Math.min(size - from, 32);
        byte[] window = new byte[Math.max(len, 0)];
        try (java.io.InputStream in = Files.newInputStream(file))
        {
            ByteStreams.skipFully(in, from);
            in.readNBytes(window, 0, window.length);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < window.length; i++)
        {
            long abs = from + i;
            if (abs == offset)
                sb.append('[');
            sb.append(String.format("%02x", window[i]));
            if (abs == offset)
                sb.append(']');
            sb.append(' ');
        }
        if (from + window.length < size)
            sb.append("...");
        return sb.toString();
    }

    /**
     * Streams a JSON dump into a digest, normalizing the wall-clock-derived "expired" flag and flushing
     * oversized lines in bounded chunks so memory stays flat for multi-GB partitions.
     */
    private static final class NormalizingDigestOutputStream extends java.io.OutputStream
    {
        private static final int FLUSH_THRESHOLD = 8 << 20;
        private static final int TAIL_KEEP = 64; // > the longest normalized token

        private final java.security.MessageDigest digest;
        private final ByteArrayOutputStream line = new ByteArrayOutputStream();
        long bytesSeen;

        NormalizingDigestOutputStream(java.security.MessageDigest digest)
        {
            this.digest = digest;
        }

        @Override
        public void write(int b)
        {
            line.write(b);
            if (b == '\n')
                flushTail();
            else if (line.size() >= FLUSH_THRESHOLD)
                flushChunk();
        }

        @Override
        public void write(byte[] b, int off, int len)
        {
            for (int i = off; i < off + len; i++)
                write(b[i]);
        }

        /** Digests all buffered content, at the end of a line or of the stream. */
        void flushTail()
        {
            if (line.size() == 0)
                return;
            update(line.toByteArray(), line.size());
            line.reset();
        }

        /** Digests all but the last TAIL_KEEP buffered bytes, so a token incomplete at the buffer end survives. */
        private void flushChunk()
        {
            byte[] buffered = line.toByteArray();
            int processed = buffered.length - TAIL_KEEP;
            update(buffered, processed);
            line.reset();
            line.write(buffered, processed, TAIL_KEEP);
        }

        private void update(byte[] bytes, int length)
        {
            byte[] normalized = EXPIRED_FLAG.matcher(new String(bytes, 0, length, StandardCharsets.UTF_8))
                                            .replaceAll("\"expired\":\"normalized\"")
                                            .getBytes(StandardCharsets.UTF_8);
            digest.update(normalized);
            bytesSeen += normalized.length;
        }
    }

    private static String firstJsonDiff(String a, String b)
    {
        String[] linesA = a.split("\n", -1);
        String[] linesB = b.split("\n", -1);
        int max = Math.max(linesA.length, linesB.length);
        for (int i = 0; i < max; i++)
        {
            if (!lineAt(linesA, i).equals(lineAt(linesB, i)))
                return renderDiffContext(linesA, linesB, i, max);
        }
        return "(no line diff found despite string inequality — check line endings)";
    }

    /** Line {@code i} of one dump, or a placeholder where that dump is the shorter one. */
    private static String lineAt(String[] lines, int i)
    {
        return i < lines.length ? lines[i] : "<missing>";
    }

    /** The differing line marked, with two lines either side, both dumps interleaved. */
    private static String renderDiffContext(String[] linesA, String[] linesB, int i, int max)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("first differing line ").append(i + 1).append(" of ").append(max).append(":\n");
        for (int j = Math.max(0, i - 2); j < Math.min(max, i + 3); j++)
        {
            sb.append(j == i ? ">>" : "  ").append(" iterator: ").append(lineAt(linesA, j)).append('\n');
            sb.append(j == i ? ">>" : "  ").append(" cursor:   ").append(lineAt(linesB, j)).append('\n');
        }
        return sb.toString();
    }
}
