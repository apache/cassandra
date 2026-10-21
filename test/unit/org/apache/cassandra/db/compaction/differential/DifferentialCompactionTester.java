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
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionPipelineCounts;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.CursorCompactor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
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
 * Differential test harness for cursor-based vs iterator-based compaction.
 *
 * Runs the SAME input sstables through both {@code IteratorCompactionPipeline} and
 * {@code CursorCompactionPipeline} (via the full production {@link CompactionTask} path,
 * selected by {@link DatabaseDescriptor#setCursorCompactionEnabled}), captures both outputs,
 * and asserts equivalence at two levels:
 *
 *  1. BYTE level: every output component must be byte-identical. There is deliberately NO
 *     exception mechanism; nothing is allowed to diverge.
 *  2. LOGICAL level: a canonical JSON dump (sstabledump format) of every output sstable
 *     must match exactly, and key stats metadata must match.
 *
 * Correctness invariants of the harness itself:
 *  - inputs are byte-identical for both runs: the first run keeps originals on disk
 *    ({@code keepOriginals=true}) and the harness restores the live set from the original
 *    descriptors without rewriting anything.
 *  - the same gcBefore is passed to both runs, so purge decisions cannot flip between runs.
 *  - the cursor run asserts {@link CursorCompactor#isSupported} up front. Both runs then assert
 *    that the compaction really selected the pipeline they asked for, because a run that
 *    silently fell back would compare a path with itself and pass.
 *
 * Each run takes its own nowInSec for TTL expiry inside CompactionTask. A scenario that needs a
 * TTL expiry boundary placed deterministically uses {@link #taskWithFixedNow} or the
 * {@code LongSupplier}-taking overloads below.
 */
public abstract class DifferentialCompactionTester extends CQLTester
{
    /** Fixed "now" used for JSON dumps so rendering cannot depend on wall clock. */
    private static final long DUMP_NOW_SEC = 0;

    /**
     * Opt-in only: preserves a failed comparison's captured sstables under scratch instead of
     * deleting them, for local post-mortem. It defaults to off because a scratch directory kept
     * on every failure fills a CI disk, especially for the multi-GB burn scenarios (BigVolume,
     * LargePartition).
     */
    private static final boolean KEEP_SCRATCH_ON_FAILURE =
        CassandraRelevantProperties.TEST_DIFFERENTIAL_KEEP_SCRATCH_ON_FAILURE.getBoolean();

    // sstabledump renders "expired" from the WALL CLOCK, not from the fixed nowInSec above, so
    // the two paths' captures can differ on it. Every capture normalizes it away; see capture().
    private static final Pattern EXPIRED_FLAG =
        Pattern.compile("\"expired\"\\s*:\\s*(true|false)");

    /**
     * The rendered form of a CELL tombstone, for absolute assertions over {@link #allJson}. Row,
     * range, partition and complex-column deletions all go through {@code serializeDeletion}, which
     * writes {@code marked_deleted} first, so this matches cell tombstones only.
     */
    protected static final String CELL_TOMBSTONE = "\"deletion_info\":{\"local_delete_time\"";

    /**
     * Scale mode for very large scenarios (millions of rows): the logical dump is streamed
     * into a SHA-256 digest instead of being retained as a String, so capture memory stays
     * flat regardless of row count. Byte comparison always streams. On a digest mismatch
     * the byte-level comparison (which still reports exact offsets) is the debugging tool;
     * rerun a reduced scenario without scale mode for a row-level JSON diff.
     */
    protected boolean scaleCapture()
    {
        return false;
    }

    public static final class CapturedSSTable
    {
        final Path dir;                 // copied component files, named by component (e.g. "Data.db")
        final String json;              // canonical logical dump; a SHA-256 digest string in scale mode
        final String statsSummary;
        final SortedMap<String, Long> componentSizes = new TreeMap<>();

        CapturedSSTable(Path dir, String json, String statsSummary)
        {
            this.dir = dir;
            this.json = json;
            this.statsSummary = statsSummary;
        }
    }

    public static final class CapturedOutput
    {
        final List<CapturedSSTable> sstables = new ArrayList<>();
    }

    /**
     * Concatenates the canonical logical dump of every captured output sstable — or, in scale mode
     * ({@link #scaleCapture()}), the per-sstable SHA-256 DIGEST strings. Any assertion of the form
     * {@code allJson(out).contains(...)} is therefore vacuous in scale mode; a scale scenario must
     * assert through the typed fields of {@link CapturedSSTable} or through a real SELECT.
     */
    protected static String allJson(CapturedOutput out)
    {
        StringBuilder sb = new StringBuilder();
        for (CapturedSSTable s : out.sstables)
            sb.append(s.json);
        return sb.toString();
    }

    /**
     * The rendered form of a text-typed cell holding {@code text}, for ABSOLUTE assertions over
     * {@link #allJson} — i.e. for stating which value a merge must have kept, rather than only that
     * the two paths agreed on one. Byte-equality cannot see a rule that both paths get wrong.
     * <p>
     * {@code JsonTransformer.serializeCell} emits cells as COMPACT objects, so the field name and its
     * value are adjacent with no whitespace. Including the closing quote matters: without it,
     * {@code cellValue("aaa1")} also matches {@code "aaa19"}.
     */
    protected static String cellValue(String text)
    {
        return "\"value\":\"" + text + '"';
    }

    /**
     * NON-OVERLAPPING occurrences of {@code needle} in {@code haystack} — the scan resumes past each
     * match, so {@code countOccurrences("aaaa", "aa")} is 2, not 3. Every needle used here is a
     * {@code "field":"value"} form that cannot overlap itself; a caller whose needle can must not use
     * this. Scenarios use it to pin how MANY cells a merge kept, so a loop of per-value assertions
     * cannot pass by covering nothing.
     */
    protected static int countOccurrences(String haystack, String needle)
    {
        int count = 0;
        for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + needle.length()))
            count++;
        return count;
    }

    /**
     * The latest local deletion time recorded by any of the given sstables that actually has one, for
     * scenarios that need a {@code gcBefore} placed relative to their own tombstones instead of relative
     * to the wall clock.
     * <p>
     * The guard is the point. An sstable with no deletions — or one holding any live cell at all —
     * reports {@link Cell#NO_DELETION_TIME} ({@code Long.MAX_VALUE}) as its max. Reading the field
     * bare and adding one therefore overflows to {@code Long.MIN_VALUE}, a {@code gcBefore} that
     * makes nothing anywhere purgeable, which a scenario asserting that tombstones were RETAINED
     * passes for entirely the wrong reason. Fails instead if the scenario produced no deletion.
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

    /** Creates the CompactionTask for one differential run. MUST honor keepOriginals=true. */
    public interface TaskFactory
    {
        CompactionTask create(ColumnFamilyStore cfs, LifecycleTransaction txn, long gcBefore);
    }

    public static final TaskFactory DEFAULT_TASK = (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, true);

    /**
     * A TaskFactory that pins CompactionTask's internal TTL-expiry "now" to a fixed value
     * instead of wall-clock time, so scenarios with short TTLs don't need to sleep past the
     * expiry boundary. keepOriginals mirrors {@link #DEFAULT_TASK}.
     */
    public static TaskFactory taskWithFixedNow(long nowInSeconds)
    {
        return (cfs, txn, gcBefore) -> new CompactionTask(cfs, txn, gcBefore, true).setNowInSecondsSupplier(() -> nowInSeconds);
    }

    /**
     * Pins the precondition of a fixed-now TTL scenario: at least one cell in the current live set
     * really has expired relative to nowInSeconds. A pinned "now" does not advance with the wall
     * clock while the scenario writes. A scenario that derived the pin before its write phase and
     * then outran it would keep passing while it compared only unexpired cells. Derive the pinned
     * value after the last flush and call this.
     * <p>
     * What it does NOT establish: that the specific cells a scenario's absolute assertions name are
     * the expired ones. Row, range and partition deletions feed {@code minLocalDeletionTime} as well
     * as TTL expiry, and a tombstone's local deletion time is its write second, hence always at or
     * below a now pinned just past it. So a scenario carrying both a tombstone and a long TTL
     * satisfies this while its expiring cells sit far above the pin. {@code StatsMetadata.minTTL}
     * cannot close that gap: a plain cell contributes the no-TTL sentinel, so any sstable holding one
     * reports the sentinel however many expiring cells it also holds. Keep the scenario's own
     * absolute assertions doing that work.
     */
    protected void assertSomethingExpiredAt(ColumnFamilyStore cfs, long nowInSeconds)
    {
        long minLocalDeletionTime = Long.MAX_VALUE;
        for (SSTableReader sstable : cfs.getLiveSSTables())
            minLocalDeletionTime = Math.min(minLocalDeletionTime, sstable.getSSTableMetadata().minLocalDeletionTime);
        assertTrue("no cell in the live set has expired relative to the pinned now " + nowInSeconds +
                   "; the earliest local deletion time on disk is " + minLocalDeletionTime,
                   minLocalDeletionTime <= nowInSeconds);
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
     * Variant for partial-set compactions (inputs is a subset of the live sstables; the rest
     * stay live and participate in purge-overlap decisions) and for custom CompactionTask
     * shapes (e.g. multi-output writers via an overridden getCompactionAwareWriter).
     * <p>
     * Returns the iterator-path capture so scenarios can assert structural expectations. A
     * multi-output scenario MUST verify that more than one sstable was produced, because a
     * scenario that does not exercise its mechanism passes vacuously.
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs,
                                                         Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory) throws Exception
    {
        return assertCursorMatchesIterator(cfs, inputs, taskFactory,
                                           cfs.getDefaultGcBefore(FBUtilities.nowInSeconds()));
    }

    /**
     * Variant with an explicit gcBefore: lets scenarios place purge decisions EXACTLY at the
     * boundary (purge requires localDeletionTime < gcBefore) without controlling the wall
     * clock — read the actual deletion time from the flushed sstable's stats, then run with
     * gcBefore == ldt (retained) and gcBefore == ldt + 1 (purged).
     */
    protected CapturedOutput assertCursorMatchesIterator(ColumnFamilyStore cfs,
                                                         Set<SSTableReader> inputs,
                                                         TaskFactory taskFactory,
                                                         long gcBefore) throws Exception
    {
        Path scratch = Files.createTempDirectory("differential-compaction");

        // Early open stays ENABLED here deliberately: SSTableRewriter.moveStarts obsoletes a
        // fully-covered input unless keepOriginals is set, and this harness depends on the
        // originals surviving, so every differential run doubles as the regression test for
        // that guard.
        //
        // scratch holds byte-for-byte copies of every captured output sstable, for both paths.
        // The harness deletes it as soon as the comparison that needs it is done: one fork runs
        // hundreds of invocations (e.g. the randomized soak), so leaving cleanup to the temp-dir
        // removal at JVM exit grows disk usage without bound over a single run.
        boolean passed = false;
        try
        {
            CapturedOutput iterator = compactPath(cfs, inputs, false, gcBefore, scratch.resolve("iterator"), taskFactory);
            // the input INSTANCES were replaced during restore; re-resolve the subset by descriptor
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
            // A failed comparison deletes the copies too, by default. restoreAfterCompaction has
            // already deleted the real output sstables and a re-run writes fresh timestamps, so
            // scratch holds the only reproducible evidence of what diverged. That matters most for
            // a LOGICAL divergence, which fails before the byte loop and so reports no offsets and
            // no hex context. KEEP_SCRATCH_ON_FAILURE preserves scratch for a local debugging run.
            if (passed || !KEEP_SCRATCH_ON_FAILURE)
            {
                // never rethrown: an IOException here would replace the AssertionError carrying the
                // whole divergence report (if any) with an unrelated "Unable to delete directory"
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
     * Differential at TWO generations: the normal differential first (gen 1), then the inputs
     * are genuinely compacted through the CURSOR path and the differential runs again over the
     * cursor-produced outputs (gen 2). What gen 2 adds is INPUT SHAPES no flush in these
     * scenarios produces. {@link org.apache.cassandra.db.SerializationHeader#make} gives its output
     * the union of the inputs' column supersets, wider than any one of them wherever the scenario's
     * flushes wrote different columns. It also gives that output an EncodingStats base merged from
     * the inputs' StatsMetadata minima, over a wider set than any single flush covers, so purge or
     * merge can leave the base strictly below every surviving row. The commit step in between also
     * runs with keepOriginals=false, the real obsoletion path, which the differential runs
     * themselves never take.
     *
     * Gen 2 is NOT a backstop for write-side corruption. Like gen 1, it hands the SAME input
     * bytes to both readers, so a defect the two interpret alike passes at either generation.
     * What the cursor WRITER put on disk is pinned by gen 1's per-component byte comparison, its
     * logical dump, and the extended verification run over every output.
     *
     * Returns the GEN-1 iterator capture: scenario structural assertions target gen 1, whose
     * shape the scenario controls directly.
     */
    protected CapturedOutput assertCursorMatchesIteratorAcrossGenerations(ColumnFamilyStore cfs) throws Exception
    {
        return assertCursorMatchesIteratorAcrossGenerations(cfs, FBUtilities::nowInSeconds);
    }

    /**
     * As above, but pins CompactionTask's internal TTL-expiry "now" to nowInSecondsSupplier
     * for every run (both generations) instead of reading the wall clock, so scenarios with
     * short TTLs don't need to sleep past the expiry boundary.
     */
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
     * Commits one compaction over the given inputs through the selected path WITHOUT restore:
     * the live set genuinely becomes the outputs. Used by the cross-generation rung so the
     * second differential reads cursor-produced sstables.
     */
    protected void commitCompaction(ColumnFamilyStore cfs, Set<SSTableReader> inputs, boolean cursor, long gcBefore) throws Exception
    {
        commitCompaction(cfs, inputs, cursor, gcBefore, FBUtilities::nowInSeconds);
    }

    /** As above, but pins CompactionTask's internal TTL-expiry "now" to nowInSecondsSupplier. */
    protected void commitCompaction(ColumnFamilyStore cfs, Set<SSTableReader> inputs, boolean cursor, long gcBefore,
                                    LongSupplier nowInSecondsSupplier) throws Exception
    {
        DatabaseDescriptor.setCursorCompactionEnabled(cursor);
        if (cursor)
            assertCursorPathWillRun(cfs, inputs, gcBefore);
        LifecycleTransaction txn = cfs.getTracker().tryModify(inputs, OperationType.COMPACTION);
        assertNotNull("unable to mark inputs compacting for commit", txn);
        // Same reasoning as compactPath: isSupported is supportability, not execution. This
        // commit feeds the cross-generation rung's gen-2 inputs, so a silent fallback here would
        // mean gen 2 never actually re-reads cursor-written output at all.
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        new CompactionTask(cfs, txn, gcBefore, false).setNowInSecondsSupplier(nowInSecondsSupplier).execute(ActiveCompactionsTracker.NOOP);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);
    }

    /**
     * Runs one compaction path over the given input subset (non-participating live sstables
     * stay live and feed purge-overlap decisions), captures the outputs, and restores the live
     * set so the other path sees identical bytes.
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
        // assertCursorPathWillRun only asserts CursorCompactor.isSupported, which is
        // supportability, not execution: AbstractCompactionPipeline.create also gates on
        // DatabaseDescriptor.cursorCompactionEnabled(), which isSupported never reads. Without
        // this, a scenario that silently fell back to the iterator path would compare the
        // iterator's output against itself and pass.
        CompactionPipelineCounts before = CompactionPipelineCounts.mark();
        taskFactory.create(cfs, txn, gcBefore).execute(ActiveCompactionsTracker.NOOP);
        CompactionPipelineCounts.assertPipelineRan(cursor, before);

        // Outputs are identified by descriptor diff against the pre-compaction live set:
        // with keepOriginals=true the originals (or early-open clones with moved starts) may
        // remain live as DIFFERENT reader instances, and non-participating sstables are live
        // throughout. Instance identity is never trusted here.
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
     * Delists + releases outputs and any retained input clones, deletes output files only,
     * then reopens every input fresh from its descriptor so a subsequent run sees pristine
     * full-range readers identical to this run's. Non-participating sstables are untouched.
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

    /** Output identification by before/after descriptor diff; see compactPath for rationale. */
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

    /**
     * Guards against the silent-fallback trap: if the cursor path would not actually run for
     * this scenario, the test would compare iterator vs iterator and pass vacuously. Uses the
     * same isSupported check production uses, on equivalent scanners and controller.
     */
    protected void assertCursorPathWillRun(ColumnFamilyStore cfs, Set<SSTableReader> inputs, long gcBefore) throws Exception
    {
        assumeBigFormatSelected();
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

    /**
     * Cursor compaction only supports BIG output (CursorCompactor.isSupported). Under a non-BIG
     * format — `ant test-latest` selects BTI — every scenario in this suite would fail for a reason
     * that is not a defect. Skip instead, and keep the supportability assertion for every other
     * unsupported-ness reason so the iterator-vs-iterator trap still fires.
     * <p>
     * Separate from {@link #assertCursorPathWillRun} so a scenario that drives the harness from
     * inside a callback can raise it OUTSIDE that callback. JUnit decides skip-versus-fail on the
     * type it receives, so an AssumptionViolatedException crossing a broad catch that rewraps — as
     * Harry's TestHelper.withRandom does — arrives as a failure.
     */
    protected static void assumeBigFormatSelected()
    {
        Assume.assumeTrue("cursor compaction requires the BIG sstable format; selected=" +
                          DatabaseDescriptor.getSelectedSSTableFormat().name(),
                          BigFormat.isSelected());
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

    private CapturedSSTable capture(ColumnFamilyStore cfs, SSTableReader sstable, Path dir) throws IOException
    {
        // 1. structural verification of the output. In scale mode the verifier's debug
        // stream must be silenced: the extended index walk debug-logs EVERY index block
        // (~560K lines for a >2GiB partition), and ant's junit formatter buffers all test
        // output in memory — the log volume, not the verification, OOMs the fork.
        OutputHandler verifyOutput = scaleCapture()
            ? new OutputHandler.LogOutput() { @Override public void debug(String msg) {} }
            : new OutputHandler.LogOutput();
        try (IVerifier verifier = sstable.getVerifier(cfs, verifyOutput, false,
                                                      IVerifier.options().invokeDiskFailurePolicy(true)
                                                                         .extendedVerification(true).build()))
        {
            verifier.verify();
        }

        // 2. canonical logical dump
        // JsonTransformer computes its "expired" fields from WALL CLOCK (currentTimeMillis),
        // ignoring the fixed nowInSec passed below. Byte-identical outputs therefore render
        // differently when a localExpirationTime falls between the two paths' captures, which run
        // seconds apart. Materialized-view expired-liveness rows sit permanently on that boundary:
        // their expiration IS the write second. The flag is derived from expires_at, which is still
        // compared, so normalize it out.
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

        // 3. stats spot-check summary
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
                              " tombstoneHist=" + stats.estimatedTombstoneDropTime +
                              " cellsPerPartition=" + stats.estimatedCellPerPartitionCount.mean() + "/" + stats.estimatedCellPerPartitionCount.count();

        // 4. copy components for byte comparison
        Files.createDirectories(dir);
        CapturedSSTable captured = new CapturedSSTable(dir, json, statsSummary);
        for (Component c : sstable.descriptor.discoverComponents())
        {
            Path source = sstable.descriptor.fileFor(c).toPath();
            Path target = dir.resolve(c.name());
            Files.copy(source, target);
            captured.componentSizes.put(c.name(), Files.size(target));
        }
        return captured;
    }

    protected void assertEquivalentOutputs(CapturedOutput iterator, CapturedOutput cursor)
    {
        assertEquals("output sstable count differs between paths", iterator.sstables.size(), cursor.sstables.size());
        for (int i = 0; i < iterator.sstables.size(); i++)
        {
            CapturedSSTable it = iterator.sstables.get(i);
            CapturedSSTable cu = cursor.sstables.get(i);

            // logical first: a row-level diff is far more debuggable than a stats mismatch.
            // In scale mode the dump is a digest — defer it below the byte comparison, which
            // still localizes divergences to exact offsets.
            boolean digestMode = it.json.startsWith("sha256:");
            if (!digestMode && !it.json.equals(cu.json))
                fail("LOGICAL divergence in output sstable " + i + " (iterator vs cursor):\n" + firstJsonDiff(it.json, cu.json) +
                     "\niterator stats: " + it.statsSummary + "\ncursor stats:   " + cu.statsSummary);

            assertEquals("stats summary divergence in output sstable " + i, it.statsSummary, cu.statsSummary);

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
            if (!divergences.isEmpty())
                fail("BYTE divergence in output sstable " + i + " (iterator vs cursor):\n" + String.join("\n", divergences) +
                     "\nNothing is allowed to diverge: every divergence found to date has been a bug in one of the paths");

            if (digestMode)
                assertEquals("logical dump digest divergence in output sstable " + i +
                             " (scale mode; rerun a reduced scenario without scale mode for a row-level diff)",
                             it.json, cu.json);
        }
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
     * Streams a JSON dump into a digest, normalizing the wall-clock-derived "expired"
     * fields. Buffers up to a line (toJsonLines emits one partition per line), but flushes
     * oversized lines in bounded chunks so memory stays flat even for multi-GB partitions.
     * The cut points are functions of CONTENT ONLY (buffer fill, not write() granularity), so
     * two captures of identical bytes cut in identical places. The cut is NOT token-safe, though.
     * A chunk boundary can fall inside an "expired" token, and both halves then miss the pattern
     * and go un-normalized. That stays harmless while the two captures render the flag
     * identically, and becomes a divergence if they do not: the two renderings differ in length,
     * so a mismatch also shifts every later cut point. Only a line above FLUSH_THRESHOLD is cut
     * at all, i.e. only the giant-partition scenario.
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

        /** Digest all buffered content (end of a line or of the stream). */
        void flushTail()
        {
            if (line.size() == 0)
                return;
            update(line.toByteArray(), line.size());
            line.reset();
        }

        /** Digest all but the last TAIL_KEEP buffered bytes, so a token still incomplete at the end
         *  of the buffer survives into the next write: TAIL_KEEP exceeds the longest token, so such
         *  a token cannot have begun before the cut. */
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
            String la = i < linesA.length ? linesA[i] : "<missing>";
            String lb = i < linesB.length ? linesB[i] : "<missing>";
            if (!la.equals(lb))
            {
                StringBuilder sb = new StringBuilder();
                sb.append("first differing line ").append(i + 1).append(" of ").append(max).append(":\n");
                for (int j = Math.max(0, i - 2); j < Math.min(max, i + 3); j++)
                {
                    String ja = j < linesA.length ? linesA[j] : "<missing>";
                    String jb = j < linesB.length ? linesB[j] : "<missing>";
                    sb.append(j == i ? ">>" : "  ").append(" iterator: ").append(ja).append('\n');
                    sb.append(j == i ? ">>" : "  ").append(" cursor:   ").append(jb).append('\n');
                }
                return sb.toString();
            }
        }
        return "(no line diff found despite string inequality — check line endings)";
    }
}
