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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Decides whether one sstable can be anticompacted by {@link ZeroCopySSTableSplitter} rather than the three-writer
 * rewrite in {@code CompactionManager.antiCompactGroup}, and if so produces the split boundaries and per-child
 * repair state. BIG and BTI, and an unsupported sstable is reported ineligible rather than thrown on, because
 * {@link #plan} runs for every sstable of every anticompaction group and its caller just falls back to the rewrite;
 * a repair session cancelled during the walk comes back the same way, as an {@link Plan#interrupted} plan.
 * <p>
 * <b>The gate.</b> Every partition is labelled {@link Label#FULL}, {@link Label#TRANSIENT} or
 * {@link Label#UNREPAIRED} by token, using the same {@code Range.OrderedRangeContainmentChecker} pair and
 * full-wins-over-transient precedence as {@code antiCompactGroup}. The labels are run-length encoded and the
 * sstable is eligible iff FULL and TRANSIENT each occupy at most one run, since a child can only be one
 * contiguous key range. UNREPAIRED may legitimately lead and trail, so {@code UNREPAIRED, FULL, UNREPAIRED} is
 * the common eligible shape; the interleaving vnodes produce is ineligible and falls back to the rewrite.
 * <p>
 * <b>An eligible sstable loses tombstone purging.</b> The rewrite path runs every partition through a
 * {@code CompactionController} and therefore drops droppable tombstones and shadowed data; a verbatim chunk copy
 * cannot, so the children RETAIN everything the parent held. That is retention, never loss -- nothing can be
 * resurrected -- but a deliberate behaviour change, independent of the droppable-tombstone ratio, gated by
 * {@code zero_copy_anticompaction_enabled} and logged at INFO on every use.
 * <p>
 * <b>What the labelling walk reads.</b> One sequential pass over the parent's primary index, driven through
 * {@code ZeroCopySSTableSplitter.walkIndex} so the planner and the splitter cannot disagree about the record order
 * the run starts index into. For BIG that is Index.db only: 1-3% of the data, page-cache warm by the time the
 * splitter re-reads it. For BTI it walks Partitions.db and reads each key from Rows.db or -- for a partition with no
 * row index -- from Data.db, so a narrow-partition BTI table has its data decompressed here and again inside
 * {@code split}. That is the cost of BTI keeping full keys nowhere but the data file (see {@code BtiZeroCopySplit});
 * it is still less work than the rewrite it replaces, and collapsing the two passes is a follow-up rather than a
 * correctness matter.
 * <p>
 * The gate is per partition, so a FULL child's {@code [first, last]} may span a hole in the full ranges where no
 * partition lives. Nothing rejects that -- {@code validateSSTableBoundsForAnticompaction} runs only on the parent
 * set, before anticompaction -- and today's {@code fullWriter} does the same with non-adjacent partitions.
 */
public final class AntiCompactionRunPlanner
{
    /**
     * Cap on runs retained in detail: the largest eligible shape is {@code UNREPAIRED, FULL, UNREPAIRED,
     * TRANSIENT, UNREPAIRED} = 5 runs, so past the cap the sstable is certainly ineligible and the walk stops
     * retaining boundary keys rather than hold one key per partition for an alternating vnode layout.
     */
    private static final int MAX_RETAINED_RUNS = 8;

    /** How one partition is classified, by token, for this repair session. FULL wins over TRANSIENT. */
    public enum Label
    {
        FULL,
        TRANSIENT,
        UNREPAIRED
    }

    /** The verdict, plus everything the split needs when the verdict is "eligible". */
    public static final class Plan
    {
        /** True iff the split can produce exactly this anticompaction's output. */
        public final boolean eligible;
        /**
         * True iff the walk was abandoned because the repair session was cancelled, so this is not a verdict about
         * the sstable at all and the caller should stop planning rather than read it as "rewrite this one". Never
         * true together with {@link #eligible}.
         */
        public final boolean interrupted;
        /** For logging; null when {@link #eligible}. */
        public final String ineligibleReason;
        /** {@code runCount - 1} interior split points, in the form {@code ZeroCopySSTableSplitter} wants. */
        public final List<DecoratedKey> boundaries;
        /** One repair state per run, in order; {@code boundaries.size() + 1} entries. */
        public final List<ZeroCopySSTableSplitter.RepairState> perChild;
        /**
         * Number of contiguous label runs found. Meaningful (and exact) for every verdict the walk reached; zero
         * for an {@link #interrupted} plan, whose walk stopped part way through.
         * <p>
         * The walk deliberately does NOT stop early once a second FULL or TRANSIENT run has settled the verdict,
         * even though no later record could change it. Keeping the count exact is what lets the fuzz test check
         * the Index.db walk against an independently computed label sequence, which is worth more than saving part
         * of one Index.db read (a couple of percent of the data) per ineligible sstable.
         */
        public final int runCount;

        private Plan(boolean eligible,
                     boolean interrupted,
                     String ineligibleReason,
                     List<DecoratedKey> boundaries,
                     List<ZeroCopySSTableSplitter.RepairState> perChild,
                     int runCount)
        {
            this.eligible = eligible;
            this.interrupted = interrupted;
            this.ineligibleReason = ineligibleReason;
            this.boundaries = boundaries;
            this.perChild = perChild;
            this.runCount = runCount;
        }

        static Plan ineligible(String reason, int runCount)
        {
            return new Plan(false, false, reason, ImmutableList.of(), ImmutableList.of(), runCount);
        }

        /** Not a verdict: the walk was abandoned, so it counted nothing and decided nothing. */
        static Plan cancelled()
        {
            return new Plan(false, true, "the repair session was cancelled during the index walk",
                            ImmutableList.of(), ImmutableList.of(), 0);
        }

        static Plan eligible(List<DecoratedKey> boundaries, List<ZeroCopySSTableSplitter.RepairState> perChild)
        {
            Preconditions.checkArgument(perChild.size() == boundaries.size() + 1,
                                        "perChild must have one entry per range, got %s for %s boundaries",
                                        perChild.size(), boundaries.size());
            return new Plan(true, false, null, ImmutableList.copyOf(boundaries), ImmutableList.copyOf(perChild),
                            perChild.size());
        }

        @Override
        public String toString()
        {
            if (interrupted)
                return String.format("Plan[interrupted: %s]", ineligibleReason);
            return eligible ? String.format("Plan[eligible runs=%d]", runCount)
                            : String.format("Plan[ineligible runs=%d: %s]", runCount, ineligibleReason);
        }
    }

    private AntiCompactionRunPlanner()
    {
    }

    /**
     * Plan the zero-copy anticompaction of one sstable, which must still be live and marked compacting;
     * {@code sessionID} is stamped onto the FULL and TRANSIENT children. Reads the parent's primary index, plus --
     * for a BTI parent whose partitions have no row indexes -- its Data.db, to recover the keys. Throws
     * {@link CorruptSSTableException} if that walk fails, but never for a merely ineligible sstable.
     */
    public static Plan plan(SSTableReader sstable, RangesAtEndpoint ranges, TimeUUID sessionID)
    {
        return plan(sstable, ranges, sessionID, () -> false);
    }

    /**
     * As {@link #plan(SSTableReader, RangesAtEndpoint, TimeUUID)}, but with {@code isCancelled} -- the repair
     * session's own cancellation predicate -- consulted periodically during the index walk. The walk is the first
     * thing in an anticompaction that takes real time and is not interruptible on its own, so a session cancelled
     * during it (coordinator timeout, {@code nodetool repair_admin cancel}) would otherwise go unnoticed until the
     * split moved bytes. A cancellation abandons the walk and comes back as an {@link Plan#interrupted} plan rather
     * than an exception, in keeping with this class answering a question rather than doing work; the caller is
     * expected to stop planning, not to read it as "rewrite this one".
     */
    public static Plan plan(SSTableReader sstable,
                            RangesAtEndpoint ranges,
                            TimeUUID sessionID,
                            BooleanSupplier isCancelled)
    {
        Preconditions.checkNotNull(sstable, "sstable");
        Preconditions.checkNotNull(ranges, "ranges");
        Preconditions.checkNotNull(isCancelled, "isCancelled");
        // a null session id would silently stamp FULL/TRANSIENT children as plain unrepaired
        Preconditions.checkNotNull(sessionID, "sessionID");

        // reported separately from the check below so an operator on a format that is neither sees the real
        // reason, not a message about compression
        if (!ZeroCopySSTableSplitter.isSupportedFormat(sstable.descriptor.getFormat()))
            return Plan.ineligible("unsupported sstable format '" + sstable.descriptor.getFormat().name() +
                                   "': the zero-copy split needs an index whose partition positions can be " +
                                   "rebased, which is BIG and BTI", 0);

        if (!ZeroCopySSTableSplitter.isSupported(sstable))
            return Plan.ineligible("not a compressed BIG- or BTI-format sstable (format=" +
                                   sstable.descriptor.getFormat().name() +
                                   ", compressed=" + sstable.compression + ')', 0);

        // Before the walk, which is the expensive part: a child only gets the components the splitter knows how to
        // write, so a parent carrying any other one -- storage-attached index components above all -- would produce
        // children that are live, readable and invisible to every index predicate. The rewrite path builds those
        // components inline, so declining costs nothing but the copy. See ZeroCopySSTableSplitter.unhandledComponents
        // for why this is a backstop and SecondaryIndexManager.hasSSTableAttachedIndexes is the authoritative gate.
        Set<Component> unhandled = ZeroCopySSTableSplitter.unhandledComponents(sstable);
        if (!unhandled.isEmpty())
            return Plan.ineligible("carries components a child would not get: " + unhandled, 0);

        return walk(sstable, ranges, sessionID, isCancelled);
    }

    /**
     * Everything after the Index.db walk, over an already-labelled sequence ({@code labels} in on-disk token order,
     * {@code keys} parallel to it), so the run logic can be unit tested with no sstable. Only the first key of each
     * run is ever used, so a verdict-only test may pass any strictly increasing keys.
     */
    @VisibleForTesting
    static Plan planFromLabels(List<Label> labels, List<DecoratedKey> keys, TimeUUID sessionID)
    {
        Preconditions.checkNotNull(labels, "labels");
        Preconditions.checkNotNull(keys, "keys");
        Preconditions.checkArgument(labels.size() == keys.size(),
                                    "labels and keys must be parallel, got %s and %s",
                                    labels.size(), keys.size());

        return finish(encodeRuns(labels, keys), sessionID);
    }

    /**
     * Pure run-length encoding of a label sequence: run {@code b} starts at partition {@code runFirstKeys.get(b)}.
     * The counters are always exact; the two lists only while the sstable can still turn out to be eligible.
     */
    @VisibleForTesting
    static final class RunEncoding
    {
        int runCount;
        int fullRuns;
        int transientRuns;
        int unrepairedRuns;
        /** Label and first key of each run, in order; both cleared once {@link #MAX_RETAINED_RUNS} is passed. */
        final List<Label> runLabels = new ArrayList<>(MAX_RETAINED_RUNS);
        final List<DecoratedKey> runFirstKeys = new ArrayList<>(MAX_RETAINED_RUNS);

        void add(Label label, DecoratedKey firstKey)
        {
            runCount++;
            switch (label)
            {
                case FULL:
                    fullRuns++;
                    break;
                case TRANSIENT:
                    transientRuns++;
                    break;
                default:
                    unrepairedRuns++;
                    break;
            }

            if (runCount > MAX_RETAINED_RUNS)
            {
                // stop retaining so a pathological interleaving cannot pin one key per partition on the heap
                runLabels.clear();
                runFirstKeys.clear();
                return;
            }
            runLabels.add(label);
            runFirstKeys.add(firstKey);
        }
    }

    @VisibleForTesting
    static RunEncoding encodeRuns(List<Label> labels, List<DecoratedKey> keys)
    {
        RunEncoding runs = new RunEncoding();
        Label previous = null;
        for (int i = 0; i < labels.size(); i++)
        {
            Label label = labels.get(i);
            if (label != previous)
            {
                runs.add(label, keys.get(i));
                previous = label;
            }
        }
        return runs;
    }

    private static Plan finish(RunEncoding runs, TimeUUID sessionID)
    {
        if (runs.runCount == 0)
            return Plan.ineligible("sstable has no partitions", 0);

        // Not "already handled elsewhere": doAntiCompaction runs only AFTER both mutateFullyContainedSSTables
        // calls, so a single-FULL-run sstable here is one that path DECLINED (it needs one normalized range around
        // both the first and the last token, and Range.normalize will not merge across a gap). All-UNREPAIRED is
        // rewritten in full; there is no no-op path.
        if (runs.runCount == 1)
            return Plan.ineligible("entire sstable is " + runs.runLabels.get(0) +
                                   " (one run, so there is no boundary to cut at; the rewrite path handles it)",
                                   1);

        if (runs.fullRuns > 1)
            return Plan.ineligible("FULL appears in " + runs.fullRuns + " runs (interleaved ranges)", runs.runCount);

        if (runs.transientRuns > 1)
            return Plan.ineligible("TRANSIENT appears in " + runs.transientRuns + " runs (interleaved ranges)",
                                   runs.runCount);

        // FULL <= 1 and TRANSIENT <= 1 caps the shape at UNREPAIRED,FULL,UNREPAIRED,TRANSIENT,UNREPAIRED
        if (runs.runLabels.size() != runs.runCount)
            throw new IllegalStateException("run detail was dropped for an eligible sstable: " + runs.runCount +
                                            " runs but " + runs.runLabels.size() + " retained");

        List<ZeroCopySSTableSplitter.RepairState> perChild = new ArrayList<>(runs.runCount);
        for (Label label : runs.runLabels)
            perChild.add(stateFor(label, sessionID));

        // The boundary into run b is the FIRST key of run b: the splitter starts run b at the first record whose
        // key is >= boundaries[b - 1]. Run 0 has no boundary (unbounded below).
        return Plan.eligible(runs.runFirstKeys.subList(1, runs.runFirstKeys.size()), perChild);
    }

    /**
     * The exact triples {@code antiCompactGroup} hands to {@code createWriterForAntiCompaction}; {@code repairedAt}
     * is set later, by {@code PendingRepairManager.RepairFinishedCompactionTask}, never at anticompaction time.
     */
    @VisibleForTesting
    static ZeroCopySSTableSplitter.RepairState stateFor(Label label, TimeUUID sessionID)
    {
        switch (label)
        {
            case FULL:
                return new ZeroCopySSTableSplitter.RepairState(ActiveRepairService.UNREPAIRED_SSTABLE,
                                                               sessionID, false);
            case TRANSIENT:
                return new ZeroCopySSTableSplitter.RepairState(ActiveRepairService.UNREPAIRED_SSTABLE,
                                                               sessionID, true);
            default:
                return new ZeroCopySSTableSplitter.RepairState(ActiveRepairService.UNREPAIRED_SSTABLE,
                                                               ActiveRepairService.NO_PENDING_REPAIR, false);
        }
    }

    /**
     * Thrown out of the walk callback when {@code isCancelled} fires, and caught by {@link #walk} alone:
     * {@code ZeroCopySSTableSplitter.walkIndex} takes a consumer that has no way to say "stop", and finishing the
     * walk to be polite about it is exactly what the check exists to avoid. Control flow, never reported, so no
     * message, no stack trace.
     */
    private static final class Cancelled extends RuntimeException
    {
        Cancelled()
        {
            super(null, null, false, false);
        }
    }

    /**
     * One sequential pass over Index.db, labelling and run-length encoding as it goes.
     * <p>
     * {@code OrderedRangeContainmentChecker} is stateful and forward-only: its cursor never rewinds, so the two
     * checkers must be distinct, fresh per sstable, and fed tokens in non-decreasing order -- which an Index.db walk
     * gives by construction, on-disk order being DecoratedKey order and so token-major. The {@code isEmpty()} guards
     * are mandatory: the constructor asserts the normalized range list is non-empty. Calling {@code transChecker}
     * only when {@code fullChecker} said no is safe because a token's cursor position is a monotone function of that
     * token alone, so skipping tokens is identical to seeing them.
     */
    private static Plan walk(SSTableReader sstable,
                             RangesAtEndpoint ranges,
                             TimeUUID sessionID,
                             BooleanSupplier isCancelled)
    {
        Predicate<Token> fullChecker = !ranges.onlyFull().isEmpty()
                                       ? new Range.OrderedRangeContainmentChecker(ranges.onlyFull().ranges())
                                       : t -> false;
        Predicate<Token> transChecker = !ranges.onlyTransient().isEmpty()
                                        ? new Range.OrderedRangeContainmentChecker(ranges.onlyTransient().ranges())
                                        : t -> false;

        IPartitioner partitioner = sstable.getPartitioner();
        RunEncoding runs = new RunEncoding();
        Label[] previous = { null };

        // One pass over whichever primary index the parent has, driven by the splitter so that the two cannot
        // disagree about the record order the run starts are indices into. For BIG that is the same buffered
        // Index.db read as before; for BTI it walks Partitions.db and resolves keys, which for a table of narrow
        // partitions means decompressing Data.db -- see ZeroCopySSTableSplitter.walkIndex. That is one such pass
        // here and a second one inside split(): a BTI anticompaction reads the parent's data twice, which is worth
        // collapsing later but is still one read fewer than the rewriting path it replaces.
        try
        {
            ZeroCopySSTableSplitter.walkIndex(sstable, (index, key, position) -> {
                // The record ordinal is the counter, so the cancellation cadence is the same 1-in-1024 the
                // splitter's own stop check uses -- and it covers the BTI walk, which is the slower of the two.
                if ((index & 0x3FF) == 0 && isCancelled.getAsBoolean())
                    throw new Cancelled();

                DecoratedKey dk = partitioner.decorateKey(key);
                Token token = dk.getToken();
                // full wins over transient, exactly as antiCompactGroup routes partitions
                Label label = fullChecker.test(token) ? Label.FULL
                                                      : transChecker.test(token) ? Label.TRANSIENT
                                                                                 : Label.UNREPAIRED;
                if (label != previous[0])
                {
                    // retainable(): the key is kept past the read buffer it came from, then handed to the splitter
                    runs.add(label, dk.retainable());
                    previous[0] = label;
                }
            });
        }
        catch (Cancelled cancelled)
        {
            // Half a label sequence is not a verdict, so nothing partial is reported: the caller stops planning.
            return Plan.cancelled();
        }

        return finish(runs, sessionID);
    }

    /** Labels an explicit key sequence, which must be ascending; same precedence and checkers as the walk. */
    @VisibleForTesting
    static List<Label> labels(List<DecoratedKey> keys, RangesAtEndpoint ranges)
    {
        Predicate<Token> fullChecker = !ranges.onlyFull().isEmpty()
                                       ? new Range.OrderedRangeContainmentChecker(ranges.onlyFull().ranges())
                                       : t -> false;
        Predicate<Token> transChecker = !ranges.onlyTransient().isEmpty()
                                        ? new Range.OrderedRangeContainmentChecker(ranges.onlyTransient().ranges())
                                        : t -> false;

        List<Label> labels = new ArrayList<>(keys.size());
        for (DecoratedKey key : keys)
        {
            Token token = key.getToken();
            labels.add(fullChecker.test(token) ? Label.FULL
                                               : transChecker.test(token) ? Label.TRANSIENT : Label.UNREPAIRED);
        }
        return Collections.unmodifiableList(labels);
    }
}
