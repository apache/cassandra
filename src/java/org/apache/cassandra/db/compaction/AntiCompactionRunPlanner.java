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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Decides whether one sstable can be anticompacted by {@link ZeroCopySSTableSplitter} instead of by the
 * three-writer rewrite in {@code CompactionManager.antiCompactGroup}, and if so produces the split boundaries
 * and the per-child repair state.
 *
 * <h2>The gate</h2>
 * Every partition of the sstable is labelled {@link Label#FULL}, {@link Label#TRANSIENT} or
 * {@link Label#UNREPAIRED} by token, using the same {@code Range.OrderedRangeContainmentChecker} pair and the
 * same full-wins-over-transient precedence as {@code antiCompactGroup}. The label sequence is then run-length
 * encoded, and the sstable is eligible iff FULL occupies at most one run and TRANSIENT occupies at most one run
 * -- i.e. every output child is a single contiguous key range, which is the only thing the splitter can
 * produce. UNREPAIRED may legitimately appear as a leading and a trailing run, so {@code UNREPAIRED, FULL,
 * UNREPAIRED} (3 runs) is the common eligible shape. Interleaved / alternating ranges, which is what vnodes
 * produce, are ineligible and fall back to the unchanged rewrite path.
 *
 * <h2>BIG format only</h2>
 * The splitter copies Data.db chunks verbatim and rebases exactly one vint field per Index.db record, so it is
 * inherently specific to the BIG format; BTI has no Index.db and no single rebaseable position field. A non-BIG
 * (or uncompressed) sstable is therefore reported ineligible with its own reason -- {@link #plan} never throws
 * for an sstable it simply cannot handle, because it is called for every sstable of every anticompaction group
 * and the caller silently falls back to the rewrite.
 *
 * <h2>An eligible sstable loses tombstone purging</h2>
 * The rewrite path runs every partition through a {@code CompactionController} and therefore drops droppable
 * tombstones and shadowed data; a verbatim chunk copy cannot. Saying "eligible" here means accepting that the
 * children RETAIN everything the parent held. That is retention, never loss -- nothing can be resurrected --
 * but it is a deliberate behaviour change, gated by {@code zero_copy_anticompaction_enabled} and logged at INFO
 * on every use. It is not conditioned on the droppable-tombstone ratio.
 *
 * <h2>Only Index.db is read</h2>
 * The labelling walk is a single sequential pass over Index.db -- the same buffered
 * {@code RandomAccessReader} pattern the splitter itself uses -- and never touches Data.db. Typically that is
 * 1-3% of the data size and page-cache warm by the time the splitter re-reads it.
 *
 * <h2>The FULL child's bounds can span a gap</h2>
 * The gate is per partition, so a FULL child's {@code [first, last]} may span a hole in the full ranges where
 * no partition happens to live. Nothing rejects that -- {@code validateSSTableBoundsForAnticompaction} runs
 * only on the parent set, before anticompaction -- and it is exactly what today's {@code fullWriter} produces
 * when it routes non-adjacent partitions into one output sstable.
 */
public final class AntiCompactionRunPlanner
{
    /**
     * The most runs worth remembering in detail. FULL and TRANSIENT are capped at one run each, so the largest
     * eligible shape is {@code UNREPAIRED, FULL, UNREPAIRED, TRANSIENT, UNREPAIRED} = 5 runs. Past this cap the
     * sstable is certainly ineligible, so the walk stops retaining boundary keys and just keeps counting -- an
     * alternating vnode layout would otherwise retain one key per partition.
     */
    private static final int MAX_RETAINED_RUNS = 8;

    /** How one partition is classified, by token, for this repair session. */
    public enum Label
    {
        /** Inside a full replica range: becomes pending-repair, non-transient. */
        FULL,
        /** Inside a transient replica range (and not a full one): becomes pending-repair, transient. */
        TRANSIENT,
        /** Owned by neither: stays plain unrepaired. */
        UNREPAIRED
    }

    /** The verdict, plus everything the split needs when the verdict is "eligible". */
    public static final class Plan
    {
        /** True iff the zero-copy split can produce exactly this anticompaction's output. */
        public final boolean eligible;
        /** Human-readable, for logging; null when {@link #eligible}. */
        public final String ineligibleReason;
        /** {@code runCount - 1} interior split points, in the form {@code ZeroCopySSTableSplitter} wants. */
        public final List<DecoratedKey> boundaries;
        /** One repair state per run, in order; {@code boundaries.size() + 1} entries. */
        public final List<ZeroCopySSTableSplitter.RepairState> perChild;
        /** Number of contiguous label runs found. Meaningful (and exact) even when ineligible. */
        public final int runCount;

        private Plan(boolean eligible,
                     String ineligibleReason,
                     List<DecoratedKey> boundaries,
                     List<ZeroCopySSTableSplitter.RepairState> perChild,
                     int runCount)
        {
            this.eligible = eligible;
            this.ineligibleReason = ineligibleReason;
            this.boundaries = boundaries;
            this.perChild = perChild;
            this.runCount = runCount;
        }

        static Plan ineligible(String reason, int runCount)
        {
            return new Plan(false, reason, ImmutableList.of(), ImmutableList.of(), runCount);
        }

        static Plan eligible(List<DecoratedKey> boundaries, List<ZeroCopySSTableSplitter.RepairState> perChild)
        {
            Preconditions.checkArgument(perChild.size() == boundaries.size() + 1,
                                        "perChild must have one entry per range, got %s for %s boundaries",
                                        perChild.size(), boundaries.size());
            return new Plan(true, null, ImmutableList.copyOf(boundaries), ImmutableList.copyOf(perChild),
                            perChild.size());
        }

        @Override
        public String toString()
        {
            return eligible ? String.format("Plan[eligible runs=%d]", runCount)
                            : String.format("Plan[ineligible runs=%d: %s]", runCount, ineligibleReason);
        }
    }

    private AntiCompactionRunPlanner()
    {
    }

    /**
     * Plan the zero-copy anticompaction of one sstable. Reads only the sstable's Index.db; never throws for an
     * ineligible sstable, only for an unreadable one.
     *
     * @param sstable   the parent, still live and marked compacting
     * @param ranges    the full and transient ranges of this repair session
     * @param sessionID the repair session id stamped onto FULL and TRANSIENT children
     * @throws CorruptSSTableException if the Index.db walk fails
     */
    public static Plan plan(SSTableReader sstable, RangesAtEndpoint ranges, TimeUUID sessionID)
    {
        Preconditions.checkNotNull(sstable, "sstable");
        Preconditions.checkNotNull(ranges, "ranges");
        // a null session id would silently stamp FULL/TRANSIENT children as plain unrepaired
        Preconditions.checkNotNull(sessionID, "sessionID");

        // Reported separately from the generic check below so that an operator running a non-BIG format sees
        // why zero-copy anticompaction never engages, rather than a message about compression.
        if (!BigFormat.is(sstable.descriptor.getFormat()))
            return Plan.ineligible("unsupported sstable format '" + sstable.descriptor.getFormat().name() +
                                   "': the zero-copy split is BIG-format only (it rebases Index.db partition " +
                                   "positions, which no other format has)", 0);

        if (!ZeroCopySSTableSplitter.isSupported(sstable))
            return Plan.ineligible("not a compressed BIG-format sstable (format=" +
                                   sstable.descriptor.getFormat().name() +
                                   ", compressed=" + sstable.compression + ')', 0);

        return walk(sstable, ranges, sessionID);
    }

    /**
     * The pure form of {@link #plan(SSTableReader, RangesAtEndpoint, TimeUUID)}: everything after the Index.db
     * walk, over an already-labelled partition sequence. Exposed so the run logic can be unit tested with no
     * sstable at all.
     *
     * @param labels one label per partition, in on-disk (token) order
     * @param keys   the matching partition keys, same size as {@code labels}; only the first key of each run is
     *               ever used, so a test that only cares about the verdict may pass any strictly increasing
     *               sequence
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
     * Pure run-length encoding of a label sequence. Run {@code b} starts at partition
     * {@code runFirstKeys.get(b)}; {@code runLabels} and {@code runFirstKeys} are only populated while the
     * sstable can still turn out to be eligible (see {@link #MAX_RETAINED_RUNS}), but the counters are always
     * exact.
     */
    @VisibleForTesting
    static final class RunEncoding
    {
        /** Total number of contiguous runs. Always exact. */
        int runCount;
        /** Per-label run counts. Always exact. */
        int fullRuns;
        int transientRuns;
        int unrepairedRuns;
        /** The label of each run, in order. Cleared and left empty once {@link #MAX_RETAINED_RUNS} is passed. */
        final List<Label> runLabels = new ArrayList<>(MAX_RETAINED_RUNS);
        /** The first key of each run, in order. Cleared and left empty alongside {@link #runLabels}. */
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
                // certainly ineligible from here on; stop retaining so a pathological interleaving cannot
                // hold one key per partition on the heap
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

    /** The eligibility decision and, when eligible, the boundaries plus per-child repair state. */
    private static Plan finish(RunEncoding runs, TimeUUID sessionID)
    {
        if (runs.runCount == 0)
            return Plan.ineligible("sstable has no partitions", 0);

        if (runs.runCount == 1)
            return Plan.ineligible("entire sstable is " + runs.runLabels.get(0) +
                                   " (nothing to split; the fully-contained or no-op path already covers this)",
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

        // The boundary for the transition into run b is the FIRST key of run b: the splitter starts run b at
        // the first record whose key is >= boundaries[b - 1]. Run 0 has no boundary (unbounded below).
        return Plan.eligible(runs.runFirstKeys.subList(1, runs.runFirstKeys.size()), perChild);
    }

    /**
     * The exact triples {@code antiCompactGroup} hands to {@code createWriterForAntiCompaction}:
     * FULL and TRANSIENT become pending-repair for this session (transient only for TRANSIENT), UNREPAIRED
     * stays plain unrepaired. {@code repairedAt} is never set at anticompaction time -- the promotion to
     * {@code repairedAt} happens later, in {@code PendingRepairManager.RepairFinishedCompactionTask}.
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
     * One sequential pass over Index.db, labelling and run-length encoding as it goes.
     * <p>
     * {@code OrderedRangeContainmentChecker} is stateful and forward-only: its cursor never rewinds, so the two
     * checkers must be distinct instances, must be fresh per sstable, and must be fed tokens in non-decreasing
     * order. An Index.db walk satisfies that by construction, since on-disk order is DecoratedKey order, which
     * is token-major. The {@code isEmpty()} guards are mandatory -- the constructor asserts the normalized range
     * list is non-empty. Calling {@code transChecker} only when {@code fullChecker} said no is safe: the cursor
     * position needed for a token is a monotone function of that token alone, so skipping tokens is identical
     * to seeing them.
     */
    private static Plan walk(SSTableReader sstable, RangesAtEndpoint ranges, TimeUUID sessionID)
    {
        Predicate<Token> fullChecker = !ranges.onlyFull().isEmpty()
                                       ? new Range.OrderedRangeContainmentChecker(ranges.onlyFull().ranges())
                                       : t -> false;
        Predicate<Token> transChecker = !ranges.onlyTransient().isEmpty()
                                        ? new Range.OrderedRangeContainmentChecker(ranges.onlyTransient().ranges())
                                        : t -> false;

        IPartitioner partitioner = sstable.getPartitioner();
        RunEncoding runs = new RunEncoding();
        Label previous = null;

        // Buffered rather than mmap'd, and opened straight off the descriptor so it starts at offset 0 with no
        // index-summary lookup: the same choice ZeroCopySSTableSplitter.scan() makes.
        File indexFile = sstable.descriptor.fileFor(BigFormat.Components.PRIMARY_INDEX);
        try (RandomAccessReader in = RandomAccessReader.open(indexFile))
        {
            long indexSize = in.length();
            while (in.getFilePointer() != indexSize)
            {
                ByteBuffer key = ByteBufferUtil.readWithShortLength(in);
                RowIndexEntry.Serializer.skip(in, sstable.descriptor.version);   // position + promoted index

                DecoratedKey dk = partitioner.decorateKey(key);
                Token token = dk.getToken();
                // full wins over transient, exactly as antiCompactGroup routes partitions
                Label label = fullChecker.test(token) ? Label.FULL
                                                      : transChecker.test(token) ? Label.TRANSIENT
                                                                                 : Label.UNREPAIRED;
                if (label != previous)
                {
                    // retainable() because the key outlives the buffer it was read from: the run boundaries are
                    // held for the whole walk and then handed to the splitter
                    runs.add(label, dk.retainable());
                    previous = label;
                }
            }
        }
        catch (IOException e)
        {
            throw new CorruptSSTableException(e, indexFile);
        }

        return finish(runs, sessionID);
    }

    /**
     * Labels for an explicit token sequence, for tests and callers that already have the keys in hand. Same
     * precedence and same checker semantics as the Index.db walk, so the keys must be in ascending order.
     */
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
