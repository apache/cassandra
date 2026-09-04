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

package org.apache.cassandra.tcm;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.sequences.LockedRanges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit coverage for the {@code REJECTED}, {@code FAILED}, {@code NOT_APPLIED} and {@code APPLIED} outcomes of
 * {@link AbstractLocalProcessor#commit}'s per-cycle handling, complementing
 * {@link AbstractLocalProcessorLockTest}, which covers only the ways a commit can give up while queued for the
 * proposal lock:
 *
 * <ol>
 *     <li>a transformation which is rejected, and stays rejected after catching up to the latest log, must return
 *     the rejection rather than looping or claiming a generic failure;</li>
 *     <li>a transformation which is rejected, but whose catch-up discovers a newer epoch, must retry against that
 *     newer state rather than returning the stale rejection;</li>
 *     <li>a proposal whose {@code tryCommitOne} throws must be retried as a plain failure, recovering once a later
 *     cycle's {@code tryCommitOne} succeeds;</li>
 *     <li>a proposal whose {@code tryCommitOne} keeps throwing must eventually give up with the generic
 *     retry-exhaustion failure, not a rejection or an admission-timeout message;</li>
 *     <li>a proposal which does not win its epoch must back off, catch up to the distributed log and retry
 *     against the epoch the winner took, rather than being treated as a failure or re-proposing the stale one;</li>
 *     <li>a proposal which wins its epoch and is appended locally, but whose subsequent
 *     {@link LocalLog#awaitAtLeast} fails, must retry as a plain failure rather than losing track of - or
 *     re-corrupting - the epoch that was already committed.</li>
 * </ol>
 *
 * All six drive the real {@link LocalLog} (in {@code sync} mode) and a real, minimal {@link AbstractLocalProcessor}
 * subclass rather than mocking either, so that what is being verified is this class's actual control flow rather
 * than a model of it.
 */
public class AbstractLocalProcessorCycleTest
{
    private static final long TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * A transformation which is rejected, and whose {@code fetchLogAndWait} catch-up afterwards reports no epoch
     * change (nothing was ever proposed, so the local log genuinely never advances), must return the rejection
     * itself - carrying its original {@link ExceptionCode} and reason - rather than falling through to the generic
     * retry-exhaustion failure, and must not loop: {@link AbstractLocalProcessor#handleRejected} decides the
     * outcome from a single catch-up fetch.
     */
    @Test
    public void transformationRejectedWithNoEpochChangeReturnsRejection() throws Exception
    {
        LocalLog log = newLog();
        RejectingProcessor processor = new RejectingProcessor(log);
        try
        {
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                     rejectingTransformation(),
                                                     null,
                                                     retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertTrue("a permanently rejected transformation must fail", result.isFailure());
            Commit.Result.Failure failure = result.failure();
            assertTrue("must be reported as a rejection, not a generic retry-exhaustion failure", failure.rejected);
            assertEquals(ExceptionCode.INVALID, failure.code);
            assertTrue("must carry the transformation's own rejection reason, but was: " + failure.message,
                       failure.message.contains("permanently rejected for testing"));
            assertEquals("a rejection with no epoch change must not loop back for a second catch-up fetch",
                        1, processor.fetchLogAndWaitCalls.get());
            // A regression that routed a rejected result into tryCommitOne would otherwise be caught only
            // indirectly, and slowly: it would grind through a real retry-exhaustion backoff before failing on the
            // assertions above with a misleading "must be reported as a rejection" message. Catch it here, first,
            // pointing at the actual defect.
            assertEquals("a rejected transformation must never reach tryCommitOne",
                        0, processor.tryCommitOneCalls.get());
        }
        finally
        {
            log.close();
        }
    }

    /**
     * A transformation which is rejected because this node has not yet observed a proposal made concurrently by
     * another CMS member must, once {@code fetchLogAndWait} catches up and reveals that newer epoch, retry rather
     * than returning the now-stale rejection - and the retried cycle must actually execute against the caught-up
     * state, not the epoch it started at.
     */
    @Test
    public void transformationRejectedTriggersCatchUpThenRetries() throws Exception
    {
        LocalLog log = newLog();
        CatchUpProcessor processor = new CatchUpProcessor(log);
        try
        {
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                     rejectAtEmptyEpoch(),
                                                     null,
                                                     retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertTrue("commit must succeed once catch-up reveals the epoch the transformation was waiting for",
                      result.isSuccess());
            assertEquals("must catch up exactly once, not loop", 1, processor.fetchLogAndWaitCalls.get());
            assertEquals("the retried cycle must propose against the caught-up epoch, one past the epoch fetchLogAndWait " +
                        "introduced",
                        2L, result.success().epoch.getEpoch());
        }
        finally
        {
            log.close();
        }
    }

    /**
     * A proposal whose {@code tryCommitOne} throws is reported as a plain failure and retried - via
     * {@link AbstractLocalProcessor#handleFailure} - rather than as a rejection or an admission-timeout: a fresh
     * cycle re-derives and re-proposes the transformation from scratch, and the commit succeeds once that later
     * cycle's {@code tryCommitOne} does not throw.
     */
    @Test
    public void tryCommitOneThrowsIsReportedAsFailedThenRetries() throws Exception
    {
        LocalLog log = newLog();
        ThrowThenSucceedProcessor processor = new ThrowThenSucceedProcessor(log);
        try
        {
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                     executableTransformation(),
                                                     null,
                                                     retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertTrue("commit must recover once a later cycle's tryCommitOne succeeds", result.isSuccess());
            assertEquals("exactly two cycles should have reached tryCommitOne: the one that threw, and the one " +
                        "that succeeded after retrying",
                        2, processor.tryCommitOneCalls.get());
            // The throw happens before log.append, so the failed first cycle must leave no trace: the retry
            // re-derives the transformation from the same, unchanged previous metadata and lands on epoch 1, not 2.
            assertEquals("the failed first cycle must leave no trace in the log", 1L, result.success().epoch.getEpoch());
        }
        finally
        {
            log.close();
        }
    }

    /**
     * A proposal whose {@code tryCommitOne} keeps throwing must eventually give up, once
     * {@link Retry#maybeSleep()} refuses to back off further, with the same generic retry-exhaustion message that
     * closes {@link AbstractLocalProcessor#commit} - not a rejection (nothing was ever rejected) and not an
     * admission-timeout message (every cycle was admitted and reached {@code tryCommitOne}).
     */
    @Test
    public void tryCommitOneThrowsRepeatedlyExhaustsAsGenericFailure() throws Exception
    {
        LocalLog log = newLog();
        AlwaysThrowingProcessor processor = new AlwaysThrowingProcessor(log);
        try
        {
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                     executableTransformation(),
                                                     null,
                                                     retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertTrue("persistent tryCommitOne failure must eventually fail the commit", result.isFailure());
            Commit.Result.Failure failure = result.failure();
            assertFalse("must not be reported as a rejection: nothing was ever rejected, tryCommitOne kept throwing",
                       failure.rejected);
            assertEquals(ExceptionCode.SERVER_ERROR, failure.code);
            assertTrue("must be the generic retry-exhaustion message, but was: " + failure.message,
                      failure.message.startsWith("Could not perform commit after"));
            // Exact, not just "more than one": the default retry policy allows getCmsDefaultRetryMaxTries() retries
            // after the first attempt, so exactly that many plus one cycles must reach tryCommitOne before
            // Retry#maybeSleep refuses to back off further - no more (would mean the attempt cap wasn't enforced)
            // and no fewer (would mean giving up too early, or spinning past the cap to the 30s deadline instead).
            assertEquals("must exhaust exactly the default retry policy's attempt budget, no more, no fewer",
                        DatabaseDescriptor.getCmsDefaultRetryMaxTries() + 1, processor.tryCommitOneCalls.get());
        }
        finally
        {
            log.close();
        }
    }

    /**
     * A proposal which is admitted and made, but does not win its epoch - the ordinary outcome for the loser of a
     * race against another CMS member - must be handled by {@link AbstractLocalProcessor#handleNotApplied} rather
     * than as a failure: back off, catch up to the distributed log, then retry. The catch-up is what separates this
     * outcome from {@code FAILED}, which backs off without fetching, and it is load-bearing: the retried cycle has
     * to propose against the epoch the winner took, not the stale one this cycle started from.
     */
    @Test
    public void proposalLosingItsEpochBacksOffCatchesUpThenRetries() throws Exception
    {
        LocalLog log = newLog();
        LostEpochProcessor processor = new LostEpochProcessor(log);
        try
        {
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                     executableTransformation(),
                                                     null,
                                                     retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertTrue("commit must recover once a later cycle's proposal wins its epoch", result.isSuccess());
            assertEquals("exactly two cycles should have reached tryCommitOne: the one that lost its epoch, and the " +
                        "one that won after catching up",
                        2, processor.tryCommitOneCalls.get());
            // The distinction from a FAILED outcome, which backs off without fetching at all: losing an epoch means
            // this node's view of the log is behind, so handleNotApplied must refetch before retrying - and exactly
            // once, since the second cycle wins outright and never reaches the handler again.
            assertEquals("a lost epoch must be followed by exactly one catch-up fetch",
                        1, processor.fetchLogAndWaitCalls.get());
            // Had the retry re-proposed against the epoch it originally read rather than the caught-up one, it would
            // have contended for epoch 1 - the slot the winner already took - and landed here at 1 instead of 2.
            assertEquals("the retried cycle must propose against the caught-up epoch, one past the epoch the winner took",
                        2L, result.success().epoch.getEpoch());
        }
        finally
        {
            log.close();
        }
    }

    /**
     * A proposal which wins its epoch and is appended to the local log, but whose subsequent
     * {@link LocalLog#awaitAtLeast} fails (here: the append is dropped from the pending buffer by a log filter,
     * exactly as if the local follower had not yet caught up), must be retried as a plain failure via
     * {@link AbstractLocalProcessor#handleApplied}'s catch, rather than the commit hanging, losing the epoch, or
     * silently reporting success. On retry, the second cycle re-derives and re-proposes the same transformation from
     * scratch; once its append is not dropped, {@code awaitAtLeast} succeeds and the commit completes normally.
     */
    @Test
    public void awaitAtLeastFailureAfterAppliedRetriesAsFailureThenSucceeds() throws Exception
    {
        LocalLog log = newLog();
        LossyAppendProcessor processor = new LossyAppendProcessor(log);
        // Drops exactly the first entry appended to the log - i.e. the first cycle's winning proposal - so that its
        // epoch never reaches `committed` and the first handleApplied's awaitAtLeast(nextEpoch) genuinely fails.
        AtomicBoolean dropNextAppend = new AtomicBoolean(true);
        log.addFilter(entry -> dropNextAppend.compareAndSet(true, false));
        try
        {
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                     executableTransformation(),
                                                     null,
                                                     retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertTrue("commit must eventually succeed once the second attempt's append is not dropped",
                      result.isSuccess());
            assertEquals("exactly two cycles should have reached tryCommitOne: the one whose append was lost, and " +
                        "the one that succeeded after retrying",
                        2, processor.tryCommitOneCalls.get());
            // Guards against the dropped first entry being resurrected rather than genuinely superseded by the
            // retry's own append: both cycles derive the same epoch 1 from the same unchanged previous metadata.
            assertEquals("must commit at epoch 1, not skip ahead", 1L, result.success().epoch.getEpoch());
        }
        finally
        {
            log.close();
        }
    }

    private static LocalLog newLog()
    {
        LocalLog log = LocalLog.logSpec()
                               .sync()
                               .withInitialState(new ClusterMetadata(Murmur3Partitioner.instance))
                               .createLog();
        log.readyUnchecked();
        return log;
    }

    private static Retry retryFor(long timeout, TimeUnit unit)
    {
        return Retry.untilElapsed(unit.toNanos(timeout), TCMMetrics.instance.commitRetries);
    }

    /**
     * Unlike a transformation which is only ever rejected before executing, this one actually runs, so it must not
     * derive its affected ranges from the metadata keyspace via {@code MetaStrategy.affectedRanges}: the bare
     * {@link ClusterMetadata} used by these tests has no such keyspace in its schema.
     */
    private static Transformation executableTransformation()
    {
        return new Transformation()
        {
            @Override
            public Kind kind()
            {
                return Kind.CUSTOM;
            }

            @Override
            public Result execute(ClusterMetadata metadata)
            {
                return Transformation.success(metadata.transformer(), LockedRanges.AffectedRanges.EMPTY);
            }
        };
    }

    private static Transformation rejectingTransformation()
    {
        return new Transformation()
        {
            @Override
            public Kind kind()
            {
                return Kind.CUSTOM;
            }

            @Override
            public Result execute(ClusterMetadata metadata)
            {
                return new Rejected(ExceptionCode.INVALID, "permanently rejected for testing");
            }
        };
    }

    /**
     * Rejects while the local metadata is still at its initial, empty epoch, and succeeds once it is not - i.e.
     * once this node has caught up to at least one entry appended by someone else.
     */
    private static Transformation rejectAtEmptyEpoch()
    {
        return new Transformation()
        {
            @Override
            public Kind kind()
            {
                return Kind.CUSTOM;
            }

            @Override
            public Result execute(ClusterMetadata metadata)
            {
                if (metadata.epoch.is(Epoch.EMPTY))
                    return new Rejected(ExceptionCode.INVALID, "rejected until a newer epoch is observed");
                return Transformation.success(metadata.transformer(), LockedRanges.AffectedRanges.EMPTY);
            }
        };
    }

    /** Accepts every commit, but its transformation always rejects, so {@code tryCommitOne} must never be reached. */
    private static class RejectingProcessor extends AbstractLocalProcessor
    {
        final AtomicInteger fetchLogAndWaitCalls = new AtomicInteger();

        RejectingProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            return true;
        }

        final AtomicInteger tryCommitOneCalls = new AtomicInteger();

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            tryCommitOneCalls.incrementAndGet();
            throw new AssertionError("A rejected transformation must never be proposed");
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            fetchLogAndWaitCalls.incrementAndGet();
            return log.waitForHighestConsecutive();
        }
    }

    /**
     * Accepts every commit and always wins its proposal. On the first {@code fetchLogAndWait} call, simulates
     * discovering a proposal committed concurrently by another CMS member by appending and enacting a foreign entry
     * directly on the log, advancing it one epoch, before returning the now-caught-up state.
     */
    private static class CatchUpProcessor extends AbstractLocalProcessor
    {
        final AtomicInteger fetchLogAndWaitCalls = new AtomicInteger();

        CatchUpProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            return true;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            return true;
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            if (fetchLogAndWaitCalls.getAndIncrement() == 0)
            {
                Epoch advanced = log.metadata().epoch.nextEpoch();
                log.append(new Entry(new Entry.Id(99), advanced, executableTransformation()));
            }
            return log.waitForHighestConsecutive();
        }
    }

    /** Accepts every commit; its {@code tryCommitOne} throws once, then succeeds on every later call. */
    private static class ThrowThenSucceedProcessor extends AbstractLocalProcessor
    {
        final AtomicInteger tryCommitOneCalls = new AtomicInteger();

        ThrowThenSucceedProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            return true;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            if (tryCommitOneCalls.incrementAndGet() == 1)
                throw new RuntimeException("injected tryCommitOne failure for testing");
            return true;
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            throw new AssertionError("A FAILED outcome must not trigger a log fetch");
        }
    }

    /** Accepts every commit; its {@code tryCommitOne} always throws. */
    private static class AlwaysThrowingProcessor extends AbstractLocalProcessor
    {
        final AtomicInteger tryCommitOneCalls = new AtomicInteger();

        AlwaysThrowingProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            return true;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            tryCommitOneCalls.incrementAndGet();
            throw new RuntimeException("injected tryCommitOne failure for testing");
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            throw new AssertionError("A FAILED outcome must not trigger a log fetch");
        }
    }

    /**
     * Accepts every commit; its first {@code tryCommitOne} loses the epoch and every later one wins. The
     * {@code fetchLogAndWait} which follows that loss simulates discovering the winning proposal by appending and
     * enacting a foreign entry at the contested epoch, so the caught-up state the retry executes against is one
     * epoch ahead of the state the losing cycle read.
     */
    private static class LostEpochProcessor extends AbstractLocalProcessor
    {
        final AtomicInteger tryCommitOneCalls = new AtomicInteger();
        final AtomicInteger fetchLogAndWaitCalls = new AtomicInteger();

        LostEpochProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            return true;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            return tryCommitOneCalls.incrementAndGet() != 1;
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            if (fetchLogAndWaitCalls.getAndIncrement() == 0)
            {
                Epoch contested = log.metadata().epoch.nextEpoch();
                log.append(new Entry(new Entry.Id(99), contested, executableTransformation()));
            }
            return log.waitForHighestConsecutive();
        }
    }

    /** Accepts every commit and always wins its proposal; whether that proposal is actually observable is up to the log. */
    private static class LossyAppendProcessor extends AbstractLocalProcessor
    {
        final AtomicInteger tryCommitOneCalls = new AtomicInteger();

        LossyAppendProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            return true;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            tryCommitOneCalls.incrementAndGet();
            return true;
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            return log.waitForHighestConsecutive();
        }
    }
}
