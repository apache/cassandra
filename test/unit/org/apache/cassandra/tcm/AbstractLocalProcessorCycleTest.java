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
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.tcm.transformations.ForceSnapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Commit outcome and recovery tests; queued admission is covered by AbstractLocalProcessorLockTest. */
public class AbstractLocalProcessorCycleTest
{
    private static final long TIMEOUT_SECONDS = 30;
    private static final Epoch BASE = Epoch.create(100);

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void transformationRejectedWithNoEpochChangeReturnsRejection()
    {
        try (LocalLog log = newLog())
        {
            RejectingProcessor processor = new RejectingProcessor(log);
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                    rejectingTransformation(),
                                                    null,
                                                    retry());

            assertTrue("a permanently rejected transformation must fail", result.isFailure());
            Commit.Result.Failure failure = result.failure();
            assertTrue("must be reported as a rejection, not a generic retry-exhaustion failure", failure.rejected);
            assertEquals(ExceptionCode.INVALID, failure.code);
            assertTrue("must carry the transformation's own rejection reason, but was: " + failure.message,
                       failure.message.contains("permanently rejected for testing"));
            assertEquals("a rejection with no epoch change must not loop back for a second catch-up fetch",
                        1, processor.fetchLogAndWaitCalls.get());
            assertEquals("a rejected transformation must never reach tryCommitOne",
                        0, processor.tryCommitOneCalls.get());
        }
    }

    // A rejection against stale metadata must be reconsidered after discovering a remote commit.
    @Test
    public void transformationRejectedTriggersCatchUpThenRetries()
    {
        try (LocalLog log = newLog())
        {
            CatchUpProcessor processor = new CatchUpProcessor(log);
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                    rejectAtBaseEpoch(),
                                                    null,
                                                    retry());

            assertTrue("commit must succeed once catch-up reveals the epoch the transformation was waiting for",
                      result.isSuccess());
            assertEquals("must catch up exactly once, not loop", 1, processor.fetchLogAndWaitCalls.get());
            assertEquals("the retried cycle must propose against the caught-up epoch, one past the epoch fetchLogAndWait " +
                        "introduced",
                        BASE.nextEpoch().nextEpoch(), result.success().epoch);
        }
    }

    @Test
    public void tryCommitOneThrowsIsReportedAsFailedThenRetries()
    {
        try (LocalLog log = newLog())
        {
            CommitFailedProcessor processor = new CommitFailedProcessor(log, true);
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                    CustomTransformation.make("abc"),
                                                    null,
                                                    retry());

            assertTrue("commit must recover once a later cycle's tryCommitOne succeeds", result.isSuccess());
            assertEquals("exactly two cycles should have reached tryCommitOne: the one that threw, and the one " +
                        "that succeeded after retrying",
                        1, processor.failCount.get());
            // The exception precedes append, so the retry must still use epoch 1.
            assertEquals("the failed first cycle must leave no trace in the log", BASE.nextEpoch(), result.success().epoch);
        }
    }

    @Test
    public void tryCommitOneThrowsRepeatedlyExhaustsAsGenericFailure()
    {
        try (LocalLog log = newLog())
        {
            CommitFailedProcessor processor = new CommitFailedProcessor(log, false);
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                    CustomTransformation.make("abc"),
                                                    null,
                                                    retry());

            assertTrue("persistent tryCommitOne failure must eventually fail the commit", result.isFailure());
            Commit.Result.Failure failure = result.failure();
            assertFalse("must not be reported as a rejection: nothing was ever rejected, tryCommitOne kept throwing",
                       failure.rejected);
            assertEquals(ExceptionCode.SERVER_ERROR, failure.code);
            assertTrue("must be the generic retry-exhaustion message, but was: " + failure.message,
                      failure.message.startsWith("Could not perform commit after"));
            // Exhaust the configured attempt cap, not the much longer deadline.
            assertEquals("must exhaust exactly the default retry policy's attempt budget, no more, no fewer",
                        3, processor.failCount.get());
        }
    }

    @Test
    public void proposalLosingItsEpochBacksOffCatchesUpThenRetries()
    {
        try (LocalLog log = newLog())
        {
            LostEpochProcessor processor = new LostEpochProcessor(log);
            Commit.Result result = processor.commit(new Entry.Id(1),
                                                    CustomTransformation.make("abc"),
                                                    null,
                                                    retry());

            assertTrue("commit must recover once a later cycle's proposal wins its epoch", result.isSuccess());
            assertEquals("exactly two cycles should have reached tryCommitOne: the one that lost its epoch, and the " +
                        "one that won after catching up",
                        2, processor.tryCommitOneCalls.get());
            // Unlike a thrown proposal, a lost epoch requires catch-up before retrying.
            assertEquals("a lost epoch must be followed by exactly one catch-up fetch",
                        1, processor.fetchLogAndWaitCalls.get());
            // Reusing the original epoch would contend with the winner again.
            assertEquals("the retried cycle must propose against the caught-up epoch, one past the epoch the winner took",
                        BASE.nextEpoch().nextEpoch(), result.success().epoch);
        }
    }

    private static LocalLog newLog()
    {
        LocalLog log = LocalLog.logSpec()
                               .sync()
                               .withStorage(new AtomicLongBackedProcessor.InMemoryStorage())
                               .withInitialState(new ClusterMetadata(Murmur3Partitioner.instance))
                               .createLog();
        log.readyUnchecked();
        // start at epoch 100
        log.append(new Entry(Entry.Id.NONE, BASE,
                             new ForceSnapshot(new ClusterMetadata(Murmur3Partitioner.instance).forceEpoch(BASE))));
        return log;
    }

    private static Retry retry()
    {
        return Retry.untilElapsed(TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS), TCMMetrics.instance.commitRetries,
                                  RetryStrategy.parse("1ms,retries=2", TimeoutStrategy.LatencySourceFactory.none()));
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

    private static Transformation rejectAtBaseEpoch()
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
                if (metadata.epoch.is(BASE))
                    return new Rejected(ExceptionCode.INVALID, "rejected until a newer epoch is observed");
                return Transformation.success(metadata.transformer(), LockedRanges.AffectedRanges.EMPTY);
            }
        };
    }

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

    /** Catch-up discovers an entry committed by another CMS member. */
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
                log.append(new Entry(new Entry.Id(99), advanced, CustomTransformation.make("abc")));
            }
            return log.waitForHighestConsecutive();
        }
    }

    private static class CommitFailedProcessor extends AbstractLocalProcessor
    {
        final AtomicInteger failCount = new AtomicInteger();
        private final boolean onlyFirst;

        public CommitFailedProcessor(LocalLog log, boolean onlyFirst)
        {
            super(log);
            this.onlyFirst = onlyFirst;
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            return log.waitForHighestConsecutive();
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            if (onlyFirst && failCount.get() > 0)
                return true;
            failCount.incrementAndGet();
            return false;
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            return true;
        }
    }

    /** Loses the first proposal and discovers the winning entry during catch-up. */
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
                log.append(new Entry(new Entry.Id(99), contested, CustomTransformation.make("abc")));
            }
            return log.waitForHighestConsecutive();
        }
    }

}
