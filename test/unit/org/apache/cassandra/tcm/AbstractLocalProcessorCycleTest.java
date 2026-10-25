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
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy;
import org.apache.cassandra.service.WaitStrategy;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.sequences.LockedRanges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Commit outcome and recovery tests; queued admission is covered by AbstractLocalProcessorLockTest. */
public class AbstractLocalProcessorCycleTest
{
    private static final long TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

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
            assertEquals("a rejected transformation must never reach tryCommitOne",
                        0, processor.tryCommitOneCalls.get());
        }
        finally
        {
            log.close();
        }
    }

    // A rejection against stale metadata must be reconsidered after discovering a remote commit.
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
            // The exception precedes append, so the retry must still use epoch 1.
            assertEquals("the failed first cycle must leave no trace in the log", 1L, result.success().epoch.getEpoch());
        }
        finally
        {
            log.close();
        }
    }

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
            // Exhaust the configured attempt cap, not the much longer deadline.
            assertEquals("must exhaust exactly the default retry policy's attempt budget, no more, no fewer",
                        3, processor.tryCommitOneCalls.get());
        }
        finally
        {
            log.close();
        }
    }

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
            // Unlike a thrown proposal, a lost epoch requires catch-up before retrying.
            assertEquals("a lost epoch must be followed by exactly one catch-up fetch",
                        1, processor.fetchLogAndWaitCalls.get());
            // Reusing the original epoch would contend with the winner again.
            assertEquals("the retried cycle must propose against the caught-up epoch, one past the epoch the winner took",
                        2L, result.success().epoch.getEpoch());
        }
        finally
        {
            log.close();
        }
    }

    @Test
    public void enactmentFailureDoesNotReportSuccess() throws Exception
    {
        LocalLog log = newLog();
        LossyAppendProcessor processor = new LossyAppendProcessor(log);
        // A durable proposal whose local append is lost must not be acknowledged as enacted.
        log.addFilter(entry -> true);
        try
        {
            Retry retry = Retry.untilElapsed(TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS), TCMMetrics.instance.commitRetries,
                                             WaitStrategy.None.INSTANCE);
            Commit.Result result = processor.commit(new Entry.Id(1), executableTransformation(), null, retry);
            assertTrue(result.isFailure());
            assertEquals(1, processor.tryCommitOneCalls.get());
            assertEquals(Epoch.EMPTY, log.metadata().epoch);
        }
        finally
        {
            log.close();
        }
    }

    @Test
    public void lostLocalAppendIsRecoveredFromTheDurableLog() throws Exception
    {
        LocalLog log = newLog();
        LossyAppendProcessor processor = new LossyAppendProcessor(log);
        AtomicBoolean dropFirst = new AtomicBoolean(true);
        log.addFilter(entry -> dropFirst.compareAndSet(true, false));
        Transformation once = new Transformation()
        {
            public Kind kind()
            {
                return Kind.CUSTOM;
            }

            public Result execute(ClusterMetadata metadata)
            {
                if (!metadata.epoch.is(Epoch.EMPTY))
                    return new Rejected(ExceptionCode.INVALID, "already applied");
                return Transformation.success(metadata.transformer(), LockedRanges.AffectedRanges.EMPTY);
            }
        };
        try
        {
            Entry.Id id = new Entry.Id(1);
            Commit.Result result = processor.commit(id, once, Epoch.EMPTY, retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue(result.isSuccess());
            assertEquals(Epoch.create(1), result.success().epoch);
            assertEquals(2, processor.tryCommitOneCalls.get());
            assertEquals(1, result.success().logState.entries.size());
            assertEquals(id, result.success().logState.entries.get(0).id);
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
                               .withStorage(new AtomicLongBackedProcessor.InMemoryStorage())
                               .withInitialState(new ClusterMetadata(Murmur3Partitioner.instance))
                               .createLog();
        log.readyUnchecked();
        return log;
    }

    private static Retry retryFor(long timeout, TimeUnit unit)
    {
        return Retry.untilElapsed(unit.toNanos(timeout), TCMMetrics.instance.commitRetries,
                                  RetryStrategy.parse("1ms,retries=2", TimeoutStrategy.LatencySourceFactory.none()));
    }

    // Bare test metadata has no metadata keyspace from which to derive affected ranges.
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
                log.append(new Entry(new Entry.Id(99), advanced, executableTransformation()));
            }
            return log.waitForHighestConsecutive();
        }
    }

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
                log.append(new Entry(new Entry.Id(99), contested, executableTransformation()));
            }
            return log.waitForHighestConsecutive();
        }
    }

    /** Keeps the durable proposal independently of the local append, which the tests can discard. */
    private static class LossyAppendProcessor extends AbstractLocalProcessor
    {
        final AtomicInteger tryCommitOneCalls = new AtomicInteger();
        final AtomicReference<Entry> durable = new AtomicReference<>();

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
            return durable.compareAndSet(null, new Entry(entryId, nextEpoch, transform));
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            log.append(durable.get());
            return log.waitForHighestConsecutive();
        }
    }
}
