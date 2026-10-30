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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.sequences.LockedRanges;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Admission failures before and after a proposal, with gates controlling the lock handoff. */
public class AbstractLocalProcessorLockTest
{
    // Safety bound for stuck threads; admission timeout tests use a shorter deadline.
    private static final long TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void interruptedWhileQueuedForProposalLock() throws Exception
    {
        LocalLog log = newLog();
        GatedProcessor processor = new GatedProcessor(log);
        try
        {
            Thread holder = startLockHolder(processor);

            AtomicReference<Commit.Result> result = new AtomicReference<>();
            AtomicBoolean interruptPreserved = new AtomicBoolean();
            Thread waiter = new Thread(() -> {
                // Keep deadline expiry distinct from the interrupt under test.
                result.set(processor.commit(new Entry.Id(2), noOpTransformation(), null, retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)));
                interruptPreserved.set(Thread.currentThread().isInterrupted());
            }, "AbstractLocalProcessorLockTest-waiter");
            waiter.start();
            // Interrupt during admission, not before commit starts.
            awaitParkedOnLock(waiter);
            waiter.interrupt();
            waiter.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse("waiter did not abandon the commit after being interrupted", waiter.isAlive());

            releaseAndJoin(processor, holder);

            Commit.Result commitResult = result.get();
            assertNotNull(commitResult);
            assertTrue("interrupted commit must fail", commitResult.isFailure());
            String message = commitResult.failure().message;
            assertTrue("failure must identify interruption, but was: " + message,
                       message.contains("Interrupted"));
            assertTrue("failure must identify that nothing was submitted, but was: " + message,
                       message.contains("waiting to submit commit"));
            assertFalse("interruption must not be reported as retry exhaustion, but was: " + message,
                        message.contains("Could not perform commit after"));
            assertTrue("interrupt status must be restored for the caller", interruptPreserved.get());
        }
        finally
        {
            processor.gate.countDown();
            log.close();
        }
    }

    @Test
    public void deadlineExpiresWhileQueuedForProposalLock() throws Exception
    {
        LocalLog log = newLog();
        GatedProcessor processor = new GatedProcessor(log);
        try
        {
            Thread holder = startLockHolder(processor);

            AtomicReference<Commit.Result> result = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Thread waiter = new Thread(() -> {
                result.set(processor.commit(new Entry.Id(2), noOpTransformation(), null, retryFor(250, TimeUnit.MILLISECONDS)));
                interrupted.set(Thread.currentThread().isInterrupted());
            }, "AbstractLocalProcessorLockTest-waiter");
            waiter.start();
            waiter.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse("waiter did not give up on admission within its deadline", waiter.isAlive());

            releaseAndJoin(processor, holder);

            Commit.Result commitResult = result.get();
            assertNotNull(commitResult);
            assertTrue("commit which was never admitted must fail", commitResult.isFailure());
            String message = commitResult.failure().message;
            assertTrue("failure must identify that nothing was submitted, but was: " + message,
                       message.contains("waiting to submit commit"));
            assertFalse("admission timeout must not be reported as retry exhaustion, but was: " + message,
                        message.contains("Could not perform commit after"));
            assertFalse("admission timeout must not be reported as an interrupt, but was: " + message,
                        message.contains("Interrupted"));
            assertFalse("interrupt status must not be set", interrupted.get());
        }
        finally
        {
            processor.gate.countDown();
            log.close();
        }
    }

    // A false CAS result can mean either a lost epoch or an ambiguous Paxos timeout.
    @Test
    public void deniedReadmissionAfterProposingMustReportUnknownOutcome() throws Exception
    {
        LocalLog log = newLog();
        ProposeThenDenyProcessor processor = new ProposeThenDenyProcessor(log);
        try
        {
            AtomicReference<Commit.Result> result = new AtomicReference<>();
            // Preserve the cause if the victim exits before signalling its proposal.
            AtomicReference<Throwable> uncaught = new AtomicReference<>();
            Thread victim = new Thread(() -> {
                try
                {
                    result.set(processor.commit(new Entry.Id(1),
                                               noOpTransformation(),
                                               null,
                                               retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)));
                }
                catch (Throwable t)
                {
                    uncaught.set(t);
                }
            }, "AbstractLocalProcessorLockTest-victim");
            victim.start();
            if (!processor.proposalMade.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                throw new AssertionError("victim never proposed", uncaught.get());
            // Queue behind the first proposal so this exercises denied readmission.
            Thread blocker = new Thread(() -> {
                try
                {
                    processor.commit(new Entry.Id(2), noOpTransformation(), null, retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                }
                catch (NotCMSException expected)
                {
                    // The blocker rejects membership after the gate opens, without proposing.
                }
            }, "AbstractLocalProcessorLockTest-blocker");
            processor.blocker = blocker;
            blocker.start();
            // Queue the blocker before releasing the victim to force the fair handoff.
            awaitParkedOnLock(blocker);
            processor.loseProposal.countDown();
            assertTrue("blocker never acquired the proposal lock",
                       processor.blockerHoldsLock.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("victim never refetched after its lost proposal",
                       processor.victimRefetched.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // Interrupt only after catch-up has finished and readmission is blocked.
            awaitParkedOnLock(victim);
            victim.interrupt();
            victim.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse("victim did not abandon the commit after being denied readmission", victim.isAlive());

            processor.gate.countDown();
            blocker.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse("blocker did not release the proposal lock", blocker.isAlive());

            if (uncaught.get() != null)
                throw new AssertionError("victim commit threw instead of failing", uncaught.get());

            Commit.Result commitResult = result.get();
            assertNotNull(commitResult);
            assertTrue("commit which was denied readmission must fail", commitResult.isFailure());
            String message = commitResult.failure().message;
            assertTrue("failure must identify that admission failed, but was: " + message,
                       message.contains("waiting to submit commit"));
            assertTrue("failure must state that the outcome is unknown, but was: " + message,
                       message.contains("whether the transformation was committed to the log is unknown"));
            assertFalse("a commit which already proposed must not claim nothing was proposed, but was: " + message,
                        message.contains("No proposal was made"));
            assertFalse("denied readmission must not be reported as retry exhaustion, but was: " + message,
                        message.contains("Could not perform commit after"));
        }
        finally
        {
            processor.gate.countDown();
            log.close();
        }
    }

    /** Returns once the holder owns admission; opening its gate releases it without proposing. */
    private Thread startLockHolder(GatedProcessor processor) throws InterruptedException
    {
        Thread holder = new Thread(() -> {
            try
            {
                processor.commit(new Entry.Id(1), noOpTransformation(), null, retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            catch (NotCMSException expected)
            {
                // The holder rejects membership after the gate opens, without proposing.
            }
        }, "AbstractLocalProcessorLockTest-holder");
        holder.start();
        assertTrue("holder never acquired the proposal lock", processor.lockHeld.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        return holder;
    }

    // Observe lock admission specifically; a follower wait must not satisfy this condition.
    static void awaitParkedOnLock(Thread waiter)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline)
        {
            Thread.State state = waiter.getState();
            if ((state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) && blockedOnLock(waiter))
                return;
            Thread.onSpinWait();
        }
        throw new AssertionError("Waiter never queued for the proposal lock; state=" + waiter.getState());
    }

    private static boolean blockedOnLock(Thread waiter)
    {
        for (StackTraceElement frame : waiter.getStackTrace())
        {
            if (frame.getClassName().equals(java.util.concurrent.locks.ReentrantLock.class.getName()))
                return true;
        }
        return false;
    }

    private void releaseAndJoin(GatedProcessor processor, Thread holder) throws InterruptedException
    {
        processor.gate.countDown();
        holder.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
        assertFalse("holder did not release the proposal lock", holder.isAlive());
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

    // Bare test metadata has no metadata keyspace from which to derive affected ranges.
    private static Transformation noOpTransformation()
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

    /** Holds admission in acceptCommit until the test opens the gate. */
    private static class GatedProcessor extends AbstractLocalProcessor
    {
        final CountDownLatch lockHeld = new CountDownLatch(1);
        // Opening the gate ends the cycle with NotCMSException.
        final CountDownLatch gate = new CountDownLatch(1);

        GatedProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            lockHeld.countDown();
            try
            {
                if (!gate.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    throw new AssertionError("Proposal lock was never released by the test");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            return false;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            throw new AssertionError("No proposal should ever be attempted by this test");
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            return log.waitForHighestConsecutive();
        }
    }

    /** Lets the victim propose once, then holds admission with the blocker during the victim's retry. */
    private static class ProposeThenDenyProcessor extends AbstractLocalProcessor
    {
        final CountDownLatch proposalMade = new CountDownLatch(1);
        // The test opens this only after the blocker has queued.
        final CountDownLatch loseProposal = new CountDownLatch(1);
        final CountDownLatch blockerHoldsLock = new CountDownLatch(1);
        final CountDownLatch victimRefetched = new CountDownLatch(1);
        // Releases the blocker so it can unwind without proposing.
        final CountDownLatch gate = new CountDownLatch(1);

        // Assigned before starting the blocker; all other calls belong to the victim.
        volatile Thread blocker;

        ProposeThenDenyProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            if (Thread.currentThread() != blocker)
                return true;

            blockerHoldsLock.countDown();
            await(gate, "Blocker was never released by the test");
            return false;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            if (Thread.currentThread() == blocker)
                throw new AssertionError("The blocker must never reach a proposal");
            proposalMade.countDown();
            await(loseProposal, "Victim's proposal was never resolved by the test");
            // The processor cannot distinguish a lost epoch from an ambiguous Paxos timeout.
            return false;
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            ClusterMetadata metadata = log.waitForHighestConsecutive();
            // The blocker exits from acceptCommit; only the victim reaches catch-up.
            victimRefetched.countDown();
            return metadata;
        }

        private static void await(CountDownLatch latch, String message)
        {
            try
            {
                if (!latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    throw new AssertionError(message);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }
}
