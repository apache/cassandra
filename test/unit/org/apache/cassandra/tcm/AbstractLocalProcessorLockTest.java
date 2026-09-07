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
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.sequences.LockedRanges;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit coverage for the ways {@link AbstractLocalProcessor#commit} can give up while queued for the local proposal
 * lock:
 *
 * <ol>
 *     <li>the caller is interrupted while waiting to be admitted, having proposed nothing;</li>
 *     <li>the caller's own deadline expires while waiting to be admitted, having proposed nothing;</li>
 *     <li>the caller had already proposed on an earlier iteration and is then denied readmission.</li>
 * </ol>
 *
 * All three are reported with a failure message distinct from retry exhaustion, because they say nothing about the
 * state of the distributed log. The first two additionally state that nothing was proposed and are therefore safe to
 * retry; the third must not, as whether the transformation applied is unknown. These tests hold the lock
 * deterministically with a gated subclass rather than relying on contention between many worker threads.
 */
public class AbstractLocalProcessorLockTest
{
    /** Generous upper bound; only ever hit if the test is genuinely stuck. */
    private static final long TIMEOUT_SECONDS = 30;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * A thread interrupted while queued for the proposal lock must be told that it was interrupted (and therefore
     * that nothing was proposed), and must have its interrupt status restored so that shutdown is not swallowed.
     */
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
                // Deliberately generous, so that a timeout cannot be mistaken for an interrupt.
                result.set(processor.commit(new Entry.Id(2), noOpTransformation(), null, retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)));
                interruptPreserved.set(Thread.currentThread().isInterrupted());
            }, "AbstractLocalProcessorLockTest-waiter");
            waiter.start();

            // Interrupt only once the waiter is provably parked in the lock queue, so that this exercises being
            // interrupted while queued rather than entering commit with the interrupt already set.
            awaitParkedOnLock(waiter);
            waiter.interrupt();
            waiter.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse("waiter did not abandon the commit after being interrupted", waiter.isAlive());

            releaseAndJoin(processor, holder);

            Commit.Result commitResult = result.get();
            assertNotNull(commitResult);
            assertTrue("interrupted commit must fail", commitResult.isFailure());
            String message = commitResult.failure().message;
            // Matching on text rather than type, as the message may have to cross class loaders.
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

    /**
     * A caller whose deadline expires while queued for the proposal lock never proposed, so it must not be reported
     * as retry exhaustion, and it must not claim to have been interrupted.
     */
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

    /**
     * The case which the serialisation of the proposal cycle actually introduced: a commit which already reached
     * {@link AbstractLocalProcessor#tryCommitOne} and was told its proposal did not win, then fails to be readmitted
     * for its next attempt.
     *
     * <p>A lost epoch is indistinguishable from an ambiguous Paxos result here, because
     * {@code DistributedMetadataLogKeyspace.tryCommit} flattens a {@code CasWriteTimeoutException} to {@code false},
     * so whether the transformation was committed to the distributed log is genuinely unknown. The failure must
     * therefore say so, and must not repeat the "No proposal was made" claim which makes a retry unambiguously safe.
     */
    @Test
    public void deniedReadmissionAfterProposingMustReportUnknownOutcome() throws Exception
    {
        LocalLog log = newLog();
        ProposeThenDenyProcessor processor = new ProposeThenDenyProcessor(log);
        try
        {
            AtomicReference<Commit.Result> result = new AtomicReference<>();
            // Captured so that a commit which unwinds by throwing is reported as itself, rather than as the much less
            // informative "victim never proposed" timeout on the latch below.
            AtomicReference<Throwable> uncaught = new AtomicReference<>();
            Thread victim = new Thread(() -> {
                try
                {
                    result.set(processor.commit(new Entry.Id(1),
                                               executableTransformation(),
                                               null,
                                               retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS)));
                }
                catch (Throwable t)
                {
                    uncaught.set(t);
                }
            }, "AbstractLocalProcessorLockTest-victim");
            victim.start();

            // Iteration one: the victim has been admitted and is inside tryCommitOne, still holding the lock.
            if (!processor.proposalMade.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                throw new AssertionError("victim never proposed", uncaught.get());

            // Started only now, so that it queues behind the victim's first cycle rather than blocking it: holding the
            // lock any earlier would produce the already-covered never-proposed case.
            Thread blocker = new Thread(() -> {
                try
                {
                    processor.commit(new Entry.Id(2), noOpTransformation(), null, retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                }
                catch (NotCMSException expected)
                {
                    // Expected: the gated acceptCommit rejects once released, unwinding without proposing anything.
                }
            }, "AbstractLocalProcessorLockTest-blocker");
            processor.blocker = blocker;
            blocker.start();

            // The victim still holds the lock, so the blocker can only be parked in the acquisition queue. Releasing
            // the victim's proposal only once the blocker is provably queued makes the fair handoff to the blocker,
            // and hence the victim's own failure to be readmitted, deterministic rather than merely likely.
            awaitParkedOnLock(blocker);
            processor.loseProposal.countDown();

            // The blocker now owns the lock and the victim has finished the post-proposal log fetch which precedes its
            // next acquisition, so readmission is impossible rather than merely unlikely.
            assertTrue("blocker never acquired the proposal lock",
                       processor.blockerHoldsLock.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertTrue("victim never refetched after its lost proposal",
                       processor.victimRefetched.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));

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
            // Matching on text rather than type, as the message may have to cross class loaders.
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

    /**
     * Starts a thread which acquires the proposal lock and holds it until {@link GatedProcessor#gate} is released,
     * returning only once the lock is provably held.
     */
    private Thread startLockHolder(GatedProcessor processor) throws InterruptedException
    {
        Thread holder = new Thread(() -> {
            try
            {
                processor.commit(new Entry.Id(1), noOpTransformation(), null, retryFor(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }
            catch (NotCMSException expected)
            {
                // Expected: the gated acceptCommit rejects once released, unwinding without proposing anything.
            }
        }, "AbstractLocalProcessorLockTest-holder");
        holder.start();
        assertTrue("holder never acquired the proposal lock", processor.lockHeld.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        return holder;
    }

    /**
     * Spins until {@code waiter} is provably blocked inside {@link AbstractLocalProcessor}, which - since the lock is
     * the only thing a commit can block on before proposing - means it is queued for the proposal lock. Deliberately
     * asserts on observed thread state rather than sleeping for a guessed interval.
     */
    private static void awaitParkedOnLock(Thread waiter)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline)
        {
            Thread.State state = waiter.getState();
            if ((state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) && blockedInProcessor(waiter))
                return;
            Thread.onSpinWait();
        }
        throw new AssertionError("Waiter never queued for the proposal lock; state=" + waiter.getState());
    }

    private static boolean blockedInProcessor(Thread waiter)
    {
        for (StackTraceElement frame : waiter.getStackTrace())
        {
            if (frame.getClassName().equals(AbstractLocalProcessor.class.getName()))
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
                return Transformation.success(metadata.transformer(), MetaStrategy.affectedRanges(metadata));
            }
        };
    }

    /**
     * As {@link #noOpTransformation()}, but reports no affected ranges rather than deriving them from the metadata
     * keyspace, which the bare {@link ClusterMetadata} used here does not have in its schema. Needed only by the test
     * whose commit is actually executed; the others are rejected in {@code acceptCommit} before executing.
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

    /**
     * Blocks in {@link #acceptCommit}, which {@code commit} calls with the proposal lock held, so that the lock can
     * be pinned for as long as a test needs it without any reliance on timing.
     */
    private static class GatedProcessor extends AbstractLocalProcessor
    {
        /** Counted down once the proposal lock is held by the gated thread. */
        final CountDownLatch lockHeld = new CountDownLatch(1);
        /** Released by the test to let the holder finish and drop the proposal lock. */
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
            // Rejecting unwinds commit() via NotCMSException, which is the cheapest way out of the proposal cycle
            // that is guaranteed not to have proposed anything.
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

    /**
     * Drives one victim commit through a lost proposal and then denies it readmission, so that the
     * proposal-attempted-then-lock-failure path can be exercised without any reliance on timing.
     *
     * <p>Two threads share this processor. The victim is admitted first and parks inside {@link #tryCommitOne} while
     * still holding the proposal lock; the blocker then queues for the lock behind it. Only once the blocker is
     * provably queued does the test release {@link #loseProposal}, so the fair handoff of the lock goes to the blocker
     * and the victim's second acquisition is guaranteed to find the lock held.
     */
    private static class ProposeThenDenyProcessor extends AbstractLocalProcessor
    {
        /** Counted down once the victim is inside tryCommitOne, i.e. a proposal has provably been attempted. */
        final CountDownLatch proposalMade = new CountDownLatch(1);
        /** Released by the test to let the victim's proposal report that it did not win its epoch. */
        final CountDownLatch loseProposal = new CountDownLatch(1);
        /** Counted down once the blocker holds the proposal lock, so the victim cannot be readmitted. */
        final CountDownLatch blockerHoldsLock = new CountDownLatch(1);
        /** Counted down once the victim has completed the post-proposal log fetch which precedes its next attempt. */
        final CountDownLatch victimRefetched = new CountDownLatch(1);
        /** Released by the test to let the blocker unwind and drop the proposal lock. */
        final CountDownLatch gate = new CountDownLatch(1);

        /** Set by the test before the blocker is started, so that the two roles can be told apart here. */
        volatile Thread blocker;

        ProposeThenDenyProcessor(LocalLog log)
        {
            super(log);
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            if (Thread.currentThread() != blocker)
                return true; // The victim proceeds to execute and propose.

            blockerHoldsLock.countDown();
            await(gate, "Blocker was never released by the test");
            // Rejecting unwinds commit() via NotCMSException, which is the cheapest way out of the proposal cycle
            // that is guaranteed not to have proposed anything.
            return false;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id entryId, Transformation transform, Epoch previousEpoch, Epoch nextEpoch)
        {
            if (Thread.currentThread() == blocker)
                throw new AssertionError("The blocker must never reach a proposal");

            // Still holding the proposal lock here, which is what lets the blocker queue behind this cycle.
            proposalMade.countDown();
            await(loseProposal, "Victim's proposal was never resolved by the test");
            // false is what commit() sees both for a genuinely lost epoch and for an ambiguous Paxos result, since
            // DistributedMetadataLogKeyspace.tryCommit flattens a CasWriteTimeoutException to false.
            return false;
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry retryPolicy)
        {
            ClusterMetadata metadata = log.waitForHighestConsecutive();
            // Only the victim gets this far; the blocker throws NotCMSException out of acceptCommit.
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
