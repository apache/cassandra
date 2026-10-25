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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.service.WaitStrategy;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.sequences.LockedRanges;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AbstractLocalProcessorConcurrencyTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void queuedCommitUsesThePrecedingProposal() throws Exception
    {
        try (Harness h = new Harness())
        {
            FutureTask<Commit.Result> first = h.start();
            await(h.proposing);
            FutureTask<Commit.Result> second = h.start();
            AbstractLocalProcessorLockTest.awaitParkedOnLock(h.threads.get(1));
            h.release.countDown();

            assertEquals(Epoch.create(1), get(first).success().epoch);
            assertEquals(Epoch.create(2), get(second).success().epoch);
            assertEquals(2, h.durableEpoch.get());
        }
    }

    @Test
    public void closedLogRejectsQueuedCommit() throws Exception
    {
        try (Harness h = new Harness())
        {
            h.start();
            await(h.proposing);
            FutureTask<Commit.Result> queued = h.start();
            AbstractLocalProcessorLockTest.awaitParkedOnLock(h.threads.get(1));
            h.log.close();
            h.release.countDown();

            assertTrue(get(queued).isFailure());
            assertEquals(1, h.durableEpoch.get());
        }
    }

    @Test
    public void membershipIsCheckedAfterAdmission() throws Exception
    {
        try (Harness h = new Harness())
        {
            h.start();
            await(h.proposing);
            FutureTask<Commit.Result> queued = h.start();
            AbstractLocalProcessorLockTest.awaitParkedOnLock(h.threads.get(1));
            h.member = false;
            h.release.countDown();

            try
            {
                get(queued);
                org.junit.Assert.fail("Expected membership rejection");
            }
            catch (ExecutionException e)
            {
                assertTrue(e.getCause() instanceof NotCMSException);
            }
            assertEquals(1, h.durableEpoch.get());
        }
    }

    @Test
    public void catchUpDoesNotHoldAdmission() throws Exception
    {
        try (Harness h = new Harness())
        {
            h.rejectFirst = true;
            h.release.countDown();
            FutureTask<Commit.Result> rejected = h.start();
            await(h.fetching);
            assertTrue(get(h.start()).isSuccess());
            h.finishFetch.countDown();
            assertTrue(get(rejected).isSuccess());
        }
    }

    @Test
    public void backoffDoesNotHoldAdmission() throws Exception
    {
        try (Harness h = new Harness())
        {
            CountDownLatch backingOff = new CountDownLatch(1);
            CountDownLatch resume = new CountDownLatch(1);
            Retry retry = new Retry(Long.MAX_VALUE, TCMMetrics.instance.commitRetries)
            {
                @Override
                public boolean maybeSleep()
                {
                    backingOff.countDown();
                    await(resume);
                    return true;
                }
            };
            h.loseFirst = true;
            h.release.countDown();
            h.finishFetch.countDown();
            try
            {
                FutureTask<Commit.Result> first = h.start(retry);
                await(backingOff);
                assertTrue(get(h.start()).isSuccess());
                resume.countDown();
                assertTrue(get(first).isSuccess());
            }
            finally
            {
                resume.countDown();
            }
        }
    }

    @Test
    public void enactmentWaitDoesNotHoldAdmission() throws Exception
    {
        try (Harness h = new Harness())
        {
            h.pauseAfterProposal = true;
            h.release.countDown();
            FutureTask<Commit.Result> first = h.start();
            await(h.proposing);
            // The follower cannot enact epoch 1, but another caller can still attempt admission.
            assertTrue(get(h.start()).isFailure());
            assertFalse(first.isDone());
            h.log.unpause();
            h.log.waitForHighestConsecutive();
            assertTrue(get(first).isSuccess());
        }
    }

    @Test
    public void concurrentProposalsFuzz() throws Exception
    {
        long seed = org.apache.cassandra.config.CassandraRelevantProperties.TEST_SEED.getLong(21537L);
        Random random = new Random(seed);
        for (int round = 0; round < 20; ++round)
        {
            try (Harness h = new Harness())
            {
                h.release.countDown();
                List<FutureTask<Commit.Result>> results = new ArrayList<>();
                int count = 2 + random.nextInt(7);
                for (int i = 0; i < count; ++i)
                {
                    results.add(h.start());
                    if (random.nextBoolean())
                        Thread.yield();
                }
                Set<Epoch> epochs = new HashSet<>();
                for (FutureTask<Commit.Result> result : results)
                {
                    Commit.Result committed = get(result);
                    assertTrue("seed=" + seed + ", round=" + round, committed.isSuccess());
                    assertTrue(epochs.add(committed.success().epoch));
                }
                assertEquals(count, h.durableEpoch.get());
                assertEquals(Epoch.create(count), h.log.waitForHighestConsecutive().epoch);
            }
        }
    }

    private static Commit.Result get(FutureTask<Commit.Result> task) throws Exception
    {
        return task.get(30, TimeUnit.SECONDS);
    }

    private static void await(CountDownLatch latch)
    {
        try
        {
            assertTrue(latch.await(30, TimeUnit.SECONDS));
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static class Harness extends AbstractLocalProcessor implements AutoCloseable
    {
        final CountDownLatch proposing = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch fetching = new CountDownLatch(1);
        final CountDownLatch finishFetch = new CountDownLatch(1);
        final AtomicLong durableEpoch = new AtomicLong();
        final AtomicBoolean first = new AtomicBoolean(true);
        final List<Thread> threads = new ArrayList<>();
        volatile boolean member = true;
        boolean rejectFirst;
        boolean pauseAfterProposal;
        boolean loseFirst;

        Harness()
        {
            super(LocalLog.logSpec().async().withInitialState(new ClusterMetadata(Murmur3Partitioner.instance)).createLog());
            log.readyUnchecked();
        }

        FutureTask<Commit.Result> start()
        {
            // A local race must fail this test rather than being concealed by retries.
            return start(Retry.untilElapsed(TimeUnit.SECONDS.toNanos(30), TCMMetrics.instance.commitRetries,
                                            WaitStrategy.None.INSTANCE));
        }

        FutureTask<Commit.Result> start(Retry retry)
        {
            int id = threads.size() + 1;
            Transformation transformation = new Transformation()
            {
                @Override
                public Kind kind()
                {
                    return Kind.CUSTOM;
                }

                @Override
                public Result execute(ClusterMetadata metadata)
                {
                    if (rejectFirst && 1 == id && metadata.epoch.is(Epoch.EMPTY))
                        return new Rejected(org.apache.cassandra.exceptions.ExceptionCode.INVALID, "stale metadata");
                    return Transformation.success(metadata.transformer(), LockedRanges.AffectedRanges.EMPTY);
                }
            };
            FutureTask<Commit.Result> task = new FutureTask<>(() -> commit(new Entry.Id(id), transformation, null, retry));
            Thread thread = new Thread(task, "local-proposal-" + id);
            threads.add(thread);
            thread.start();
            return task;
        }

        @Override
        protected boolean acceptCommit(ClusterMetadata metadata)
        {
            return member;
        }

        @Override
        protected boolean tryCommitOne(Entry.Id id, Transformation transform, Epoch previous, Epoch next)
        {
            if (first.compareAndSet(true, false))
            {
                if (pauseAfterProposal)
                    log.pause();
                proposing.countDown();
                await(release);
                if (loseFirst)
                    return false;
            }
            return durableEpoch.compareAndSet(previous.getEpoch(), next.getEpoch());
        }

        @Override
        public ClusterMetadata fetchLogAndWait(Epoch epoch, Retry retry)
        {
            fetching.countDown();
            await(finishFetch);
            return log.waitForHighestConsecutive();
        }

        @Override
        public void close() throws Exception
        {
            release.countDown();
            finishFetch.countDown();
            for (Thread thread : threads)
            {
                thread.interrupt();
                thread.join(TimeUnit.SECONDS.toMillis(30));
                assertFalse(thread.isAlive());
            }
            log.close();
        }
    }
}
