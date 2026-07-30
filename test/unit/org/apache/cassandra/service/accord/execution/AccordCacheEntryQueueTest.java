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

package org.apache.cassandra.service.accord.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;

import org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus;
import org.apache.cassandra.service.accord.execution.AccordCacheEntryQueue.RemoveMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This test has been authored entirely by Claude.
 *
 * Direct test of {@link AccordCacheEntryQueue}'s structure, independent of the executor: an entry's claims form a
 * sequence whose <em>runnable prefix</em> is the fifo head if any, else the first sorted member if any, else the whole
 * unsorted bag. The queue exists to maintain four properties, and this test states each of them explicitly after every
 * mutation rather than relying on the class's own paranoid-mode assertions:
 *
 * <ul>
 *   <li><b>Q1</b> the sorted prefix is sorted by {@link AccordCacheEntryQueue#compare};</li>
 *   <li><b>Q2</b> every bag member sorts after every member of the sorted prefix;</li>
 *   <li><b>Q3</b> bag members are mutually unordered, so none waits for another;</li>
 *   <li><b>Q4</b> the runnable prefix is as above.</li>
 * </ul>
 *
 * Q2 is what lets an arrival that sorts past the prefix be bagged in O(1) without comparing it against the bag, and is
 * maintained by {@link AccordCacheEntryQueue#extendPriorityRegion}: a sorted claim landing at the end of the prefix first
 * takes with it every bag member that sorts before it.
 *
 * <p>The queue only reads a task's {@code position}, {@code createdAt} and execution kind, so the tasks here are mocks
 * rather than real {@link SafeTask}s - no command store, cache or executor is involved.
 */
public class AccordCacheEntryQueueTest
{
    private static int nextId = 0;

    /** The regions, and Q1, Q2 and Q4. Called after every mutation, so a violation is caught where it happens. */
    private static void check(AccordCacheEntryQueue q)
    {
        for (int i = AccordCacheEntryQueue.PRIORITY_START_INDEX; i < q.priorityHead; ++i)
            assertNullAt(q, i);
        for (int i = q.priorityHead; i < q.priorityTail + q.unsequencedSize(); ++i)
            assertNotNullAt(q, i);
        for (int i = q.priorityTail + q.unsequencedSize(); i <= q.fifoTail; ++i)
            assertNullAt(q, i);
        for (int i = q.fifoTail + 1; i <= q.fifoHead; ++i)
            assertNotNullAt(q, i);
        for (int i = q.fifoHead + 1; i < q.tasks.length; ++i)
            assertNullAt(q, i);

        for (int i = q.priorityHead + 1; i < q.priorityTail; ++i) // Q1
        {
            assertTrue("Q1: sorted prefix out of order at " + i,
                       AccordCacheEntryQueue.compare(q.tasks[i - 1], q.tasks[i]) <= 0);
        }

        for (int i = q.priorityTail; i < q.priorityTail + q.unsequencedSize(); ++i) // Q2
        {
            if (q.prioritySize() == 0)
                continue;

            assertTrue("Q2: bag member at " + i + " sorts before the prefix",
                       AccordCacheEntryQueue.compare(q.tasks[q.priorityTail - 1], q.tasks[i]) <= 0);
        }

        for (int i = q.fifoTail + 2; i <= q.fifoHead; ++i) // Q5
        {
            assertTrue("Q5: fifo region out of fifoAt order at " + i,
                       q.tasks[i].fifoAt <= q.tasks[i - 1].fifoAt);
        }

        int expectedPrefix = q.hasFifo() || q.hasPriority() ? 1 : q.unsequencedSize(); // Q4
        assertEquals("Q4: runnable prefix", expectedPrefix, q.runnablePrefix());
        if (q.totalSize() > 0 && q.peekFifoOrPriority() != null)
            assertNotSame("the head must be runnable", RunnableStatus.NOT_RUNNABLE, q.statusIfPresent(q.peekFifoOrPriority()));
    }

    private static void assertNullAt(AccordCacheEntryQueue q, int i)
    {
        assertTrue("expected no task at " + i + " but found " + q.tasks[i], q.tasks[i] == null);
    }

    private static void assertNotNullAt(AccordCacheEntryQueue q, int i)
    {
        assertTrue("expected a task at " + i, q.tasks[i] != null);
    }

    /**
     * Randomised property test over a single queue. Rather than mirror the region layout, we model only what the queue
     * promises semantically - Q4's runnable prefix: the earliest fifo arrival if any, else the least prioritised task by
     * compare, else the whole bag - and then require that
     * <ul>
     *   <li>every task's {@code status} agrees with that model, and</li>
     *   <li>the notifications the queue emits are exactly the delta of the runnable set, so a task is never demoted
     *       twice without an intervening promotion. That last property is what {@code waitingForState} depends on: it
     *       counts demotions and uncounts promotions, so a duplicate of either leaves it permanently wrong and the task
     *       can never be woken.</li>
     * </ul>
     * On failure the operation sequence is printed, which replays deterministically from the seed.
     */
    /**
     * Coverage of the placement paths a monotone generator cannot reach; asserted non-zero below, because a generator
     * that stops reaching them is an append-only test with a model that agrees by construction.
     */
    private static int fifoHeadDisplacements, fifoInteriorInsertions, sortedHeadDemotions, partialPullForwards, unsequencedStrengthenings;

    @Test
    public void randomisedRunnablePrefixAndNotifications()
    {
        fifoHeadDisplacements = fifoInteriorInsertions = sortedHeadDemotions = partialPullForwards = unsequencedStrengthenings = 0;
        long seed = 20260850;
        for (int iteration = 0 ; iteration < 500 ; ++iteration)
        {
            Random rnd = new Random(seed);
            List<String> log = new ArrayList<>();
            try
            {
                runOneRandomised(rnd, log);
            }
            catch (Throwable t)
            {
                throw new AssertionError("seed " + seed + " failed after:\n  " + String.join("\n  ", log), t);
            }
            ++seed;
        }

        System.out.println("single-queue fuzz coverage: fifo head displaced=" + fifoHeadDisplacements
                           + " fifo interior insertions=" + fifoInteriorInsertions
                           + " sorted head demoted=" + sortedHeadDemotions
                           + " partial pull-forwards=" + partialPullForwards
                           + " unsequenced strengthened=" + unsequencedStrengthenings);
        assertTrue("Q5: no fifo arrival ever displaced the existing head", fifoHeadDisplacements > 0);
        assertTrue("Q5: no fifo arrival ever landed inside the region", fifoInteriorInsertions > 0);
        assertTrue("no prioritised arrival ever became the new sorted head", sortedHeadDemotions > 0);
        assertTrue("R4/Q2: extendPriorityRegion never split the bag", partialPullForwards > 0);
        assertTrue("no unsequenced arrival was ever strengthened into the ordered region", unsequencedStrengthenings > 0);
    }

    private static final int FIFO = 0, SORTED = 1, BAG = 2;

    /**
     * The task pool a randomised run draws from, and the reason there is a pool at all: an add op that allocates a
     * <em>fresh</em> task per operation gives the arrival the largest {@code position} and the largest {@code fifoAt}
     * every time, so it can only ever generate an append. {@code addFifo}'s insertion loop breaks on the first
     * comparison, {@code addPrioritised} always sorts last (so the previous sorted head is never demoted and
     * {@code extendPriorityRegion} always pulls the whole bag forward rather than splitting it, which is the case R4/Q2
     * exist for) and {@code modelAddUnsequenced}'s strengthening branch is dead. Drawing {@code position} and
     * {@code fifoAt} as two independent permutations, and re-adding tasks that have been removed, is what makes those
     * paths reachable.
     *
     * <p>No two tasks share a {@code position}, and none share a {@code fifoAt}: {@code compare} breaks a position tie
     * by {@code createdAt}, and {@code addFifo} breaks a stamp tie by it too, but {@link Task#createdAt} is
     * {@code final} and therefore 0 on every mock - so a tie would leave {@code compare} a non-strict order (O1) and
     * the model unable to predict which of the pair the queue picks. Position ties are covered by
     * {@link #sortedPrefixBreaksTiesByExecutionKind}, where the execution kind decides instead.
     */
    private static List<SafeTask<?>> pool(Random rnd, int count, Map<SafeTask<?>, Integer> region,
                                          List<SafeTask<?>> notified, List<RunnableStatus> notifiedWith)
    {
        List<Long> positions = new ArrayList<>(), stamps = new ArrayList<>();
        for (int i = 0 ; i < count ; ++i)
        {
            positions.add(10L * (i + 1));
            stamps.add(nextFifoAt++);
        }
        Collections.shuffle(positions, rnd);
        Collections.shuffle(stamps, rnd);

        List<SafeTask<?>> pool = new ArrayList<>();
        for (int i = 0 ; i < count ; ++i)
        {
            SafeTask<?> task = recording(task(positions.get(i)), notified, notifiedWith);
            task.fifoAt = stamps.get(i);
            // O3: a task's region is a function of the task alone, so it is fixed here rather than chosen per add
            region.put(task, rnd.nextInt(3));
            pool.add(task);
        }
        return pool;
    }

    private void runOneRandomised(Random rnd, List<String> log)
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        List<SafeTask<?>> notified = new ArrayList<>();
        List<RunnableStatus> notifiedWith = new ArrayList<>();
        Map<SafeTask<?>, Integer> region = new IdentityHashMap<>();
        List<SafeTask<?>> pool = pool(rnd, 3 + rnd.nextInt(5), region, notified, notifiedWith);

        List<SafeTask<?>> fifo = new ArrayList<>(), prioritised = new ArrayList<>(), bag = new ArrayList<>();
        Set<SafeTask<?>> wasRunnable = new HashSet<>();

        for (int op = 0 ; op < 40 ; ++op)
        {
            List<SafeTask<?>> present = new ArrayList<>(fifo);
            present.addAll(prioritised);
            present.addAll(bag);
            List<SafeTask<?>> absent = new ArrayList<>(pool);
            absent.removeAll(present);

            notified.clear();
            notifiedWith.clear();
            SafeTask<?> arriving = null;
            // add where we can, remove a third of the time: a removal is what lets a task be re-added later, out of
            // both its position order and its stamp order
            if (!absent.isEmpty() && (present.isEmpty() || rnd.nextInt(3) != 0))
            {
                arriving = absent.get(rnd.nextInt(absent.size()));
                switch (region.get(arriving))
                {
                    default: throw new IllegalStateException();
                    case FIFO:
                    {
                        log.add("addFifo " + arriving + " fifoAt=" + arriving.fifoAt);
                        int at = fifoInsertionIndex(fifo, arriving);
                        if (!fifo.isEmpty())
                        {
                            if (at == 0) ++fifoHeadDisplacements;
                            else if (at < fifo.size()) ++fifoInteriorInsertions;
                        }
                        modelAddFifo(fifo, arriving);
                        q.addFifo(owner, arriving);
                        break;
                    }
                    case SORTED:
                    {
                        log.add("addPrioritised " + arriving);
                        if (!prioritised.isEmpty() && AccordCacheEntryQueue.compare(arriving, prioritised.get(0)) < 0)
                            ++sortedHeadDemotions;
                        int pulled = 0;
                        for (SafeTask<?> bagged : bag)
                        {
                            if (AccordCacheEntryQueue.compare(bagged, arriving) < 0)
                                ++pulled;
                        }
                        if (pulled > 0 && pulled < bag.size())
                            ++partialPullForwards;
                        modelAddPrioritised(prioritised, bag, arriving);
                        q.addPrioritised(owner, arriving);
                        break;
                    }
                    case BAG:
                        log.add("addUnsequenced " + arriving);
                        if (!prioritised.isEmpty()
                            && AccordCacheEntryQueue.compare(arriving, prioritised.get(prioritised.size() - 1)) < 0)
                            ++unsequencedStrengthenings;
                        modelAddUnsequenced(prioritised, bag, arriving);
                        q.addUnsequenced(owner, arriving);
                        break;
                }
            }
            else
            {
                SafeTask<?> remove = present.remove(rnd.nextInt(present.size()));
                log.add("remove " + remove);
                fifo.remove(remove); prioritised.remove(remove); bag.remove(remove);
                q.remove(owner, remove, RemoveMode.IF_PRESENT);
            }

            check(q);
            Set<SafeTask<?>> runnable = expectedRunnable(fifo, prioritised, bag);

            // the model and the queue agree on who may run
            for (SafeTask<?> task : present)
                assertEquals(task + " runnable", runnable.contains(task), isRunnable(q, task));

            if (arriving != null)
                assertEquals(arriving + " runnable", runnable.contains(arriving), isRunnable(q, arriving));

            // and the notifications are exactly the transitions, for everyone but the arriving task, whose status is
            // reported by the return value rather than a notification
            for (SafeTask<?> task : runnable)
            {
                if (task != arriving && !wasRunnable.contains(task))
                    assertEquals(task + " should have been promoted once", 1, count(notified, notifiedWith, task, true));
            }
            for (int i = 0 ; i < notified.size() ; ++i)
            {
                SafeTask<?> task = notified.get(i);
                if (task == arriving)
                    continue;
                boolean isPromotion = isPromotion(notifiedWith.get(i));
                boolean isDemotion = notifiedWith.get(i) == RunnableStatus.NOT_RUNNABLE;
                if (isDemotion)
                    assertTrue(task + " demoted but was not runnable: " + notifiedWith.get(i), wasRunnable.contains(task));
                if (isPromotion)
                    assertTrue(task + " promoted but was already runnable", !wasRunnable.contains(task));
            }

            wasRunnable = runnable;
        }
    }

    /** Q4, stated without reference to the region layout */
    private static Set<SafeTask<?>> expectedRunnable(List<SafeTask<?>> fifo, List<SafeTask<?>> prioritised, List<SafeTask<?>> bag)
    {
        Set<SafeTask<?>> runnable = new HashSet<>();
        if (!fifo.isEmpty()) runnable.add(fifo.get(0));
        else if (!prioritised.isEmpty()) runnable.add(prioritised.stream().min(AccordCacheEntryQueue::compare).get());
        else runnable.addAll(bag);
        return runnable;
    }

    private static boolean isPromotion(RunnableStatus status)
    {
        return status == RunnableStatus.NEWLY_RUNNABLE || status == RunnableStatus.NEWLY_BLOCKING_RUNNABLE;
    }

    private static int count(List<SafeTask<?>> notified, List<RunnableStatus> with, SafeTask<?> task, boolean promotions)
    {
        int count = 0;
        for (int i = 0 ; i < notified.size() ; ++i)
        {
            if (notified.get(i) == task && isPromotion(with.get(i)) == promotions)
                ++count;
        }
        return count;
    }

    private static SafeTask<?> recording(SafeTask<?> task, List<SafeTask<?>> notified, List<RunnableStatus> with)
    {
        doAnswer(invocation -> {
            notified.add(task);
            with.add(invocation.getArgument(1));
            return null;
        }).when(task).onChangeRunnableStatus(any(), any());
        doAnswer(invocation -> {
            notified.add(task);
            with.add(invocation.getArgument(1));
            return null;
        }).when(task).onChangeRunnableStatus(any(), any());
        return task;
    }

    private static long nextPosition = 1;
    private static long nextFifoAt = 1;

    /**
     * The fifo region is ordered by {@code fifoAt}, which production stamps when a task first becomes fifo - at setup for
     * an ATOMIC task, at first prepare for an INCR one. A task's rank is therefore global rather than per-entry, so the
     * model inserts by it rather than appending.
     */
    private static void modelAddFifo(List<SafeTask<?>> fifo, SafeTask<?> arriving)
    {
        fifo.add(fifoInsertionIndex(fifo, arriving), arriving);
    }

    private static int fifoInsertionIndex(List<SafeTask<?>> fifo, SafeTask<?> arriving)
    {
        int i = 0;
        while (i < fifo.size() && fifo.get(i).fifoAt <= arriving.fifoAt)
            ++i;
        return i;
    }



    /**
     * A prioritised task requires every task to be sorted with respect to it, so a bag member that sorts before the
     * arrival is strengthened into the ordered region. Optimally there would be a bag between each ordered task; instead
     * the ordering is imposed only where a prioritised task needs it.
     */
    private static void modelAddPrioritised(List<SafeTask<?>> ordered, List<SafeTask<?>> bag, SafeTask<?> arriving)
    {
        for (Iterator<SafeTask<?>> iter = bag.iterator() ; iter.hasNext() ; )
        {
            SafeTask<?> bagged = iter.next();
            if (AccordCacheEntryQueue.compare(bagged, arriving) < 0)
            {
                ordered.add(bagged);
                iter.remove();
            }
        }
        ordered.add(arriving);
        ordered.sort(AccordCacheEntryQueue::compare);
    }

    /** an unsequenced arrival is itself strengthened when an ordered task requires it to be sorted with respect to it */
    private static void modelAddUnsequenced(List<SafeTask<?>> ordered, List<SafeTask<?>> bag, SafeTask<?> arriving)
    {
        if (!ordered.isEmpty() && AccordCacheEntryQueue.compare(arriving, ordered.get(ordered.size() - 1)) < 0)
        {
            ordered.add(arriving);
            ordered.sort(AccordCacheEntryQueue::compare);
        }
        else bag.add(arriving);
    }


    /**
     * {@link AccordCacheEntryQueue#addFifo} orders the fifo region by {@code fifoAt}, so a claim stamped later queues
     * behind one stamped earlier and the earlier one must be told it now blocks somebody.
     */
    @Test
    public void addFifoQueuesBehindAnExistingClaim()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> first = fifoTask(1), second = fifoTask(2), bagged = task(9);
        q.addUnsequenced(owner, bagged);
        assertSame(RunnableStatus.NEWLY_BLOCKING_RUNNABLE, q.addFifo(owner, first));
        check(q);
        assertSame(first, q.peekFifoOrPriority());
        verify(bagged).onChangeRunnableStatus(owner, RunnableStatus.NOT_RUNNABLE);

        // the second claim does not take the lead: within the fifo region fifoAt order is the order
        assertSame(RunnableStatus.NOT_RUNNABLE, q.addFifo(owner, second));
        check(q);
        assertSame(first, q.peekFifoOrPriority());
        assertFalse(isRunnable(q, second));
        assertEquals(Arrays.asList(first, second, bagged), drain(q));
    }

    /** adding to an empty queue makes us runnable outright, with nobody to demote */
    @Test
    public void addFifoOntoAnEmptyQueueIsRunnable()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> only = fifoTask(1);
        assertSame(RunnableStatus.NEWLY_RUNNABLE, q.addFifo(owner, only));
        check(q);
        assertTrue(isRunnable(q, only));
        assertEquals(Arrays.asList(only), drain(q));
    }

    /** a task that is fifo, with an empty refs map so the queue cannot infer a position for it from a held reference */
    private static SafeTask<?> fifoTask(long position)
    {
        SafeTask<?> task = task(position);
        when(task.isCacheQueuedFifo()).thenReturn(true);
        return task;
    }

    private static SafeTask<?> fifoTask(long position, long fifoAt)
    {
        SafeTask<?> task = fifoTask(position);
        task.fifoAt = fifoAt;
        return task;
    }

    /**
     * Whether {@code Invariants.expect} throws rather than logs. Read from the property rather than from
     * {@code Invariants}, which exposes no accessor; the value is latched at its class initialisation, so this is the
     * same answer as long as nobody rewrites the property mid-run.
     */
    private static final boolean EXPECT_FAILS = Boolean.parseBoolean(System.getProperty("accord.testing", "false"));

    /**
     * O8: the fifo region is ordered by {@code fifoAt}, and the lock holder is pinned at its head - everything behind it
     * waits for it, so an arrival that sorts ahead of it must not take the head. That can only happen if a stamp was
     * issued out of order (O6/O7), so {@code addFifo} reports it via {@code Invariants.expect} and then pins the arrival
     * behind the holder rather than corrupting the order silently. Nothing else in the suite constructs a fifo claim
     * whose {@code fifoAt} is lower than an existing member's <em>against a lock holder</em>, so without this the branch
     * is never taken.
     */
    @Test
    public void addFifoPinsALowerStampedArrivalBehindTheLockHolder()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> holder = fifoTask(10, 5), older = fifoTask(20, 1);
        assertSame(RunnableStatus.NEWLY_RUNNABLE, q.addFifo(owner, holder));
        q.lock(holder);
        check(q);

        boolean reported = false;
        try
        {
            q.addFifo(owner, older);
        }
        catch (IllegalStateException e)
        {
            reported = true;
            assertTrue("O8 must name the displacement: " + e.getMessage(),
                       e.getMessage().contains("would displace lock holder"));
        }
        assertEquals("O8's report is only a failure when Invariants.expect throws", EXPECT_FAILS, reported);
        if (reported)
            return; // the report fired before the insertion, so there is no post-state to check

        // Reported and pinned: the holder still leads and the arrival sits immediately behind it. That is deliberately
        // *not* fifoAt order, so check(q) would rightly reject it and is not called - the pin trades Q5 for R5, because
        // the holder's followers are already waiting on it and demoting it would strand them.
        assertSame(holder, q.peekFifoOrPriority());
        assertEquals(2, q.fifoSize());
        assertSame(holder, q.tasks[q.fifoHead]);
        assertSame(older, q.tasks[q.fifoHead - 1]);
        assertFalse(isRunnable(q, older));
    }

    /** the same arrival with no lock holder legitimately takes the head, and demotes the previous one */
    @Test
    public void addFifoTakesTheHeadFromAnUnlockedLowerPriorityClaim()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> later = fifoTask(10, 5), older = fifoTask(20, 1);
        assertSame(RunnableStatus.NEWLY_RUNNABLE, q.addFifo(owner, later));
        // Q5: the arrival is stamped earlier, so it is inserted ahead of the incumbent, which must be told it no longer
        // leads. This is the branch that is unreachable while every arrival carries the largest stamp.
        assertSame(RunnableStatus.NEWLY_BLOCKING_RUNNABLE, q.addFifo(owner, older));
        check(q);
        assertSame(older, q.peekFifoOrPriority());
        verify(later).onChangeRunnableStatus(owner, RunnableStatus.NOT_RUNNABLE);
        assertEquals(Arrays.asList(older, later), drain(q));
    }

    /** a notification, attributed to the entry that sent it */
    private static class Note
    {
        final SafeTask<?> task;
        final RunnableStatus status;

        Note(SafeTask<?> task, RunnableStatus status)
        {
            this.task = task;
            this.status = status;
        }
    }

    /**
     * Randomised property test over several queues sharing a set of tasks, checking the property a single queue cannot:
     * that each task's wait count stays in agreement with reality across all of its entries.
     *
     * <p>Production keeps this count in {@code SafeTask.waitingForState}, incrementing it on each demotion and
     * decrementing it on each promotion, and runs the task when it reaches zero. We rebuild the same count from the
     * notifications the queues emit, and require it to equal the number of entries on which the model says the task is
     * not in the runnable prefix. A duplicated demotion, or a promotion that never arrives, leaves the count permanently
     * above zero - which is a stall in production, and here is an assertion failure with a replayable op sequence.
     */
    @Test
    public void randomisedMultiEntryWaitAccounting()
    {
        for (int iteration = 0 ; iteration < 300 ; ++iteration)
        {
            long seed = 20260901L + iteration;
            List<String> log = new ArrayList<>();
            try
            {
                runOneMultiEntry(new Random(seed), log);
            }
            catch (Throwable t)
            {
                throw new AssertionError("seed " + seed + " failed after:\n  " + String.join("\n  ", log), t);
            }
        }
    }

    private void runOneMultiEntry(Random rnd, List<String> log)
    {
        int entries = 2 + rnd.nextInt(3);
        List<AccordCacheEntryQueue> queues = new ArrayList<>();
        List<AccordCacheEntry<?, ?, ?>> owners = new ArrayList<>();
        List<List<SafeTask<?>>> fifo = new ArrayList<>(), prioritised = new ArrayList<>(), bag = new ArrayList<>();
        for (int i = 0 ; i < entries ; ++i)
        {
            queues.add(new AccordCacheEntryQueue());
            owners.add(owner());
            fifo.add(new ArrayList<>());
            prioritised.add(new ArrayList<>());
            bag.add(new ArrayList<>());
        }

        List<Note> notes = new ArrayList<>();
        List<SafeTask<?>> tasks = new ArrayList<>();
        Map<SafeTask<?>, Integer> kinds = new IdentityHashMap<>(), waits = new IdentityHashMap<>();
        for (int i = 0, count = 3 + rnd.nextInt(4) ; i < count ; ++i)
        {
            SafeTask<?> task = task(nextPosition++);
            doAnswer(invocation -> {
                notes.add(new Note(task, invocation.getArgument(1)));
                return null;
            }).when(task).onChangeRunnableStatus(any(), any());
            doAnswer(invocation -> {
                notes.add(new Note(task, invocation.getArgument(1)));
                return null;
            }).when(task).onChangeRunnableStatus(any(), any());
            tasks.add(task);
            // a task's kind is fixed: it is fifo on every entry, or ordered on every entry, or unsequenced on every one
            kinds.put(task, rnd.nextInt(3));
            waits.put(task, 0);
        }

        for (int op = 0 ; op < 60 ; ++op)
        {
            int e = rnd.nextInt(entries);
            AccordCacheEntryQueue q = queues.get(e);
            AccordCacheEntry<?, ?, ?> owner = owners.get(e);
            List<SafeTask<?>> onEntry = new ArrayList<>(fifo.get(e));
            onEntry.addAll(prioritised.get(e));
            onEntry.addAll(bag.get(e));
            List<SafeTask<?>> absent = new ArrayList<>(tasks);
            absent.removeAll(onEntry);

            notes.clear();
            if (absent.isEmpty() || (!onEntry.isEmpty() && rnd.nextInt(3) == 0))
            {
                SafeTask<?> remove = onEntry.get(rnd.nextInt(onEntry.size()));
                log.add("entry" + e + " remove " + remove);
                // our own wait for this entry goes away with our position, as it does when we release a reference
                if (!expectedRunnable(fifo.get(e), prioritised.get(e), bag.get(e)).contains(remove))
                    waits.merge(remove, -1, Integer::sum);
                fifo.get(e).remove(remove);
                prioritised.get(e).remove(remove);
                bag.get(e).remove(remove);
                q.remove(owner, remove, RemoveMode.IF_PRESENT);
            }
            else
            {
                SafeTask<?> add = absent.get(rnd.nextInt(absent.size()));
                RunnableStatus status;
                switch (kinds.get(add))
                {
                    default: throw new IllegalStateException();
                    case 0:
                        log.add("entry" + e + " addFifo " + add);
                        modelAddFifo(fifo.get(e), add);
                        status = q.addFifo(owner, add);
                        break;
                    case 1:
                        log.add("entry" + e + " addPrioritised " + add);
                        modelAddPrioritised(prioritised.get(e), bag.get(e), add);
                        status = q.addPrioritised(owner, add);
                        break;
                    case 2:
                        log.add("entry" + e + " addUnsequenced " + add);
                        modelAddUnsequenced(prioritised.get(e), bag.get(e), add);
                        status = q.addUnsequenced(owner, add);
                        break;
                }
                // an arrival learns its own status from the return value rather than a notification
                if (status == RunnableStatus.NOT_RUNNABLE)
                    waits.merge(add, 1, Integer::sum);
            }

            for (Note note : notes)
            {
                if (note.status == RunnableStatus.NOT_RUNNABLE)
                    waits.merge(note.task, 1, Integer::sum);
                else if (isPromotion(note.status))
                    waits.merge(note.task, -1, Integer::sum);
            }

            check(q);
            for (int i = 0 ; i < entries ; ++i)
            {
                Set<SafeTask<?>> runnable = expectedRunnable(fifo.get(i), prioritised.get(i), bag.get(i));
                for (SafeTask<?> task : tasks)
                {
                    if (fifo.get(i).contains(task) || prioritised.get(i).contains(task) || bag.get(i).contains(task))
                        assertEquals(task + " runnable on entry" + i, runnable.contains(task), isRunnable(queues.get(i), task));
                }
            }

            for (SafeTask<?> task : tasks)
            {
                int expected = 0;
                for (int i = 0 ; i < entries ; ++i)
                {
                    boolean queued = fifo.get(i).contains(task) || prioritised.get(i).contains(task) || bag.get(i).contains(task);
                    if (queued && !expectedRunnable(fifo.get(i), prioritised.get(i), bag.get(i)).contains(task))
                        ++expected;
                }
                assertEquals(task + " waits on " + expected + " entries", expected, (int) waits.get(task));
            }
        }
    }

    /** an entry that is loaded and holds keys, so validate() accepts a non-sync task as waiting on it */
    private static AccordCacheEntry<?, ?, ?> owner()
    {
        AccordCacheEntry<?, ?, ?> owner = mock(AccordCacheEntry.class);
        when(owner.isCommandsForKey()).thenReturn(true);
        when(owner.isLoaded()).thenReturn(true);
        return owner;
    }

    private static SafeTask<?> task(long position, ExecutionKind kind)
    {
        SafeTask<?> task = mock(SafeTask.class);
        ExecutionContext context = mock(ExecutionContext.class);
        when(context.executionKind()).thenReturn(kind);
        when(task.executionContext()).thenReturn(context);
        when(task.toString()).thenReturn("task" + nextId++ + '@' + position);
        // validate() requires every task it does not expect to be runnable to be waiting on its caches, which for a
        // non-sync task is implied by its kind
        when(task.isNonSync()).thenReturn(true);
        // validate() requires a task it does not expect to be runnable to be waiting on its caches, and a queued task
        // here stands for one that is waiting to run
        when(task.is(Task.State.WAITING_TO_RUN)).thenReturn(true);
        // reposition asks whether a task that is not queued here nonetheless holds a reference to the entry
        task.refs = new org.agrona.collections.Object2ObjectHashMap<>();
        task.position = position;
        // addFifo requires a stamp, and orders the region by it; these tasks are created in the order they become fifo
        task.fifoAt = nextFifoAt++;
        return task;
    }

    private static SafeTask<?> task(long position)
    {
        return task(position, ExecutionKind.OTHER);
    }

    private static boolean isRunnable(AccordCacheEntryQueue q, SafeTask<?> task)
    {
        return q.statusIfPresent(task) != RunnableStatus.NOT_RUNNABLE;
    }

    /**
     * Drain the queue by repeatedly removing whatever leads it. {@link AccordCacheEntryQueue#peekFifoOrPriority} exposes only the
     * fifo or sorted head, so once those are exhausted we take the bag - whose members are all runnable (Q4) and
     * mutually unordered (Q3), so they come out in no particular order.
     */
    private static List<SafeTask<?>> drain(AccordCacheEntryQueue q)
    {
        List<SafeTask<?>> order = new ArrayList<>();
        for (SafeTask<?> head = q.peekFifoOrPriority(); head != null; head = q.peekFifoOrPriority())
        {
            assertTrue("the head must be runnable", isRunnable(q, head));
            q.remove(null, head, RemoveMode.REQUIRE_RUNNABLE);
            order.add(head);
            check(q);
        }

        while (q.unsequencedSize() > 0)
        {
            SafeTask<?> bagged = q.tasks[q.priorityTail];
            assertTrue("every bag member must be runnable once nothing is sorted", isRunnable(q, bagged));
            q.remove(null, bagged, RemoveMode.REQUIRE_RUNNABLE);
            order.add(bagged);
            check(q);
        }
        return order;
    }

    @Test
    public void sortedPrefixOrdersByPosition()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> a = task(1), b = task(2), c = task(3), d = task(4);
        for (SafeTask<?> task : new SafeTask<?>[]{ c, a, d, b })
        {
            q.addPrioritised(null, task);
            check(q);
        }

        assertSame(a, q.peekFifoOrPriority());
        assertEquals(Arrays.asList(a, b, c, d), drain(q));
    }

    @Test
    public void sortedPrefixBreaksTiesByExecutionKind()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        // the same position, so the kind decides
        SafeTask<?> apply = task(1, ExecutionKind.APPLY), commit = task(1, ExecutionKind.COMMIT), preaccept = task(1, ExecutionKind.PREACCEPT);
        for (SafeTask<?> task : new SafeTask<?>[]{ apply, preaccept, commit })
        {
            q.addPrioritised(null, task);
            check(q);
        }

        assertEquals(Arrays.asList(preaccept, commit, apply), drain(q));
    }

    @Test
    public void bagIsTheWholePrefixWhileNothingIsSorted()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> a = task(3), b = task(1), c = task(2);
        for (SafeTask<?> task : new SafeTask<?>[]{ a, b, c })
        {
            q.addUnsequenced(null, task);
            check(q);
        }

        // Q3/Q4: unordered, and all of them may run
        assertEquals(3, q.runnablePrefix());
        assertTrue(isRunnable(q, a));
        assertTrue(isRunnable(q, b));
        assertTrue(isRunnable(q, c));
        assertEquals(0, q.prioritySize());
    }

    @Test
    public void whatSortsPastThePrefixIsBagged()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> sorted = task(5);
        q.addPrioritised(null, sorted);
        check(q);

        SafeTask<?> after = task(9);
        assertFalse("sorts past the prefix, so it may be bagged", q.placeInPriorityRegion(after));
        q.addUnsequenced(null, after);
        check(q);

        // Q4: only the sorted claim leads the entry
        assertSame(sorted, q.peekFifoOrPriority());
        assertEquals(1, q.runnablePrefix());
        assertTrue(isRunnable(q, sorted));
        assertFalse(isRunnable(q, after));

        // one that sorts within the prefix may not be bagged: the entry sorts it instead
        assertTrue(q.placeInPriorityRegion(task(1)));
    }

    @Test
    public void pullForwardTakesOnlyWhatSortsBefore()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> bagged1 = task(1), bagged9 = task(9), bagged2 = task(2), bagged8 = task(8);
        for (SafeTask<?> task : new SafeTask<?>[]{ bagged1, bagged9, bagged2, bagged8 })
            q.addUnsequenced(null, task);
        check(q);

        SafeTask<?> arrival = task(5);
        assertEquals(2, q.extendPriorityRegion(arrival));
        check(q);
        q.addPrioritised(null, arrival);
        check(q);

        // those that sort before the arrival are now the sorted prefix, in order; the rest are still bagged (Q2)
        assertEquals(3, q.prioritySize());
        assertEquals(2, q.unsequencedSize());
        assertSame(bagged1, q.peekFifoOrPriority());
        assertEquals(1, q.runnablePrefix());
        assertFalse(isRunnable(q, bagged9));
        assertFalse(isRunnable(q, bagged8));

        // the bag regains the prefix as soon as nothing is sorted
        q.remove(null, bagged1, RemoveMode.REQUIRE_RUNNABLE);
        q.remove(null, bagged2, RemoveMode.REQUIRE_RUNNABLE);
        q.remove(null, arrival, RemoveMode.REQUIRE_RUNNABLE);
        check(q);
        assertEquals(2, q.runnablePrefix());
        assertTrue(isRunnable(q, bagged8));
        assertTrue(isRunnable(q, bagged9));
        q.addPrioritised(null, bagged1);
        q.addPrioritised(null, bagged2);
        q.addPrioritised(null, arrival);
        check(q);

        // once the sorted prefix drains, the bag is the prefix again (Q4) and follows in no particular order (Q3)
        List<SafeTask<?>> order = drain(q);
        assertEquals(Arrays.asList(bagged1, bagged2, arrival), order.subList(0, 3));
        assertTrue(order.subList(3, 5).containsAll(Arrays.asList(bagged8, bagged9)));
    }

    /** a task becoming fifo where others already lead the entry */
    @Test
    public void moveToFifoJoinsAPopulatedRegionAtTheBack()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> hasRun = task(1), alsoRan = task(2), becomesFifo = task(3), bagged = task(9);
        when(hasRun.hasStartedRunning()).thenReturn(true);
        when(alsoRan.hasStartedRunning()).thenReturn(true);
        q.addFifo(null, hasRun);
        q.addFifo(null, alsoRan);
        q.addUnsequenced(null, bagged);
        q.addPrioritised(null, becomesFifo);
        check(q);

        // we join behind those that have already run, and are not runnable
        assertSame(RunnableStatus.NOT_RUNNABLE, q.moveToFifo(null, becomesFifo));
        check(q);
        assertSame(hasRun, q.peekFifoOrPriority());
        assertFalse(isRunnable(q, becomesFifo));
        assertEquals(3, q.fifoSize());

        // and a second call is a no-op, since we already hold our position
        assertSame(RunnableStatus.NOT_RUNNABLE, q.moveToFifo(null, becomesFifo));
        check(q);
        assertEquals(3, q.fifoSize());
        assertEquals(Arrays.asList(hasRun, alsoRan, becomesFifo, bagged), drain(q));
    }

    /** a task that already leads the entry keeps the lead, and nobody's status changes */
    @Test
    public void moveToFifoKeepsAnExistingLead()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> leads = task(1), bagged = task(9);
        q.addPrioritised(null, leads);
        q.addUnsequenced(null, bagged);
        check(q);

        assertSame(RunnableStatus.STILL_RUNNABLE, q.moveToFifo(null, leads));
        check(q);
        assertEquals(1, q.fifoSize());
        assertEquals(0, q.prioritySize());
        assertTrue(isRunnable(q, leads));

        assertSame(RunnableStatus.STILL_RUNNABLE, q.moveToFifo(null, leads));
        check(q);
        assertEquals(Arrays.asList(leads, bagged), drain(q));
    }

    @Test
    public void fifoPrecedesSortedAndBagged()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> bagged = task(9), sorted = task(1), fifo = task(7);
        q.addUnsequenced(null, bagged);
        q.addPrioritised(null, sorted);
        check(q);
        q.addFifo(null, fifo);
        check(q);

        // a fifo claim leads the entry whatever its position
        assertSame(fifo, q.peekFifoOrPriority());
        assertEquals(1, q.runnablePrefix());
        assertTrue(isRunnable(q, fifo));
        assertFalse(isRunnable(q, sorted));
        assertFalse(isRunnable(q, bagged));

        assertEquals(Arrays.asList(fifo, sorted, bagged), drain(q));
    }

    @Test
    public void removingWithinTheSortedPrefixKeepsTheBagContiguous()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> a = task(1), b = task(2), c = task(3), bagged = task(9);
        q.addPrioritised(null, a);
        q.addPrioritised(null, b);
        q.addPrioritised(null, c);
        q.addUnsequenced(null, bagged);
        check(q);

        // removing from the middle of the prefix closes the gap, which the bag sits immediately above
        q.remove(null, b, RemoveMode.REQUIRE_PRESENT);
        check(q);
        assertEquals(2, q.prioritySize());
        assertEquals(1, q.unsequencedSize());
        assertEquals(1, q.unsequencedSize());

        assertEquals(Arrays.asList(a, c, bagged), drain(q));
    }

    @Test
    public void removesFromTheBagWithoutDisturbingTheOrder()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> sorted = task(1), first = task(7), second = task(8), third = task(9);
        q.addPrioritised(null, sorted);
        q.addUnsequenced(null, first);
        q.addUnsequenced(null, second);
        q.addUnsequenced(null, third);
        check(q);

        q.remove(null, second, RemoveMode.REQUIRE_PRESENT);
        check(q);
        assertEquals(2, q.unsequencedSize());
        q.remove(null, second, RemoveMode.IF_PRESENT); // idempotent

        assertEquals(sorted, drain(q).get(0));
    }

    @Test
    public void peekAnyReportsALoneBagMember()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        SafeTask<?> bagged = task(1);
        q.addUnsequenced(null, bagged);
        check(q);

        // peek() reports only the ordered regions, so an entry that collapses its queue on the strength of peek() would
        // drop a lone bag member; single() is what that decision needs
        assertEquals(1, q.totalSize());
        assertEquals(null, q.peekFifoOrPriority());
        assertSame(bagged, q.peekAny());
    }

    @Test
    public void moveToFifoDemotesThePreviousPriorityHead()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> head = task(1), mover = task(2), bagged = task(9);
        q.addPrioritised(null, head);
        q.addPrioritised(null, mover);
        q.addUnsequenced(null, bagged);
        check(q);

        // we were queued behind head, so taking the fifo head makes us runnable and costs head the prefix
        assertSame(RunnableStatus.NEWLY_BLOCKING_RUNNABLE, q.moveToFifo(owner, mover));
        check(q);
        assertSame(mover, q.peekFifoOrPriority());
        verify(head).onChangeRunnableStatus(owner, RunnableStatus.NOT_RUNNABLE);
        // the bag was already behind an ordered claim, so it must not be told again
        verify(bagged, never()).onChangeRunnableStatus(owner, RunnableStatus.NOT_RUNNABLE);
    }

    @Test
    public void ensureHeadFifoDemotesTheRestOfTheBag()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> mover = task(2), other1 = task(7), other2 = task(9);
        q.addUnsequenced(null, mover);
        q.addUnsequenced(null, other1);
        q.addUnsequenced(null, other2);
        check(q);

        // the whole bag was the prefix; once we take the fifo head, the rest of it is not runnable
        assertSame(RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING, q.moveToFifo(owner, mover));
        check(q);
        assertSame(mover, q.peekFifoOrPriority());
        assertEquals(2, q.unsequencedSize());
        verify(other1).onChangeRunnableStatus(owner, RunnableStatus.NOT_RUNNABLE);
        verify(other2).onChangeRunnableStatus(owner, RunnableStatus.NOT_RUNNABLE);
    }

    @Test
    public void ensureHeadFifoKeepsTheHeadRunnableWithoutNotifying()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> mover = task(1), behind = task(2), bagged = task(9);
        q.addPrioritised(null, mover);
        q.addPrioritised(null, behind);
        q.addUnsequenced(null, bagged);
        check(q);

        // we already led the entry: nobody gains or loses the prefix
        assertSame(RunnableStatus.STILL_RUNNABLE, q.moveToFifo(owner, mover));
        check(q);
        assertSame(mover, q.peekFifoOrPriority());
        verify(behind, never()).onChangeRunnableStatus(owner, RunnableStatus.NOT_RUNNABLE);
        verify(bagged, never()).onChangeRunnableStatus(owner, RunnableStatus.NOT_RUNNABLE);
    }

    @Test
    public void hintsTheHeadOnlyWhenItFirstAcquiresAFollower()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> head = task(1), second = task(2), third = task(3);
        q.addPrioritised(owner, head);
        check(q);

        // the head is still runnable but now blocks someone: that is what a batched task uses to run before it has a
        // full batch, and to prefer the keys others are waiting on
        assertSame(RunnableStatus.NOT_RUNNABLE, q.addPrioritised(owner, second));
        check(q);
        verify(head).onChangeRunnableStatus(owner, RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING);

        // it already knows it is blocking, so a further follower says nothing
        assertSame(RunnableStatus.NOT_RUNNABLE, q.addPrioritised(owner, third));
        check(q);
        verify(head, times(1)).onChangeRunnableStatus(owner, RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING);
    }

    @Test
    public void hintsTheHeadWhenBaggedBehindIt()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> head = task(1), bagged = task(9);
        q.addPrioritised(owner, head);
        check(q);

        assertSame(RunnableStatus.NOT_RUNNABLE, q.addUnsequenced(owner, bagged));
        check(q);
        assertEquals(1, q.unsequencedSize());
        verify(head).onChangeRunnableStatus(owner, RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING);
    }

    @Test
    public void doesNotHintWhenJoiningABagThatBlocksNobody()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        AccordCacheEntry<?, ?, ?> owner = owner();
        SafeTask<?> first = task(7), second = task(9);
        q.addUnsequenced(owner, first);
        // Q3: bag members do not wait for each other, so nobody is blocked and there is nothing to hint
        assertSame(RunnableStatus.NEWLY_RUNNABLE, q.addUnsequenced(owner, second));
        check(q);
        verify(first, never()).onChangeRunnableStatus(owner, RunnableStatus.STILL_RUNNABLE_NEWLY_BLOCKING);
    }

    @Test
    public void growsAndCompactsWithABag()
    {
        AccordCacheEntryQueue q = new AccordCacheEntryQueue();
        List<SafeTask<?>> sorted = new ArrayList<>(), bagged = new ArrayList<>();
        // far more than the initial capacity, so that we both grow and compact with a bag present
        for (int i = 0; i < 12; ++i)
        {
            SafeTask<?> task = task(100 - i);
            sorted.add(task);
            q.addPrioritised(null, task);
            check(q);

            SafeTask<?> bag = task(1000 + i);
            bagged.add(bag);
            q.addUnsequenced(null, bag);
            check(q);
        }

        assertEquals(12, q.prioritySize());
        assertEquals(12, q.unsequencedSize());

        List<SafeTask<?>> order = drain(q);
        assertEquals(24, order.size());
        // the sorted claims come out in position order, ahead of the bag
        List<SafeTask<?>> expectedSorted = new ArrayList<>(sorted);
        expectedSorted.sort(AccordCacheEntryQueue::compare);
        assertEquals(expectedSorted, order.subList(0, 12));
        assertTrue(order.subList(12, 24).containsAll(bagged));
    }
}
