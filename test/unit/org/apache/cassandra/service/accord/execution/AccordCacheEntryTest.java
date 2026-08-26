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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import accord.local.Command;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;
import accord.local.SafeState;
import accord.primitives.TxnId;

import org.apache.cassandra.service.accord.execution.AccordCache.Type;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AccordCacheEntryTest
{
    static class TestSafeState extends SafeState<String> implements SaferState<String, String, TestSafeState>
    {
        @Override public AccordCacheEntry<String, String, TestSafeState> global() { return null; }
        @Override public void preExecute(SafeTask<?> owner, LockMode lockMode) {}
        @Override public void postExecute(SafeTask<?> owner) {}
    }

    static class CacheEntry extends AccordCacheEntry<String, String, TestSafeState>
    {
        public CacheEntry(String key, Type<String, String, TestSafeState>.Instance instance)
        {
            super(key, instance);
        }

        public CacheEntry(String key)
        {
            this(key, null);
        }
    }

    private static void assertIllegalState(Runnable runnable)
    {
        try
        {
            runnable.run();
            Assert.fail("Expected IllegalStateException");
        }
        catch (IllegalStateException ise)
        {
            // expected
        }
    }

    @Test
    public void loadSuccessTest()
    {
        CacheEntry state = new CacheEntry("K");

        Assert.assertEquals(Status.UNINITIALIZED, state.status());
        assertIllegalState(state::getExclusive);
        assertIllegalState(() -> state.setExclusive("VVVV"));
        assertIllegalState(state::loading);

        state.readyToLoad();
        state.testLoad();
        Assert.assertEquals(Status.LOADING, state.status());

        state.testLoaded("V");
        Assert.assertEquals(Status.LOADED, state.status());
        Assert.assertEquals("V", state.getExclusive());

        assertIllegalState(state::testLoad);
        assertIllegalState(() -> state.loaded(null));
        assertIllegalState(state::loading);
    }

    @Test
    public void loadNullTest()
    {
        CacheEntry state = new CacheEntry("K");
        Assert.assertEquals(Status.UNINITIALIZED, state.status());

        assertIllegalState(state::getExclusive);
        assertIllegalState(() -> state.setExclusive("VVVV"));
        assertIllegalState(state::loading);

        state.readyToLoad();
        state.testLoad();
        Assert.assertEquals(Status.LOADING, state.status());

        // TODO (expected): this is sort of a pointless test now - remove it?
        state.testLoaded(null);
        Assert.assertEquals(Status.LOADED, state.status());
        Assert.assertNull(state.getExclusive());

        assertIllegalState(state::testLoad);
        assertIllegalState(state::failedToLoad);
        assertIllegalState(state::loading);
    }

    @Test
    public void loadFailureTest()
    {
        CacheEntry state = new CacheEntry("K");

        Assert.assertEquals(Status.UNINITIALIZED, state.status());
        assertIllegalState(state::getExclusive);
        assertIllegalState(() -> state.setExclusive("VVVV"));
        assertIllegalState(state::loading);

        state.readyToLoad();
        state.testLoad();
        state.failedToLoad();
        Assert.assertEquals(Status.FAILED_TO_LOAD, state.status());
        assertIllegalState(state::getExclusive);
    }

    // The test methods below have been authored entirely by Claude.

    // ---------------------------------------------------------------------------------------------------------------
    // R8: the queue union - null | SafeTask | AccordCacheEntryMiniQueue | AccordCacheEntryQueue.
    //
    // spec/accord-execution/README.md delegates this algebra to "validate() and AccordCacheEntryQueueTest", but that
    // suite constructs an AccordCacheEntryQueue directly and so never goes through ensureQueue/maybeUnwrap; the mini
    // queue - the two-claim case, which exists only while locked, and whose head is the holder if HOLD_QUEUE and the
    // other claim otherwise - had no test at all. The ladders below drive
    // null -> 1 claim (bare) -> lock -> 2nd claim (mini) -> 3rd claim (full) -> removals (back to bare, then null),
    // asserting membership, the runnable prefix, the fifo count and statusIfPresent at each rung.
    // ---------------------------------------------------------------------------------------------------------------

    /**
     * What a failed run owes the command it holds: discard the mutations the run made, keep the ones already handed to the
     * journal. An incremental fan-out journals per round, so those two sets are both non-empty from its second round on -
     * abandoning the reference would drop what the log already has and leave the cache behind it, and releasing as-is would
     * publish a half-applied command from the run that threw.
     */
    @Test
    public void unjournalledUpdatesAreDiscardedAndJournalledOnesKept()
    {
        TxnId txnId = TxnId.fromValues(1, 1, 0, new accord.local.Node.Id(1));
        Command loaded = mock(Command.class), journalled = mock(Command.class), failed = mock(Command.class);
        AccordCacheEntry<TxnId, Command, SaferCommand> entry = AccordExecutionTestUtils.loaded(txnId, loaded);
        SaferCommand safeCommand = new SaferCommand(entry);
        AccordExecutionTestUtils.preExecute(safeCommand);

        assertFalse("nothing has changed, so there is nothing to journal", safeCommand.hasUpdate());

        // round one mutates and journals
        safeCommand.set(journalled);
        assertTrue(safeCommand.hasUpdate());
        safeCommand.saveUpdate();
        assertFalse("what we handed over is no longer outstanding", safeCommand.hasUpdate());
        assertSame(journalled, safeCommand.current());

        // round two mutates and throws
        safeCommand.set(failed);
        assertTrue(safeCommand.hasUpdate());
        safeCommand.discardUpdate();

        assertSame("the failed run's mutation must be discarded", journalled, safeCommand.current());
        assertTrue("but the journalled state is still owed to the entry, or the cache falls behind the log",
                   safeCommand.isModified());
        assertFalse(safeCommand.hasUpdate());
        // and the diff we would write now is empty, so a later round journals nothing
        assertSame(journalled, safeCommand.update().before);
        assertSame(journalled, safeCommand.update().after);
    }

    private static CacheEntry loadedEntry()
    {
        CacheEntry entry = new CacheEntry("K");
        entry.readyToLoad();
        entry.testLoad();
        entry.testLoaded("V");
        Assert.assertEquals(Status.LOADED, entry.status());
        return entry;
    }

    /**
     * An INCR-shaped claim: a fifo task with a stamp. Only fifo claims are used here, so the fifo region orders the
     * whole queue by {@code fifoAt} and the promotion to the full representation cannot reorder anybody.
     */
    private static SafeTask<?> fifoClaim(String name, long fifoAt)
    {
        SafeTask<?> task = mock(SafeTask.class);
        ExecutionContext context = mock(ExecutionContext.class);
        when(context.executionKind()).thenReturn(ExecutionKind.OTHER);
        when(task.executionContext()).thenReturn(context);
        when(task.toString()).thenReturn(name);
        // validate() requires a non-prefix member to be waiting on its caches
        when(task.isNonSync()).thenReturn(true);
        when(task.is(Task.State.WAITING_TO_RUN)).thenReturn(true);
        // the bare and mini representations record no region: it is re-derived from this flag (ensureQueue/addToQueue)
        when(task.isCacheQueuedFifo()).thenReturn(true);
        task.refs = new org.agrona.collections.Object2ObjectHashMap<>();
        task.position = fifoAt;
        task.fifoAt = fifoAt;
        return task;
    }

    private static void assertUnclaimed(AccordCacheEntry<?, ?, ?> entry)
    {
        assertTrue("R8: no claims and no lock is the null representation", entry.hasNoTasks());
        assertSame(null, entry.unsafeGetQueue());
        assertEquals(Collections.emptyList(), entry.unsafeQueuedTasks());
        assertEquals(0, entry.unsafeRunnablePrefix());
        assertEquals(0, entry.unsafeFifoSize());
        assertFalse(entry.hasFifo());
        assertFalse(entry.hasFifoOrLocked());
    }

    /** every representation must agree about who holds a position, how many of them may run, and how many are fifo */
    private static void assertMembers(AccordCacheEntry<?, ?, ?> entry, List<SafeTask<?>> expected)
    {
        List<SafeTask<?>> queued = entry.unsafeQueuedTasks();
        assertEquals("R8: membership in wait order", expected, queued);
        assertEquals("Q4: the runnable prefix is the fifo head, if there is one", expected.isEmpty() ? 0 : 1,
                     entry.unsafeRunnablePrefix());
        // every member here is a fifo claim, so the fifo count is the membership count (R5: and hasFifo agrees)
        assertEquals("R5/O8: the fifo region is the whole membership", expected.size(), entry.unsafeFifoSize());
        assertEquals(!expected.isEmpty(), entry.hasFifo());
        for (int i = 0 ; i < queued.size() ; ++i)
        {
            if (i == 0) assertNotSame("the head must be runnable", RunnableStatus.NOT_RUNNABLE, entry.statusOfPresent(queued.get(i)));
            else assertSame("only the head may run", RunnableStatus.NOT_RUNNABLE, entry.statusOfPresent(queued.get(i)));
            assertTrue("R8: a member must be contained", entry.contains(queued.get(i)));
        }
    }

    /**
     * A HOLD_QUEUE holder keeps its fifo position across runs, so it is a member of every representation as well as the
     * lock holder, and it must be the head (R5).
     */
    @Test
    public void unionLadderWithAHoldQueueHolder()
    {
        CacheEntry entry = loadedEntry();
        SafeTask<?> t0 = fifoClaim("t0", 1), t1 = fifoClaim("t1", 2), t2 = fifoClaim("t2", 3);

        assertUnclaimed(entry);
        assertFalse(entry.contains(t0));

        // one claim: the bare representation
        assertSame(RunnableStatus.NEWLY_RUNNABLE, entry.addFifo(t0));
        assertSame(t0, entry.unsafeGetQueue());
        assertMembers(entry, Arrays.asList(t0));

        // the holder keeps its position, so nothing about the membership changes
        entry.lockExclusive(t0, LockMode.HOLD_QUEUE);
        assertTrue(entry.isLocked());
        assertTrue(entry.isLockedHoldingQueue());
        assertSame(t0, entry.lockedBy());
        assertSame(t0, entry.unsafeGetQueue());
        assertMembers(entry, Arrays.asList(t0));

        // a second claim onto a locked bare entry is what creates the mini queue
        assertSame(RunnableStatus.NOT_RUNNABLE, entry.addFifo(t1));
        assertTrue("R8: the two-claim case is the mini queue", entry.unsafeGetQueue() instanceof AccordCacheEntryMiniQueue);
        assertTrue("R8: a mini queue exists only while locked", entry.isLocked());
        assertMembers(entry, Arrays.asList(t0, t1));

        // a third claim promotes to the full representation, preserving the abstract queue
        assertSame(RunnableStatus.NOT_RUNNABLE, entry.addFifo(t2));
        assertTrue(entry.unsafeGetQueue() instanceof AccordCacheEntryQueue);
        assertSame("R5: the HOLD_QUEUE holder is still recorded", t0, entry.lockedBy());
        assertMembers(entry, Arrays.asList(t0, t1, t2));

        // and demotes back down. Note it demotes full -> bare, not full -> mini: only an arrival onto a locked bare
        // entry builds a mini queue, so the ladder is not symmetric.
        entry.remove(t2, false, AccordCacheEntryQueue.RemoveMode.IF_PRESENT);
        assertTrue(entry.unsafeGetQueue() instanceof AccordCacheEntryQueue);
        assertMembers(entry, Arrays.asList(t0, t1));

        entry.remove(t1, false, AccordCacheEntryQueue.RemoveMode.IF_PRESENT);
        assertSame("R8: one claim collapses back to the bare representation", t0, entry.unsafeGetQueue());
        assertTrue(entry.isLocked());
        assertMembers(entry, Arrays.asList(t0));

        entry.remove(t0, true, null);
        assertFalse(entry.isLocked());
        assertUnclaimed(entry);
    }

    /**
     * The commoner mode: RELEASE_QUEUE gives the position up when it locks and returns the lock inside the run, so the
     * holder is <em>not</em> a member of any representation, and the other claim leads the mini queue.
     */
    @Test
    public void unionLadderWithAReleaseQueueHolder()
    {
        CacheEntry entry = loadedEntry();
        SafeTask<?> t0 = fifoClaim("t0", 1), t1 = fifoClaim("t1", 2), t2 = fifoClaim("t2", 3);

        assertSame(RunnableStatus.NEWLY_RUNNABLE, entry.addFifo(t0));
        assertMembers(entry, Arrays.asList(t0));

        // locking gives the position up: the bare representation now records a holder that occupies nothing
        entry.lockExclusive(t0, LockMode.RELEASE_QUEUE);
        assertTrue(entry.isLocked());
        assertFalse(entry.isLockedHoldingQueue());
        assertSame(t0, entry.unsafeGetQueue());
        assertMembers(entry, Collections.emptyList());
        assertFalse("the entry is still claimed: the holder must be released before it can be evicted", entry.hasNoTasks());
        assertTrue(entry.hasFifoOrLocked());
        // NB entry.contains(t0) is true here (the bare branch tests queue == task) while the mini and full
        // representations both report false for a holder that is not HOLD_QUEUE. Not asserted either way: the
        // divergence is in AccordCacheEntry.contains, i.e. component B, and is reported rather than pinned here.

        // the arrival becomes the mini queue's head, because the holder gave up its position
        assertSame(RunnableStatus.NEWLY_RUNNABLE, entry.addFifo(t1));
        assertTrue(entry.unsafeGetQueue() instanceof AccordCacheEntryMiniQueue);
        assertMembers(entry, Arrays.asList(t1));
        assertFalse("a RELEASE_QUEUE holder holds no position", entry.contains(t0));

        assertSame(RunnableStatus.NOT_RUNNABLE, entry.addFifo(t2));
        assertTrue(entry.unsafeGetQueue() instanceof AccordCacheEntryQueue);
        assertSame(t0, entry.lockedBy());
        assertMembers(entry, Arrays.asList(t1, t2));
        assertFalse(entry.contains(t0));

        // releasing the lock removes no position, since none was held
        entry.remove(t0, true, null);
        assertFalse(entry.isLocked());
        assertMembers(entry, Arrays.asList(t1, t2));

        entry.remove(t2, false, AccordCacheEntryQueue.RemoveMode.IF_PRESENT);
        assertSame("R8: one claim and no lock collapses to the bare representation", t1, entry.unsafeGetQueue());
        assertMembers(entry, Arrays.asList(t1));

        entry.remove(t1, false, AccordCacheEntryQueue.RemoveMode.IF_PRESENT);
        assertUnclaimed(entry);
    }

    /**
     * {@code waitingCount()} reads the same union while the entry is still loading, where the drain rather than the
     * prefix decides who runs. The mini queue cannot occur here - it exists only while locked, and locking reads the
     * value, which requires the entry to be loaded - so only the null, bare and full forms are reachable.
     */
    @Test
    public void waitingCountAcrossTheUnionWhileLoading()
    {
        CacheEntry entry = new CacheEntry("K");
        entry.readyToLoad();
        entry.testLoad();
        Assert.assertEquals(Status.LOADING, entry.status());
        assertEquals(0, entry.waitingCount());

        SafeTask<?> w0 = fifoClaim("w0", 1), w1 = fifoClaim("w1", 2);
        entry.addWaitingToLoad(w0);
        assertSame(w0, entry.unsafeGetQueue());
        assertEquals(1, entry.waitingCount());

        entry.addWaitingToLoad(w1);
        assertTrue(entry.unsafeGetQueue() instanceof AccordCacheEntryQueue);
        assertEquals(2, entry.waitingCount());

        // A1: nothing on a loading entry may run, whatever the representation says about positions
        assertTrue(entry.isLoading());
        assertFalse(entry.isLoaded());
    }

    /**
     * What a failed round retains, at the level of the entry: a RELEASE_QUEUE lock is turned back into a fifo claim
     * plus a HOLD_QUEUE lock, in all three representations. The point of it is the pair of notifications - the head we
     * promoted when we locked is demoted again, and re-promoted if and when the claim is released - because the task
     * behind us must <em>block</em>, and a task that has been told it may run will be scheduled and then fail
     * {@code require(!isLocked())} in {@link AccordCacheEntry#lockExclusive}.
     */
    @Test
    public void retainedClaimBlocksAcrossTheUnion()
    {
        // bare: nothing behind us, so only the lock mode changes
        CacheEntry bare = loadedEntry();
        SafeTask<?> b0 = fifoClaim("b0", 1);
        bare.addFifo(b0);
        bare.lockExclusive(b0, LockMode.RELEASE_QUEUE);
        assertMembers(bare, Collections.emptyList());
        bare.setInconsistent();
        bare.reclaimFifoHead(b0);
        assertTrue("the retained claim is a HOLD_QUEUE lock", bare.isLockedHoldingQueue());
        assertMembers(bare, Arrays.asList(b0));
        bare.remove(b0, true, null);
        assertUnclaimed(bare);

        // mini: one claim behind us, which was told it could run when we locked
        CacheEntry mini = loadedEntry();
        SafeTask<?> m0 = fifoClaim("m0", 1), m1 = fifoClaim("m1", 2);
        mini.addFifo(m0);
        mini.lockExclusive(m0, LockMode.RELEASE_QUEUE);
        assertSame(RunnableStatus.NEWLY_RUNNABLE, mini.addFifo(m1));
        assertMembers(mini, Arrays.asList(m1));
        mini.setInconsistent();
        mini.reclaimFifoHead(m0);
        verify(m1).onChangeRunnableStatus(mini, RunnableStatus.NOT_RUNNABLE);
        assertTrue(mini.isLockedHoldingQueue());
        assertMembers(mini, Arrays.asList(m0, m1));
        mini.remove(m0, true, null);
        verify(m1).onChangeRunnableStatus(mini, RunnableStatus.NEWLY_RUNNABLE);
        assertMembers(mini, Arrays.asList(m1));

        // full: two claims behind us, so we go back into the fifo region proper, at its head - which we must be, since
        // our stamp was issued before theirs and we could not have locked otherwise
        CacheEntry full = loadedEntry();
        SafeTask<?> f0 = fifoClaim("f0", 1), f1 = fifoClaim("f1", 2), f2 = fifoClaim("f2", 3);
        full.addFifo(f0);
        full.lockExclusive(f0, LockMode.RELEASE_QUEUE);
        assertSame(RunnableStatus.NEWLY_RUNNABLE, full.addFifo(f1));
        assertSame(RunnableStatus.NOT_RUNNABLE, full.addFifo(f2));
        assertMembers(full, Arrays.asList(f1, f2));
        full.setInconsistent();
        full.reclaimFifoHead(f0);
        verify(f1).onChangeRunnableStatus(full, RunnableStatus.NOT_RUNNABLE);
        assertTrue(full.isLockedHoldingQueue());
        assertMembers(full, Arrays.asList(f0, f1, f2));
        // REQUIRE_PRESENT explicitly: production passes null and derives it, but the derivation reads
        // AccordExecutor.CACHE_QUEUES_ENABLED, and this test deliberately initialises no DatabaseDescriptor
        full.remove(f0, true, AccordCacheEntryQueue.RemoveMode.REQUIRE_PRESENT);
        verify(f1).onChangeRunnableStatus(full, RunnableStatus.NEWLY_BLOCKING_RUNNABLE);
        assertMembers(full, Arrays.asList(f1, f2));
    }

    /**
     * The claim a non-fifo task cannot retain, and the entry refuses to fake one. A task with no fifo stamp has nothing
     * to hold - a fresh stamp would sort behind everything that queued after it, and so would block nothing - so
     * {@code reclaimFifoHead} requires the stamp rather than degrading to a claim that blocks nobody.
     *
     * <p>Production never asks: {@code NonSyncState.postRunExclusive} retains only for {@code isAtomic()}, which is
     * exactly the set of non-sync tasks that hold a fifo claim (ATOMIC, or INCR with a txnId, which
     * {@code prepareExclusiveMayThrow} upgrades on its first run). A failed round of any other fan-out releases
     * everything instead, which is
     * {@code AccordFailedKeyTest#bodyFailureThatNeedNotBeWitnessedReleasesEverything}.
     */
    @Test
    public void aNonFifoClaimCannotBeRetained()
    {
        CacheEntry entry = loadedEntry();
        SafeTask<?> t0 = priorityClaim("t0", 1), t1 = priorityClaim("t1", 2);
        entry.addPrioritised(t0);
        entry.lockExclusive(t0, LockMode.RELEASE_QUEUE);
        assertSame(RunnableStatus.NEWLY_RUNNABLE, entry.addPrioritised(t1));
        entry.setInconsistent();
        try
        {
            entry.reclaimFifoHead(t0);
            fail("a task with no fifo stamp must not be given a retained claim: it would block nobody");
        }
        catch (IllegalStateException e)
        {
            // expected: the release path is the only one open to it
        }
        assertTrue("and the lock it took for the round is untouched, for its release to give up", entry.isLocked());
        assertNotSame(RunnableStatus.NOT_RUNNABLE, entry.statusOfPresent(t1));
    }

    /** as {@link #fifoClaim}, but prioritised: no fifo stamp, so it cannot retain a claim */
    private static SafeTask<?> priorityClaim(String name, long position)
    {
        SafeTask<?> task = mock(SafeTask.class);
        ExecutionContext context = mock(ExecutionContext.class);
        when(context.executionKind()).thenReturn(ExecutionKind.OTHER);
        when(task.executionContext()).thenReturn(context);
        when(task.toString()).thenReturn(name);
        when(task.isNonSync()).thenReturn(true);
        when(task.is(Task.State.WAITING_TO_RUN)).thenReturn(true);
        when(task.isCacheQueuedFifo()).thenReturn(false);
        task.refs = new org.agrona.collections.Object2ObjectHashMap<>();
        task.position = position;
        return task;
    }
}
