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

import accord.local.ExecutionContext;
import accord.local.ExecutionContext.ExecutionKind;
import accord.local.SafeState;

import org.apache.cassandra.service.accord.execution.AccordCache.Type;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.RunnableStatus;
import org.apache.cassandra.service.accord.execution.AccordCacheEntry.Status;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
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
        assertTrue("R8: no claims and no lock is the null representation", entry.isUnclaimed());
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
            if (i == 0) assertNotSame("the head must be runnable", RunnableStatus.NOT_RUNNABLE, entry.statusIfPresent(queued.get(i)));
            else assertSame("only the head may run", RunnableStatus.NOT_RUNNABLE, entry.statusIfPresent(queued.get(i)));
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
        assertFalse("the entry is still claimed: the holder must be released before it can be evicted", entry.isUnclaimed());
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
}
