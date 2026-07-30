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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import accord.local.SafeState;

/**
 * This test has been authored entirely by Claude.
 *
 * Explains an execution stall by inspecting the cache entry queues the stalled tasks are waiting in.
 *
 * <p>The relation built here is {@code W = Wpos u Wlock} of {@code spec/accord-execution/INVARIANTS.md} 2, and nothing
 * wider: a waiter must itself hold a position ({@code pos(a,e) != BOTTOM}), the entry must not be loading (A1) and only a
 * {@code HOLD_QUEUE} holder yields a lock edge (A2). Anything looser invents edges and reports a cycle that does not
 * exist, which for the only stall explanation the suite has means a wrong diagnosis of a real hang.
 *
 * <p>Ahead is defined by {@link AccordCacheEntryQueue}'s regions: the fifo region leads, then the sorted region in
 * {@code compare} order, then the bag - whose members are mutually unordered, so they do not wait for each other. That
 * is exactly {@link AccordCacheEntry#unsafeQueuedTasks()} (wait order) plus
 * {@link AccordCacheEntry#unsafeRunnablePrefix()}, which is why they are read here rather than the queue array: the
 * {@code queue} field is the {@code null | SafeTask | AccordCacheEntryMiniQueue | AccordCacheEntryQueue} union of R8, and
 * the mini queue - the two-claim case, i.e. exactly the shape a stall has - is not an {@code AccordCacheEntryQueue}.
 * Because a position in the sorted region is a function of the pair being compared, two tasks sharing two entries agree
 * on their order on both, so a cycle in this relation means some region is ordering by something other than
 * {@code compare} - arrival, for instance.
 *
 * <p>Two failure modes produce the same symptom, so we report both: a genuine cycle, and a task whose
 * {@code waitingForState} does not match the number of entries it is actually blocked on, which can never reach zero.
 */
public class QueueCycleDetector
{
    /** an edge: {@code waiter} cannot run until {@code blocker} does, because of {@code entry} */
    private static class Edge
    {
        final SafeTask<?> blocker;
        final AccordCacheEntry<?, ?, ?> entry;
        final String why;

        Edge(SafeTask<?> blocker, AccordCacheEntry<?, ?, ?> entry, String why)
        {
            this.blocker = blocker;
            this.entry = entry;
            this.why = why;
        }
    }

    private final Map<SafeTask<?>, List<Edge>> waitsFor = new IdentityHashMap<>();
    /** entries a task holds that have not loaded: it waits for these without anybody being ahead of it in the queue */
    private final Map<SafeTask<?>, List<AccordCacheEntry<?, ?, ?>>> waitsToLoad = new IdentityHashMap<>();

    private QueueCycleDetector(Collection<SafeTask<?>> tasks)
    {
        for (SafeTask<?> task : tasks)
        {
            if (task == null)
                continue;
            waitsFor.put(task, new ArrayList<>());
            waitsToLoad.put(task, new ArrayList<>());
        }
        for (SafeTask<?> task : waitsFor.keySet())
            addEdges(task);
    }

    /**
     * @return a description of the smallest cycle among these tasks, or of any mis-counted waiter, or null if the
     * queues are acyclic and consistently counted - in which case the stall is elsewhere.
     */
    public static String explainStall(Collection<SafeTask<?>> tasks)
    {
        QueueCycleDetector detector = new QueueCycleDetector(tasks);
        StringBuilder out = new StringBuilder();
        String cycle = detector.smallestCycle();
        String contradictions = detector.contradictions();
        if (contradictions != null)
            out.append(contradictions);
        if (cycle != null)
            out.append(out.length() > 0 ? "\n" : "").append(cycle);
        String miscounted = detector.miscounted();
        if (miscounted != null)
            out.append(out.length() > 0 ? "\n" : "").append(miscounted);
        return out.length() == 0 ? null : out.toString();
    }

    /**
     * Only a {@code HOLD_QUEUE} holder yields a lock edge (A2), so this describes a holder we have already decided to
     * add an edge for. The <em>mode</em> is still worth printing, because the three modes answer different questions and
     * a dump that says only "holds the lock" is ambiguous:
     *
     * <ul>
     *   <li>{@code HOLD_QUEUE} - the holder keeps a fifo position across its runs, so it is <em>also</em> a queue member
     *       and legitimately blocks everyone behind it between rounds. Only an INCR task does this.</li>
     *   <li>{@code RELEASE_QUEUE} - the holder gave its position up when it locked, so it holds the lock only for the
     *       duration of one run. Seeing this on a task that is <em>not</em> running is a leak, not a wait.</li>
     *   <li>{@code UNQUEUED} - optimistic referencing, which skips queue accounting entirely.</li>
     * </ul>
     *
     * It also reports {@link AccordCacheEntry#isLocked()} disagreeing with {@link AccordCacheEntry#lockedBy()}: the two
     * are maintained separately, and a stale slot would make this detector invent an edge to a task that holds nothing.
     */
    private static String lockDescription(AccordCacheEntry<?, ?, ?> entry, SafeTask<?> lockedBy)
    {
        StringBuilder sb = new StringBuilder("holds the lock");
        if (!entry.isLocked())
            sb.append(" BUT entry.isLocked() is false - stale LOCKED_INDEX");
        else sb.append(entry.isLockedHoldingQueue() ? ", HOLD_QUEUE (keeps its position across runs)"
                                                   : ", not HOLD_QUEUE (gave its position up, so should be mid-run)");
        if (!entry.isLockedBy(lockedBy))
            sb.append(" BUT entry.isLockedBy(holder) is false");
        sb.append(", holder is ").append(lockedBy.currentState());
        return sb.toString();
    }

    /** contradictions that make a dump self-inconsistent, reported alongside the cycle */
    private String contradictions()
    {
        StringBuilder sb = new StringBuilder();
        for (SafeTask<?> task : waitsFor.keySet())
        {
            // the upgrade at a task's first prepare applies to a task that will hold a txnId locked across its runs, and
            // the sequence bits are set-only, so such a task that has started and is not fifo cannot have got there
            // through prepareExclusiveMayThrow. A task that holds no lock is deliberately left where it is.
            if (task.hasIncrementalStarted() && !task.isCacheQueuedFifo() && task.holdsLocksBetweenRuns())
                sb.append("  CONTRADICTION: ").append(describe(task)).append(" has started, holds a lock between runs, but is not fifo\n");

            task.refs.forEach((key, safeState) -> {
                AccordCacheEntry<?, ?, ?> entry = entryOf(safeState);
                if (entry == null)
                    return;
                // a task parked waiting for a txnId it owns will never be woken: the notification that would release it
                // comes from the entry it is holding
                if (entry.isLockedBy(task) && task.is(Task.State.WAITING_ON_TXN) && !entry.isCommandsForKey())
                    sb.append("  CONTRADICTION: ").append(describe(task)).append(" is WAITING_ON_TXN while owning the lock on ").append(key).append('\n');
                // likewise for keys
                if (entry.isLockedBy(task) && task.is(Task.State.WAITING_ON_KEY) && entry.isCommandsForKey() && !entry.isLockedHoldingQueue())
                    sb.append("  CONTRADICTION: ").append(describe(task)).append(" is WAITING_ON_KEY while holding a non-HOLD_QUEUE lock on ").append(key).append('\n');
            });
        }
        return sb.length() == 0 ? null : "self-inconsistent state (explain these before the cycle):\n" + sb;
    }

    private void addEdges(SafeTask<?> task)
    {
        task.refs.forEach((key, safeState) -> {
            AccordCacheEntry<?, ?, ?> entry = entryOf(safeState);
            if (entry == null)
                return;

            // A1: a not-yet-loaded entry carries no edge - nothing is runnable on it and the load completes
            // independently of any task, so a fifo claim queued on a loading entry (which AccordCacheEntry.add permits)
            // must not produce "ahead of" edges for an order the drain is going to re-establish. It blocks us with
            // nobody ahead of us, so it is counted separately or a task waiting on a load looks mis-counted.
            // (Wider than LOADING, deliberately: for FAILED_TO_LOAD the waiters are failed rather than blocked, and a
            // diagnostic must never invent an edge.)
            if (!entry.isLoaded())
            {
                waitsToLoad.get(task).add(entry);
                return;
            }

            // pos(task,entry) != BOTTOM. We iterate refs, and a ref is not a position: acquireIfLoadedAndPermitted
            // references UNQUEUED and a RELEASE_QUEUE holder gave its position up when it locked, both keeping the ref.
            // unsafeQueuedTasks() is the membership those two cases are excluded from, in wait order.
            List<SafeTask<?>> queued = entry.unsafeQueuedTasks();
            int index = indexOf(queued, task);
            if (index < 0)
                return;

            int runnable = entry.unsafeRunnablePrefix();

            // Wlock, with A2's mode(e) = HOLD_QUEUE: a RELEASE_QUEUE or UNQUEUED holder returns the lock inside the run
            // that took it, so waiting for it is not waiting for a task.
            if (entry.isLockedHoldingQueue())
            {
                SafeTask<?> lockedBy = entry.lockedBy();
                if (lockedBy != null && lockedBy != task)
                    waitsFor.get(task).add(new Edge(lockedBy, entry, lockDescription(entry, lockedBy)));
            }

            // Wpos: we wait for the runnable prefix, and only if we are not part of it. Bag members are all in the
            // prefix together (Q4) and so contribute no edges to each other.
            if (index >= runnable)
            {
                String why = String.format("position %d of %d, runnable prefix %d, fifo %d",
                                           index, queued.size(), runnable, entry.unsafeFifoSize());
                for (int i = 0 ; i < runnable ; ++i)
                {
                    SafeTask<?> ahead = queued.get(i);
                    if (ahead != task)
                        waitsFor.get(task).add(new Edge(ahead, entry, why));
                }
            }
        });
    }

    /** breadth first from every node, so the first cycle we close is the smallest through that node */
    private String smallestCycle()
    {
        List<SafeTask<?>> best = null;
        Map<SafeTask<?>, Edge> bestVia = null;
        for (SafeTask<?> from : waitsFor.keySet())
        {
            Map<SafeTask<?>, Edge> via = new IdentityHashMap<>();
            Map<SafeTask<?>, SafeTask<?>> parent = new IdentityHashMap<>();
            Deque<SafeTask<?>> queue = new ArrayDeque<>();
            queue.add(from);
            while (!queue.isEmpty())
            {
                SafeTask<?> at = queue.poll();
                for (Edge edge : edgesOf(at))
                {
                    if (edge.blocker == from)
                    {
                        List<SafeTask<?>> cycle = new ArrayList<>();
                        for (SafeTask<?> t = at ; t != null ; t = parent.get(t))
                            cycle.add(0, t);
                        via.put(from, edge);
                        if (best == null || cycle.size() < best.size())
                        {
                            best = cycle;
                            bestVia = new IdentityHashMap<>();
                            bestVia.putAll(via); // the simulator intercepts IdentityHashMap and has no copy constructor
                            bestVia.put(at, edge);
                        }
                        queue.clear();
                        break;
                    }
                    if (parent.containsKey(edge.blocker) || edge.blocker == from || !waitsFor.containsKey(edge.blocker))
                        continue;
                    parent.put(edge.blocker, at);
                    via.put(edge.blocker, edge);
                    queue.add(edge.blocker);
                }
            }
            if (best != null && best.size() == 2)
                break; // cannot do better than a pair
        }
        if (best == null)
            return null;

        StringBuilder sb = new StringBuilder("found a wait cycle of ").append(best.size()).append(" tasks:\n");
        for (int i = 0 ; i < best.size() ; ++i)
        {
            SafeTask<?> task = best.get(i);
            SafeTask<?> next = best.get((i + 1) % best.size());
            Edge edge = null;
            for (Edge candidate : edgesOf(task))
            {
                if (candidate.blocker == next) { edge = candidate; break; }
            }
            sb.append(String.format("  %s [%s, waitingFor=%d/%d, %s, incr=%s, started=%s, fifo=%s, fifoAt=%s]%n      waits for %s%n      because of %s (%s)%n",
                                    describe(task), task.currentState(), task.waitingForKeyCount(), task.waitingForTxnCount(),
                                    task.isUnsequenced() ? "UNSEQUENCED" : task.isSequencedByPriorityAtomic() ? "ATOMIC" : "BY_PRIORITY",
                                    task.isIncremental(), task.hasIncrementalStarted(), task.isCacheQueuedFifo(), task.fifoAt, describe(next),
                                    edge == null ? "?" : String.valueOf(edge.entry.key()),
                                    edge == null ? "?" : edge.why));
        }
        return sb.toString();
    }

    /** a task blocked on n entries but counting m of them will never be woken, with no cycle to show for it */
    private String miscounted()
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<SafeTask<?>, List<Edge>> e : waitsFor.entrySet())
        {
            SafeTask<?> task = e.getKey();
            int blockedOn = (int) e.getValue().stream().map(edge -> edge.entry).distinct().count();
            List<AccordCacheEntry<?, ?, ?>> loading = waitsToLoad.get(task);
            // waitingFor packs keys and txnIds separately; a blocking edge or a pending load is counted in whichever
            // half its entry belongs to, so compare the total the two halves make up
            int waits = task.waitingForKeyCount() + task.waitingForTxnCount();
            if (waits > blockedOn + loading.size())
            {
                sb.append(String.format("  %s [%s] counts %d waits but is blocked on %d entries and %d loads (of %d refs)%n",
                                        describe(task), task.currentState(), waits, blockedOn,
                                        loading.size(), task.refs.size()));
            }
            else if (waits > 0 && blockedOn == 0 && !loading.isEmpty())
            {
                // not a mis-count: we are waiting for loads that have not completed, so report them with their status
                sb.append(String.format("  %s [%s] counts %d waits and is waiting only on loads:%n",
                                        describe(task), task.currentState(), waits));
                for (AccordCacheEntry<?, ?, ?> entry : loading)
                    sb.append(String.format("      %s is %s%n", entry.key(), entry.status()));
            }
        }
        return sb.length() == 0 ? null : "waiters that cannot reach zero, or are waiting on loads:\n" + sb;
    }

    private List<Edge> edgesOf(SafeTask<?> task)
    {
        List<Edge> edges = waitsFor.get(task);
        return edges == null ? new ArrayList<>() : edges;
    }

    private static int indexOf(List<SafeTask<?>> list, SafeTask<?> task)
    {
        for (int i = 0 ; i < list.size() ; ++i)
        {
            if (list.get(i) == task)
                return i;
        }
        return -1;
    }

    private static AccordCacheEntry<?, ?, ?> entryOf(SafeState<?> safeState)
    {
        return safeState == null ? null : SaferState.global(safeState);
    }

    /**
     * Why a non-sync task believes it is not ready to run. isWaitReady compares readyCount() against
     * min(keys - processed, MIN_BATCH), so a task with no commands-for-key references should always be ready: a non-zero
     * keys count with no such reference is accounting without anything behind it, and can never be satisfied.
     */
    public static String describeReadiness(SafeTask<?> task)
    {
        StringBuilder sb = new StringBuilder();
        int cfkRefs = 0;
        for (SafeState<?> safeState : task.refs.values())
        {
            AccordCacheEntry<?, ?, ?> entry = entryOf(safeState);
            if (entry != null && entry.isCommandsForKey())
                ++cfkRefs;
        }
        sb.append(String.format("seq=%s incr=%s started=%s fifo=%s keys=%d cfkRefs=%d refs=%d waits=%d/%d",
                                task.isUnsequenced() ? "UNSEQUENCED" : task.isSequencedByPriorityAtomic() ? "ATOMIC" : "BY_PRIORITY",
                                task.isIncremental(), task.hasIncrementalStarted(), task.isCacheQueuedFifo(),
                                task.keys, cfkRefs, task.refs.size(), task.waitingForKeyCount(), task.waitingForTxnCount()));
        SafeTask.NonSyncState nonSync = task.nonSync();
        if (nonSync == null)
        {
            sb.append(" nonSync=null");
        }
        else
        {
            sb.append(String.format(" nonSync=%s loaded=%d processed=%d blocking=%d notBlocking=%d alwaysReady=%s active=%s",
                                    nonSync.getClass().getSimpleName(), nonSync.loaded, nonSync.processed,
                                    nonSync.blocking == null ? 0 : nonSync.blocking.size(),
                                    nonSync.notBlocking == null ? 0 : nonSync.notBlocking.size(),
                                    nonSync.alwaysReady, nonSync.active == null ? "null" : String.valueOf(nonSync.active.size())));
            if (task.keys > 0 && cfkRefs == 0)
                sb.append("  <-- keys counted with no commands-for-key reference: readiness is unreachable");
        }
        return sb.toString();
    }

    private static String describe(SafeTask<?> task)
    {
        try
        {
            return task.description();
        }
        catch (Throwable t)
        {
            return task.getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(task));
        }
    }
}
