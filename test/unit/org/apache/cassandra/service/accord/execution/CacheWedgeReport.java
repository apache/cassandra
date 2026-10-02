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
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import accord.local.CommandStore;
import accord.local.Node;

import org.apache.cassandra.service.accord.AccordCommandStore;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * This class was authored by Claude (Anthropic).
 *
 * Runs {@link QueueCycleDetector} over the live cache entry queues of every command store of a running node, for the
 * stall report of a long-running dtest. The detector was written for the simulator, where it is handed the set of
 * stalled tasks; here we cannot know which tasks are stalled, so we take every task that occupies a position in, or
 * holds the lock of, any loaded entry of the store's command and commands-for-key caches, and let the detector find
 * cycles and mis-counted waiters among them. Independently of any cycle, we list the oldest such tasks: a task that
 * has been queued on a cache entry for a minute is a wedge whether or not it closes a cycle (for example a lock holder
 * that is no longer running, or a waiter on a load that never completes).
 *
 * <p>Unlike the simulator, the cluster here is live, so each store's caches are read under its executor's lock. The
 * lock is only tried, for a bounded time: an executor whose lock cannot be had for that long is itself the finding,
 * and is reported rather than joined. The same goes for the per-store {@link ExclusiveExecutor}'s hand-rolled
 * owner/waiting handoff, which no JVM deadlock detector can see.
 *
 * <p>Must be invoked on the instance (i.e. inside {@code callOnInstance}) so it resolves the instance's classes; it
 * lives in this package because the queue accessors it relies on are package-private.
 */
public final class CacheWedgeReport
{
    private static final long LOCK_WAIT_NANOS = TimeUnit.SECONDS.toNanos(2);

    private CacheWedgeReport() {}

    /**
     * @param minAgeNanos only report tasks (outside a cycle) that were created at least this long ago
     * @param maxTasksPerStore cap on the aged tasks listed per store
     * @return one line per store with anything worth reporting, or a single line saying nothing was found
     */
    public static String describe(Node node, long minAgeNanos, int maxTasksPerStore)
    {
        StringBuilder out = new StringBuilder();
        int storesChecked = 0, tasksChecked = 0;
        for (CommandStore commandStore : node.commandStores().all())
        {
            if (!(commandStore instanceof AccordCommandStore))
                continue;

            ++storesChecked;
            AccordCommandStore store = (AccordCommandStore) commandStore;
            try
            {
                tasksChecked += describe(store, minAgeNanos, maxTasksPerStore, out);
            }
            catch (Throwable t)
            {
                out.append("\n    store").append(store.id()).append(": inspection threw ").append(t);
            }
        }
        if (out.length() == 0)
            return " no cycles, no mis-counted waiters, no task older than " + TimeUnit.NANOSECONDS.toSeconds(minAgeNanos)
                   + "s on any cache entry (" + storesChecked + " stores, " + tasksChecked + " queued/locking tasks)";
        return " (" + storesChecked + " stores, " + tasksChecked + " queued/locking tasks)" + out;
    }

    private static int describe(AccordCommandStore store, long minAgeNanos, int maxTasks, StringBuilder out)
    {
        String prefix = "\n    store" + store.id() + ": ";

        // the per-store exclusive executor hands ownership off with park/unpark; a waiter whose owner is not running
        // our store's work is invisible to ThreadMXBean.findDeadlockedThreads, so say who holds it
        ExclusiveExecutor exclusive = store.exclusiveExecutor();
        Thread owner = exclusive.owner, waiting = exclusive.waiting;
        if (waiting != null)
            out.append(prefix).append("ExclusiveExecutor waiter ").append(waiting.getName()).append(" (").append(waiting.getState())
               .append(") is waiting for owner ").append(owner == null ? "<released>" : owner.getName() + " (" + owner.getState() + ')');

        AccordCommandStore.ExclusiveCaches caches = null;
        long deadline = nanoTime() + LOCK_WAIT_NANOS;
        while ((caches = store.tryLockCaches()) == null && nanoTime() < deadline)
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));

        if (caches == null)
        {
            out.append(prefix).append("could not obtain executor").append(store.executor().executorId())
               .append("'s lock within ").append(TimeUnit.NANOSECONDS.toMillis(LOCK_WAIT_NANOS)).append("ms; running=")
               .append(store.executor().unsafeRunningCount());
            return 0;
        }

        Set<SafeTask<?>> tasks = Collections.newSetFromMap(new IdentityHashMap<>());
        int lockedEntries = 0, queuedEntries = 0;
        String explanation;
        List<String> aged = new ArrayList<>();
        try
        {
            for (AccordCacheEntry<?, ?, ?> entry : caches.commands())
            {
                int n = collect(entry, tasks);
                if (entry.isLocked()) ++lockedEntries;
                if (n > 0) ++queuedEntries;
            }
            for (AccordCacheEntry<?, ?, ?> entry : caches.commandsForKeys())
            {
                int n = collect(entry, tasks);
                if (entry.isLocked()) ++lockedEntries;
                if (n > 0) ++queuedEntries;
            }

            explanation = QueueCycleDetector.explainStall(tasks);
            String queues = describeExclusiveQueues(exclusive, minAgeNanos / 6);
            if (queues != null)
                aged.add(queues);

            long now = nanoTime();
            List<SafeTask<?>> sorted = new ArrayList<>();
            for (SafeTask<?> task : tasks)
                if (now - task.createdAt >= minAgeNanos)
                    sorted.add(task);
            sorted.sort(Comparator.comparingLong(t -> t.createdAt));
            // the chains explain *why* the oldest tasks wait, which a cycle search cannot when there is no cycle
            String chains = QueueCycleDetector.explainBlockers(tasks, sorted.subList(0, Math.min(2 * maxTasks, sorted.size())), 12, now);
            if (chains != null)
                aged.add(chains.trim().replace("\n", "\n      "));
            for (SafeTask<?> task : sorted.subList(0, Math.min(maxTasks, sorted.size())))
                aged.add(String.format("%s age=%ds state=%s %s", describe(task), TimeUnit.NANOSECONDS.toSeconds(now - task.createdAt),
                                       task.currentState(), QueueCycleDetector.describeReadiness(task)));
            if (sorted.size() > maxTasks)
                aged.add("... and " + (sorted.size() - maxTasks) + " more older than " + TimeUnit.NANOSECONDS.toSeconds(minAgeNanos) + 's');
        }
        finally
        {
            caches.close();
        }

        if (explanation == null && aged.isEmpty())
            return tasks.size();

        out.append(prefix).append(tasks.size()).append(" tasks on ").append(queuedEntries).append(" entries, ")
           .append(lockedEntries).append(" entries locked");
        if (explanation != null)
            out.append("\n      ").append(explanation.replace("\n", "\n      "));
        for (String line : aged)
            out.append(line.startsWith("queues:") || line.startsWith("wait chains") ? "\n      " : "\n      aged: ").append(line);
        return tasks.size();
    }

    /**
     * The per-group state the ExclusiveExecutor's QoS chooses between (see TaskQueueMulti.pollGroupByBlended): for each
     * non-empty group its size, head position and head age, and the decayed arrival/dispatch counters the flow arm
     * uses. Reported only if some group's head has waited at least {@code minHeadAgeNanos}: a group whose head is old
     * while another group is being dispatched is starving. Must hold the executor lock.
     */
    private static String describeExclusiveQueues(ExclusiveExecutor exclusive, long minHeadAgeNanos)
    {
        long now = nanoTime();
        long oldest = 0;
        StringBuilder sb = new StringBuilder();
        Task current = exclusive.task;
        sb.append("queues: next=").append(current == null ? "none" : describeTask(current) + " pos=" + current.position
                                                                        + " age=" + TimeUnit.NANOSECONDS.toMillis(now - current.createdAt) + "ms");
        sb.append(" waiting=").append(exclusive.waitingCount()).append(" nextPosition=").append(exclusive.selfTask.executor().nextPosition);
        for (int g = 0 ; g < exclusive.queues.length ; ++g)
        {
            TaskQueue<Task> queue = exclusive.queues[g];
            int arrivals = (int) ((exclusive.arrivals >>> (8 * g)) & 0x7f), dispatches = (int) ((exclusive.dispatches >>> (8 * g)) & 0x7f);
            if (queue == null || queue.isEmptySingle())
            {
                if (arrivals > 0 || dispatches > 0)
                    sb.append("\n        ").append(Task.ExclusiveGroup.values()[g]).append(": empty arr=").append(arrivals).append(" disp=").append(dispatches);
                continue;
            }
            Task head = queue.peekSingle();
            long headAge = now - head.createdAt;
            oldest = Math.max(oldest, headAge);
            boolean cachedPositionStale = exclusive.positions.length > g && (exclusive.dirty & TaskQueueMulti.overflowBit(g)) == 0
                                          && exclusive.positions[g] != head.position;
            sb.append("\n        ").append(Task.ExclusiveGroup.values()[g]).append(": size=").append(queue.size())
              .append(" headPos=").append(head.position).append(" headAge=").append(TimeUnit.NANOSECONDS.toMillis(headAge)).append("ms")
              .append(" arr=").append(arrivals).append(" disp=").append(dispatches)
              .append(" stopped=").append((exclusive.stopped & TaskQueueMulti.overflowBit(g)) != 0)
              .append(cachedPositionStale ? " STALE cachedPos=" + exclusive.positions[g] : "")
              .append(" head=").append(describeTask(head));
        }
        return oldest >= minHeadAgeNanos ? sb.toString() : null;
    }

    private static String describeTask(Task task)
    {
        if (task instanceof SafeTask<?>) return describe((SafeTask<?>) task);
        try { return task.briefDescription(); }
        catch (Throwable t) { return task.getClass().getSimpleName(); }
    }

    private static int collect(AccordCacheEntry<?, ?, ?> entry, Set<SafeTask<?>> into)
    {
        // the number of tasks on this entry, whether or not we have already seen them on another
        int count = 0;
        for (SafeTask<?> task : entry.unsafeQueuedTasks())
        {
            if (task == null) continue;
            into.add(task);
            ++count;
        }
        SafeTask<?> lockedBy = entry.lockedBy();
        if (lockedBy != null)
        {
            into.add(lockedBy);
            ++count;
        }
        return count;
    }

    private static String describe(SafeTask<?> task)
    {
        try { return task.description(); }
        catch (Throwable t) { return task.getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(task)); }
    }
}
