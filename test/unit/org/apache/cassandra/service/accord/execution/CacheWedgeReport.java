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
    private static final long LOCK_WAIT_NANOS = TimeUnit.SECONDS.toNanos(1);

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
        Set<AccordExecutor> unavailable = Collections.newSetFromMap(new IdentityHashMap<>());
        for (CommandStore commandStore : node.commandStores().all())
        {
            if (!(commandStore instanceof AccordCommandStore))
                continue;

            ++storesChecked;
            AccordCommandStore store = (AccordCommandStore) commandStore;
            try
            {
                if (unavailable.contains(store.executor()))
                {
                    out.append("\n    store").append(store.id()).append(": skipped, executor").append(store.executor().executorId()).append("'s lock was unavailable above");
                    continue;
                }
                int checked = describe(store, minAgeNanos, maxTasksPerStore, out);
                if (checked < 0) unavailable.add(store.executor());
                else tasksChecked += checked;
            }
            catch (Throwable t)
            {
                out.append("\n    store").append(store.id()).append(": inspection threw ").append(t);
            }
        }
        // executor-level loading state: loads are paused while the cache is over its working set and other work is
        // runnable, so anything waiting on a load (incl. this report's own lookups) waits for the backlog to drain
        StringBuilder execs = new StringBuilder();
        for (AccordExecutor executor : executors(node))
        {
            String paused = "?";
            try
            {
                java.lang.reflect.Field f = AccordExecutor.class.getDeclaredField("hasPausedLoading");
                f.setAccessible(true);
                paused = String.valueOf(f.get(executor));
                f = AccordExecutor.class.getDeclaredField("maxWorkingCapacityInBytes");
                f.setAccessible(true);
                paused += ", cache=" + (executor.weightedSize() >> 20) + "MiB of working-set " + (((Long) f.get(executor)) >> 20) + "MiB";
            }
            catch (Throwable t) { paused += " (" + t + ')'; }
            // read without the lock (it may be unobtainable): racy, so tolerate a concurrently resized heap
            long oldestLoad = 0;
            int loading = executor.loading.size();
            try
            {
                for (int i = 0 ; i < loading ; ++i)
                {
                    Task task = executor.loading.getSingle(i);
                    if (task != null) oldestLoad = Math.max(oldestLoad, nanoTime() - task.createdAt);
                }
            }
            catch (Throwable ignore) {}
            execs.append("\n    executor").append(executor.executorId()).append(": loadingPaused=").append(paused)
               .append(" loading=").append(loading).append(" oldestLoadingTask=").append(TimeUnit.NANOSECONDS.toMillis(oldestLoad)).append("ms")
               .append(" waiting=").append(executor.waiting.size());
        }
        if (out.length() == 0)
            return " no cycles, no mis-counted waiters, no task older than " + TimeUnit.NANOSECONDS.toSeconds(minAgeNanos)
                   + "s on any cache entry (" + storesChecked + " stores, " + tasksChecked + " queued/locking tasks)" + execs;
        return " (" + storesChecked + " stores, " + tasksChecked + " queued/locking tasks)" + out + execs;
    }

    /** for tests driving a bare command store */
    public static String describe(AccordCommandStore store, long minAgeNanos, int maxTasks)
    {
        StringBuilder out = new StringBuilder();
        describe(store, minAgeNanos, maxTasks, out);
        return out.toString();
    }

    private static List<AccordExecutor> executors(Node node)
    {
        List<AccordExecutor> executors = new ArrayList<>();
        for (CommandStore commandStore : node.commandStores().all())
        {
            if (!(commandStore instanceof AccordCommandStore)) continue;
            AccordExecutor executor = ((AccordCommandStore) commandStore).executor();
            boolean seen = false;
            for (AccordExecutor e : executors) seen |= e == executor;
            if (!seen) executors.add(executor);
        }
        return executors;
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
        java.util.concurrent.locks.Lock lock = store.executor().unsafeLock();
        String lockBefore = String.valueOf(lock);
        long deadline = nanoTime() + LOCK_WAIT_NANOS;
        while ((caches = store.tryLockCaches()) == null && nanoTime() < deadline)
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));

        if (caches == null)
        {
            AccordExecutor executor = store.executor();
            out.append(prefix).append("could not obtain executor").append(executor.executorId())
               .append("'s lock within ").append(TimeUnit.NANOSECONDS.toMillis(LOCK_WAIT_NANOS)).append("ms; running=")
               .append(executor.unsafeRunningCount());
            // SignalLock refuses tryLock while the lock is owned *or signalled* to a waiting thread, so a handoff that
            // is never taken (or an owner that is not running) wedges the executor invisibly to ThreadMXBean
            out.append("\n      lock before: ").append(lockBefore).append("\n      lock after:  ").append(lock);
            // our own thread can also be the reason: tryLock refuses if this thread is already inside another executor
            TaskRunner self = TaskRunner.get();
            out.append("\n      this thread (").append(Thread.currentThread().getName()).append("): activeExecutor=")
               .append(self.accordActiveExecutor() == null ? "none" : "executor" + self.accordActiveExecutor().executorId())
               .append(" lockedExecutor=").append(self.accordLockedExecutor() == null ? "none" : "executor" + self.accordLockedExecutor().executorId());
            if (lock instanceof org.apache.cassandra.utils.concurrent.SignalLock)
            {
                org.apache.cassandra.utils.concurrent.SignalLock signalLock = (org.apache.cassandra.utils.concurrent.SignalLock) lock;
                Thread lockOwner = signalLock.unsafeOwner();
                out.append("\n      owner: ").append(lockOwner == null ? "none" : lockOwner.getName() + " " + lockOwner.getState());
                if (lockOwner != null)
                    appendStack(lockOwner, MAX_LOCK_HOLDER_FRAMES, out);
                for (int i = 0 ; i < signalLock.threadCount() ; ++i)
                {
                    Thread registered = signalLock.registeredThread(i);
                    out.append("\n      registered[").append(i).append("]: ").append(registered == null ? "none" : registered.getName() + " " + registered.getState());
                    if (registered != null && registered != lockOwner)
                        appendStack(registered, 8, out);
                }
            }
            // whoever holds it is one of this executor's threads (or a thread inside lockCaches): show what they do
            appendExecutorThreadStacks(executor.executorId(), out);
            return -1;
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
        sb.append(" waiting=").append(exclusive.waitingCount()).append(" nextPosition=").append(exclusive.selfTask.executor().positions.nextPosition());
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

    private static final int MAX_LOCK_HOLDER_FRAMES = 25;

    private static void appendStack(Thread thread, int maxFrames, StringBuilder out)
    {
        StackTraceElement[] stack = thread.getStackTrace();
        for (int i = 0 ; i < Math.min(stack.length, maxFrames) ; ++i)
            out.append("\n          at ").append(stack[i]);
    }

    /** stacks of this instance's threads for the given executor that are not idly waiting for work */
    private static void appendExecutorThreadStacks(int executorId, StringBuilder out)
    {
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        String marker = "AccordExecutor[" + executorId + ',';
        for (java.util.Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet())
        {
            Thread thread = e.getKey();
            if (thread.getThreadGroup() != group || !thread.getName().contains(marker))
                continue;
            StackTraceElement[] stack = e.getValue();
            boolean idle = false;
            for (StackTraceElement frame : stack)
            {
                if (frame.getClassName().startsWith("java.") || frame.getClassName().startsWith("jdk.")) continue;
                idle = frame.getMethodName().equals("awaitExclusive");
                break;
            }
            if (idle)
                continue;
            out.append("\n      ").append(thread.getName()).append(' ').append(thread.getState());
            for (int i = 0 ; i < Math.min(stack.length, MAX_LOCK_HOLDER_FRAMES) ; ++i)
                out.append("\n          at ").append(stack[i]);
        }
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
