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

import java.util.HashMap;
import java.util.Map;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * DIAGNOSTIC (authored by Claude): where a command store's (i.e. one {@link ExclusiveExecutor}'s) time goes.
 *
 * <p>A store runs one task at a time, and each task costs one trip through the executor's global runnable queue:
 * the store's selfTask is enqueued when it has a next task, waits for a thread ({@link #globalWaitNanos}), the task
 * is prepared under the executor lock ({@link #prepareNanos}), run store-exclusively without the lock
 * ({@link #runNanos}, attributed by kind), and completed under the lock ({@link #completeNanos}, which includes
 * picking the next task). Anything else is time the store had nothing to do. Comparing these tells "the store is busy
 * running X" from "the store is waiting for a thread" from "the lock-held phases are expensive".
 *
 * <p>Enabled with {@code -Daccord.executor_stats=true}; costs a few {@code nanoTime()} calls and a map update per
 * task. Counters are cumulative: callers sample twice and take the difference. Updated only by the thread that owns
 * the store (under the executor lock or the store's exclusive ownership); readers are racy, which is fine for
 * reporting.
 */
public final class ExclusiveExecutorStats
{
    public static final boolean ENABLED = Boolean.getBoolean("accord.executor_stats");
    static final long SLOW_RUN_NANOS = 100_000_000L;
    static final int SLOW_RUNS = 16;

    public static final class Kind
    {
        public long count, nanos, maxNanos;
    }

    public static final class SlowRun
    {
        public final long at, nanos;
        public final String kind, description;
        SlowRun(long at, long nanos, String kind, String description) { this.at = at; this.nanos = nanos; this.kind = kind; this.description = description; }
    }

    public volatile long turns, globalWaitNanos, maxGlobalWaitNanos, prepareNanos, runNanos, completeNanos;
    public final Map<String, Kind> byKind = new HashMap<>();
    public final SlowRun[] slowRuns = new SlowRun[SLOW_RUNS];
    public volatile int slowRunCount;

    long enqueuedAt, preparingAt;

    void onSelfEnqueued()
    {
        enqueuedAt = nanoTime();
    }

    void onPrepareStart()
    {
        long now = nanoTime();
        preparingAt = now;
        if (enqueuedAt > 0)
        {
            long wait = now - enqueuedAt;
            globalWaitNanos += wait;
            if (wait > maxGlobalWaitNanos) maxGlobalWaitNanos = wait;
            enqueuedAt = 0;
        }
        ++turns;
    }

    void onPrepareEnd()
    {
        prepareNanos += nanoTime() - preparingAt;
    }

    void onRun(Task task, long startedAt, long endedAt)
    {
        long nanos = endedAt - startedAt;
        runNanos += nanos;
        String kind = kind(task);
        Kind k = byKind.get(kind);
        if (k == null)
        {
            // bounded: kinds are execution-context reasons / class names, a small fixed set
            synchronized (byKind) { byKind.put(kind, k = new Kind()); }
        }
        ++k.count;
        k.nanos += nanos;
        if (nanos > k.maxNanos) k.maxNanos = nanos;
        if (nanos >= SLOW_RUN_NANOS)
        {
            String description;
            try { description = task.briefDescription(); } catch (Throwable t) { description = task.getClass().getSimpleName(); }
            slowRuns[slowRunCount % SLOW_RUNS] = new SlowRun(endedAt, nanos, kind, description);
            ++slowRunCount;
        }
    }

    void onComplete(long startedAt)
    {
        completeNanos += nanoTime() - startedAt;
    }

    static String kind(Task task)
    {
        task = task.unwrap();
        if (task instanceof SafeTask<?>)
        {
            try { return ((SafeTask<?>) task).executionContext().reason(); }
            catch (Throwable ignore) {}
        }
        return task.getClass().getSimpleName();
    }
}
