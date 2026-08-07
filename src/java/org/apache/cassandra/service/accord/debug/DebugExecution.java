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

package org.apache.cassandra.service.accord.debug;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Command;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.metrics.LogLinearHistogram;
import org.apache.cassandra.service.accord.execution.Task;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.WithResources;

import static org.apache.cassandra.config.CassandraRelevantProperties.DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class DebugExecution
{
    private static final Logger logger = LoggerFactory.getLogger(DebugExecution.class);
    public static final boolean DEBUG_EXECUTION = CassandraRelevantProperties.ACCORD_DEBUG_EXECUTION.getBoolean(false);
    private static final long REPORT_MIN_LATENCY_MICROS = 50_000;
    private static final long REPORT_CPU_RATIO = 2;
    private static final long REPORT_MAX_LATENCY_MICROS = 100_000;
    private static final long REPORT_CPU_MICROS = 50_000;

    // TODO (expected): use sharded histogram so we can report global stats
    public static class DebugExecutor
    {
        public static DebugExecutor maybeDebug() { return DEBUG_EXECUTION ? new DebugExecutor() : null; }

        private DebugExecutor() {}

        final LogLinearHistogram waitingToLock = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);
        final LogLinearHistogram locked = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);
        final LogLinearHistogram sequentialExecutorWaitingToRunLatency = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);
        final LogLinearHistogram sequentialExecutorSetHeadToRunLatency = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);
        final LogLinearHistogram pollToRun = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);
        final LogLinearHistogram running = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);
        final LogLinearHistogram runToCleanup = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);
        final LogLinearHistogram cleanup = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);
        final LogLinearHistogram taskTotal = new LogLinearHistogram(REPORT_MAX_LATENCY_MICROS);

        long lockedAt, lockedAtCpu;
        long unlockedAt, unlockedAtCpu;

        public void onEnterLock()
        {
            onEnterLock(0);
        }

        public void onEnterLock(long lockAt)
        {
            lockedAt = nanoTime();
            lockedAtCpu = nowCpu();
            if (lockAt > 0)
            {
                long waitingToLockForMicros = (lockedAt - lockAt)/1000;
                waitingToLock.increment(waitingToLockForMicros);
                if (waitingToLockForMicros > REPORT_MAX_LATENCY_MICROS)
                {
                    report("Took {}us to aquire executor lock", waitingToLockForMicros);
                }
            }
        }

        public void onExitLock()
        {
            // TODO (expected): specialise this for despatch loop, which can reasonably handle longer lock hold periods
            unlockedAt = nanoTime();
            unlockedAtCpu = nowCpu();
            long lockedForMicros = (unlockedAt - lockedAt)/1000;
            long lockedForCpuMicros = (unlockedAtCpu - lockedAtCpu)/1000;
            if (lockedForMicros >= REPORT_MAX_LATENCY_MICROS)
            {
                report("Held lock for {}us (cpu:{}us)", lockedForMicros, lockedForCpuMicros);
            }
            else if (lockedForMicros >= REPORT_MIN_LATENCY_MICROS && (lockedForMicros / lockedForCpuMicros) >= REPORT_CPU_RATIO)
            {
                report("Held lock for {}us with cpu time only {}us", lockedForMicros, lockedForCpuMicros);
            }
            locked.increment(lockedForMicros);
        }
    }

    public static class DebugExclusiveExecutor
    {
        public static DebugExclusiveExecutor maybeDebug(DebugExecutor owner, int commandStoreId)
        {
            return DEBUG_EXECUTION ? new DebugExclusiveExecutor(owner, commandStoreId) : null;
        }

        final DebugExecutor owner;
        final int commandStoreId;

        long setTaskAt, waitingAt;
        Task prev;

        public DebugExclusiveExecutor(DebugExecutor owner, int commandStoreId)
        {
            this.owner = owner;
            this.commandStoreId = commandStoreId;
        }

        public void onSetTask(Task next)
        {
            if (next == null) setTaskAt = 0;
            else setTaskAt = nanoTime();
        }

        public void onComplete(Task completed)
        {
            long readyAt = setTaskAt;
            long runningAt = DebugTask.get(completed).runningAt;
            if (waitingAt > setTaskAt)
            {
                readyAt = waitingAt;
                long waitingMicros = (runningAt - waitingAt)/1000;
                owner.sequentialExecutorWaitingToRunLatency.increment(waitingMicros);
                if (waitingMicros > REPORT_MAX_LATENCY_MICROS)
                    report("{} spent {}us blocked by a direct execution on queue {}", completed, waitingMicros, commandStoreId);
            }
            long atHeadMicros = (runningAt - readyAt)/1000;
            owner.sequentialExecutorSetHeadToRunLatency.increment(atHeadMicros);
            if (atHeadMicros > REPORT_MAX_LATENCY_MICROS)
            {
                report("{} spent {}us at head of queue {}", completed, atHeadMicros, commandStoreId);
            }
            this.prev = completed;
        }

        public void onWaiting()
        {
            waitingAt = nanoTime();
        }
    }

    public static final class DebugTask implements WithResources
    {
        public static final boolean SANITY_CHECK = DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED.getBoolean();
        private static final boolean DEBUG = DEBUG_EXECUTION || SANITY_CHECK;

        public static WithResources maybeDebug(WithResources resources, Task task) { return DEBUG ? new DebugTask(resources, task) : resources; }
        public static DebugTask get(Task task) { return (DebugTask)task.unwrap().resources; }

        final WithResources resources;
        final Task task;
        public DebugTask(WithResources resources, Task task)
        {
            this.resources = resources;
            this.task = task;
        }

        public List<Command> sanityCheck; // for AccordTask only
        long polledAt, preRunAt, runningAt, runCompleteAt, completeAt, completedAt;
        long releasedRangeScannerAt, releasedStateAt;
        long runningAtCpu, runCompleteAtCpu;
        Thread thread;

        public void onPolled()
        {
            polledAt = nanoTime();
        }

        public void onPreRun()
        {
            preRunAt = nanoTime();
        }

        public void onRunning()
        {
            thread = Thread.currentThread();
            runningAtCpu = nowCpu();
            runningAt = nanoTime();
        }

        public void onRunComplete()
        {
            runCompleteAtCpu = nowCpu();
            runCompleteAt = nanoTime();
        }

        public void onReleasedRangeScanner()
        {
            releasedRangeScannerAt = nanoTime();
        }

        public void onReleasedState()
        {
            releasedStateAt = nanoTime();
        }

        public void onComplete()
        {
            completeAt = nanoTime();
        }

        public void onCompleted(DebugExecutor owner)
        {
            completedAt = nanoTime();
            if (runningAt > 0 && polledAt > 0)
            {
                long pollToRunMicros = (runningAt - polledAt)/1000;
                owner.pollToRun.increment(pollToRunMicros);
                long runningMicros = -1;
                if (runCompleteAt > 0)
                {
                    runningMicros = (runCompleteAt - runningAt) / 1000;
                    owner.running.increment(runningMicros);
                }
                long runToCleanMicros = (completeAt - runCompleteAt) / 1000;
                owner.runToCleanup.increment(runToCleanMicros);
                long cleanupMicros = (completedAt - completeAt) / 1000;
                owner.cleanup.increment(cleanupMicros);
                long totalMicros = (completedAt - polledAt)/1000;
                owner.taskTotal.increment(totalMicros);
                long totalCpu = (runCompleteAtCpu - runningAtCpu)/1000;
                if (totalMicros > REPORT_MAX_LATENCY_MICROS || totalCpu > REPORT_CPU_MICROS || (totalMicros > REPORT_MIN_LATENCY_MICROS && (totalCpu == 0 || totalMicros/totalCpu >= REPORT_CPU_RATIO)))
                {
                    String reason = "";
                    if (totalMicros > REPORT_MAX_LATENCY_MICROS) reason += "LONG TIME ";
                    if (totalCpu > REPORT_CPU_MICROS) reason += "HIGH CPU ";
                    if ((totalMicros > REPORT_MIN_LATENCY_MICROS && (totalCpu == 0 || (totalMicros/totalCpu) >= REPORT_CPU_RATIO))) reason += "LOW RATIO ";
                    report("{}{}: total {}us cpu:{}us ({}), pollToRun {}us, running {}us, runToClean {}us, cleanup {}us",
                           reason, task, totalMicros, totalCpu, thread, pollToRunMicros, runningMicros, runToCleanMicros, cleanupMicros);
                }
            }
        }

        @Override
        public Closeable get()
        {
            return resources.get();
        }
    }

    private static void report(String message, Object ... params)
    {
        logger.warn(Thread.currentThread() + " " + message, params);
    }

    private static final ThreadMXBean runtime = ManagementFactory.getThreadMXBean();
    private static long nowCpu()
    {
        return runtime.getCurrentThreadCpuTime();
    }
}
