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

package org.apache.cassandra.service.accord;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;
import org.apache.cassandra.concurrent.DebuggableTask.DebuggableTaskRunner;
import org.apache.cassandra.service.accord.AccordExecutor.Mode;
import org.apache.cassandra.service.accord.AccordExecutor.TaskRunner;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SimulatorThreadTag.INFINITE_LOOP;
import static org.apache.cassandra.concurrent.ExecutorFactory.SystemThreadTag.NON_DAEMON;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

class AccordExecutorLoops
{
    static abstract class LoopTask extends TaskRunner implements Runnable
    {
        final String id;
        LoopTask(String id) { this.id = id; }
        @Override public String id() { return id; }
    }

    private final Long2ObjectHashMap<Thread> loops;
    private final Long2ObjectHashMap<LoopTask> tasks;

    private final AtomicInteger running = new AtomicInteger();
    private final Condition terminated = Condition.newOneTimeCondition();

    public AccordExecutorLoops(Mode mode, int threads, IntFunction<String> loopName, BiFunction<String, Mode, LoopTask> loopFactory)
    {
        Invariants.require(mode == RUN_WITH_LOCK ? threads == 1 : threads >= 1);
        running.addAndGet(threads);
        loops = new Long2ObjectHashMap<>(threads, 0.65f);
        tasks = new Long2ObjectHashMap<>(threads, 0.65f);
        for (int i = 0; i < threads; ++i)
        {
            String name = loopName.apply(i);
            LoopTask task = loopFactory.apply(name, mode);
            Thread thread = executorFactory().startThread(name, wrap(task), NON_DAEMON, INFINITE_LOOP);
            Thread conflict = loops.putIfAbsent(thread.getId(), thread);
            Invariants.require(conflict == null || !conflict.isAlive(), "Allocated two threads with the same threadId!");
            tasks.put(thread.getId(), task);
        }
    }

    private Runnable wrap(Runnable run)
    {
        return () ->
        {
            try
            {
                run.run();
            }
            finally
            {
                if (0 == running.decrementAndGet())
                    terminated.signalAll();
            }
        };
    }

    public Stream<? extends DebuggableTaskRunner> active()
    {
        return tasks.values().stream();
    }

    public boolean isInLoop()
    {
        Thread thread = Thread.currentThread();
        return loops.get(thread.getId()) == thread;
    }

    public boolean isTerminated()
    {
        return terminated.isSignalled();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        long deadline = nanoTime() + unit.toNanos(timeout);
        return terminated.awaitUntil(deadline);
    }
}
