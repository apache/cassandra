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
import java.util.function.Function;
import java.util.function.IntFunction;

import accord.utils.Invariants;
import org.agrona.collections.LongHashSet;
import org.apache.cassandra.concurrent.InfiniteLoopExecutor;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.service.accord.AccordExecutor.Mode;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

class AccordExecutorInfiniteLoops implements Shutdownable
{
    private final Interruptible[] loops;
    private final LongHashSet threadIds;

    public AccordExecutorInfiniteLoops(Mode mode, int threads, IntFunction<String> name, Function<Mode, Interruptible.Task> tasks)
    {
        Invariants.checkState(mode == RUN_WITH_LOCK ? threads == 1 : threads >= 1);
        final LongHashSet threadIds = new LongHashSet(threads, 0.5f);
        this.loops = new Interruptible[threads];
        for (int i = 0; i < threads; ++i)
        {
            loops[i] = executorFactory().infiniteLoop(name.apply(i), tasks.apply(mode), SAFE, NON_DAEMON, UNSYNCHRONIZED);
            if (loops[i] instanceof InfiniteLoopExecutor)
                threadIds.add(((InfiniteLoopExecutor) loops[i]).threadId());
        }
        this.threadIds = threadIds;
    }

    public boolean isInLoop()
    {
        return threadIds.contains(Thread.currentThread().getId());
    }

    @Override
    public void shutdown()
    {
        for (Interruptible loop : loops)
            loop.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        for (Interruptible loop : loops)
            loop.shutdownNow();
        return null;
    }

    @Override
    public boolean isTerminated()
    {
        for (Interruptible loop : loops)
        {
            if (!loop.isTerminated())
                return false;
        }
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        long deadline = nanoTime() + unit.toNanos(timeout);
        for (Interruptible loop : loops)
        {
            long wait = deadline - nanoTime();
            if (!loop.awaitTermination(wait, unit))
                return false;
        }
        return true;
    }
}
