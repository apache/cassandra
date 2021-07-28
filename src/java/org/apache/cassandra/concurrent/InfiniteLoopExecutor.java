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

package org.apache.cassandra.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.InternalState.TERMINATED;
import static org.apache.cassandra.concurrent.Interruptible.State.INTERRUPTED;
import static org.apache.cassandra.concurrent.Interruptible.State.NORMAL;
import static org.apache.cassandra.concurrent.Interruptible.State.SHUTTING_DOWN;

public class InfiniteLoopExecutor implements Interruptible
{
    private static final Logger logger = LoggerFactory.getLogger(InfiniteLoopExecutor.class);

    public enum InternalState { TERMINATED }

    private static final AtomicReferenceFieldUpdater<InfiniteLoopExecutor, Object> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(InfiniteLoopExecutor.class, Object.class, "state");
    private final Thread thread;
    private final Task task;
    private volatile Object state = NORMAL;
    private final Consumer<Thread> interruptHandler;

    public InfiniteLoopExecutor(String name, Task task)
    {
        this(ExecutorFactory.Global.executorFactory(), name, task, Thread::interrupt);
    }

    public InfiniteLoopExecutor(ExecutorFactory factory, String name, Task task)
    {
        this(factory, name, task, Thread::interrupt);
    }

    public InfiniteLoopExecutor(ExecutorFactory factory, String name, Task task, Consumer<Thread> interruptHandler)
    {
        this.task = task;
        this.thread = factory.startThread(name, this::loop);
        this.interruptHandler = interruptHandler;
    }

    public InfiniteLoopExecutor(BiFunction<String, Runnable, Thread> threadStarter, String name, Task task, Consumer<Thread> interruptHandler)
    {
        this.task = task;
        this.thread = threadStarter.apply(name, this::loop);
        this.interruptHandler = interruptHandler;
    }

    private void loop()
    {
        boolean interrupted = false;
        while (true)
        {
            try
            {
                Object cur = state;
                if (cur == TERMINATED) break;

                interrupted |= Thread.interrupted();
                if (cur == NORMAL && interrupted) cur = INTERRUPTED;
                task.run((State) cur);

                interrupted = false;
                if (cur == SHUTTING_DOWN) state = TERMINATED;
            }
            catch (TerminateException ignore)
            {
                state = TERMINATED;
            }
            catch (UncheckedInterruptedException | InterruptedException ignore)
            {
                interrupted = true;
            }
            catch (Throwable t)
            {
                logger.error("Exception thrown by runnable, continuing with loop", t);
            }
        }
    }

    public void interrupt()
    {
        interruptHandler.accept(thread);
    }

    public void shutdown()
    {
        stateUpdater.updateAndGet(this, cur -> cur != TERMINATED ? SHUTTING_DOWN : TERMINATED);
        interruptHandler.accept(thread);
    }

    public Object shutdownNow()
    {
        state = TERMINATED;
        interruptHandler.accept(thread);
        return null;
    }

    @Override
    public boolean isTerminated()
    {
        return state == TERMINATED && !thread.isAlive();
    }

    public boolean awaitTermination(long time, TimeUnit unit) throws InterruptedException
    {
        thread.join(unit.toMillis(time));
        return isTerminated();
    }

    @VisibleForTesting
    public boolean isAlive()
    {
        return this.thread.isAlive();
    }
}
