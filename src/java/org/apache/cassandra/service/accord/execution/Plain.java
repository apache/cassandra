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

import java.util.concurrent.CancellationException;

import accord.utils.async.Cancellable;

import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

abstract class Plain extends Task implements Cancellable
{
    final AccordExecutor executor;

    Plain(AccordExecutor executor, GlobalGroup group)
    {
        super(group);
        this.executor = executor;
    }

    Plain(AccordExecutor executor, ExclusiveGroup group)
    {
        super(group);
        this.executor = executor;
    }

    abstract ExclusiveExecutor exclusiveExecutor();

    @Override
    public final void cancel()
    {
        executor.submit(Task::tryCancelExclusive, CancelTask::new, this);
    }

    @Override
    final void tryCancelExclusive()
    {
        tryFailAndCompleteExclusive(new CancellationException(), CANCELLED);
    }

    @Override
    final void submitExclusiveMayThrow()
    {
        executor.registerExclusive(this);
        setStateExclusive(WAITING_TO_RUN);
        ExclusiveExecutor exclusiveExecutor = exclusiveExecutor();
        if (exclusiveExecutor == null) executor.runnable.enqueue(this, true);
        else exclusiveExecutor.enqueue(this, true);
    }

    @Override
    void maybeCompleteExclusiveMayThrow()
    {
        if (completeState())
        {
            long completeAt = nanoTime();
            executor.elapsedWaitingToRun.increment(runningAt - createdAt, runningAt);
            executor.elapsedRunning.increment(completeAt - runningAt, completeAt);
            executor.elapsed.increment(completeAt - createdAt, completeAt);
        }
    }

    @Override
    void unqueueIfQueued()
    {
        if (isQueued())
        {
            ExclusiveExecutor exclusiveExecutor = exclusiveExecutor();
            if (exclusiveExecutor != null) exclusiveExecutor.unqueue(this);
            else executor.runnable.unqueue(this);
        }
    }

    @Override
    protected boolean isNewWork()
    {
        return true;
    }

    @Override
    AccordExecutor executor()
    {
        return executor;
    }
}
