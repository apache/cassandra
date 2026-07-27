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

import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;

abstract class Plain extends Task implements Cancellable
{
    final AccordExecutor executor;

    Plain(AccordExecutor executor, GlobalGroup group, long position, int tranche)
    {
        super(group, position, tranche);
        this.executor = executor;
    }

    Plain(AccordExecutor executor, ExclusiveGroup group, long position, int tranche)
    {
        super(group, position, tranche);
        this.executor = executor;
    }

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
    public void cancel()
    {
        executor.submit(Task::cancelExclusive, CancelTask::new, this);
    }

    void cancelExclusive()
    {
        ExclusiveExecutor exclusiveExecutor = exclusiveExecutor();
        if ((exclusiveExecutor == null ? executor.runnable : exclusiveExecutor).tryUnqueueWaiting(this))
        {
            try
            {
                failExclusive(new CancellationException(), CANCELLED);
            }
            catch (Throwable t)
            {
                executor.agent.onException(t);
            }
            finally
            {
                executor.cleanupTaskExclusive(this, false);
            }
        }
    }

    @Override
    final void submitExclusive()
    {
        submitExclusive(exclusiveExecutor(), null);
    }

    final Cancellable submitExclusive(ExclusiveExecutor exclusiveExecutor, Task parent)
    {
        Invariants.require(executor.isOwningThread());
        setStateExclusive(WAITING_TO_RUN);

        if (parent == null) executor.registerExclusive(this);
        else executor.registerConsequenceExclusive(parent, this);
        onLoaded();

        if (exclusiveExecutor == null) executor.runnable.enqueue(this, true);
        else exclusiveExecutor.enqueue(this, true);
        return this;
    }

    @Override
    protected boolean isNewWork()
    {
        return true;
    }
}
