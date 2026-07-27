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

import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.DebuggableTask;

// a task that may be submitted to this executor or another
public abstract class IOTask extends Plain implements Cancellable, DebuggableTask
{
    IOTask(AccordExecutor executor, GlobalGroup group, long position, int tranche)
    {
        super(executor, group, position, tranche);
    }

    IOTask(AccordExecutor executor, GlobalGroup group)
    {
        super(executor, group);
    }

    abstract void postRunExclusive();

    @Override
    void cleanupExclusive(AccordExecutor executor, boolean executed)
    {
        super.cleanupExclusive(executor, executed);
        postRunExclusive();
    }

    @Override
    ExclusiveExecutor exclusiveExecutor()
    {
        return null;
    }

    @Override
    public long creationTimeNanos()
    {
        return createdAt;
    }

    @Override
    public long startTimeNanos()
    {
        return runningAt;
    }
}
