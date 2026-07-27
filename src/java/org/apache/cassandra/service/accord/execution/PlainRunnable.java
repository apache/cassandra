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

import javax.annotation.Nullable;

import accord.utils.async.Cancellable;

import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

class PlainRunnable extends Plain implements Cancellable
{
    final @Nullable AsyncPromise<Void> result;
    final Runnable run;
    final @Nullable ExclusiveExecutor exclusiveExecutor;

    PlainRunnable(AccordExecutor executor, AsyncPromise<Void> result, Runnable run, GlobalGroup group, long position, int tranche)
    {
        super(executor, group, position, tranche);
        this.result = result;
        this.run = run;
        this.exclusiveExecutor = null;
    }

    PlainRunnable(AccordExecutor executor, AsyncPromise<Void> result, Runnable run, GlobalGroup group)
    {
        super(executor, group);
        this.result = result;
        this.run = run;
        this.exclusiveExecutor = null;
    }

    PlainRunnable(AccordExecutor executor, AsyncPromise<Void> result, Runnable run, ExclusiveExecutor exclusiveExecutor, ExclusiveGroup group, long position, int tranche)
    {
        super(executor, group, position, tranche);
        this.result = result;
        this.run = run;
        this.exclusiveExecutor = exclusiveExecutor;
    }

    PlainRunnable(AccordExecutor executor, AsyncPromise<Void> result, Runnable run, ExclusiveExecutor exclusiveExecutor, ExclusiveGroup group)
    {
        super(executor, group);
        this.result = result;
        this.run = run;
        this.exclusiveExecutor = exclusiveExecutor;
    }

    @Override
    String toDescription()
    {
        // TODO (expected): ensure this is usefully descriptive, or accept a separate description
        return run.toString();
    }

    @Override
    protected void run()
    {
        onRunning();
        try (Closeable close = resources.get())
        {
            run.run();
        }
        if (result != null)
            result.trySuccess(null);
        onRunComplete();
    }

    @Override
    protected void reportFailure(Throwable t)
    {
        if (result != null)
            result.tryFailure(t);
        executor.agent.onException(t);
    }

    @Override
    ExclusiveExecutor exclusiveExecutor()
    {
        return exclusiveExecutor;
    }
}
