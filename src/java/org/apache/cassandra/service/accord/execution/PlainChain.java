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

import accord.utils.async.AsyncCallbacks;

import org.apache.cassandra.utils.Closeable;

class PlainChain extends Plain
{
    final AsyncCallbacks.RunOrFail runOrFail;
    final @Nullable ExclusiveExecutor exclusiveExecutor;

    PlainChain(AccordExecutor executor, AsyncCallbacks.RunOrFail runOrFail, ExclusiveExecutor exclusiveExecutor, ExclusiveGroup group)
    {
        super(executor, group);
        this.runOrFail = runOrFail;
        this.exclusiveExecutor = exclusiveExecutor;
    }

    PlainChain(AccordExecutor executor, AsyncCallbacks.RunOrFail runOrFail, ExclusiveExecutor exclusiveExecutor, ExclusiveGroup group, long position, int tranche)
    {
        super(executor, group, position, tranche);
        this.runOrFail = runOrFail;
        this.exclusiveExecutor = exclusiveExecutor;
    }

    @Override
    ExclusiveExecutor exclusiveExecutor()
    {
        return exclusiveExecutor;
    }

    @Override
    String toDescription()
    {
        return runOrFail.toString();
    }

    @Override
    protected void run()
    {
        onRunning();
        try (Closeable close = resources.get())
        {
            runOrFail.run();
        }
        catch (Throwable t)
        {
            // shouldn't throw exceptions
            executor.agent.onException(t);
        }
        onRunComplete();
    }

    @Override
    protected void reportFailure(Throwable fail)
    {
        try
        {
            runOrFail.fail(fail);
        }
        catch (Throwable t)
        {
            fail.addSuppressed(t);
            executor.agent.onException(fail);
        }
    }
}
