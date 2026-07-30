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

import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_FAILED;

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

    @Override
    public String description()
    {
        return runOrFail.toString();
    }

    @Override
    public String briefDescription()
    {
        return description();
    }

    @Override
    boolean runMayThrow()
    {
        boolean success;
        try
        {
            success = runOrFail.runMayThrow();
        }
        catch (Throwable t)
        {
            // RunOrFail throws only callback exceptions, so just report them
            try { executor.agent.onException(t); }
            catch (Throwable t2) { /* nothing more to be safely done */ }
            return true;
        }

        if (success)
            return true;

        // If runOrFail internally failed, we should record this as RUN_FAILED,
        // in particular to ensure continuation cancellation is performed
        setRunState(RUN_FAILED);
        return false;
    }

    @Override
    void reportFailureMayThrow(Throwable fail)
    {
        runOrFail.fail(fail);
    }

    @Override
    ExclusiveExecutor exclusiveExecutor()
    {
        return exclusiveExecutor;
    }
}
