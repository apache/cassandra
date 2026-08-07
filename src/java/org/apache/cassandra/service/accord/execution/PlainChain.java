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
        runOrFail.run();
        return true;
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
