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

import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;
import java.util.function.Function;

import accord.api.Agent;

abstract class AbstractSemiSyncSubmit extends AbstractLockLoop
{
    AbstractSemiSyncSubmit(Lock lock, int executorId, Agent agent)
    {
        super(lock, executorId, agent);
    }

    abstract void awaitExclusive() throws InterruptedException;

    <P1> void submitExternal(Consumer<P1> sync, Function<P1, Task> async, P1 p1)
    {
        if (push(async.apply(p1)) == null && !isInLoop())
            notifyWork();
    }
}
