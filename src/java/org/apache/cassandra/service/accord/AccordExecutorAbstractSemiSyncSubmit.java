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

import java.util.concurrent.locks.Lock;

import accord.api.Agent;
import accord.utils.QuadFunction;
import accord.utils.QuintConsumer;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.metrics.AccordCacheMetrics;

import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;

abstract class AccordExecutorAbstractSemiSyncSubmit extends AccordExecutorAbstractLockLoop
{
    AccordExecutorAbstractSemiSyncSubmit(Lock lock, int executorId, AccordCacheMetrics metrics, ExecutorFunctionFactory loadExecutor, ExecutorFunctionFactory saveExecutor, ExecutorFunctionFactory rangeLoadExecutor, Agent agent)
    {
        super(lock, executorId, metrics, loadExecutor, saveExecutor, rangeLoadExecutor, agent);
    }

    abstract void notifyWorkAsync();
    abstract void awaitExclusive() throws InterruptedException;

    Interruptible.Task task(Mode mode)
    {
        return mode == RUN_WITH_LOCK ? this::runWithLock : this::runWithoutLock;
    }

    <P1s, P1a, P2, P3, P4> void submitExternal(QuintConsumer<AccordExecutor, P1s, P2, P3, P4> sync, QuadFunction<P1a, P2, P3, P4, Object> async, P1s p1s, P1a p1a, P2 p2, P3 p3, P4 p4)
    {
        if (!lock.tryLock())
        {
            submitted.push(async.apply(p1a, p2, p3, p4));
            notifyWorkAsync();
            return;
        }

        try
        {
            submitExternalExclusive(sync, async, p1s, p1a, p2, p3, p4);
        }
        finally
        {
            lock.unlock();
        }
    }
}
