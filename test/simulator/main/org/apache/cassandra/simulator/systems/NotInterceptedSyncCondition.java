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

package org.apache.cassandra.simulator.systems;

import org.apache.cassandra.utils.concurrent.Awaitable;
import org.apache.cassandra.utils.concurrent.Condition;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class NotInterceptedSyncCondition extends Awaitable.AbstractAwaitable implements Condition
{
    private volatile boolean isSignalled;

    @Override
    public synchronized boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException
    {
        while (true)
        {
            if (isSignalled()) return true;
            if (!notInterceptedWaitUntil(this, nanoTimeDeadline)) return false;
        }
    }

    @Override
    public synchronized Awaitable await() throws InterruptedException
    {
        while (!isSignalled)
            wait();
        return this;
    }

    @Override
    public boolean isSignalled()
    {
        return isSignalled;
    }

    @Override
    public synchronized void signal()
    {
        isSignalled = true;
        notifyAll();
    }

    private static boolean notInterceptedWaitUntil(Object monitor, long deadlineNanos) throws InterruptedException
    {
        long wait = deadlineNanos - nanoTime();
        if (wait <= 0)
            return false;

        monitor.wait((wait + 999999) / 1000000);
        return true;
    }
}
