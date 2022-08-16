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

package org.apache.cassandra.service.accord.api;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import accord.api.Scheduler;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;

public class AccordScheduler implements Scheduler
{
    private final ScheduledExecutorPlus scheduledExecutor = ExecutorFactory.Global.executorFactory().scheduled("AccordScheduled");

    private static class ScheduledFutureWrapper implements Scheduled
    {
        private final ScheduledFuture<?> future;

        public ScheduledFutureWrapper(ScheduledFuture<?> future)
        {
            this.future = future;
        }

        @Override
        public void cancel()
        {
            future.cancel(false);
        }
    }

    @Override
    public Scheduled recurring(Runnable run, long delay, TimeUnit units)
    {
        ScheduledFuture<?> future = scheduledExecutor.scheduleAtFixedRate(run, delay, delay, units);
        return new ScheduledFutureWrapper(future);
    }

    @Override
    public Scheduled once(Runnable run, long delay, TimeUnit units)
    {
        ScheduledFuture<?> future = scheduledExecutor.schedule(run, delay, units);
        return new ScheduledFutureWrapper(future);
    }

    @Override
    public void now(Runnable run)
    {
        // called from the mutation stage configured by the verb
        run.run();
    }
}
