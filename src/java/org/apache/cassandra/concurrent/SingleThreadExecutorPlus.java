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
package org.apache.cassandra.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.utils.concurrent.Future;

public class SingleThreadExecutorPlus extends ThreadPoolExecutorPlus implements SequentialExecutorPlus
{
    public static class AtLeastOnce extends AtomicBoolean implements AtLeastOnceTrigger, Runnable
    {
        protected final SequentialExecutorPlus executor;
        protected final Runnable run;

        public AtLeastOnce(SequentialExecutorPlus executor, Runnable run)
        {
            this.executor = executor;
            this.run = run;
        }

        public boolean trigger()
        {
            boolean success;
            if (success = compareAndSet(false, true))
                executor.execute(this);
            return success;
        }

        public void runAfter(Runnable run)
        {
            executor.execute(run);
        }

        public void sync()
        {
            Future<?> done = executor.submit(() -> {});
            done.awaitThrowUncheckedOnInterrupt();
            done.rethrowIfFailed(); // executor might get shutdown before we execute; propagate cancellation exception
        }

        public void run()
        {
            set(false);
            run.run();
        }

        @Override
        public String toString()
        {
            return run.toString();
        }
    }

    SingleThreadExecutorPlus(ThreadPoolExecutorBuilder<? extends SingleThreadExecutorPlus> builder)
    {
        this(builder, TaskFactory.standard());
    }

    SingleThreadExecutorPlus(ThreadPoolExecutorBuilder<? extends SingleThreadExecutorPlus> builder, TaskFactory taskFactory)
    {
        super(builder, taskFactory);
    }

    @Override
    public int getCorePoolSize()
    {
        return 1;
    }
    @Override
    public void setCorePoolSize(int number)
    {
        throw new UnsupportedOperationException();
    }
    @Override
    public int getMaximumPoolSize()
    {
        return 1;
    }
    @Override
    public void setMaximumPoolSize(int number)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public AtLeastOnce atLeastOnceTrigger(Runnable run)
    {
        return new AtLeastOnce(this, run);
    }
}
