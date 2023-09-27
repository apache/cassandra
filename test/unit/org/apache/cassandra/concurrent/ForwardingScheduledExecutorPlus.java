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

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.primitives.Longs.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class ForwardingScheduledExecutorPlus extends ForwardingExecutorPlus implements ScheduledExecutorPlus
{
    private final ScheduledExecutorService delegate;

    public ForwardingScheduledExecutorPlus(ScheduledExecutorService delegate)
    {
        super(delegate);
        this.delegate = delegate;
    }

    protected ScheduledExecutorService delegate()
    {
        return delegate;
    }

    @Override
    public ScheduledFuture<?> scheduleSelfRecurring(Runnable run, long delay, TimeUnit units)
    {
        return schedule(run, delay, units);
    }

    @Override
    public ScheduledFuture<?> scheduleAt(Runnable run, long deadline)
    {
        return schedule(run, max(0, deadline - nanoTime()), NANOSECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleTimeoutAt(Runnable run, long deadline)
    {
        return scheduleTimeoutWithDelay(run, max(0, deadline - nanoTime()), NANOSECONDS);
    }

    @Override
    public ScheduledFuture<?> scheduleTimeoutWithDelay(Runnable run, long delay, TimeUnit units)
    {
        return schedule(run, delay, units);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
    {
        return delegate().schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
    {
        return delegate().schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
    {
        return delegate().scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
    {
        return delegate().scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
}
