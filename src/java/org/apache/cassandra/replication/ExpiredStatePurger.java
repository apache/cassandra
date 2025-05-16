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
package org.apache.cassandra.replication;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.Clock;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SimulatorSemantics.NORMAL;

/**
 * Since most of our state machines don't use 2 way callbacks, expirations are handled here.
 * </p>
 * TODO (expected): switch to using accord.impl.RequestCallbacks instead of walking the maps
 */
public class ExpiredStatePurger implements Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(ExpiredStatePurger.class);
    public static final ExpiredStatePurger instance = new ExpiredStatePurger();

    private final ScheduledExecutorPlus executor = executorFactory().scheduled("Expired-State-Purger", NORMAL);
    private final CopyOnWriteArrayList<Expireable> expireables = new CopyOnWriteArrayList<>();

    public interface Expireable
    {
        int expire(long nanoTime);
    }

    public ExpiredStatePurger()
    {
        long expirationInterval = defaultExpirationInterval();
        executor.scheduleWithFixedDelay(this::expire, expirationInterval, expirationInterval, NANOSECONDS);
    }

    public void register(Expireable expireable)
    {
        expireables.add(expireable);
    }

    private void expire()
    {
        long nanoTime = Clock.Global.nanoTime();
        for (Expireable expireable : expireables)
        {
            int n = expireable.expire(nanoTime);
            if (n > 0) logger.trace("Expired {} {} entries", n, expireable);
        }
    }

    private long defaultExpirationInterval()
    {
        return DatabaseDescriptor.getMinRpcTimeout(NANOSECONDS) / 2;
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    @Override
    public Object shutdownNow()
    {
        return executor.shutdownNow();
    }

    public void shutdownBlocking() throws InterruptedException
    {
        if (executor == null || executor.isTerminated())
            return;

        executor.shutdown();
        executor.awaitTermination(1, MINUTES);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return executor.awaitTermination(timeout, units);
    }
}
