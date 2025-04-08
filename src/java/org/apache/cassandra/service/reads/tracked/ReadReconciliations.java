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

package org.apache.cassandra.service.reads.tracked;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.concurrent.Shutdownable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.utils.Clock;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SimulatorSemantics.DISCARD;
import static org.apache.cassandra.utils.MonotonicClock.Global.preciseTime;

/**
 * Since the read reconciliations don't use 2 way callbacks, a map of active reconciliations
 * are maintained and expired here.
 *
 * Borrowed heavily from RequestCallbacks
 */
public class ReadReconciliations implements Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(ReadReconciliations.class);

    private final ConcurrentMap<Long, Info> reconciliations = new ConcurrentHashMap<>();
    private final ScheduledExecutorPlus executor = executorFactory().scheduled("Reconciliation-Map-Reaper", DISCARD);

    private static final AtomicLong lastReconciliationId = new AtomicLong();

    public static long nextReconciliationId()
    {
        long nextId = TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
        while (true)
        {
            long lastId = lastReconciliationId.get();
            if (nextId <= lastId)
                nextId = lastId + 1;

            if (lastReconciliationId.compareAndSet(lastId, nextId))
                return nextId;
        }

    }

    public static class Info
    {
        final long id = nextReconciliationId();
        final long expiresAtNanos;
        final TrackedReadReconciliation<?, ?> reconciliation;

        public Info(long expiresAtNanos, TrackedReadReconciliation<?, ?> reconciliation)
        {
            this.expiresAtNanos = expiresAtNanos;
            this.reconciliation = reconciliation;
        }

        boolean hasTimedOutAt(long nanoTime)
        {
            return nanoTime > expiresAtNanos;
        }
    }


    private void expire()
    {
        long start = preciseTime.now();
        int n = 0;
        for (Map.Entry<Long, Info> entry : reconciliations.entrySet())
        {
            Info info = entry.getValue();
            if (info.hasTimedOutAt(start))
            {
                if (reconciliations.remove(entry.getKey(), entry.getValue()))
                    n++;
            }
        }
        if (n > 0)
            logger.trace("Expired {} entries", n);
    }

    public ReadReconciliations()
    {
        long expirationInterval = defaultExpirationInterval();
        executor.scheduleWithFixedDelay(this::expire, expirationInterval, expirationInterval, NANOSECONDS);
    }

    public long newReconciliation(TrackedReadReconciliation<?, ?> reconciliation, long expiresAtNanos)
    {
        long now = preciseTime.now();
        Info info = new Info(expiresAtNanos, reconciliation);
        reconciliations.put(info.id, info);
        return info.id;
    }

    public void acknowledgeSync(long reconciliationId, int syncId)
    {
        Info info = reconciliations.get(reconciliationId);
        if (info == null)
            return;

        if (info.reconciliation.acknowledgeSync(syncId))
            reconciliations.remove(reconciliationId);
    }

    public void addMutationsToRead(long reconciliationId, List<Mutation> mutations)
    {
        Info info = reconciliations.get(reconciliationId);
        if (info == null)
            return;

        if (info.reconciliation.addMutationsToRead(mutations))
            reconciliations.remove(reconciliationId);
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

    public static long defaultExpirationInterval()
    {
        return DatabaseDescriptor.getMinRpcTimeout(NANOSECONDS) / 2;
    }
}
