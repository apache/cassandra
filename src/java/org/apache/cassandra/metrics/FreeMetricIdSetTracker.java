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

package org.apache.cassandra.metrics;

import java.util.BitSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.ScheduledExecutors;

class FreeMetricIdSetTracker
{
    /** How long a released id is withheld before it may be handed out again.
     * An id is not reusable the moment it is released. There is no
     * happens-before edge between that clear and a first read of the same slot by a new metric, so a reused id could
     * pick up a stale write. The delay below is a workaround that makes the clear visible in practice even though the
     * JMM does not guarantee it.  */
    static final long RECYCLE_DELAY_SECONDS = 5;

    private final BitSet freeMetricIdSet = new BitSet();

    private final BitSet tickDelayedToFreeMetricIdSet = new BitSet();
    private final BitSet tockDelayedToFreeMetricIdSet = new BitSet();

    private BitSet delayedToFreeMetricIdSet = tickDelayedToFreeMetricIdSet;

    private ScheduledFuture<?> cleanupTask;

    @VisibleForTesting
    synchronized void triggerRecycling()
    {
        cleanupTask = null;
        BitSet toProcess = otherSet(delayedToFreeMetricIdSet);
        freeMetricIdSet.or(toProcess);
        toProcess.clear();
        if (!delayedToFreeMetricIdSet.isEmpty())
            scheduleCleanupTask();
        delayedToFreeMetricIdSet = toProcess;
    }

    private BitSet otherSet(BitSet set)
    {
        return set == tickDelayedToFreeMetricIdSet ? tockDelayedToFreeMetricIdSet : tickDelayedToFreeMetricIdSet;
    }

    /**
     * @return an id that may be reused, or -1 if none is available and the caller should allocate a fresh one
     */
    public synchronized int getFreeMetricId()
    {
        int metricId = freeMetricIdSet.nextSetBit(0);
        if (metricId >= 0)
            freeMetricIdSet.clear(metricId);
        return metricId;
    }

    /**
     * Releases an id. It becomes available to {@link #getFreeMetricId()} only after the recycling delay, never
     * immediately.
     */
    public synchronized void markAsFree(int metricId)
    {
        delayedToFreeMetricIdSet.set(metricId);
        scheduleCleanupTask();
    }

    // must be called while holding this monitor (from a synchronized method)
    @VisibleForTesting
    protected void scheduleCleanupTask()
    {
        try
        {
            if (cleanupTask == null)
                cleanupTask = ScheduledExecutors.scheduledTasks.schedule(this::triggerRecycling,
                                                                         RECYCLE_DELAY_SECONDS, TimeUnit.SECONDS);
        }
        catch (RejectedExecutionException e)
        {
            // ignore theoretically possible rejections during a shutdown
        }
    }

    public synchronized int getFreeMetricSetCardinality()
    {
        return freeMetricIdSet.cardinality();
    }

    @Override
    public synchronized String toString()
    {
        return "FreeMetricIdSetTracker{" +
               "freeMetricIdSet=" + freeMetricIdSet +
               ", tickDelayedToFreeMetricIdSet=" + tickDelayedToFreeMetricIdSet +
               ", tockDelayedToFreeMetricIdSet=" + tockDelayedToFreeMetricIdSet +
               ", delayedToFreeMetricIdSet=" + (delayedToFreeMetricIdSet == tickDelayedToFreeMetricIdSet ? "tick" : "tock") +
               '}';
    }
}
