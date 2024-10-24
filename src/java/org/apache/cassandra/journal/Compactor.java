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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public final class Compactor<K, V> implements Runnable, Shutdownable
{
    private final Journal<K, V> journal;
    private final SegmentCompactor<K, V> segmentCompactor;
    private final ScheduledExecutorPlus executor;
    private Future<?> scheduled;

    Compactor(Journal<K, V> journal, SegmentCompactor<K, V> segmentCompactor)
    {
        this.executor = executorFactory().scheduled(false, journal.name + "-compactor");
        this.journal = journal;
        this.segmentCompactor = segmentCompactor;
    }

    synchronized void start()
    {
        if (journal.params.enableCompaction())
            schedule(journal.params.compactionPeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    private synchronized void schedule(long period, TimeUnit units)
    {
        scheduled = executor.scheduleWithFixedDelay(this, period, period, units);
    }

    public synchronized void updateCompactionPeriod(int period, TimeUnit units)
    {
        if (!journal.params.enableCompaction())
            return;

        if (scheduled != null)
            scheduled.cancel(false);

        schedule(period, units);
    }

    @Override
    public void run()
    {
        Set<StaticSegment<K, V>> toCompact = new HashSet<>();
        journal.segments().selectStatic(toCompact);
        if (toCompact.size() < 2)
            return;

        try
        {
            Collection<StaticSegment<K, V>> newSegments = segmentCompactor.compact(toCompact);
            // No-op compaction
            if (newSegments == null)
                return;

            for (StaticSegment<K, V> segment : newSegments)
                toCompact.remove(segment);

            journal.replaceCompactedSegments(toCompact, newSegments);
            for (StaticSegment<K, V> segment : toCompact)
                segment.discard(journal);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not compact segments: " + toCompact);
        }
    }

    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return executor.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return executor.awaitTermination(timeout, units);
    }
}
