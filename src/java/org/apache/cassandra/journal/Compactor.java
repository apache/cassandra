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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.journal.SegmentCompactor.Stopped;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public final class Compactor<K, V> implements Runnable, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    private final Journal<K, V> journal;
    private final SegmentCompactor<K, V> segmentCompactor;
    private final ScheduledExecutorPlus executor;
    private Future<?> scheduled;
    public final WaitQueue compacted = WaitQueue.newWaitQueue();

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
        List<StaticSegment<K, V>> toCompact = new ArrayList<>();
        journal.segments().selectStatic(toCompact);
        if (toCompact.isEmpty())
            return;

        int limit = journal.params.compactMaxSegments();
        if (toCompact.size() > limit)
        {
            toCompact.sort(StaticSegment::compareTo);
            toCompact.subList(limit, toCompact.size()).clear();
        }

        try
        {
            Collection<StaticSegment<K, V>> newSegments = segmentCompactor.compact(toCompact);

            for (StaticSegment<K, V> segment : newSegments)
                toCompact.remove(segment);

            journal.replaceCompactedSegments(toCompact, newSegments);
            for (StaticSegment<K, V> segment : toCompact)
                segment.discard(journal);

            compacted.signalAll();
        }
        catch (Stopped e)
        {
            logger.info("Cancelling compaction of {}", toCompact);
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
        logger.debug("Shutting down " + executor);
        if (scheduled != null)
            scheduled.cancel(false);
        segmentCompactor.stop();
        executor.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        // if we call executor.shutdownNow() we can cause ClosedByInterruptException
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return executor.awaitTermination(timeout, units);
    }
}
