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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public final class Compactor<K, V> implements Runnable, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);

    private final Journal<K, V> journal;
    private final SegmentCompactor<K, V> segmentCompactor;
    private final ScheduledExecutorPlus executor;
    private Future<?> scheduled;
    private final AtomicBoolean triggerPending = new AtomicBoolean();
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

    public synchronized void updateCompactionPeriod(long period, TimeUnit units)
    {
        cancelPeriodic();

        if (journal.params.enableCompaction() && !executor.isShutdown())
            schedule(period, units);
    }

    @Override
    public void run()
    {
        runInternal(true);
    }

    private void runInternal(boolean runPostCompaction)
    {
        triggerPending.set(false);
        try
        {
            List<StaticSegment<K, V>> candidates = new ArrayList<>();
            journal.segments().selectStatic(candidates);
            if (candidates.isEmpty())
                return;

            List<StaticSegment<K, V>> toCompact = maybeWrapArrayList(segmentCompactor.select(candidates));
            if (toCompact.isEmpty())
                return;

            int limit = journal.params.compactMaxSegments();
            if (limit < 0)
            {
                noSpamLogger.warn("Misconfigured Journal's compaction max segments (\"{}\") for journal {}. " +
                                  "Compacting all segments ({})",
                                  limit, journal.name, toCompact.size());
            }
            else if (toCompact.size() > limit)
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
            }
            catch (IOException e)
            {
                throw new RuntimeException("Could not compact segments: " + toCompact, e);
            }
        }
        finally
        {
            compacted.signalAll();
            if (runPostCompaction)
                segmentCompactor.onCompacted();
        }
    }

    /**
     * Runs a compaction pass ahead of the next scheduled tick, without introducing any additional concurrency.
     */
    public void triggerNow()
    {
        if (journal.params.enableCompaction() && !executor.isShutdown() && triggerPending.compareAndSet(false, true))
            executor.execute(this);
    }

    /**
     * Like {@link #triggerNow()}, but runs on the same dedicated executor and blocks until the pass completes.
     * Skip when the executor is already shutdown
     */
    public void runNowBlocking()
    {
        submitBlocking(this);
    }

    public void drainBlocking()
    {
        cancelPeriodic();
        submitBlocking(() -> runInternal(false));
    }

    private void submitBlocking(Runnable compactorTask)
    {
        // Must not be called from the compactor executor thread: submit(...).get() would self-deadlock
        if (executor.isShutdown())
            return;

        try
        {
            executor.submit(compactorTask).get();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            Throwable cause = e.getCause();
            Throwables.throwIfUnchecked(cause);
            throw new RuntimeException(cause);
        }
        catch (CancellationException ignored)
        {
            // ignore when it's too late to schedule
        }
    }

    private synchronized void cancelPeriodic()
    {
        if (scheduled == null)
            return;

        scheduled.cancel(false);
        scheduled = null;
    }

    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    @Override
    public void shutdown()
    {
        logger.debug("Shutting down {}", executor);
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

    private List<StaticSegment<K, V>> maybeWrapArrayList(Collection<StaticSegment<K, V>> collection)
    {
        if (collection instanceof ArrayList<?>)
            return (List<StaticSegment<K, V>>) collection;
        return new ArrayList<>(collection);
    }
}
