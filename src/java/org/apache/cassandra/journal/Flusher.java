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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.UnhandledEnum;
import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.concurrent.Interruptible.State;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.journal.ActiveSegment.Allocation;
import org.apache.cassandra.journal.Params.FlushMode;
import org.apache.cassandra.utils.Clock.Global;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.jctools.queues.MpscUnboundedArrayQueue;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.Interrupts.SYNCHRONIZED;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;

/**
 * Flusher is responsible for calling fsync on a corresponding file channel according to requested mode semantics.
 *
 * Flusher is notified about outstanding allocations for which writes have been completed. Flusher orders them by
 * position, and, when possible, will pick the highest outstanding offset of the flush, and notify all pending
 * allocations about successful fsync.
 *
 * Flusher relies on journal active segment to know when there will be no more writes done to the segment. In other
 * words, once flusher sees the active segment has been switched, it waits for all writes to this segment to finish
 * before calling fsync and starting flushes for the next segment. Main reason for this is preserving history: no
 * allocation can be considered done until each and every allocation preceeding it is.
 */
@SuppressWarnings("rawtypes")
public class Flusher implements Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(Flusher.class);

    private final MpscUnboundedArrayQueue<Allocation> queue;
    private final FlushMode mode;
    private final Semaphore semaphore;
    private final Interruptible executor;

    // counts of total pending write and written entries
    private final AtomicLong pending = new AtomicLong(0);
    private final AtomicLong written = new AtomicLong(0);

    private long lastFlushNanos;

    private final long flushPeriodNanos;
    private final long periodicFlushLagBlockNanos;

    private final Journal<?, ?> journal;

    public Flusher(String name, Params params, Journal<?, ?> journal)
    {
        this.semaphore = Semaphore.newSemaphore(1);
        this.queue = new MpscUnboundedArrayQueue<>(1024);
        this.mode = params.flushMode();
        String flushExecutorName = String.format("%s-flusher-%s", name, mode.toString().toLowerCase());
        this.executor = executorFactory().infiniteLoop(flushExecutorName, this::run, SAFE, NON_DAEMON, SYNCHRONIZED);

        this.flushPeriodNanos = mode == FlushMode.BATCH ? -1 : params.flushPeriod(TimeUnit.NANOSECONDS);
        this.periodicFlushLagBlockNanos = mode == FlushMode.PERIODIC ? params.periodicBlockPeriod(TimeUnit.NANOSECONDS) : -1;
        this.journal = journal;
    }

    public void flush(Allocation allocation)
    {
        switch (mode)
        {
            // A write is successful only after flushing to disk. Mutations form a group (hence the name) that waits for the same sync that happens every flushPeriod
            case GROUP:
                pending.incrementAndGet();
                queue.add(allocation);
                break;
            // A write is successful after writing to a buffer in memory. Sync to disk happens every flushPeriod or after reaching the segment size limit.
            // If flush is lagging by more than periodicFlushLagBlock, start blocking until flushed.
            case PERIODIC:
                queue.add(allocation);
                if (Global.nanoTime() <= allocation.writtenAtNanos + periodicFlushLagBlockNanos)
                    allocation.flushed();
                break;
            //  A write is successful only after flushing to disk. Every mutation invokes fsync.
            case BATCH:
                queue.add(allocation);
                semaphore.release(1);
                break;
        }
    }

    public void requestExtraFlush()
    {
        semaphore.release(1);
    }

    private List<Allocation> ordered = new ArrayList<>();
    private ActiveSegment flushingSegment = null;

    @SuppressWarnings("unchecked")
    private void run(State state)
    {
        try
        {
            if (state == State.NORMAL)
            {
                switch (mode)
                {
                    default: throw new UnhandledEnum(mode);
                    case BATCH:
                        semaphore.acquire(1);
                        break;
                    case GROUP:
                        long now = Global.nanoTime();
                        if (lastFlushNanos != -1 && lastFlushNanos + flushPeriodNanos < now)
                            semaphore.tryAcquire(1, lastFlushNanos + flushPeriodNanos - now, TimeUnit.NANOSECONDS);
                        break;
                    case PERIODIC:
                        semaphore.tryAcquire(1, flushPeriodNanos, TimeUnit.NANOSECONDS);
                        break;
                }
            }

            ActiveSegment activeSegment = journal.currentActiveSegment();
            if (flushingSegment == null)
            {
                flushingSegment = activeSegment;
            }
            else if (flushingSegment != activeSegment && flushingSegment.descriptor.timestamp + 1 == activeSegment.descriptor.timestamp)
            {
                if (flushingSegment.fullyFlushed())
                {
                    ActiveSegment fullyFlushed = flushingSegment;
                    flushingSegment = activeSegment;
                    journal.closeActiveSegmentAndOpenAsStatic(fullyFlushed);
                }
                else
                {
                    // Work through allocations that got propagated out-of-order before we switch the segment
                    semaphore.release(1);
                }
            }

            queue.drain(ordered::add);
            if (ordered.isEmpty())
                return;
            ordered.sort(Allocation::compareTo);

            int entriesToFlush = 0;
            Allocation last = null;
            for (int i = 0; i < ordered.size(); i++)
            {
                Allocation current = ordered.get(i);

                // Include all consecutive entries
                if (last != null && last.end() != current.start())
                    break;

                entriesToFlush++;
                last = current;
            }

            if (entriesToFlush > 0)
            {
                Throwable t = null;
                try
                {
                    last.holder().fsync(last.end());
                }
                catch (Throwable e)
                {
                    t = e;
                }
                pending.addAndGet(-entriesToFlush);
                written.addAndGet(entriesToFlush);
                List<Allocation> next = new ArrayList<>(Math.max(ordered.size() - entriesToFlush, 2));
                for (int i = 0; i < ordered.size(); i++)
                {
                    Allocation allocation = ordered.get(i);
                    if (i < entriesToFlush)
                    {
                        if (t != null)
                            allocation.flushFailed(t);
                        else
                            allocation.flushed();
                    }
                    else
                    {
                        next.add(allocation);
                    }
                }
                ordered = next;
            }
            lastFlushNanos = Global.nanoTime();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Caught an exception while flushing", t);
            List<Allocation> tmp = ordered;
            ordered = null;
            for (Allocation allocation : tmp)
                allocation.flushFailed(t);
        }
    }

    long pendingEntries()
    {
        return pending.get();
    }

    long writtenEntries()
    {
        return written.get();
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
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        return executor.awaitTermination(timeout, unit);
    }
}
