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

package org.apache.cassandra.harry.tracker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import org.apache.cassandra.harry.core.Configuration;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.utils.concurrent.WaitQueue;

public class DefaultDataTracker implements DataTracker
{
    protected final AtomicLong maxSeenLts;
    protected final AtomicLong maxCompleteLts;
    protected final PriorityBlockingQueue<Long> reorderBuffer;
    protected final DrainReorderQueueTask reorderTask;

    protected List<LongConsumer> onStarted = new ArrayList<>();
    protected List<LongConsumer> onFinished = new ArrayList<>();

    public DefaultDataTracker()
    {
        this.maxSeenLts = new AtomicLong(-1);
        this.maxCompleteLts = new AtomicLong(-1);
        this.reorderBuffer = new PriorityBlockingQueue<>(100);
        this.reorderTask = new DrainReorderQueueTask();
        this.reorderTask.start();
    }

    @Override
    public void onLtsStarted(LongConsumer onLts)
    {
        this.onStarted.add(onLts);
    }

    @Override
    public void onLtsFinished(LongConsumer onLts)
    {
        this.onFinished.add(onLts);
    }

    @Override
    public void beginModification(long lts)
    {
        assert lts >= 0;
        startedInternal(lts);
        for (LongConsumer consumer : onStarted)
            consumer.accept(lts);
    }

    @Override
    public void endModification(long lts)
    {
        finishedInternal(lts);
        for (LongConsumer consumer : onFinished)
            consumer.accept(lts);
    }

    void startedInternal(long lts)
    {
        recordEvent(lts, false);
    }

    void finishedInternal(long lts)
    {
        recordEvent(lts, true);
    }

    private void recordEvent(long lts, boolean finished)
    {
        // all seen LTS are allowed to be "in-flight"
        maxSeenLts.getAndUpdate((old) -> Math.max(lts, old));

        if (!finished)
            return;

        if (!maxCompleteLts.compareAndSet(lts - 1, lts))
            reorderBuffer.offer(lts);

        reorderTask.notify.signalAll();
    }

    private class DrainReorderQueueTask extends Thread
    {
        private final WaitQueue notify;

        private DrainReorderQueueTask()
        {
            super("DrainReorderQueueTask");
            this.notify = WaitQueue.newWaitQueue();
        }

        public void run()
        {
            while (!Thread.interrupted())
            {
                try
                {
                    WaitQueue.Signal signal = notify.register();
                    runOnce();
                    signal.awaitUninterruptibly();
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                }
            }
        }

        public void runOnce()
        {
            long maxAchievedConsecutive = maxCompleteLts.get();

            Long smallest = reorderBuffer.peek();
            while (smallest != null && smallest == maxAchievedConsecutive + 1)
            {
                boolean res = maxCompleteLts.compareAndSet(maxAchievedConsecutive, smallest);
                assert res : String.format("Should have exclusive access to maxCompleteLts, but someone wrote %d, while %d was expected", maxCompleteLts.get(), maxAchievedConsecutive);
                maxAchievedConsecutive = smallest;
                long removed = reorderBuffer.remove();
                assert smallest == removed : String.format("Tried to remove %d but removed %d", smallest, removed);
                smallest = reorderBuffer.peek();
            }
        }
    }


    public long maxStarted()
    {
        return maxSeenLts.get();
    }

    public long maxConsecutiveFinished()
    {
        return maxCompleteLts.get();
    }

    public boolean isFinished(long lts)
    {
        // Since we _first_ add the item to maxConsecutive, and only then yank it from reorderBuffer,
        // it may happen that we have checked for lts against maxConsecutive while it was still in reorderBuffer
        // but then, by the time we check for it in the reorderBuffer, it is already removed;
        return reorderBuffer.contains(lts) || lts <= maxConsecutiveFinished();
    }

    public Configuration.DataTrackerConfiguration toConfig()
    {
        return new Configuration.DefaultDataTrackerConfiguration(maxSeenLts.get(), maxCompleteLts.get(), new ArrayList<>(reorderBuffer));
    }

    @VisibleForTesting
    public void forceLts(long maxSeen, long maxComplete, List<Long> reorderBuffer)
    {
        System.out.printf("Forcing maxSeen: %d, maxComplete: %d, reorderBuffer: %s%n", maxSeen, maxComplete, reorderBuffer);
        this.maxSeenLts.set(maxSeen);
        this.maxCompleteLts.set(maxComplete);
        if (reorderBuffer != null)
        {
            reorderBuffer.sort(Long::compareTo);
            this.reorderBuffer.addAll(reorderBuffer);
        }
    }

    public String toString()
    {
        List<Long> buf = new ArrayList<>(reorderBuffer);
        return "DataTracker{" +
               "maxSeenLts=" + maxSeenLts +
               ", maxCompleteLts=" + maxCompleteLts +
               ", reorderBuffer=" + buf +
               '}';
    }
}
