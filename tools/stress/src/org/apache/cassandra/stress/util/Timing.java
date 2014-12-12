package org.apache.cassandra.stress.util;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

// relatively simple timing class for getting a uniform sample of latencies, and saving other metrics
// ensures accuracy of timing by having single threaded timers that are check-pointed by the snapping thread,
// which waits for them to report back. They report back the data up to the last event prior to the check-point.
// if the threads are blocked/paused this may mean a period of time longer than the checkpoint elapses, but that all
// metrics calculated over the interval are accurate
public class Timing
{

    private final CopyOnWriteArrayList<Timer> timers = new CopyOnWriteArrayList<>();
    private volatile TimingInterval history;
    private final int historySampleCount;
    private final int reportSampleCount;
    private boolean done;

    public Timing(int historySampleCount, int reportSampleCount)
    {
        this.historySampleCount = historySampleCount;
        this.reportSampleCount = reportSampleCount;
    }

    // TIMING

    public static class TimingResult<E>
    {
        public final E extra;
        public final TimingInterval timing;
        public TimingResult(E extra, TimingInterval timing)
        {
            this.extra = extra;
            this.timing = timing;
        }
    }

    public <E> TimingResult<E> snap(Callable<E> call) throws InterruptedException
    {
        final Timer[] timers = this.timers.toArray(new Timer[0]);
        final CountDownLatch ready = new CountDownLatch(timers.length);
        for (int i = 0 ; i < timers.length ; i++)
        {
            final Timer timer = timers[i];
            timer.requestReport(ready);
        }

        E extra;
        try
        {
            extra = call.call();
        }
        catch (Exception e)
        {
            if (e instanceof InterruptedException)
                throw (InterruptedException) e;
            throw new RuntimeException(e);
        }

        // TODO fail gracefully after timeout if a thread is stuck
        if (!ready.await(5L, TimeUnit.MINUTES))
            throw new RuntimeException("Timed out waiting for a timer thread - seems one got stuck. Check GC/Heap size");

        boolean done = true;
        // reports have been filled in by timer threadCount, so merge
        List<TimingInterval> intervals = new ArrayList<>();
        for (Timer timer : timers)
        {
            intervals.add(timer.report);
            done &= !timer.running();
        }

        this.done = done;
        TimingResult<E> result = new TimingResult<>(extra, TimingInterval.merge(intervals, reportSampleCount, history.endNanos()));
        history = TimingInterval.merge(Arrays.asList(result.timing, history), historySampleCount, history.startNanos());
        return result;
    }

    // build a new timer and add it to the set of running timers
    public Timer newTimer(int sampleCount)
    {
        final Timer timer = new Timer(sampleCount);
        timers.add(timer);
        return timer;
    }

    public void start()
    {
        history = new TimingInterval(System.nanoTime());
    }

    public boolean done()
    {
        return done;
    }

    public TimingInterval getHistory()
    {
        return history;
    }

}
