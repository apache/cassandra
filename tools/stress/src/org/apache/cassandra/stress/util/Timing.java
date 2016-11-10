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


import java.util.*;
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
    // concurrency: this should be ok as the consumers are created serially by StressAction.run / warmup
    // Probably the CopyOnWriteArrayList could be changed to an ordinary list as well.
    private final Map<String, List<Timer>> timers = new TreeMap<>();
    private volatile TimingIntervals history;
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
        public final TimingIntervals intervals;
        public TimingResult(E extra, TimingIntervals intervals)
        {
            this.extra = extra;
            this.intervals = intervals;
        }
    }

    public <E> TimingResult<E> snap(Callable<E> call) throws InterruptedException
    {
        // Count up total # of timers
        int timerCount = 0;
        for (List<Timer> timersForOperation : timers.values())
        {
            timerCount += timersForOperation.size();
        }
        final CountDownLatch ready = new CountDownLatch(timerCount);

        // request reports
        for (List <Timer> timersForOperation : timers.values())
        {
            for(Timer timer : timersForOperation)
            {
                timer.requestReport(ready);
            }
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
        {
            throw new RuntimeException("Timed out waiting for a timer thread - seems one got stuck. Check GC/Heap size");
        }

        boolean done = true;

        // reports have been filled in by timer threadCount, so merge
        Map<String, TimingInterval> intervals = new TreeMap<>();
        for (Map.Entry<String, List<Timer>> entry : timers.entrySet())
        {
            List<TimingInterval> operationIntervals = new ArrayList<>();
            for (Timer timer : entry.getValue())
            {
                operationIntervals.add(timer.report);
                done &= !timer.running();
            }

            intervals.put(entry.getKey(), TimingInterval.merge(operationIntervals, reportSampleCount,
                                                              history.get(entry.getKey()).endNanos()));
        }

        TimingIntervals result = new TimingIntervals(intervals);
        this.done = done;
        history = history.merge(result, historySampleCount, history.startNanos());
        return new TimingResult<>(extra, result);
    }

    // build a new timer and add it to the set of running timers.
    public Timer newTimer(String opType, int sampleCount)
    {
        final Timer timer = new Timer(sampleCount);

        if (!timers.containsKey(opType))
            timers.put(opType, new ArrayList<Timer>());

        timers.get(opType).add(timer);
        return timer;
    }

    public void start()
    {
        history = new TimingIntervals(timers.keySet());
    }

    public boolean done()
    {
        return done;
    }

    public TimingIntervals getHistory()
    {
        return history;
    }
}
