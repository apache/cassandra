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
import java.util.Random;
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
    private final Random rnd = new Random();

    // TIMING

    private TimingInterval snapInterval(Random rnd) throws InterruptedException
    {
        final Timer[] timers = this.timers.toArray(new Timer[0]);
        final CountDownLatch ready = new CountDownLatch(timers.length);
        for (int i = 0 ; i < timers.length ; i++)
        {
            final Timer timer = timers[i];
            timer.requestReport(ready);
        }

        // TODO fail gracefully after timeout if a thread is stuck
        if (!ready.await(2L, TimeUnit.MINUTES))
            throw new RuntimeException("Timed out waiting for a timer thread - seems one got stuck");

        // reports have been filled in by timer threadCount, so merge
        List<TimingInterval> intervals = new ArrayList<>();
        for (Timer timer : timers)
            intervals.add(timer.report);

        return TimingInterval.merge(rnd, intervals, Integer.MAX_VALUE, history.endNanos());
    }

    // build a new timer and add it to the set of running timers
    public Timer newTimer()
    {
        final Timer timer = new Timer();
        timers.add(timer);
        return timer;
    }

    public void start()
    {
        history = new TimingInterval(System.nanoTime());
    }

    public TimingInterval snapInterval() throws InterruptedException
    {
        final TimingInterval interval = snapInterval(rnd);
        history = TimingInterval.merge(rnd, Arrays.asList(interval, history), 50000, history.startNanos());
        return interval;
    }

    public TimingInterval getHistory()
    {
        return history;
    }

}
