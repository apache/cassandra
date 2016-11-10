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


import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

// a timer - this timer must be used by a single thread, and co-ordinates with other timers by
public final class Timer
{
    private ThreadLocalRandom rnd;

    // in progress snap start
    private long sampleStartNanos;

    // each entry is present with probability 1/p(opCount) or 1/(p(opCount)-1)
    private final long[] sample;
    private int opCount;

    // aggregate info
    private long errorCount;
    private long partitionCount;
    private long rowCount;
    private long total;
    private long max;
    private long maxStart;
    private long upToDateAsOf;
    private long lastSnap = System.nanoTime();

    // communication with summary/logging thread
    private volatile CountDownLatch reportRequest;
    volatile TimingInterval report;
    private volatile TimingInterval finalReport;

    public Timer(int sampleCount)
    {
        int powerOf2 = 32 - Integer.numberOfLeadingZeros(sampleCount - 1);
        this.sample = new long[1 << powerOf2];
    }

    public void init()
    {
        rnd = ThreadLocalRandom.current();
    }

    public void start(){
        // decide if we're logging this event
        sampleStartNanos = System.nanoTime();
    }

    private int p(int index)
    {
        return 1 + (index / sample.length);
    }

    public boolean running()
    {
        return finalReport == null;
    }

    public void stop(long partitionCount, long rowCount, boolean error)
    {
        maybeReport();
        long now = System.nanoTime();
        long time = now - sampleStartNanos;
        if (rnd.nextInt(p(opCount)) == 0)
            sample[index(opCount)] = time;
        if (time > max)
        {
            maxStart = sampleStartNanos;
            max = time;
        }
        total += time;
        opCount += 1;
        this.partitionCount += partitionCount;
        this.rowCount += rowCount;
        if (error)
            this.errorCount++;
        upToDateAsOf = now;
    }

    private int index(int count)
    {
        return count & (sample.length - 1);
    }

    private TimingInterval buildReport()
    {
        final List<SampleOfLongs> sampleLatencies = Arrays.asList
                (       new SampleOfLongs(Arrays.copyOf(sample, index(opCount)), p(opCount)),
                        new SampleOfLongs(Arrays.copyOfRange(sample, index(opCount), Math.min(opCount, sample.length)), p(opCount) - 1)
                );
        final TimingInterval report = new TimingInterval(lastSnap, upToDateAsOf, max, maxStart, max, partitionCount,
                rowCount, total, opCount, errorCount, SampleOfLongs.merge(rnd, sampleLatencies, Integer.MAX_VALUE));
        // reset counters
        opCount = 0;
        partitionCount = 0;
        rowCount = 0;
        total = 0;
        max = 0;
        errorCount = 0;
        lastSnap = upToDateAsOf;
        return report;
    }

    // checks to see if a report has been requested, and if so produces the report, signals and clears the request
    private void maybeReport()
    {
        if (reportRequest != null)
        {
            synchronized (this)
            {
                report = buildReport();
                reportRequest.countDown();
                reportRequest = null;
            }
        }
    }

    // checks to see if the timer is dead; if not requests a report, and otherwise fulfills the request itself
    synchronized void requestReport(CountDownLatch signal)
    {
        if (finalReport != null)
        {
            report = finalReport;
            finalReport = new TimingInterval(0);
            signal.countDown();
        }
        else
            reportRequest = signal;
    }

    // closes the timer; if a request is outstanding, it furnishes the request, otherwise it populates finalReport
    public synchronized void close()
    {
        if (reportRequest == null)
            finalReport = buildReport();
        else
        {
            finalReport = new TimingInterval(0);
            report = buildReport();
            reportRequest.countDown();
            reportRequest = null;
        }
    }
}