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


import java.util.concurrent.CountDownLatch;

import org.HdrHistogram.Histogram;

// a timer - this timer must be used by a single thread, and co-ordinates with other timers by
public final class Timer
{
    private Histogram responseTime = new Histogram(3);
    private Histogram serviceTime = new Histogram(3);
    private Histogram waitTime = new Histogram(3);

    // event timing info
    private long intendedTimeNs;
    private long startTimeNs;
    private long endTimeNs;


    // aggregate info
    private long errorCount;
    private long partitionCount;
    private long rowCount;
    private long max;
    private long maxStart;
    private long upToDateAsOf;
    private long lastSnap = System.nanoTime();

    // communication with summary/logging thread
    private volatile CountDownLatch reportRequest;
    volatile TimingInterval report;
    private volatile TimingInterval finalReport;
    private final boolean isFixed;

    public Timer(boolean isFixed)
    {
        this.isFixed = isFixed;
    }

    public boolean running()
    {
        return finalReport == null;
    }

    public void stop(long partitionCount, long rowCount, boolean error)
    {
        endTimeNs = System.nanoTime();
        maybeReport();
        long now = System.nanoTime();
        if (intendedTimeNs != 0)
        {
            long rTime = endTimeNs - intendedTimeNs;
            responseTime.recordValue(rTime);
            long wTime = startTimeNs - intendedTimeNs;
            waitTime.recordValue(wTime);
        }

        long sTime = endTimeNs - startTimeNs;
        serviceTime.recordValue(sTime);

        if (sTime > max)
        {
            maxStart = startTimeNs;
            max = sTime;
        }
        this.partitionCount += partitionCount;
        this.rowCount += rowCount;
        if (error)
            this.errorCount++;
        upToDateAsOf = now;
        resetTimes();
    }

    private void resetTimes()
    {
        intendedTimeNs = startTimeNs = endTimeNs = 0;
    }

    private TimingInterval buildReport()
    {
        final TimingInterval report = new TimingInterval(lastSnap, upToDateAsOf, maxStart, partitionCount,
                rowCount, errorCount, responseTime, serviceTime, waitTime, isFixed);
        // reset counters
        partitionCount = 0;
        rowCount = 0;
        max = 0;
        errorCount = 0;
        lastSnap = upToDateAsOf;
        responseTime = new Histogram(3);
        serviceTime = new Histogram(3);
        waitTime = new Histogram(3);

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

    public void intendedTimeNs(long v)
    {
        intendedTimeNs = v;
    }

    public void start()
    {
        startTimeNs = System.nanoTime();
    }
}