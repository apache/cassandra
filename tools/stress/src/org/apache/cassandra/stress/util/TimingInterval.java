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

import org.HdrHistogram.Histogram;

// represents measurements taken over an interval of time
// used for both single timer results and merged timer results
public final class TimingInterval
{
    private final Histogram responseTime;
    private final Histogram serviceTime;
    private final Histogram waitTime;

    public static final long[] EMPTY_SAMPLE = new long[0];
    // nanos
    private final long startNs;
    private final long endNs;
    public final long pauseStart;

    // discrete
    public final long partitionCount;
    public final long rowCount;
    public final long errorCount;
    public final boolean isFixed;


    public String toString()
    {
        return String.format("Start: %d end: %d maxLatency: %d pauseStart: %d" +
                             " pCount: %d rcount: %d opCount: %d errors: %d",
                             startNs, endNs, getLatencyHistogram().getMaxValue(), pauseStart,
                             partitionCount, rowCount, getLatencyHistogram().getTotalCount(), errorCount);
    }

    TimingInterval(long time)
    {
        startNs = endNs = time;
        partitionCount = rowCount = errorCount = 0;
        pauseStart = 0;
        responseTime = new Histogram(3);
        serviceTime = new Histogram(3);
        waitTime = new Histogram(3);
        isFixed = false;
    }

    TimingInterval(long start, long end, long maxPauseStart, long partitionCount,
                   long rowCount, long errorCount, Histogram r, Histogram s, Histogram w, boolean isFixed)
    {
        this.startNs = start;
        this.endNs = Math.max(end, start);
        this.partitionCount = partitionCount;
        this.rowCount = rowCount;
        this.errorCount = errorCount;
        this.pauseStart = maxPauseStart;
        this.responseTime = r;
        this.serviceTime = s;
        this.waitTime = w;
        this.isFixed = isFixed;

    }

    // merge multiple timer intervals together
    static TimingInterval merge(Iterable<TimingInterval> intervals, long start)
    {
        long partitionCount = 0, rowCount = 0, errorCount = 0;
        long end = 0;
        long pauseStart = 0;
        Histogram responseTime = new Histogram(3);
        Histogram serviceTime = new Histogram(3);
        Histogram waitTime = new Histogram(3);
        boolean isFixed = false;
        for (TimingInterval interval : intervals)
        {
            if (interval != null)
            {
                end = Math.max(end, interval.endNs);
                partitionCount += interval.partitionCount;
                rowCount += interval.rowCount;
                errorCount += interval.errorCount;

                if (interval.getLatencyHistogram().getMaxValue() > serviceTime.getMaxValue())
                {
                    pauseStart = interval.pauseStart;
                }
                responseTime.add(interval.responseTime);
                serviceTime.add(interval.serviceTime);
                waitTime.add(interval.waitTime);
                isFixed |= interval.isFixed;
            }
        }


        return new TimingInterval(start, end, pauseStart, partitionCount, rowCount,
                                  errorCount, responseTime, serviceTime, waitTime, isFixed);

    }

    public double opRate()
    {
        return getLatencyHistogram().getTotalCount() / ((endNs - startNs) * 0.000000001d);
    }

    public double adjustedRowRate()
    {
        return rowCount / ((endNs - (startNs + getLatencyHistogram().getMaxValue())) * 0.000000001d);
    }

    public double partitionRate()
    {
        return partitionCount / ((endNs - startNs) * 0.000000001d);
    }

    public double rowRate()
    {
        return rowCount / ((endNs - startNs) * 0.000000001d);
    }

    public double meanLatencyMs()
    {
        return getLatencyHistogram().getMean() * 0.000001d;
    }

    public double maxLatencyMs()
    {
        return getLatencyHistogram().getMaxValue() * 0.000001d;
    }

    public double medianLatencyMs()
    {
        return getLatencyHistogram().getValueAtPercentile(50.0) * 0.000001d;
    }


    /**
     * @param percentile between 0.0 and 100.0
     * @return latency in milliseconds at percentile
     */
    public double latencyAtPercentileMs(double percentile)
    {
        return getLatencyHistogram().getValueAtPercentile(percentile) * 0.000001d;
    }

    public long runTimeMs()
    {
        return (endNs - startNs) / 1000000;
    }

    public long endNanos()
    {
        return endNs;
    }

    public long startNanos()
    {
        return startNs;
    }

    public Histogram responseTime()
    {
        return responseTime;
    }

    public Histogram serviceTime()
    {
        return serviceTime;
    }

    public Histogram waitTime()
    {
        return waitTime;
    }

    private Histogram getLatencyHistogram()
    {
        if (!isFixed || responseTime.getTotalCount() == 0)
            return serviceTime;
        else
            return responseTime;
    }

    public static enum TimingParameter
    {
        OPRATE, ROWRATE, ADJROWRATE, PARTITIONRATE, MEANLATENCY, MAXLATENCY, MEDIANLATENCY, RANKLATENCY,
        ERRORCOUNT, PARTITIONCOUNT
    }

    String getStringValue(TimingParameter value)
    {
        return getStringValue(value, Float.NaN);
    }

    String getStringValue(TimingParameter value, double rank)
    {
        switch (value)
        {
            case OPRATE:         return String.format("%,.0f", opRate());
            case ROWRATE:        return String.format("%,.0f", rowRate());
            case ADJROWRATE:     return String.format("%,.0f", adjustedRowRate());
            case PARTITIONRATE:  return String.format("%,.0f", partitionRate());
            case MEANLATENCY:    return String.format("%,.1f", meanLatencyMs());
            case MAXLATENCY:     return String.format("%,.1f", maxLatencyMs());
            case MEDIANLATENCY:  return String.format("%,.1f", medianLatencyMs());
            case RANKLATENCY:    return String.format("%,.1f", latencyAtPercentileMs(rank));
            case ERRORCOUNT:     return String.format("%,d", errorCount);
            case PARTITIONCOUNT: return String.format("%,d", partitionCount);
            default:             throw new IllegalStateException();
        }
    }

    public long operationCount()
    {
        return getLatencyHistogram().getTotalCount();
    }
 }

