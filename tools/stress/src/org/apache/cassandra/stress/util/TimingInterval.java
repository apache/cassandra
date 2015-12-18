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
import java.util.concurrent.ThreadLocalRandom;

// represents measurements taken over an interval of time
// used for both single timer results and merged timer results
public final class TimingInterval
{
    public static final long[] EMPTY_SAMPLE = new long[0];
    // nanos
    private final long start;
    private final long end;
    public final long maxLatency;
    public final long pauseLength;
    public final long pauseStart;
    public final long totalLatency;

    // discrete
    public final long partitionCount;
    public final long rowCount;
    public final long operationCount;
    public final long errorCount;

    final SampleOfLongs sample;

    public String toString()
    {
        return String.format("Start: %d end: %d maxLatency: %d pauseLength: %d pauseStart: %d totalLatency: %d" +
                             " pCount: %d rcount: %d opCount: %d errors: %d", start, end, maxLatency, pauseLength,
                             pauseStart, totalLatency, partitionCount, rowCount, operationCount, errorCount);
    }

    TimingInterval(long time)
    {
        start = end = time;
        maxLatency = totalLatency = 0;
        partitionCount = rowCount = operationCount = errorCount = 0;
        pauseStart = pauseLength = 0;
        sample = new SampleOfLongs(EMPTY_SAMPLE, 1d);
    }

    TimingInterval(long start, long end, long maxLatency, long pauseStart, long pauseLength, long partitionCount,
                   long rowCount, long totalLatency, long operationCount, long errorCount, SampleOfLongs sample)
    {
        this.start = start;
        this.end = Math.max(end, start);
        this.maxLatency = maxLatency;
        this.partitionCount = partitionCount;
        this.rowCount = rowCount;
        this.totalLatency = totalLatency;
        this.errorCount = errorCount;
        this.operationCount = operationCount;
        this.pauseStart = pauseStart;
        this.pauseLength = pauseLength;
        this.sample = sample;
    }

    // merge multiple timer intervals together
    static TimingInterval merge(Iterable<TimingInterval> intervals, int maxSamples, long start)
    {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long operationCount = 0, partitionCount = 0, rowCount = 0, errorCount = 0;
        long maxLatency = 0, totalLatency = 0;
        List<SampleOfLongs> latencies = new ArrayList<>();
        long end = 0;
        long pauseStart = 0, pauseEnd = Long.MAX_VALUE;
        for (TimingInterval interval : intervals)
        {
            if (interval != null)
            {
                end = Math.max(end, interval.end);
                operationCount += interval.operationCount;
                maxLatency = Math.max(interval.maxLatency, maxLatency);
                totalLatency += interval.totalLatency;
                partitionCount += interval.partitionCount;
                rowCount += interval.rowCount;
                errorCount += interval.errorCount;
                latencies.addAll(Arrays.asList(interval.sample));
                if (interval.pauseLength > 0)
                {
                    pauseStart = Math.max(pauseStart, interval.pauseStart);
                    pauseEnd = Math.min(pauseEnd, interval.pauseStart + interval.pauseLength);
                }
            }
        }

        if (pauseEnd < pauseStart || pauseStart <= 0)
        {
            pauseEnd = pauseStart = 0;
        }

        return new TimingInterval(start, end, maxLatency, pauseStart, pauseEnd - pauseStart, partitionCount, rowCount,
                                  totalLatency, operationCount, errorCount, SampleOfLongs.merge(rnd, latencies, maxSamples));

    }

    public double opRate()
    {
        return operationCount / ((end - start) * 0.000000001d);
    }

    public double adjustedRowRate()
    {
        return rowCount / ((end - (start + pauseLength)) * 0.000000001d);
    }

    public double partitionRate()
    {
        return partitionCount / ((end - start) * 0.000000001d);
    }

    public double rowRate()
    {
        return rowCount / ((end - start) * 0.000000001d);
    }

    public double meanLatency()
    {
        return (totalLatency / (double) operationCount) * 0.000001d;
    }

    public double maxLatency()
    {
        return maxLatency * 0.000001d;
    }

    public double medianLatency()
    {
        return sample.medianLatency();
    }

    // 0 < rank < 1
    public double rankLatency(float rank)
    {
        return sample.rankLatency(rank);
    }

    public long runTime()
    {
        return (end - start) / 1000000;
    }

    public final long endNanos()
    {
        return end;
    }

    public long startNanos()
    {
        return start;
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

    String getStringValue(TimingParameter value, float rank)
    {
        switch (value)
        {
            case OPRATE:         return String.format("%.0f", opRate());
            case ROWRATE:        return String.format("%.0f", rowRate());
            case ADJROWRATE:     return String.format("%.0f", adjustedRowRate());
            case PARTITIONRATE:  return String.format("%.0f", partitionRate());
            case MEANLATENCY:    return String.format("%.1f", meanLatency());
            case MAXLATENCY:     return String.format("%.1f", maxLatency());
            case MEDIANLATENCY:  return String.format("%.1f", medianLatency());
            case RANKLATENCY:    return String.format("%.1f", rankLatency(rank));
            case ERRORCOUNT:     return String.format("%d", errorCount);
            case PARTITIONCOUNT: return String.format("%d", partitionCount);
            default:             throw new IllegalStateException();
        }
    }
 }

