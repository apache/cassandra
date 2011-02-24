package org.apache.cassandra.utils;
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


import java.util.concurrent.atomic.AtomicLong;

public class LatencyTracker
{
    private final AtomicLong opCount = new AtomicLong(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private long lastLatency = 0;
    private long lastOpCount = 0;
    private EstimatedHistogram totalHistogram = new EstimatedHistogram();
    private EstimatedHistogram recentHistogram = new EstimatedHistogram();

    /** takes nanoseconds **/
    public void addNano(long nanos)
    {
        // convert to microseconds.  1 millionth
        addMicro(nanos / 1000);
    }

    public void addMicro(long micros)
    {
        opCount.incrementAndGet();
        totalLatency.addAndGet(micros);
        totalHistogram.add(micros);
        recentHistogram.add(micros);
    }

    public long getOpCount()
    {
        return opCount.get();
    }

    /** returns  microseconds */
    public long getTotalLatencyMicros()
    {
        return totalLatency.get();
    }

    /** returns microseconds */
    public double getRecentLatencyMicros()
    {
        long ops = opCount.get();
        long n = totalLatency.get();
        try
        {
            return ((double)n - lastLatency) / (ops - lastOpCount);
        }
        finally
        {
            lastLatency = n;
            lastOpCount = ops;
        }
    }

    public long[] getTotalLatencyHistogramMicros()
    {
        return totalHistogram.getBuckets(false);
    }

    public long[] getRecentLatencyHistogramMicros()
    {
        return recentHistogram.getBuckets(true);
    }
}
