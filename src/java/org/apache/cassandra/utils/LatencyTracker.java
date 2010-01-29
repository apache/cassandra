package org.apache.cassandra.utils;

import java.util.concurrent.atomic.AtomicLong;

public class LatencyTracker
{
    private final AtomicLong opCount = new AtomicLong(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private long lastLatency = 0;
    private long lastOpCount = 0;

    public void add(long n)
    {
        opCount.incrementAndGet();
        totalLatency.addAndGet(n);
    }

    public long getOpCount()
    {
        return opCount.get();
    }

    public long getTotalLatency()
    {
        return totalLatency.get();
    }

    public double getRecentLatency()
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
}
