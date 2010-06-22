package org.apache.cassandra.io.sstable;

import java.util.concurrent.atomic.AtomicLong;

public class BloomFilterTracker
{
    private AtomicLong falsePositiveCount = new AtomicLong(0);
    private AtomicLong truePositiveCount = new AtomicLong(0);
    private long lastFalsePositiveCount = 0L;
    private long lastTruePositiveCount = 0L;

    public void addFalsePositive()
    {
        falsePositiveCount.incrementAndGet();
    }

    public void addTruePositive()
    {
        truePositiveCount.incrementAndGet();
    }

    public long getFalsePositiveCount()
    {
        return falsePositiveCount.get();
    }

    public long getRecentFalsePositiveCount()
    {
        long fpc = getFalsePositiveCount();
        try
        {
            return (fpc - lastFalsePositiveCount);
        }
        finally
        {
            lastFalsePositiveCount = fpc;
        }
    }

    public long getTruePositiveCount()
    {
        return truePositiveCount.get();
    }

    public long getRecentTruePositiveCount()
    {
        long tpc = getTruePositiveCount();
        try
        {
            return (tpc - lastTruePositiveCount);
        }
        finally
        {
            lastTruePositiveCount = tpc;
        }
    }
}
