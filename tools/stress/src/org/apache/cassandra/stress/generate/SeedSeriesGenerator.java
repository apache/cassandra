package org.apache.cassandra.stress.generate;

public class SeedSeriesGenerator implements SeedGenerator
{

    final long min;
    final long count;

    public SeedSeriesGenerator(long min, long max)
    {
        if (min > max)
            throw new IllegalStateException();
        this.min = min;
        this.count = 1 + max - min;
    }

    public long next(long workIndex)
    {
        return min + (workIndex % count);
    }
}
