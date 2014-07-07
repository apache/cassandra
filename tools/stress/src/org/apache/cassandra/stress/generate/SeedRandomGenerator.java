package org.apache.cassandra.stress.generate;

public class SeedRandomGenerator implements SeedGenerator
{

    final Distribution distribution;
    final Distribution clustering;

    private long next;
    private int count;

    public SeedRandomGenerator(Distribution distribution, Distribution clustering)
    {
        this.distribution = distribution;
        this.clustering = clustering;
    }

    public long next(long workIndex)
    {
        if (count == 0)
        {
            next = distribution.next();
            count = (int) clustering.next();
        }
        long result = next;
        count--;
        if (next == distribution.maxValue())
            next = distribution.minValue();
        else
            next++;
        return result;
    }
}
