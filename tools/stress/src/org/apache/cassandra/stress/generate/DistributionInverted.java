package org.apache.cassandra.stress.generate;

public class DistributionInverted extends Distribution
{

    final Distribution wrapped;
    final long min;
    final long max;

    public DistributionInverted(Distribution wrapped)
    {
        this.wrapped = wrapped;
        this.min = wrapped.minValue();
        this.max = wrapped.maxValue();
    }

    public long next()
    {
        return max - (wrapped.next() - min);
    }

    public double nextDouble()
    {
        return max - (wrapped.nextDouble() - min);
    }

    public long inverseCumProb(double cumProb)
    {
        return max - (wrapped.inverseCumProb(cumProb) - min);
    }

    public void setSeed(long seed)
    {
        wrapped.setSeed(seed);
    }

}
