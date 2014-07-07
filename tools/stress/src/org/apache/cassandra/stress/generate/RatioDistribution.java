package org.apache.cassandra.stress.generate;

public class RatioDistribution
{

    final Distribution distribution;
    final double divisor;

    public RatioDistribution(Distribution distribution, double divisor)
    {
        this.distribution = distribution;
        this.divisor = divisor;
    }

    // yields a value between 0 and 1
    public double next()
    {
        return Math.max(0f, Math.min(1f, distribution.nextDouble() / divisor));
    }

    public double max()
    {
        return Math.min(1d, distribution.maxValue() / divisor);
    }
}
