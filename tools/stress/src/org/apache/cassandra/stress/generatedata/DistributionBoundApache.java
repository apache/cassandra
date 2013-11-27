package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.*;

public class DistributionBoundApache extends Distribution
{

    final AbstractRealDistribution delegate;
    final long min, max;

    public DistributionBoundApache(AbstractRealDistribution delegate, long min, long max)
    {
        this.delegate = delegate;
        this.min = min;
        this.max = max;
    }

    @Override
    public long next()
    {
        return bound(min, max, delegate.sample());
    }

    @Override
    public long inverseCumProb(double cumProb)
    {
        return bound(min, max, delegate.inverseCumulativeProbability(cumProb));
    }

    private static long bound(long min, long max, double val)
    {
        long r = (long) val;
        if ((r >= min) & (r <= max))
            return r;
        if (r < min)
            return min;
        if (r > max)
            return max;
        throw new IllegalStateException();
    }

}