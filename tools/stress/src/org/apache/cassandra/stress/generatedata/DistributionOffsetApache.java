package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.AbstractRealDistribution;

public class DistributionOffsetApache extends Distribution
{

    final AbstractRealDistribution delegate;
    final long min, delta;

    public DistributionOffsetApache(AbstractRealDistribution delegate, long min, long max)
    {
        this.delegate = delegate;
        this.min = min;
        this.delta = max - min;
    }

    @Override
    public long next()
    {
        return offset(min, delta, delegate.sample());
    }

    @Override
    public long inverseCumProb(double cumProb)
    {
        return offset(min, delta, delegate.inverseCumulativeProbability(cumProb));
    }

    private long offset(long min, long delta, double val)
    {
        long r = (long) val;
        if (r < 0)
            r = 0;
        if (r > delta)
            r = delta;
        return min + r;
    }

}