package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.*;

public class DistributionFixed extends Distribution
{

    final long key;

    public DistributionFixed(long key)
    {
        this.key = key;
    }

    @Override
    public long next()
    {
        return key;
    }

    @Override
    public long inverseCumProb(double cumProb)
    {
        return key;
    }

}