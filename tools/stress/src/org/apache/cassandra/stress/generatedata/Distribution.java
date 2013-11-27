package org.apache.cassandra.stress.generatedata;

public abstract class Distribution
{

    public abstract long next();
    public abstract long inverseCumProb(double cumProb);

    public long maxValue()
    {
        return inverseCumProb(1d);
    }

    public long minValue()
    {
        return inverseCumProb(0d);
    }

}