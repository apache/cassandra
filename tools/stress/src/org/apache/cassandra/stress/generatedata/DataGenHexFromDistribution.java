package org.apache.cassandra.stress.generatedata;

import org.apache.commons.math3.distribution.*;

public class DataGenHexFromDistribution extends DataGenHex
{

    final Distribution distribution;

    public DataGenHexFromDistribution(Distribution distribution)
    {
        this.distribution = distribution;
    }

    @Override
    public boolean isDeterministic()
    {
        return false;
    }

    @Override
    long next(long operationIndex)
    {
        return distribution.next();
    }

    public static DataGenHex buildGaussian(long minKey, long maxKey, double stdevsToLimit)
    {
        double midRange = (maxKey + minKey) / 2d;
        double halfRange = (maxKey - minKey) / 2d;
        return new DataGenHexFromDistribution(new DistributionBoundApache(new NormalDistribution(midRange, halfRange / stdevsToLimit), minKey, maxKey));
    }

    public static DataGenHex buildGaussian(long minKey, long maxKey, double mean, double stdev)
    {
        return new DataGenHexFromDistribution(new DistributionBoundApache(new NormalDistribution(mean, stdev), minKey, maxKey));
    }

    public static DataGenHex buildUniform(long minKey, long maxKey)
    {
        return new DataGenHexFromDistribution(new DistributionBoundApache(new UniformRealDistribution(minKey, maxKey), minKey, maxKey));
    }

}
