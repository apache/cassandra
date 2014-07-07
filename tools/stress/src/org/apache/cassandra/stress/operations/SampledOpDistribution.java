package org.apache.cassandra.stress.operations;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.generate.Distribution;

public class SampledOpDistribution implements OpDistribution
{

    final EnumeratedDistribution<Operation> operations;
    final Distribution clustering;
    private Operation cur;
    private long remaining;

    public SampledOpDistribution(EnumeratedDistribution<Operation> operations, Distribution clustering)
    {
        this.operations = operations;
        this.clustering = clustering;
    }

    public int maxBatchSize()
    {
        int max = 1;
        for (Pair<Operation, Double> pair : operations.getPmf())
            max = Math.max(max, (int) pair.getFirst().partitionCount.maxValue());
        return max;
    }

    public Operation next()
    {
        while (remaining == 0)
        {
            remaining = clustering.next();
            cur = operations.sample();
        }
        remaining--;
        return cur;
    }
}
