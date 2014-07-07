package org.apache.cassandra.stress.operations;

import org.apache.cassandra.stress.Operation;

public class FixedOpDistribution implements OpDistribution
{

    final Operation operation;

    public FixedOpDistribution(Operation operation)
    {
        this.operation = operation;
    }

    public Operation next()
    {
        return operation;
    }

    public int maxBatchSize()
    {
        return (int) operation.partitionCount.maxValue();
    }

}
