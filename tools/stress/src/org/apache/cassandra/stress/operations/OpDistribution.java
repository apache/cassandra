package org.apache.cassandra.stress.operations;

import org.apache.cassandra.stress.Operation;

public interface OpDistribution
{

    Operation next();
    public int maxBatchSize();

}
