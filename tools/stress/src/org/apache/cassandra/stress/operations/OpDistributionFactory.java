package org.apache.cassandra.stress.operations;

import org.apache.cassandra.stress.util.Timer;

public interface OpDistributionFactory
{

    public OpDistribution get(Timer timer);
    public String desc();
    Iterable<OpDistributionFactory> each();

}
