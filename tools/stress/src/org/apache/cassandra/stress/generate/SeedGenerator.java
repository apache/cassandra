package org.apache.cassandra.stress.generate;

public interface SeedGenerator
{

    long next(long workIndex);

}
