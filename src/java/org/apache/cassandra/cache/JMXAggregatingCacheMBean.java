package org.apache.cassandra.cache;

public interface JMXAggregatingCacheMBean
{
    public int getCapacity();
    public int getSize();
    public double getHitRate();
}