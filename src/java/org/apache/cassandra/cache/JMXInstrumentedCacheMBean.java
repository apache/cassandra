package org.apache.cassandra.cache;

public interface JMXInstrumentedCacheMBean
{
    public int getCapacity();
    public int getSize();
    public double getHitRate();
}
