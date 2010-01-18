package org.apache.cassandra.cache;

public interface InstrumentedCacheMBean
{
    public int getCapacity();
    public int getSize();
    public double getHitRate();
}
