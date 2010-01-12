package org.apache.cassandra.utils;

public interface InstrumentedCacheMBean
{
    public int getCapacity();
    public int getSize();
    public double getHitRate();
}
