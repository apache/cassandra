package org.apache.cassandra.cache;

public interface CacheSize
{

    long capacity();

    void setCapacity(long capacity);

    int size();

    long weightedSize();

}