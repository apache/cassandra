package org.apache.cassandra.io;

public interface ICompactionInfo
{
    public long getTotalBytes();

    public long getBytesRead();
}
