package org.apache.cassandra.io;

public interface ICompactSerializer3<T> extends ICompactSerializer2<T>
{
    public long serializedSize(T t);
}
