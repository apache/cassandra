package org.apache.cassandra.utils;

public interface IMergeIterator<In, Out> extends CloseableIterator<Out>
{
    Iterable<? extends CloseableIterator<In>> iterators();
}
