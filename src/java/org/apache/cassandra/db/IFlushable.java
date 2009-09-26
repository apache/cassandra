package org.apache.cassandra.db;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.io.SSTableReader;

public interface IFlushable<T>
{
    public List<T> getSortedKeys();
    public SSTableReader writeSortedContents(List<T> sortedKeys) throws IOException;
}
