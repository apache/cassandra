package org.apache.cassandra.db;

import java.io.IOException;

import org.apache.cassandra.io.SSTableReader;

public interface IFlushable
{
    public ColumnFamilyStore.SortedFlushable getSortedContents();
    public SSTableReader writeSortedContents(ColumnFamilyStore.SortedFlushable sortedFlushable) throws IOException;
}
