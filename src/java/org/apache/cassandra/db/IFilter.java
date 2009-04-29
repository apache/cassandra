package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SSTable;


public interface IFilter
{
	public boolean isDone();
	public ColumnFamily filter(String cfName, ColumnFamily cf);
    public IColumn filter(IColumn column, DataInputStream dis) throws IOException;
    public DataInputBuffer next(String key, String cf, SSTable ssTable) throws IOException;
}
