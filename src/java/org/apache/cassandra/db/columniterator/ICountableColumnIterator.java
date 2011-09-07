package org.apache.cassandra.db.columniterator;

public interface ICountableColumnIterator extends IColumnIterator
{
    public int getColumnCount();

    public void reset();
}
