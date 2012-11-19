package org.apache.cassandra.db.columniterator;

public interface IColumnIteratorFactory
{
    IColumnIterator create();
}
