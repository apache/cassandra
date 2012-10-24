package org.apache.cassandra.db.columniterator;

public interface IColumnIteratorFactory
{
    OnDiskAtomIterator create();
}
