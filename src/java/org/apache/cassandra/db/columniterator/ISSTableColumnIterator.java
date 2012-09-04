package org.apache.cassandra.db.columniterator;

import org.apache.cassandra.io.sstable.SSTableReader;

public interface ISSTableColumnIterator extends IColumnIterator
{
    public SSTableReader getSStable();
}
