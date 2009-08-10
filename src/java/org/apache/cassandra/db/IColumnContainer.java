package org.apache.cassandra.db;

import org.apache.cassandra.db.marshal.AbstractType;

public interface IColumnContainer
{
    public void addColumn(IColumn column);

    public AbstractType getComparator();
}
