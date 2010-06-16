package org.apache.cassandra.io.util;

import java.util.Comparator;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;

public interface IIterableColumns extends Iterable<IColumn>
{
    public int getEstimatedColumnCount();

    AbstractType getComparator();
}
