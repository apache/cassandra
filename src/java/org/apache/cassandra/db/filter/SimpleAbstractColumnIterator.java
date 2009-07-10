package org.apache.cassandra.db.filter;

import java.io.IOException;

import org.apache.cassandra.db.IColumn;
import com.google.common.collect.AbstractIterator;

public abstract class SimpleAbstractColumnIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    public void close() throws IOException {}
}
