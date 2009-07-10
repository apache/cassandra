package org.apache.cassandra.db.filter;

import java.io.IOException;

public abstract class AbstractColumnIterator implements ColumnIterator
{
    public void close() throws IOException
    {}

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
