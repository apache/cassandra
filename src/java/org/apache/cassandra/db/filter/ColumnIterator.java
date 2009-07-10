package org.apache.cassandra.db.filter;

import java.util.Iterator;
import java.io.IOException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ColumnFamily;

public interface ColumnIterator extends Iterator<IColumn>
{
    /**
     *  returns the CF of the column being iterated.
     *  The CF is only guaranteed to be available after a call to next() or hasNext().
     */
    public abstract ColumnFamily getColumnFamily();

    /** clean up any open resources */
    public void close() throws IOException;
}

