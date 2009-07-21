package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.SortedSet;
import java.util.Iterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.DataInputBuffer;

public class SSTableNamesIterator extends SimpleAbstractColumnIterator
{
    private ColumnFamily cf;
    private Iterator<IColumn> iter;
    public final SortedSet<byte[]> columns;

    // TODO make this actually iterate so we don't have to read + deserialize + filter data that we don't need due to merging other sstables
    public SSTableNamesIterator(String filename, String key, String cfName, SortedSet<byte[]> columns) throws IOException
    {
        this.columns = columns;
        SSTableReader ssTable = SSTableReader.open(filename);
        DataInputBuffer buffer = ssTable.next(key, cfName, columns);
        if (buffer.getLength() > 0)
        {
            cf = ColumnFamily.serializer().deserialize(buffer);
            iter = cf.getSortedColumns().iterator();
        }
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    protected IColumn computeNext()
    {
        if (iter == null)
            return endOfData();
        while (iter.hasNext())
        {
            IColumn c = iter.next();
            if (columns.contains(c.name()))
                return c;
        }
        return endOfData();
    }
}
