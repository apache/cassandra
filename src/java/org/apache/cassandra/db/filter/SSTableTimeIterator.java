package org.apache.cassandra.db.filter;

import java.util.Iterator;
import java.io.IOException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.IndexHelper;

public class SSTableTimeIterator extends SimpleAbstractColumnIterator
{
    private ColumnFamily cf;
    private Iterator<IColumn> iter;
    public final long since;

    public SSTableTimeIterator(String filename, String key, String cfName, long since) throws IOException
    {
        this.since = since;
        SSTableReader ssTable = SSTableReader.open(filename);
        DataInputBuffer buffer = ssTable.next(key, cfName, null, new IndexHelper.TimeRange(since, Long.MAX_VALUE));
        if (buffer.getLength() > 0)
        {
            cf = ColumnFamily.serializer().deserialize(buffer);
            iter = cf.getAllColumns().iterator();
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
            if (c.timestamp() < since)
                break;
            return c;
        }
        return endOfData();
    }
}
