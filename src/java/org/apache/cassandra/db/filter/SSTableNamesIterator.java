package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.SortedSet;
import java.util.Iterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.*;

public class SSTableNamesIterator extends SimpleAbstractColumnIterator
{
    private ColumnFamily cf;
    private Iterator<IColumn> iter;
    public final SortedSet<byte[]> columns;

    // TODO make this actually iterate so we don't have to read + deserialize + filter data that we don't need, only to skip it later in computeNext
    public SSTableNamesIterator(String filename, String key, String cfName, SortedSet<byte[]> columns) throws IOException
    {
        this.columns = columns;
        SSTableReader ssTable = SSTableReader.open(filename);

        IFileReader dataReader = null;
        DataOutputBuffer bufOut = new DataOutputBuffer();
        DataInputBuffer bufIn = new DataInputBuffer();

        try
        {
            dataReader = SequenceFile.bufferedReader(ssTable.getFilename(), 64 * 1024);
            String decoratedKey = ssTable.getPartitioner().decorateKey(key);
            long position = ssTable.getPosition(decoratedKey);
            if (position >= 0)
            {
                long bytesRead = dataReader.next(decoratedKey, bufOut, cfName, columns, position);
                assert bytesRead > 0;
                assert bufOut.getLength() > 0;
                bufIn.reset(bufOut.getData(), bufOut.getLength());
                /* read the key even though we do not use it */
                bufIn.readUTF();
                bufIn.readInt();
            }
        }
        finally
        {
            if (dataReader != null)
            {
                dataReader.close();
            }
        }

        if (bufIn.getLength() > 0)
        {
            cf = ColumnFamily.serializer().deserialize(bufIn);
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
