package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.*;
import org.apache.cassandra.config.DatabaseDescriptor;

public class SSTableNamesIterator extends SimpleAbstractColumnIterator
{
    private ColumnFamily cf;
    private Iterator<IColumn> iter;
    public final SortedSet<byte[]> columns;

    public SSTableNamesIterator(String filename, String key, String cfName, SortedSet<byte[]> columns) throws IOException
    {
        assert columns != null;
        this.columns = columns;
        SSTableReader ssTable = SSTableReader.open(filename);

        String decoratedKey = ssTable.getPartitioner().decorateKey(key);
        long position = ssTable.getPosition(decoratedKey);
        if (position < 0)
            return;

        BufferedRandomAccessFile file = new BufferedRandomAccessFile(filename, "r");
        try
        {
            file.seek(position);

            /* note the position where the key starts */
            String keyInDisk = file.readUTF();
            assert keyInDisk.equals(decoratedKey) : keyInDisk;
            file.readInt(); // data size

            /* Read the bloom filter summarizing the columns */
            IndexHelper.defreezeBloomFilter(file);

            List<IndexHelper.IndexInfo> indexList = IndexHelper.deserializeIndex(file);

            cf = ColumnFamily.serializer().deserializeEmpty(file);
            file.readInt(); // column count

            /* get the various column ranges we have to read */
            AbstractType comparator = DatabaseDescriptor.getComparator(SSTable.parseTableName(filename), cfName);
            SortedSet<IndexHelper.IndexInfo> ranges = new TreeSet<IndexHelper.IndexInfo>(IndexHelper.getComparator(comparator));
            for (byte[] name : columns)
            {
                ranges.add(indexList.get(IndexHelper.indexFor(name, indexList, comparator, false)));
            }

            /* seek to the correct offset to the data */
            long columnBegin = file.getFilePointer();
            /* now read all the columns from the ranges */
            for (IndexHelper.IndexInfo indexInfo : ranges)
            {
                file.seek(columnBegin + indexInfo.offset);
                // TODO only completely deserialize columns we are interested in
                while (file.getFilePointer() < columnBegin + indexInfo.offset + indexInfo.width)
                {
                    final IColumn column = cf.getColumnSerializer().deserialize(file);
                    if (columns.contains(column.name()))
                    {
                        cf.addColumn(column);
                    }
                }
            }
        }
        finally
        {
            file.close();
        }

        iter = cf.getSortedColumns().iterator();
    }

    public ColumnFamily getColumnFamily()
    {
        return cf;
    }

    protected IColumn computeNext()
    {
        if (iter == null || !iter.hasNext())
            return endOfData();
        return iter.next();
    }
}
