package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.SortedSet;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.io.*;

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
            int dataSize = file.readInt();

            /* Read the bloom filter summarizing the columns */
            long preBfPos = file.getFilePointer();
            IndexHelper.defreezeBloomFilter(file);
            long postBfPos = file.getFilePointer();
            dataSize -= (postBfPos - preBfPos);

            List<IndexHelper.ColumnIndexInfo> columnIndexList = new ArrayList<IndexHelper.ColumnIndexInfo>();
            dataSize -= IndexHelper.readColumnIndexes(file, ssTable.getTableName(), cfName, columnIndexList);

            cf = ColumnFamily.serializer().deserializeEmpty(file);
            int totalColumns = file.readInt();
            dataSize -= cf.serializedSize();

            /* get the various column ranges we have to read */
            List<IndexHelper.ColumnRange> columnRanges = IndexHelper.getMultiColumnRangesFromNameIndex(columns, columnIndexList, dataSize, totalColumns);

            int prevPosition = 0;
            /* now read all the columns from the ranges */
            for (IndexHelper.ColumnRange columnRange : columnRanges)
            {
                /* seek to the correct offset to the data */
                long columnBegin = file.getFilePointer();
                Coordinate coordinate = columnRange.coordinate();
                file.skipBytes((int)(coordinate.start_ - prevPosition));
                // read the columns in this range
                // TODO only completely deserialize columns we are interested in
                while (file.getFilePointer() - columnBegin < coordinate.end_ - coordinate.start_)
                {
                    final IColumn column = cf.getColumnSerializer().deserialize(file);
                    if (columns.contains(column.name()))
                    {
                        cf.addColumn(column);
                    }
                }

                prevPosition = (int) coordinate.end_;
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
