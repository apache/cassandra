package org.apache.cassandra.db.filter;

import java.util.ArrayList;
import java.io.IOException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.DataOutputBuffer;
import org.apache.cassandra.io.DataInputBuffer;
import org.apache.cassandra.io.SequenceFile;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.config.DatabaseDescriptor;
import com.google.common.collect.AbstractIterator;

/**
 *  A Column Iterator over SSTable
 */
class SSTableSliceIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    protected boolean isAscending;
    private byte[] startColumn;
    private DataOutputBuffer outBuf = new DataOutputBuffer();
    private DataInputBuffer inBuf = new DataInputBuffer();
    private int curColumnIndex;
    private ColumnFamily curCF = null;
    private ArrayList<IColumn> curColumns = new ArrayList<IColumn>();
    private SequenceFile.ColumnGroupReader reader;
    private AbstractType comparator;

    public SSTableSliceIterator(String filename, String key, String cfName, AbstractType comparator, byte[] startColumn, boolean isAscending)
    throws IOException
    {
        this.isAscending = isAscending;
        SSTableReader ssTable = SSTableReader.open(filename);

        /* Morph key into actual key based on the partition type. */
        String decoratedKey = ssTable.getPartitioner().decorateKey(key);
        long position = ssTable.getPosition(decoratedKey);
        AbstractType comparator1 = DatabaseDescriptor.getComparator(ssTable.getTableName(), cfName);
        reader = new SequenceFile.ColumnGroupReader(ssTable.getFilename(), decoratedKey, cfName, comparator1, startColumn, isAscending, position);
        this.comparator = comparator;
        this.startColumn = startColumn;
        curColumnIndex = isAscending ? 0 : -1;
    }

    private boolean isColumnNeeded(IColumn column)
    {
        if (isAscending)
        {
            return comparator.compare(column.name(), startColumn) >= 0;
        }
        else
        {
            if (startColumn.length == 0)
            {
                /* assuming scanning from the largest column in descending order */
                return true;
            }
            else
            {
                return comparator.compare(column.name(), startColumn) <= 0;
            }
        }
    }

    private void getColumnsFromBuffer() throws IOException
    {
        inBuf.reset(outBuf.getData(), outBuf.getLength());
        ColumnFamily columnFamily = ColumnFamily.serializer().deserialize(inBuf);

        if (curCF == null)
            curCF = columnFamily.cloneMeShallow();
        curColumns.clear();
        for (IColumn column : columnFamily.getSortedColumns())
            if (isColumnNeeded(column))
                curColumns.add(column);

        if (isAscending)
            curColumnIndex = 0;
        else
            curColumnIndex = curColumns.size() - 1;
    }

    public ColumnFamily getColumnFamily()
    {
        return curCF;
    }

    protected IColumn computeNext()
    {
        while (true)
        {
            if (isAscending)
            {
                if (curColumnIndex < curColumns.size())
                {
                    return curColumns.get(curColumnIndex++);
                }
            }
            else
            {
                if (curColumnIndex >= 0)
                {
                    return curColumns.get(curColumnIndex--);
                }
            }

            try
            {
                if (!reader.getNextBlock(outBuf))
                    return endOfData();
                getColumnsFromBuffer();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() throws IOException
    {
        reader.close();
    }
}
