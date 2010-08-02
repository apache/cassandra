package org.apache.cassandra.db.columniterator;

import java.io.IOError;
import java.io.IOException;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;

class SimpleSliceReader extends AbstractIterator<IColumn> implements IColumnIterator
{
    private final FileDataInput file;
    private final byte[] finishColumn;
    private final AbstractType comparator;
    private final ColumnFamily emptyColumnFamily;
    private final int columns;
    private int i;
    private FileMark mark;

    public SimpleSliceReader(SSTableReader sstable, FileDataInput input, byte[] finishColumn)
    {
        this.file = input;
        this.finishColumn = finishColumn;
        comparator = sstable.getColumnComparator();
        try
        {
            IndexHelper.skipBloomFilter(file);
            IndexHelper.skipIndex(file);

            emptyColumnFamily = ColumnFamily.serializer().deserializeFromSSTableNoColumns(sstable.makeColumnFamily(), file);
            columns = file.readInt();
            mark = file.mark();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    protected IColumn computeNext()
    {
        if (i++ >= columns)
            return endOfData();

        IColumn column;
        try
        {
            file.reset(mark);
            column = emptyColumnFamily.getColumnSerializer().deserialize(file);
        }
        catch (IOException e)
        {
            throw new RuntimeException("error reading " + i + " of " + columns, e);
        }
        if (finishColumn.length > 0 && comparator.compare(column.name(), finishColumn) > 0)
            return endOfData();

        mark = file.mark();
        return column;
    }

    public ColumnFamily getColumnFamily() throws IOException
    {
        return emptyColumnFamily;
    }

    public void close() throws IOException
    {
    }

    public DecoratedKey getKey()
    {
        throw new UnsupportedOperationException();
    }
}
