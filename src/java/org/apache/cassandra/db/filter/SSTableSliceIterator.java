package org.apache.cassandra.db.filter;

import java.util.*;
import java.io.IOException;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.*;
import com.google.common.collect.AbstractIterator;

/**
 *  A Column Iterator over SSTable
 */
class SSTableSliceIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    private final boolean isAscending;
    private final byte[] startColumn;
    private final AbstractType comparator;
    private ColumnGroupReader reader;

    public SSTableSliceIterator(String filename, String key, AbstractType comparator, byte[] startColumn, boolean isAscending)
    throws IOException
    {
        // TODO push finishColumn down here too, so we can tell when we're done and optimize away the slice when the index + start/stop shows there's nothing to scan for
        this.isAscending = isAscending;
        SSTableReader ssTable = SSTableReader.open(filename);

        /* Morph key into actual key based on the partition type. */
        String decoratedKey = ssTable.getPartitioner().decorateKey(key);
        long position = ssTable.getPosition(decoratedKey);
        this.comparator = comparator;
        this.startColumn = startColumn;
        if (position >= 0)
            reader = new ColumnGroupReader(filename, decoratedKey, position);
    }

    private boolean isColumnNeeded(IColumn column)
    {
        return isAscending
               ? comparator.compare(column.name(), startColumn) >= 0
               : startColumn.length == 0 || comparator.compare(column.name(), startColumn) <= 0;
    }

    public ColumnFamily getColumnFamily()
    {
        return reader.getEmptyColumnFamily();
    }

    protected IColumn computeNext()
    {
        if (reader == null)
            return endOfData();

        while (true)
        {
            IColumn column = reader.pollColumn();
            if (column == null)
                return endOfData();
            if (isColumnNeeded(column))
                return column;
        }
    }

    public void close() throws IOException
    {
        if (reader != null)
            reader.close();
    }

    /**
     *  This is a reader that finds the block for a starting column and returns
     *  blocks before/after it for each next call. This function assumes that
     *  the CF is sorted by name and exploits the name index.
     */
    class ColumnGroupReader
    {
        private final ColumnFamily emptyColumnFamily;

        private final List<IndexHelper.IndexInfo> indexes;
        private final long columnStartPosition;
        private final BufferedRandomAccessFile file;

        private int curRangeIndex;
        private Deque<IColumn> blockColumns = new ArrayDeque<IColumn>();

        public ColumnGroupReader(String filename, String key, long position) throws IOException
        {
            this.file = new BufferedRandomAccessFile(filename, "r");

            file.seek(position);
            String keyInDisk = file.readUTF();
            assert keyInDisk.equals(key);

            file.readInt(); // row size
            IndexHelper.skipBloomFilter(file);
            indexes = IndexHelper.deserializeIndex(file);

            emptyColumnFamily = ColumnFamily.serializer().deserializeEmpty(file);
            file.readInt(); // column count

            columnStartPosition = file.getFilePointer();
            curRangeIndex = IndexHelper.indexFor(startColumn, indexes, comparator, isAscending);
        }

        public ColumnFamily getEmptyColumnFamily()
        {
            return emptyColumnFamily;
        }

        public IColumn pollColumn()
        {
            IColumn column = blockColumns.poll();
            if (column == null)
            {
                try
                {
                    if (getNextBlock())
                        column = blockColumns.poll();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return column;
        }

        public boolean getNextBlock() throws IOException
        {
            if (curRangeIndex < 0 || curRangeIndex >= indexes.size())
                return false;

            /* seek to the correct offset to the data, and calculate the data size */
            IndexHelper.IndexInfo curColPostion = indexes.get(curRangeIndex);
            file.seek(columnStartPosition + curColPostion.offset);
            while (file.getFilePointer() < columnStartPosition + curColPostion.offset + curColPostion.width)
            {
                IColumn column = emptyColumnFamily.getColumnSerializer().deserialize(file);
                if (isAscending)
                    blockColumns.addLast(column);
                else
                    blockColumns.addFirst(column);
            }

            if (isAscending)
                curRangeIndex++;
            else
                curRangeIndex--;
            return true;
        }

        public void close() throws IOException
        {
            file.close();
        }
    }
}
