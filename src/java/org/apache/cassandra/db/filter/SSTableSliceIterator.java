package org.apache.cassandra.db.filter;

import java.util.*;
import java.io.IOException;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import com.google.common.collect.AbstractIterator;

/**
 *  A Column Iterator over SSTable
 */
class SSTableSliceIterator extends AbstractIterator<IColumn> implements ColumnIterator
{
    protected boolean isAscending;
    private byte[] startColumn;
    private int curColumnIndex;
    private ColumnFamily curCF = null;
    private ArrayList<IColumn> curColumns = new ArrayList<IColumn>();
    private ColumnGroupReader reader;
    private AbstractType comparator;

    public SSTableSliceIterator(String filename, String key, String cfName, AbstractType comparator, byte[] startColumn, boolean isAscending)
    throws IOException
    {
        this.isAscending = isAscending;
        SSTableReader ssTable = SSTableReader.open(filename);

        /* Morph key into actual key based on the partition type. */
        String decoratedKey = ssTable.getPartitioner().decorateKey(key);
        AbstractType comparator1 = DatabaseDescriptor.getComparator(ssTable.getTableName(), cfName);
        long position = ssTable.getPosition(decoratedKey);
        if (position >= 0)
            reader = new ColumnGroupReader(ssTable.getFilename(), decoratedKey, cfName, comparator1, startColumn, isAscending, position);
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
        if (curCF == null)
            curCF = reader.getEmptyColumnFamily().cloneMeShallow();
        curColumns.clear();
        while (true)
        {
            IColumn column = reader.pollColumn();
            if (column == null)
                break;
            if (isColumnNeeded(column))
                curColumns.add(column);
        }

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
        if (reader == null)
            return endOfData();

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
                if (!reader.getNextBlock())
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
        if (reader != null)
            reader.close();
    }

    /**
     *  This is a reader that finds the block for a starting column and returns
     *  blocks before/after it for each next call. This function assumes that
     *  the CF is sorted by name and exploits the name index.
     */
    public static class ColumnGroupReader
    {
        private String key_;
        private String cfName_;
        private AbstractType comparator_;
        private boolean isAscending_;
        private ColumnFamily emptyColumnFamily;

        private List<IndexHelper.IndexInfo> indexList_;
        private long columnStartPosition_;
        private int curRangeIndex_;
        private BufferedRandomAccessFile file_;
        private Queue<IColumn> blockColumns = new ArrayDeque<IColumn>();

        public ColumnGroupReader(String filename, String key, String cfName, AbstractType comparator, byte[] startColumn, boolean isAscending, long position) throws IOException
        {
            this.file_ = new BufferedRandomAccessFile(filename, "r");
            this.cfName_ = cfName;
            this.comparator_ = comparator;
            this.key_ = key;
            this.isAscending_ = isAscending;
            init(startColumn, position);
        }

        private void init(byte[] startColumn, long position) throws IOException
        {
            file_.seek(position);
            String keyInDisk = file_.readUTF();
            assert keyInDisk.equals(key_);

            file_.readInt(); // row size
            IndexHelper.skipBloomFilter(file_);

            /* read the index */
            indexList_ = IndexHelper.deserializeIndex(SSTableReader.parseTableName(file_.getPath()), cfName_, file_);

            /* need to do two things here.
             * 1. move the file pointer to the beginning of the list of stored columns
             * 2. calculate the size of all columns */
            emptyColumnFamily = ColumnFamily.serializer().deserializeEmpty(file_);
            file_.readInt(); // column count

            columnStartPosition_ = file_.getFilePointer();

            if (startColumn.length == 0 && !isAscending_)
            {
                /* in this case, we assume that we want to scan from the largest column in descending order. */
                curRangeIndex_ = indexList_.size() - 1;
            }
            else
            {
                int index = Collections.binarySearch(indexList_, new IndexHelper.IndexInfo(startColumn, startColumn, 0, 0, comparator_));
                curRangeIndex_ = index < 0 ? -1 * (index + 1): index;
                if (curRangeIndex_ < 0)
                    curRangeIndex_ = 0;
            }
        }

        public ColumnFamily getEmptyColumnFamily()
        {
            return emptyColumnFamily;
        }

        public IColumn pollColumn()
        {
            return blockColumns.poll();
        }

        private boolean getBlockFromCurIndex() throws IOException
        {
            if (curRangeIndex_ < 0 || curRangeIndex_ >= indexList_.size())
                return false;
            IndexHelper.IndexInfo curColPostion = indexList_.get(curRangeIndex_);

            /* seek to the correct offset to the data, and calculate the data size */
            file_.seek(columnStartPosition_ + curColPostion.offset);
            while (file_.getFilePointer() < columnStartPosition_ + curColPostion.offset + curColPostion.width)
            {
                blockColumns.add(emptyColumnFamily.getColumnSerializer().deserialize(file_));
            }
            return true;
        }

        public boolean getNextBlock() throws IOException
        {
            boolean result = getBlockFromCurIndex();
            if (isAscending_)
                curRangeIndex_++;
            else
                curRangeIndex_--;
            return result;
        }

        public void close() throws IOException
        {
            file_.close();
        }
    }
}
