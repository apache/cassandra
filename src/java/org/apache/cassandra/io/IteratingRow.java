package org.apache.cassandra.io;

import java.io.IOException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import com.google.common.collect.AbstractIterator;

public class IteratingRow extends AbstractIterator<IColumn>
{
    private final String key;
    private final long finishedAt;
    private final ColumnFamily emptyColumnFamily;
    private final BufferedRandomAccessFile file;

    public IteratingRow(BufferedRandomAccessFile file) throws IOException
    {
        this.file = file;

        key = file.readUTF();
        long dataSize = file.readInt();
        long dataStart = file.getFilePointer();
        finishedAt = dataStart + dataSize;
        IndexHelper.skipBloomFilterAndIndex(file);
        emptyColumnFamily = ColumnFamily.serializer().deserializeEmpty(file);
        file.readInt(); // column count. breaking serializer encapsulation is less fugly than adding a wrapper class to allow deserializeEmpty to return both values
    }

    public String getKey()
    {
        return key;
    }

    public ColumnFamily getEmptyColumnFamily()
    {
        return emptyColumnFamily;
    }

    public void skipRemaining() throws IOException
    {
        file.seek(finishedAt);
    }

    protected IColumn computeNext()
    {
        try
        {
            assert file.getFilePointer() <= finishedAt;
            if (file.getFilePointer() == finishedAt)
            {
                return endOfData();
            }

            return emptyColumnFamily.getColumnSerializer().deserialize(file);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
