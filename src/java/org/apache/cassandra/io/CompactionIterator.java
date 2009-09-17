package org.apache.cassandra.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.utils.ReducingIterator;
import org.apache.cassandra.db.ColumnFamily;

public class CompactionIterator extends ReducingIterator<IteratingRow, CompactionIterator.CompactedRow> implements Closeable
{
    private final List<IteratingRow> rows = new ArrayList<IteratingRow>();

    @SuppressWarnings("unchecked")
    public CompactionIterator(Iterable<SSTableReader> sstables) throws IOException
    {
        super(getCollatingIterator(sstables));
    }

    @SuppressWarnings("unchecked")
    private static CollatingIterator getCollatingIterator(Iterable<SSTableReader> sstables) throws IOException
    {
        // CollatingIterator has a bug that causes NPE when you try to use default comparator. :(
        CollatingIterator iter = new CollatingIterator(new Comparator()
        {
            public int compare(Object o1, Object o2)
            {
                return ((Comparable)o1).compareTo(o2);
            }
        });
        for (SSTableReader sstable : sstables)
        {
            iter.addIterator(sstable.getScanner());
        }
        return iter;
    }

    @Override
    protected boolean isEqual(IteratingRow o1, IteratingRow o2)
    {
        return o1.getKey().equals(o2.getKey());
    }

    public void reduce(IteratingRow current)
    {
        rows.add(current);
    }

    protected CompactedRow getReduced()
    {
        try
        {
            return getReducedRaw();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected CompactedRow getReducedRaw() throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        String key = rows.get(0).getKey();
        if (rows.size() > 1)
        {
            ColumnFamily cf = null;
            for (IteratingRow row : rows)
            {
                if (cf == null)
                {
                    cf = row.getColumnFamily();
                }
                else
                {
                    cf.addAll(row.getColumnFamily());
                }
            }
            ColumnFamily.serializer().serializeWithIndexes(cf, buffer);
        }
        else
        {
            assert rows.size() == 1;
            rows.get(0).echoData(buffer);
        }
        rows.clear();
        return new CompactedRow(key, buffer);
    }

    public void close() throws IOException
    {
        for (Object o : ((CollatingIterator)source).getIterators())
        {
            ((SSTableScanner)o).close();
        }
    }

    public static class CompactedRow
    {
        public final String key;
        public final DataOutputBuffer buffer;

        public CompactedRow(String key, DataOutputBuffer buffer)
        {
            this.key = key;
            this.buffer = buffer;
        }
    }
}
