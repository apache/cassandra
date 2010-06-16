package org.apache.cassandra.io;

import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ReducingIterator;

/**
 * LazilyCompactedRow only computes the row bloom filter and column index in memory
 * (at construction time); it does this by reading one column at a time from each
 * of the rows being compacted, and merging them as it does so.  So the most we have
 * in memory at a time is the bloom filter, the index, and one column from each
 * pre-compaction row.
 *
 * When write() or update() is called, a second pass is made over the pre-compaction
 * rows to write the merged columns or update the hash, again with at most one column
 * from each row deserialized at a time.
 */
public class LazilyCompactedRow extends AbstractCompactedRow implements IIterableColumns
{
    private final List<SSTableIdentityIterator> rows;
    private final boolean major;
    private final int gcBefore;
    private final DataOutputBuffer headerBuffer;
    private ColumnFamily emptyColumnFamily;
    private LazyColumnIterator iter;
    private int columnCount;
    private long columnSerializedSize;

    public LazilyCompactedRow(List<SSTableIdentityIterator> rows, boolean major, int gcBefore)
    {
        super(rows.get(0).getKey());
        this.major = major;
        this.gcBefore = gcBefore;
        this.rows = new ArrayList<SSTableIdentityIterator>(rows);

        for (SSTableIdentityIterator row : rows)
        {
            ColumnFamily cf = row.getColumnFamily();

            if (emptyColumnFamily == null)
                emptyColumnFamily = cf;
            else
                emptyColumnFamily.delete(cf);
        }

        // initialize row header so isEmpty can be called
        headerBuffer = new DataOutputBuffer();
        ColumnIndexer.serialize(this, headerBuffer);
        // reach into iterator used by ColumnIndexer to get column count and size
        columnCount = iter.size;
        columnSerializedSize = iter.serializedSize;
        iter = null;
    }

    public void write(DataOutput out) throws IOException
    {
        if (rows.size() == 1 && !major)
        {
            SSTableIdentityIterator row = rows.get(0);
            out.writeLong(row.getDataSize());
            row.echoData(out);
            return;
        }

        DataOutputBuffer clockOut = new DataOutputBuffer();
        ColumnFamily.serializer().serializeCFInfo(emptyColumnFamily, clockOut);

        out.writeLong(headerBuffer.getLength() + clockOut.getLength() + columnSerializedSize);
        out.write(headerBuffer.getData(), 0, headerBuffer.getLength());
        out.write(clockOut.getData(), 0, clockOut.getLength());
        out.writeInt(columnCount);

        Iterator<IColumn> iter = iterator();
        while (iter.hasNext())
        {
            IColumn column = iter.next();
            emptyColumnFamily.getColumnSerializer().serialize(column, out);
        }
    }

    public void update(MessageDigest digest)
    {
        // no special-case for rows.size == 1, we're actually skipping some bytes here so just
        // blindly updating everything wouldn't be correct
        digest.update(headerBuffer.getData(), 0, headerBuffer.getLength());
        DataOutputBuffer out = new DataOutputBuffer();
        Iterator<IColumn> iter = iterator();
        while (iter.hasNext())
        {
            IColumn column = iter.next();
            out.reset();
            try
            {
                emptyColumnFamily.getColumnSerializer().serialize(column, out);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            digest.update(out.getData(), 0, out.getLength());
        }
    }

    public boolean isEmpty()
    {
        boolean cfIrrelevant = ColumnFamilyStore.removeDeleted(emptyColumnFamily, gcBefore) == null;
        return cfIrrelevant && columnCount == 0;
    }

    public int getEstimatedColumnCount()
    {
        int n = 0;
        for (SSTableIdentityIterator row : rows)
            n += row.getColumnCount();
        return n;
    }

    public AbstractType getComparator()
    {
        return emptyColumnFamily.getComparator();
    }

    public Iterator<IColumn> iterator()
    {
        for (SSTableIdentityIterator row : rows)
        {
            row.reset();
        }
        Comparator<IColumn> nameComparator = new Comparator<IColumn>()
        {
            public int compare(IColumn o1, IColumn o2)
            {
                return getComparator().compare(o1.name(), o2.name());
            }
        };
        iter = new LazyColumnIterator(new CollatingIterator(nameComparator, rows));
        return Iterators.filter(iter, Predicates.notNull());
    }

    private class LazyColumnIterator extends ReducingIterator<IColumn, IColumn>
    {
        ColumnFamily container = emptyColumnFamily.cloneMeShallow();
        long serializedSize = 4; // int for column count
        int size = 0;

        public LazyColumnIterator(Iterator<IColumn> source)
        {
            super(source);
        }

        @Override
        protected boolean isEqual(IColumn o1, IColumn o2)
        {
            return Arrays.equals(o1.name(), o2.name());
        }

        public void reduce(IColumn current)
        {
            container.addColumn(current);
        }

        protected IColumn getReduced()
        {
            assert container != null;
            IColumn reduced = container.iterator().next();
            ColumnFamily purged = major ? ColumnFamilyStore.removeDeleted(container, gcBefore) : container;
            if (purged == null || !purged.iterator().hasNext())
            {
                container.clear();
                return null;
            }
            container.clear();
            serializedSize += reduced.serializedSize();
            size++;
            return reduced;
        }
    }
}
