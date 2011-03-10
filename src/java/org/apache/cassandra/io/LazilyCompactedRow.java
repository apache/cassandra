package org.apache.cassandra.io;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.*;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.commons.collections.iterators.CollatingIterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ColumnIndexer;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.IIterableColumns;
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
    private final boolean shouldPurge;
    private final int gcBefore;
    private final DataOutputBuffer headerBuffer;
    private final boolean forceDeserialize;
    private ColumnFamily emptyColumnFamily;
    private LazyColumnIterator iter;
    private int columnCount;
    private long columnSerializedSize;

    public LazilyCompactedRow(ColumnFamilyStore cfStore, List<SSTableIdentityIterator> rows, boolean major, int gcBefore, boolean forceDeserialize)
    {
        super(rows.get(0).getKey());
        this.gcBefore = gcBefore;
        this.forceDeserialize = forceDeserialize;
        this.rows = new ArrayList<SSTableIdentityIterator>(rows);

        Set<SSTable> sstables = new HashSet<SSTable>();
        for (SSTableIdentityIterator row : rows)
        {
            sstables.add(row.sstable);
            ColumnFamily cf = row.getColumnFamily();

            if (emptyColumnFamily == null)
                emptyColumnFamily = cf;
            else
                emptyColumnFamily.delete(cf);
        }
        this.shouldPurge = major || !cfStore.isKeyInRemainingSSTables(key, sstables);

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
        if (rows.size() == 1 && !shouldPurge && rows.get(0).sstable.descriptor.isLatestVersion && !forceDeserialize)
        {
            SSTableIdentityIterator row = rows.get(0);
            assert row.dataSize > 0;
            out.writeLong(row.dataSize);
            row.echoData(out);
            return;
        }

        DataOutputBuffer clockOut = new DataOutputBuffer();
        ColumnFamily.serializer().serializeCFInfo(emptyColumnFamily, clockOut);

        long dataSize = headerBuffer.getLength() + clockOut.getLength() + columnSerializedSize;
        assert dataSize > 0;
        out.writeLong(dataSize);
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
        DataOutputBuffer out = new DataOutputBuffer();

        try
        {
            ColumnFamily.serializer().serializeCFInfo(emptyColumnFamily, out);
            out.writeInt(columnCount);
            digest.update(out.getData(), 0, out.getLength());
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        Iterator<IColumn> iter = iterator();
        while (iter.hasNext())
        {
            iter.next().updateDigest(digest);
        }
    }

    public boolean isEmpty()
    {
        boolean cfIrrelevant = ColumnFamilyStore.removeDeletedCF(emptyColumnFamily, gcBefore) == null;
        return cfIrrelevant && columnCount == 0;
    }

    public int getEstimatedColumnCount()
    {
        int n = 0;
        for (SSTableIdentityIterator row : rows)
            n += row.columnCount;
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

    public int columnCount()
    {
        return columnCount;
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
            return o1.name().equals(o2.name());
        }

        public void reduce(IColumn current)
        {
            container.addColumn(current);
        }

        protected IColumn getReduced()
        {
            assert container != null;
            IColumn reduced = container.iterator().next();
            ColumnFamily purged = shouldPurge ? ColumnFamilyStore.removeDeleted(container, gcBefore) : container;
            if (purged != null && purged.metadata().getDefaultValidator().isCommutative())
            {
                CounterColumn.removeOldShards(purged, gcBefore);
            }
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
