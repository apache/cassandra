package org.apache.cassandra.db.compaction;
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
import java.nio.channels.ClosedByInterruptException;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IColumnIterator;
import org.apache.cassandra.db.columniterator.ICountableColumnIterator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.IIterableColumns;
import org.apache.cassandra.utils.MergeIterator;

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
    private static Logger logger = LoggerFactory.getLogger(LazilyCompactedRow.class);

    private final List<? extends ICountableColumnIterator> rows;
    private final CompactionController controller;
    private final boolean shouldPurge;
    private final DataOutputBuffer headerBuffer;
    private ColumnFamily emptyColumnFamily;
    private Reducer reducer;
    private int columnCount;
    private long maxTimestamp;
    private long columnSerializedSize;
    private boolean closed;

    public LazilyCompactedRow(CompactionController controller, List<? extends ICountableColumnIterator> rows)
    {
        super(rows.get(0).getKey());
        this.rows = rows;
        this.controller = controller;
        this.shouldPurge = controller.shouldPurge(key);

        for (IColumnIterator row : rows)
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
        // reach into the reducer used during iteration to get column count, size, max column timestamp
        // (however, if there are zero columns, iterator() will not be called by ColumnIndexer and reducer will be null)
        columnCount = reducer == null ? 0 : reducer.size;
        columnSerializedSize = reducer == null ? 0 : reducer.serializedSize;
        maxTimestamp = reducer == null ? Long.MIN_VALUE : reducer.maxTimestampSeen;
        reducer = null;
    }

    public long write(DataOutput out) throws IOException
    {
        assert !closed;

        DataOutputBuffer clockOut = new DataOutputBuffer();
        ColumnFamily.serializer().serializeCFInfo(emptyColumnFamily, clockOut);

        long dataSize = headerBuffer.getLength() + clockOut.getLength() + columnSerializedSize;
        if (logger.isDebugEnabled())
            logger.debug(String.format("header / clock / column sizes are %s / %s / %s",
                         headerBuffer.getLength(), clockOut.getLength(), columnSerializedSize));
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
        long secondPassColumnSize = reducer == null ? 0 : reducer.serializedSize;
        assert secondPassColumnSize == columnSerializedSize
               : "originally calculated column size of " + columnSerializedSize + " but now it is " + secondPassColumnSize;

        close();
        return dataSize;
    }

    public void update(MessageDigest digest)
    {
        assert !closed;

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
        close();
    }

    public boolean isEmpty()
    {
        boolean cfIrrelevant = shouldPurge
                             ? ColumnFamilyStore.removeDeletedCF(emptyColumnFamily, controller.gcBefore) == null
                             : !emptyColumnFamily.isMarkedForDelete(); // tombstones are relevant
        return cfIrrelevant && columnCount == 0;
    }

    public int getEstimatedColumnCount()
    {
        int n = 0;
        for (ICountableColumnIterator row : rows)
            n += row.getColumnCount();
        return n;
    }

    public AbstractType getComparator()
    {
        return emptyColumnFamily.getComparator();
    }

    public Iterator<IColumn> iterator()
    {
        for (ICountableColumnIterator row : rows)
            row.reset();
        reducer = new Reducer();
        Iterator<IColumn> iter = MergeIterator.get(rows, getComparator().columnComparator, reducer);
        return Iterators.filter(iter, Predicates.notNull());
    }

    public int columnCount()
    {
        return columnCount;
    }

    public long maxTimestamp()
    {
        return maxTimestamp;
    }

    private void close()
    {
        for (IColumnIterator row : rows)
        {
            try
            {
                row.close();
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
        closed = true;
    }

    private class Reducer extends MergeIterator.Reducer<IColumn, IColumn>
    {
        ColumnFamily container = emptyColumnFamily.cloneMeShallow();
        long serializedSize = 4; // int for column count
        int size = 0;
        long maxTimestampSeen = Long.MIN_VALUE;

        public void reduce(IColumn current)
        {
            container.addColumn(current);
        }

        protected IColumn getReduced()
        {
            ColumnFamily purged = PrecompactedRow.removeDeletedAndOldShards(key, shouldPurge, controller, container);
            if (purged == null || !purged.iterator().hasNext())
            {
                container.clear();
                return null;
            }
            IColumn reduced = purged.iterator().next();
            container.clear();
            serializedSize += reduced.serializedSize();
            size++;
            maxTimestampSeen = Math.max(maxTimestampSeen, reduced.maxTimestamp());
            return reduced;
        }
    }
}
