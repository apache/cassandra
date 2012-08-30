/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.ICountableColumnIterator;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.StreamingHistogram;

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
public class LazilyCompactedRow extends AbstractCompactedRow implements Iterable<OnDiskAtom>
{
    private static Logger logger = LoggerFactory.getLogger(LazilyCompactedRow.class);

    private final List<? extends ICountableColumnIterator> rows;
    private final CompactionController controller;
    private final boolean shouldPurge;
    private ColumnFamily emptyColumnFamily;
    private Reducer reducer;
    private final ColumnStats columnStats;
    private long columnSerializedSize;
    private boolean closed;
    private ColumnIndex.Builder indexBuilder;
    private ColumnIndex columnsIndex;
    private final SecondaryIndexManager.Updater indexer;

    public LazilyCompactedRow(CompactionController controller, List<? extends ICountableColumnIterator> rows)
    {
        super(rows.get(0).getKey());
        this.rows = rows;
        this.controller = controller;
        this.shouldPurge = controller.shouldPurge(key);
        indexer = controller.cfs.indexManager.updaterFor(key, false);

        for (OnDiskAtomIterator row : rows)
        {
            ColumnFamily cf = row.getColumnFamily();

            if (emptyColumnFamily == null)
                emptyColumnFamily = cf;
            else
                emptyColumnFamily.delete(cf);
        }

        try
        {
            indexAndWrite(null);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        // reach into the reducer used during iteration to get column count, size, max column timestamp
        // (however, if there are zero columns, iterator() will not be called by ColumnIndexer and reducer will be null)
        columnStats = new ColumnStats(reducer == null ? 0 : reducer.columns, reducer == null ? Long.MIN_VALUE : reducer.maxTimestampSeen,
                                      reducer == null ? new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE) : reducer.tombstones
        );
        columnSerializedSize = reducer == null ? 0 : reducer.serializedSize;
        reducer = null;
    }

    private void indexAndWrite(DataOutput out) throws IOException
    {
        this.indexBuilder = new ColumnIndex.Builder(emptyColumnFamily, key.key, getEstimatedColumnCount(), out);
        this.columnsIndex = indexBuilder.build(this);
    }

    public long write(DataOutput out) throws IOException
    {
        assert !closed;

        DataOutputBuffer clockOut = new DataOutputBuffer();
        DeletionInfo.serializer().serializeForSSTable(emptyColumnFamily.deletionInfo(), clockOut);

        long dataSize = clockOut.getLength() + columnSerializedSize;
        if (logger.isDebugEnabled())
            logger.debug(String.format("clock / column sizes are %s / %s", clockOut.getLength(), columnSerializedSize));
        assert dataSize > 0;
        out.writeLong(dataSize);
        out.write(clockOut.getData(), 0, clockOut.getLength());
        out.writeInt(indexBuilder.writtenAtomCount());

        // We rebuild the column index uselessly, but we need to do that because range tombstone markers depend
        // on indexing. If we're able to remove the two-phase compaction, we'll avoid that.
        indexAndWrite(out);

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
            DeletionInfo.serializer().serializeForSSTable(emptyColumnFamily.deletionInfo(), out);
            out.writeInt(columnStats.columnCount);
            digest.update(out.getData(), 0, out.getLength());
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        Iterator<OnDiskAtom> iter = iterator();
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
        return cfIrrelevant && columnStats.columnCount == 0;
    }

    public int getEstimatedColumnCount()
    {
        int n = 0;
        for (ICountableColumnIterator row : rows)
            n += row.getColumnCount();
        return n;
    }

    public AbstractType<?> getComparator()
    {
        return emptyColumnFamily.getComparator();
    }

    public Iterator<OnDiskAtom> iterator()
    {
        for (ICountableColumnIterator row : rows)
            row.reset();
        reducer = new Reducer();
        Iterator<OnDiskAtom> iter = MergeIterator.get(rows, getComparator().onDiskAtomComparator, reducer);
        return Iterators.filter(iter, Predicates.notNull());
    }

    public ColumnStats columnStats()
    {
        return columnStats;
    }

    private void close()
    {
        for (OnDiskAtomIterator row : rows)
        {
            try
            {
                row.close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
        closed = true;
    }

    public DeletionInfo deletionInfo()
    {
        return emptyColumnFamily.deletionInfo();
    }

    /**
     * @return the column index for this row.
     */
    public ColumnIndex index()
    {
        return columnsIndex;
    }

    private class Reducer extends MergeIterator.Reducer<OnDiskAtom, OnDiskAtom>
    {
        // all columns reduced together will have the same name, so there will only be one column
        // in the container; we just want to leverage the conflict resolution code from CF
        ColumnFamily container = emptyColumnFamily.cloneMeShallow();

        // tombstone reference; will be reconciled w/ column during getReduced
        RangeTombstone tombstone;

        long serializedSize = 4; // int for column count
        int columns = 0;
        long maxTimestampSeen = Long.MIN_VALUE;
        StreamingHistogram tombstones = new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE);

        public void reduce(OnDiskAtom current)
        {
            if (current instanceof RangeTombstone)
            {
                tombstone = (RangeTombstone)current;
            }
            else
            {
                IColumn column = (IColumn) current;
                container.addColumn(column);
                if (container.getColumn(column.name()) != column)
                    indexer.remove(column);
            }
        }

        protected OnDiskAtom getReduced()
        {
            if (tombstone != null)
            {
                RangeTombstone t = tombstone;
                tombstone = null;

                if (t.data.isGcAble(controller.gcBefore))
                {
                    return null;
                }
                else
                {
                    serializedSize += t.serializedSizeForSSTable();
                    return t;
                }
            }
            else
            {
                ColumnFamily purged = PrecompactedRow.removeDeletedAndOldShards(key, shouldPurge, controller, container);
                if (purged == null || !purged.iterator().hasNext())
                {
                    container.clear();
                    return null;
                }
                IColumn reduced = purged.iterator().next();
                container.clear();

                // PrecompactedRow.removeDeletedAndOldShards have only checked the top-level CF deletion times,
                // not the range tombstone. For that we use the columnIndexer tombstone tracker.
                // Note that this doesn't work for super columns.
                if (indexBuilder.tombstoneTracker().isDeleted(reduced))
                    return null;

                serializedSize += reduced.serializedSizeForSSTable();
                columns++;
                maxTimestampSeen = Math.max(maxTimestampSeen, reduced.maxTimestamp());
                int deletionTime = reduced.getLocalDeletionTime();
                if (deletionTime < Integer.MAX_VALUE)
                {
                    tombstones.update(deletionTime);
                }
                return reduced;
            }
        }
    }
}
