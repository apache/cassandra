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
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.ColumnNameHelper;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.StreamingHistogram;

/**
 * LazilyCompactedRow only computes the row bloom filter and column index in memory
 * (at construction time); it does this by reading one column at a time from each
 * of the rows being compacted, and merging them as it does so.  So the most we have
 * in memory at a time is the bloom filter, the index, and one column from each
 * pre-compaction row.
 */
public class LazilyCompactedRow extends AbstractCompactedRow
{
    private final List<? extends OnDiskAtomIterator> rows;
    private final CompactionController controller;
    private final long maxPurgeableTimestamp;
    private final ColumnFamily emptyColumnFamily;
    private ColumnStats columnStats;
    private boolean closed;
    private ColumnIndex.Builder indexBuilder;
    private final SecondaryIndexManager.Updater indexer;
    private final Reducer reducer;
    private final Iterator<OnDiskAtom> merger;
    private DeletionTime maxRowTombstone;

    public LazilyCompactedRow(CompactionController controller, List<? extends OnDiskAtomIterator> rows)
    {
        super(rows.get(0).getKey());
        this.rows = rows;
        this.controller = controller;
        indexer = controller.cfs.indexManager.gcUpdaterFor(key);

        // Combine top-level tombstones, keeping the one with the highest markedForDeleteAt timestamp.  This may be
        // purged (depending on gcBefore), but we need to remember it to properly delete columns during the merge
        maxRowTombstone = DeletionTime.LIVE;
        for (OnDiskAtomIterator row : rows)
        {
            DeletionTime rowTombstone = row.getColumnFamily().deletionInfo().getTopLevelDeletion();
            if (maxRowTombstone.compareTo(rowTombstone) < 0)
                maxRowTombstone = rowTombstone;
        }

        // tombstones with a localDeletionTime before this can be purged.  This is the minimum timestamp for any sstable
        // containing `key` outside of the set of sstables involved in this compaction.
        maxPurgeableTimestamp = controller.maxPurgeableTimestamp(key);

        emptyColumnFamily = ArrayBackedSortedColumns.factory.create(controller.cfs.metadata);
        emptyColumnFamily.delete(maxRowTombstone);
        if (maxRowTombstone.markedForDeleteAt < maxPurgeableTimestamp)
            emptyColumnFamily.purgeTombstones(controller.gcBefore);

        reducer = new Reducer();
        merger = Iterators.filter(MergeIterator.get(rows, emptyColumnFamily.getComparator().onDiskAtomComparator(), reducer), Predicates.notNull());
    }

    private static void removeDeleted(ColumnFamily cf, boolean shouldPurge, DecoratedKey key, CompactionController controller)
    {
        // We should only purge cell tombstones if shouldPurge is true, but regardless, it's still ok to remove cells that
        // are shadowed by a row or range tombstone; removeDeletedColumnsOnly(cf, Integer.MIN_VALUE) will accomplish this
        // without purging tombstones.
        int overriddenGCBefore = shouldPurge ? controller.gcBefore : Integer.MIN_VALUE;
        ColumnFamilyStore.removeDeletedColumnsOnly(cf, overriddenGCBefore, controller.cfs.indexManager.gcUpdaterFor(key));
    }

    public RowIndexEntry write(long currentPosition, DataOutput out) throws IOException
    {
        assert !closed;

        ColumnIndex columnsIndex;
        try
        {
            indexBuilder = new ColumnIndex.Builder(emptyColumnFamily, key.key, out);
            columnsIndex = indexBuilder.buildForCompaction(merger);

            // if there aren't any columns or tombstones, return null
            if (columnsIndex.columnsIndex.isEmpty() && !emptyColumnFamily.isMarkedForDelete())
                return null;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        // reach into the reducer (created during iteration) to get column count, size, max column timestamp
        columnStats = new ColumnStats(reducer.columns,
                                      reducer.minTimestampSeen,
                                      Math.max(emptyColumnFamily.deletionInfo().maxTimestamp(), reducer.maxTimestampSeen),
                                      reducer.maxLocalDeletionTimeSeen,
                                      reducer.tombstones,
                                      reducer.minColumnNameSeen,
                                      reducer.maxColumnNameSeen
        );

        // in case no columns were ever written, we may still need to write an empty header with a top-level tombstone
        indexBuilder.maybeWriteEmptyRowHeader();

        out.writeShort(SSTableWriter.END_OF_ROW);

        close();

        return RowIndexEntry.create(currentPosition, emptyColumnFamily.deletionInfo().getTopLevelDeletion(), columnsIndex);
    }

    public void update(MessageDigest digest)
    {
        assert !closed;

        // no special-case for rows.size == 1, we're actually skipping some bytes here so just
        // blindly updating everything wouldn't be correct
        DataOutputBuffer out = new DataOutputBuffer();

        try
        {
            DeletionTime.serializer.serialize(emptyColumnFamily.deletionInfo().getTopLevelDeletion(), out);
            digest.update(out.getData(), 0, out.getLength());
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }

        // initialize indexBuilder for the benefit of its tombstoneTracker, used by our reducing iterator
        indexBuilder = new ColumnIndex.Builder(emptyColumnFamily, key.key, out);
        while (merger.hasNext())
            merger.next().updateDigest(digest);
        close();
    }

    public ColumnStats columnStats()
    {
        return columnStats;
    }

    public void close()
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

    private class Reducer extends MergeIterator.Reducer<OnDiskAtom, OnDiskAtom>
    {
        // all columns reduced together will have the same name, so there will only be one column
        // in the container; we just want to leverage the conflict resolution code from CF.
        // (Note that we add the row tombstone in getReduced.)
        final ColumnFamily container = ArrayBackedSortedColumns.factory.create(emptyColumnFamily.metadata());

        // tombstone reference; will be reconciled w/ column during getReduced.  Note that the top-level (row) tombstone
        // is held by LCR.deletionInfo.
        RangeTombstone tombstone;

        int columns = 0;
        long minTimestampSeen = Long.MAX_VALUE;
        long maxTimestampSeen = Long.MIN_VALUE;
        int maxLocalDeletionTimeSeen = Integer.MIN_VALUE;
        StreamingHistogram tombstones = new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE);
        List<ByteBuffer> minColumnNameSeen = Collections.emptyList();
        List<ByteBuffer> maxColumnNameSeen = Collections.emptyList();

        /**
         * Called once per version of a cell that we need to merge, after which getReduced() is called.  In other words,
         * this will be called one or more times with cells that share the same column name.
         */
        public void reduce(OnDiskAtom current)
        {
            if (current instanceof RangeTombstone)
            {
                if (tombstone == null || current.maxTimestamp() >= tombstone.maxTimestamp())
                    tombstone = (RangeTombstone)current;
            }
            else
            {
                Cell cell = (Cell) current;
                container.addColumn(cell);

                // skip the index-update checks if there is no indexing needed since they are a bit expensive
                if (indexer == SecondaryIndexManager.nullUpdater)
                    return;

                if (!cell.isMarkedForDelete(System.currentTimeMillis())
                    && !container.getColumn(cell.name()).equals(cell))
                {
                    indexer.remove(cell);
                }
            }
        }

        /**
         * Called after reduce() has been called for each cell sharing the same name.
         */
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
                    return t;
                }
            }
            else
            {
                boolean shouldPurge = container.getSortedColumns().iterator().next().timestamp() < maxPurgeableTimestamp;
                // when we clear() the container, it removes the deletion info, so this needs to be reset each time
                container.delete(maxRowTombstone);
                removeDeleted(container, shouldPurge, key, controller);
                Iterator<Cell> iter = container.iterator();
                if (!iter.hasNext())
                {
                    container.clear();
                    return null;
                }

                int localDeletionTime = container.deletionInfo().getTopLevelDeletion().localDeletionTime;
                if (localDeletionTime < Integer.MAX_VALUE)
                    tombstones.update(localDeletionTime);
                Iterator<RangeTombstone> rangeTombstoneIterator = container.deletionInfo().rangeIterator();
                while (rangeTombstoneIterator.hasNext())
                {
                    RangeTombstone rangeTombstone = rangeTombstoneIterator.next();
                    tombstones.update(rangeTombstone.getLocalDeletionTime());
                }

                Cell reduced = iter.next();
                container.clear();

                // removeDeleted have only checked the top-level CF deletion times,
                // not the range tombstone. For that we use the columnIndexer tombstone tracker.
                if (indexBuilder.tombstoneTracker().isDeleted(reduced))
                {
                    indexer.remove(reduced);
                    return null;
                }

                columns++;
                minTimestampSeen = Math.min(minTimestampSeen, reduced.minTimestamp());
                maxTimestampSeen = Math.max(maxTimestampSeen, reduced.maxTimestamp());
                maxLocalDeletionTimeSeen = Math.max(maxLocalDeletionTimeSeen, reduced.getLocalDeletionTime());
                minColumnNameSeen = ColumnNameHelper.minComponents(minColumnNameSeen, reduced.name(), controller.cfs.metadata.comparator);
                maxColumnNameSeen = ColumnNameHelper.maxComponents(maxColumnNameSeen, reduced.name(), controller.cfs.metadata.comparator);

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
