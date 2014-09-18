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
import org.apache.cassandra.db.marshal.AbstractType;
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
public class LazilyCompactedRow extends AbstractCompactedRow implements Iterable<OnDiskAtom>
{
    private final List<? extends OnDiskAtomIterator> rows;
    private final CompactionController controller;
    private final boolean shouldPurge;
    private final ColumnFamily emptyColumnFamily;
    private Reducer reducer;
    private ColumnStats columnStats;
    private boolean closed;
    private ColumnIndex.Builder indexBuilder;
    private final SecondaryIndexManager.Updater indexer;
    private DeletionTime maxRowTombstone;

    public LazilyCompactedRow(CompactionController controller, List<? extends OnDiskAtomIterator> rows)
    {
        super(rows.get(0).getKey());
        this.rows = rows;
        this.controller = controller;
        indexer = controller.cfs.indexManager.updaterFor(key);

        // Combine top-level tombstones, keeping the one with the highest markedForDeleteAt timestamp.  This may be
        // purged (depending on gcBefore), but we need to remember it to properly delete columns during the merge
        maxRowTombstone = DeletionTime.LIVE;
        for (OnDiskAtomIterator row : rows)
        {
            DeletionTime rowTombstone = row.getColumnFamily().deletionInfo().getTopLevelDeletion();
            if (maxRowTombstone.compareTo(rowTombstone) < 0)
                maxRowTombstone = rowTombstone;
        }


        // Don't pass maxTombstoneTimestamp to shouldPurge since we might well have cells with
        // tombstones newer than the row-level tombstones we've seen -- but we won't know that
        // until we iterate over them.  By passing MAX_VALUE we will only purge if there are
        // no other versions of this row present.
        this.shouldPurge = controller.shouldPurge(key, Long.MAX_VALUE);

        emptyColumnFamily = EmptyColumns.factory.create(controller.cfs.metadata);
        emptyColumnFamily.delete(maxRowTombstone);
        if (shouldPurge)
            emptyColumnFamily.purgeTombstones(controller.gcBefore);
    }

    public RowIndexEntry write(long currentPosition, DataOutput out) throws IOException
    {
        assert !closed;

        ColumnIndex columnsIndex;
        try
        {
            indexBuilder = new ColumnIndex.Builder(emptyColumnFamily, key.key, out);
            columnsIndex = indexBuilder.buildForCompaction(iterator());

            // if there aren't any columns or tombstones, return null
            if (columnsIndex.columnsIndex.isEmpty() && !emptyColumnFamily.isMarkedForDelete())
                return null;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        // reach into the reducer (created during iteration) to get column count, size, max column timestamp
        // (however, if there are zero columns, iterator() will not be called by ColumnIndexer and reducer will be null)
        columnStats = new ColumnStats(reducer == null ? 0 : reducer.columns,
                                      reducer == null ? Long.MAX_VALUE : reducer.minTimestampTracker.get(),
                                      reducer == null ? emptyColumnFamily.maxTimestamp() : Math.max(emptyColumnFamily.maxTimestamp(), reducer.maxTimestampTracker.get()),
                                      reducer == null ? Integer.MIN_VALUE : reducer.maxDeletionTimeTracker.get(),
                                      reducer == null ? new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE) : reducer.tombstones,
                                      reducer == null ? Collections.<ByteBuffer>emptyList() : reducer.minColumnNameSeen,
                                      reducer == null ? Collections.<ByteBuffer>emptyList() : reducer.maxColumnNameSeen
        );
        reducer = null;

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
        Iterator<OnDiskAtom> iter = iterator();
        while (iter.hasNext())
            iter.next().updateDigest(digest);
        close();
    }

    public AbstractType<?> getComparator()
    {
        return emptyColumnFamily.getComparator();
    }

    public Iterator<OnDiskAtom> iterator()
    {
        reducer = new Reducer();
        Iterator<OnDiskAtom> iter = MergeIterator.get(rows, getComparator().onDiskAtomComparator, reducer);
        return Iterators.filter(iter, Predicates.notNull());
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
        ColumnFamily container = ArrayBackedSortedColumns.factory.create(emptyColumnFamily.metadata());

        // tombstone reference; will be reconciled w/ column during getReduced.  Note that the top-level (row) tombstone
        // is held by LCR.deletionInfo.
        RangeTombstone tombstone;

        int columns = 0;
        // if the row tombstone is 'live' we need to set timestamp to MAX_VALUE to be able to overwrite it later
        // markedForDeleteAt is MIN_VALUE for 'live' row tombstones (which we use to default maxTimestampSeen)

        ColumnStats.MinTracker<Long> minTimestampTracker = new ColumnStats.MinTracker<>(Long.MIN_VALUE);
        ColumnStats.MaxTracker<Long> maxTimestampTracker = new ColumnStats.MaxTracker<>(Long.MAX_VALUE);
        // we need to set MIN_VALUE if we are 'live' since we want to overwrite it later
        // we are bound to have either a RangeTombstone or standard cells will set this properly:
        ColumnStats.MaxTracker<Integer> maxDeletionTimeTracker = new ColumnStats.MaxTracker<>(Integer.MAX_VALUE);

        StreamingHistogram tombstones = new StreamingHistogram(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE);
        List<ByteBuffer> minColumnNameSeen = Collections.emptyList();
        List<ByteBuffer> maxColumnNameSeen = Collections.emptyList();

        public Reducer()
        {
            minTimestampTracker.update(maxRowTombstone.isLive() ? Long.MAX_VALUE : maxRowTombstone.markedForDeleteAt);
            maxTimestampTracker.update(maxRowTombstone.markedForDeleteAt);
            maxDeletionTimeTracker.update(maxRowTombstone.isLive() ? Integer.MIN_VALUE : maxRowTombstone.localDeletionTime);
        }

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
                Column column = (Column) current;
                container.addColumn(column);

                // skip the index-update checks if there is no indexing needed since they are a bit expensive
                if (indexer == SecondaryIndexManager.nullUpdater)
                    return;

                if (!column.isMarkedForDelete(System.currentTimeMillis())
                    && !container.getColumn(column.name()).equals(column))
                {
                    indexer.remove(column);
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

                if (shouldPurge && t.data.isGcAble(controller.gcBefore))
                {
                    indexBuilder.tombstoneTracker().update(t, true);
                    return null;
                }
                else
                {
                    tombstones.update(t.getLocalDeletionTime());
                    minTimestampTracker.update(t.minTimestamp());
                    maxTimestampTracker.update(t.maxTimestamp());
                    maxDeletionTimeTracker.update(t.getLocalDeletionTime());
                    minColumnNameSeen = ColumnNameHelper.minComponents(minColumnNameSeen, t.min, controller.cfs.metadata.comparator);
                    maxColumnNameSeen = ColumnNameHelper.maxComponents(maxColumnNameSeen, t.max, controller.cfs.metadata.comparator);

                    return t;
                }
            }
            else
            {
                // when we clear() the container, it removes the deletion info, so this needs to be reset each time
                container.delete(maxRowTombstone);
                ColumnFamily purged = PrecompactedRow.removeDeleted(key, shouldPurge, controller, container);
                if (purged == null || !purged.iterator().hasNext())
                {
                    // don't call clear() because that resets the deletion time. See CASSANDRA-7808.
                    container = ArrayBackedSortedColumns.factory.create(emptyColumnFamily.metadata());;
                    return null;
                }
                Column reduced = purged.iterator().next();
                container = ArrayBackedSortedColumns.factory.create(emptyColumnFamily.metadata());

                // PrecompactedRow.removeDeleted has only checked the top-level CF deletion times,
                // not the range tombstones. For that we use the columnIndexer tombstone tracker.
                if (indexBuilder.tombstoneTracker().isDeleted(reduced))
                {
                    indexer.remove(reduced);
                    return null;
                }
                int localDeletionTime = purged.deletionInfo().getTopLevelDeletion().localDeletionTime;
                if (localDeletionTime < Integer.MAX_VALUE)
                    tombstones.update(localDeletionTime);
                columns++;
                minTimestampTracker.update(reduced.minTimestamp());
                maxTimestampTracker.update(reduced.maxTimestamp());
                maxDeletionTimeTracker.update(reduced.getLocalDeletionTime());
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
