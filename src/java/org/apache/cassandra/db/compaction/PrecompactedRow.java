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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MergeIterator;

/**
 * PrecompactedRow merges its rows in its constructor in memory.
 */
public class PrecompactedRow extends AbstractCompactedRow
{
    private final ColumnFamily compactedCf;

    /** it is caller's responsibility to call removeDeleted + removeOldShards from the cf before calling this constructor */
    public PrecompactedRow(DecoratedKey key, ColumnFamily cf)
    {
        super(key);
        compactedCf = cf;
    }

    public static ColumnFamily removeDeletedAndOldShards(DecoratedKey key, CompactionController controller, ColumnFamily cf)
    {
        assert key != null;
        assert controller != null;
        assert cf != null;

        // avoid calling shouldPurge unless we actually need to: it can be very expensive if LCS
        // gets behind and has hundreds of overlapping L0 sstables.  Essentially, this method is an
        // ugly refactor of removeDeletedAndOldShards(controller.shouldPurge(key), controller, cf),
        // taking this into account.
        Boolean shouldPurge = null;

        if (cf.hasIrrelevantData(controller.gcBefore))
            shouldPurge = controller.shouldPurge(key, cf.maxTimestamp());

        // We should only gc tombstone if shouldPurge == true. But otherwise,
        // it is still ok to collect column that shadowed by their (deleted)
        // container, which removeDeleted(cf, Integer.MAX_VALUE) will do
        ColumnFamily compacted = ColumnFamilyStore.removeDeleted(cf, shouldPurge != null && shouldPurge ? controller.gcBefore : Integer.MIN_VALUE);

        if (compacted != null && compacted.metadata().getDefaultValidator().isCommutative())
        {
            if (shouldPurge == null)
                shouldPurge = controller.shouldPurge(key, cf.deletionInfo().maxTimestamp());
            if (shouldPurge)
                CounterColumn.mergeAndRemoveOldShards(key, compacted, controller.gcBefore, controller.mergeShardBefore);
        }

        return compacted;
    }

    public static ColumnFamily removeDeletedAndOldShards(DecoratedKey key, boolean shouldPurge, CompactionController controller, ColumnFamily cf)
    {
        // See comment in preceding method
        ColumnFamily compacted = ColumnFamilyStore.removeDeleted(cf,
                                                                 shouldPurge ? controller.gcBefore : Integer.MIN_VALUE,
                                                                 controller.cfs.indexManager.updaterFor(key));
        if (shouldPurge && compacted != null && compacted.metadata().getDefaultValidator().isCommutative())
            CounterColumn.mergeAndRemoveOldShards(key, compacted, controller.gcBefore, controller.mergeShardBefore);
        return compacted;
    }

    public PrecompactedRow(CompactionController controller, List<SSTableIdentityIterator> rows)
    {
        this(rows.get(0).getKey(),
             removeDeletedAndOldShards(rows.get(0).getKey(), controller, merge(rows, controller)));
    }

    private static ColumnFamily merge(List<SSTableIdentityIterator> rows, CompactionController controller)
    {
        assert !rows.isEmpty();

        final ColumnFamily returnCF = ArrayBackedSortedColumns.factory.create(controller.cfs.metadata);

        // transform into iterators that MergeIterator will like, and apply row-level tombstones
        List<CloseableIterator<Column>> data = new ArrayList<>(rows.size());
        for (SSTableIdentityIterator row : rows)
        {
            ColumnFamily cf = row.getColumnFamilyWithColumns(ArrayBackedSortedColumns.factory);
            returnCF.delete(cf);
            data.add(FBUtilities.closeableIterator(cf.iterator()));
        }

        merge(returnCF, data, controller.cfs.indexManager.updaterFor(rows.get(0).getKey()));

        return returnCF;
    }

    // returnCF should already have row-level tombstones applied
    public static void merge(final ColumnFamily returnCF, List<CloseableIterator<Column>> data, final SecondaryIndexManager.Updater indexer)
    {
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Comparator<Column> fcomp = filter.getColumnComparator(returnCF.getComparator());

        MergeIterator.Reducer<Column, Column> reducer = new MergeIterator.Reducer<Column, Column>()
        {
            ColumnFamily container = returnCF.cloneMeShallow();

            public void reduce(Column column)
            {
                container.addColumn(column);

                // skip the index-update checks if there is no indexing needed since they are a bit expensive
                if (indexer == SecondaryIndexManager.nullUpdater)
                    return;

                // notify the index that the column has been overwritten if the value being reduced has been
                // superceded by another directly, or indirectly by a range tombstone
                if ((!column.isMarkedForDelete(System.currentTimeMillis()) && !container.getColumn(column.name()).equals(column))
                    || returnCF.deletionInfo().isDeleted(column.name(), CompactionManager.NO_GC))
                {
                    indexer.remove(column);
                }
            }

            protected Column getReduced()
            {
                Column c = container.iterator().next();
                container.clear();
                return c;
            }
        };

        Iterator<Column> reduced = MergeIterator.get(data, fcomp, reducer);
        filter.collectReducedColumns(returnCF, reduced, CompactionManager.NO_GC, System.currentTimeMillis());
    }

    public RowIndexEntry write(long currentPosition, DataOutput out) throws IOException
    {
        if (compactedCf == null)
            return null;

        return SSTableWriter.rawAppend(compactedCf, currentPosition, key, out);
    }

    public void update(MessageDigest digest)
    {
        assert compactedCf != null;
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            DeletionTime.serializer.serialize(compactedCf.deletionInfo().getTopLevelDeletion(), buffer);
            digest.update(buffer.getData(), 0, buffer.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        compactedCf.updateDigest(digest);
    }

    public ColumnStats columnStats()
    {
        return compactedCf.getColumnStats();
    }

    /**
     * @return the full column family represented by this compacted row.
     *
     * We do not provide this method for other AbstractCompactedRow, because this fits the whole row into
     * memory and don't make sense for those other implementations.
     */
    public ColumnFamily getFullColumnFamily()
    {
        return compactedCf;
    }

    public void close() { }
}
