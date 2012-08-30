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
import java.util.List;

import com.google.common.base.Functions;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.ColumnStats;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.HeapAllocator;

/**
 * PrecompactedRow merges its rows in its constructor in memory.
 */
public class PrecompactedRow extends AbstractCompactedRow
{
    private final ColumnFamily compactedCf;
    private ColumnIndex columnIndex;

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
            shouldPurge = controller.shouldPurge(key);

        // We should only gc tombstone if shouldPurge == true. But otherwise,
        // it is still ok to collect column that shadowed by their (deleted)
        // container, which removeDeleted(cf, Integer.MAX_VALUE) will do
        ColumnFamily compacted = ColumnFamilyStore.removeDeleted(cf, shouldPurge != null && shouldPurge ? controller.gcBefore : Integer.MIN_VALUE);

        if (compacted != null && compacted.metadata().getDefaultValidator().isCommutative())
        {
            if (shouldPurge == null)
                shouldPurge = controller.shouldPurge(key);
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
                                                                 controller.cfs.indexManager.updaterFor(key, false));
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
        ColumnFamily cf = null;
        SecondaryIndexManager.Updater indexer = null;
        for (SSTableIdentityIterator row : rows)
        {
            ColumnFamily thisCF;
            try
            {
                // use a map for the first once since that will be the one we merge into
                ISortedColumns.Factory factory = cf == null ? TreeMapBackedSortedColumns.factory() : ArrayBackedSortedColumns.factory();
                thisCF = row.getColumnFamilyWithColumns(factory);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Failed merge of rows on row with key: " + row.getKey(), e);
            }

            if (cf == null)
            {
                cf = thisCF;
                indexer = controller.cfs.indexManager.updaterFor(row.getKey(), false); // only init indexer once
            }
            else
            {
                // addAll is ok even if cf is an ArrayBackedSortedColumns
                cf.addAllWithSizeDelta(thisCF, HeapAllocator.instance, Functions.<IColumn>identity(), indexer);
            }
        }
        return cf;
    }

    public long write(DataOutput out) throws IOException
    {
        assert compactedCf != null;
        DataOutputBuffer buffer = new DataOutputBuffer();
        ColumnIndex.Builder builder = new ColumnIndex.Builder(compactedCf, key.key, compactedCf.getColumnCount(), buffer);
        columnIndex = builder.build(compactedCf);

        TypeSizes typeSizes = TypeSizes.NATIVE;
        long delSize = DeletionTime.serializer.serializedSize(compactedCf.deletionInfo().getTopLevelDeletion(), typeSizes);
        long dataSize = buffer.getLength() + delSize + typeSizes.sizeof(0);
        out.writeLong(dataSize);
        DeletionInfo.serializer().serializeForSSTable(compactedCf.deletionInfo(), out);
        out.writeInt(builder.writtenAtomCount());
        out.write(buffer.getData(), 0, buffer.getLength());
        return dataSize;
    }

    public void update(MessageDigest digest)
    {
        assert compactedCf != null;
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            DeletionInfo.serializer().serializeForSSTable(compactedCf.deletionInfo(), buffer);
            buffer.writeInt(compactedCf.getColumnCount());
            digest.update(buffer.getData(), 0, buffer.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        compactedCf.updateDigest(digest);
    }

    public boolean isEmpty()
    {
        return compactedCf == null;
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

    public DeletionInfo deletionInfo()
    {
        return compactedCf.deletionInfo();
    }

    /**
     * @return the column index for this row.
     */
    public ColumnIndex index()
    {
        return columnIndex;
    }
}
