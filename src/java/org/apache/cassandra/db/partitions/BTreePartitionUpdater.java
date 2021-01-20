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

package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.HeapAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 *  the function we provide to the trie and btree utilities to perform any row and column replacements
 */
public class BTreePartitionUpdater implements UpdateFunction<Row, Row>
{
    final MemtableAllocator allocator;
    final OpOrder.Group writeOp;
    final UpdateTransaction indexer;
    Row.Builder regularBuilder;
    public long dataSize;
    long heapSize;
    public long colUpdateTimeDelta = Long.MAX_VALUE;

    public BTreePartitionUpdater(MemtableAllocator allocator, OpOrder.Group writeOp, UpdateTransaction indexer)
    {
        this.allocator = allocator;
        this.writeOp = writeOp;
        this.indexer = indexer;
        this.heapSize = 0;
        this.dataSize = 0;
    }

    private Row.Builder builder(Clustering<?> clustering)
    {
        boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;
        // We know we only insert/update one static per PartitionUpdate, so no point in saving the builder
        if (isStatic)
            return allocator.rowBuilder(writeOp);

        if (regularBuilder == null)
            regularBuilder = allocator.rowBuilder(writeOp);
        return regularBuilder;
    }

    public Row apply(Row insert)
    {
        Row data = Rows.copy(insert, builder(insert.clustering())).build();
        indexer.onInserted(insert);

        this.dataSize += data.dataSize();
        allocated(data.unsharedHeapSizeExcludingData());
        return data;
    }

    public Row apply(Row existing, Row update)
    {
        Row.Builder builder = builder(existing.clustering());
        colUpdateTimeDelta = Math.min(colUpdateTimeDelta, Rows.merge(existing, update, builder));

        Row reconciled = builder.build();

        indexer.onUpdated(existing, reconciled);

        dataSize += reconciled.dataSize() - existing.dataSize();
        allocated(reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData());

        return reconciled;
    }

    private DeletionInfo apply(DeletionInfo existing, DeletionInfo update)
    {
        if (update.isLive() || !update.mayModify(existing))
            return existing;

        if (!update.getPartitionDeletion().isLive())
            indexer.onPartitionDeletion(update.getPartitionDeletion());

        if (update.hasRanges())
            update.rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);

        // Like for rows, we have to clone the update in case internal buffers (when it has range tombstones) reference
        // memory we shouldn't hold into. But we don't ever store this off-heap currently so we just default to the
        // HeapAllocator (rather than using 'allocator').
        DeletionInfo newInfo = existing.mutableCopy().add(update.copy(HeapAllocator.instance));
        allocated(newInfo.unsharedHeapSize() - existing.unsharedHeapSize());
        return newInfo;
    }

    public BTreePartitionData mergePartitions(BTreePartitionData current, final PartitionUpdate update)
    {
        if (current == null)
        {
            current = BTreePartitionData.EMPTY;
            this.allocated(BTreePartitionData.UNSHARED_HEAP_SIZE);
        }

        try
        {
            indexer.start();

            return makeMergedPartition(current, update);
        }
        finally
        {
            indexer.commit();
            reportAllocatedMemory();
        }
    }

    protected BTreePartitionData makeMergedPartition(BTreePartitionData current, PartitionUpdate update)
    {
        DeletionInfo newDeletionInfo = apply(current.deletionInfo, update.deletionInfo());

        RegularAndStaticColumns columns = current.columns;
        RegularAndStaticColumns newColumns = update.columns().mergeTo(columns);
        allocated(newColumns.unsharedHeapSize() - columns.unsharedHeapSize());
        Row newStatic = update.staticRow();
        newStatic = newStatic.isEmpty()
                    ? current.staticRow
                    : (current.staticRow.isEmpty()
                       ? this.apply(newStatic)
                       : this.apply(current.staticRow, newStatic));

        Object[] tree = BTree.update(current.tree, update.metadata().comparator, update, update.rowCount(), this);
        EncodingStats newStats = current.stats.mergeWith(update.stats());
        allocated(newStats.unsharedHeapSize() - current.stats.unsharedHeapSize());

        return new BTreePartitionData(newColumns, tree, newDeletionInfo, newStatic, newStats);
    }

    public boolean abortEarly()
    {
        return false;
    }

    public void allocated(long heapSize)
    {
        this.heapSize += heapSize;
    }

    public void reportAllocatedMemory()
    {
        allocator.onHeap().adjust(heapSize, writeOp);
    }
}
