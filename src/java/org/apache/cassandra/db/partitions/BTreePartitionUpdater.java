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

import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 *  the function we provide to the trie and btree utilities to perform any row and column replacements
 */
public class BTreePartitionUpdater implements UpdateFunction<Row, Row>, ColumnData.PostReconciliationFunction
{
    final MemtableAllocator allocator;
    final OpOrder.Group writeOp;
    final Cloner cloner;
    final UpdateTransaction indexer;
    public long dataSize;
    long heapSize;
    public long colUpdateTimeDelta = Long.MAX_VALUE;

    public BTreePartitionUpdater(MemtableAllocator allocator, Cloner cloner, OpOrder.Group writeOp, UpdateTransaction indexer)
    {
        this.allocator = allocator;
        this.cloner = cloner;
        this.writeOp = writeOp;
        this.indexer = indexer;
        this.heapSize = 0;
        this.dataSize = 0;
    }

    public BTreePartitionData mergePartitions(BTreePartitionData current, final PartitionUpdate update)
    {
        if (current == null)
        {
            current = BTreePartitionData.EMPTY;
            onAllocatedOnHeap(BTreePartitionData.UNSHARED_HEAP_SIZE);
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
        DeletionInfo newDeletionInfo = merge(current.deletionInfo, update.deletionInfo());

        RegularAndStaticColumns columns = current.columns;
        RegularAndStaticColumns newColumns = update.columns().mergeTo(columns);
        onAllocatedOnHeap(newColumns.unsharedHeapSize() - columns.unsharedHeapSize());
        Row newStatic = mergeStatic(current.staticRow, update.staticRow());

        Object[] tree = BTree.update(current.tree, update.holder().tree, update.metadata().comparator, this);
        EncodingStats newStats = current.stats.mergeWith(update.stats());
        onAllocatedOnHeap(newStats.unsharedHeapSize() - current.stats.unsharedHeapSize());

        return new BTreePartitionData(newColumns, tree, newDeletionInfo, newStatic, newStats);
    }

    private Row mergeStatic(Row current, Row update)
    {
        if (update.isEmpty())
            return current;
        if (current.isEmpty())
            return insert(update);

        return merge(current, update);
    }

    private DeletionInfo merge(DeletionInfo existing, DeletionInfo update)
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
        DeletionInfo newInfo = existing.mutableCopy().add(update.clone(HeapCloner.instance));
        onAllocatedOnHeap(newInfo.unsharedHeapSize() - existing.unsharedHeapSize());
        return newInfo;
    }

    @Override
    public Row insert(Row insert)
    {
        Row data = insert.clone(cloner);
        indexer.onInserted(insert);

        dataSize += data.dataSize();
        heapSize += data.unsharedHeapSizeExcludingData();
        return data;
    }

    public Row merge(Row existing, Row update)
    {
        Row reconciled = Rows.merge(existing, update, this);
        indexer.onUpdated(existing, reconciled);

        return reconciled;
    }

    public Cell<?> merge(Cell<?> previous, Cell<?> insert)
    {
        if (insert == previous)
            return insert;

        long timeDelta = Math.abs(insert.timestamp() - previous.timestamp());
        if (timeDelta < colUpdateTimeDelta)
            colUpdateTimeDelta = timeDelta;
        if (cloner != null)
            insert = cloner.clone(insert);
        dataSize += insert.dataSize() - previous.dataSize();
        heapSize += insert.unsharedHeapSizeExcludingData() - previous.unsharedHeapSizeExcludingData();
        return insert;
    }

    public ColumnData insert(ColumnData insert)
    {
        if (cloner != null)
            insert = insert.clone(cloner);
        dataSize += insert.dataSize();
        heapSize += insert.unsharedHeapSizeExcludingData();
        return insert;
    }

    @Override
    public void delete(ColumnData existing)
    {
        dataSize -= existing.dataSize();
        heapSize -= existing.unsharedHeapSizeExcludingData();
    }

    public void onAllocatedOnHeap(long heapSize)
    {
        this.heapSize += heapSize;
    }

    public void reportAllocatedMemory()
    {
        allocator.onHeap().adjust(heapSize, writeOp);
    }
}
