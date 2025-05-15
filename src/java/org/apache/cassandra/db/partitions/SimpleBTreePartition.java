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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * Single threaded btree partition, no waste or allocation tracking
 */
public class SimpleBTreePartition extends AbstractBTreePartition implements UpdateFunction<Row, Row>
{
    private final TableMetadata metadata;
    private final UpdateTransaction indexer;
    private BTreePartitionData data = BTreePartitionData.EMPTY;

    public SimpleBTreePartition(DecoratedKey partitionKey, TableMetadata metadata, UpdateTransaction indexer)
    {
        super(partitionKey);
        this.metadata = metadata;
        this.indexer = indexer;
    }

    @Override
    protected BTreePartitionData holder()
    {
        return data;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    protected boolean canHaveShadowedData()
    {
        return true;
    }

    @Override
    public void onAllocatedOnHeap(long heapSize)
    {

    }

    protected BTreePartitionData makeMergedPartition(BTreePartitionData current, PartitionUpdate update)
    {
        DeletionInfo newDeletionInfo = merge(current.deletionInfo, update.deletionInfo());

        RegularAndStaticColumns columns = current.columns;
        RegularAndStaticColumns newColumns = update.columns().mergeTo(columns);
        Row newStatic = mergeStatic(current.staticRow, update.staticRow());

        Object[] tree = BTree.update(current.tree, update.holder().tree, update.metadata().comparator, this);
        EncodingStats newStats = current.stats.mergeWith(update.stats());

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
        return newInfo;
    }

    public Row insert(Row insert)
    {
        indexer.onInserted(insert);
        return insert;
    }

    public Row merge(Row existing, Row update)
    {
        Row reconciled = Rows.merge(existing, update, ColumnData.noOp);
        indexer.onUpdated(existing, reconciled);

        return reconciled;
    }

    public void update(PartitionUpdate update)
    {
        data = makeMergedPartition(data, update);
    }
}
