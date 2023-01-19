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
package org.apache.cassandra.index.sai;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.metrics.TableStateMetrics;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Orchestrates building of storage-attached indices, and manages lifecycle of resources shared between them.
 */
@ThreadSafe
public class StorageAttachedIndexGroup implements Index.Group, INotificationConsumer
{
    private final TableQueryMetrics queryMetrics;
    private final TableStateMetrics stateMetrics;
    private final Set<Index> indexes = Sets.newConcurrentHashSet();
    private final ColumnFamilyStore baseCfs;

    StorageAttachedIndexGroup(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
        this.queryMetrics = new TableQueryMetrics(baseCfs.metadata());
        this.stateMetrics = new TableStateMetrics(baseCfs.metadata(), this);

        Tracker tracker = baseCfs.getTracker();
        tracker.subscribe(this);
    }

    @Nullable
    public static StorageAttachedIndexGroup getIndexGroup(ColumnFamilyStore cfs)
    {
        return (StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.class);
    }

    @Override
    public Set<Index> getIndexes()
    {
        return indexes;
    }

    @Override
    public void addIndex(Index index)
    {
        assert index instanceof StorageAttachedIndex;
        indexes.add(index);
    }

    @Override
    public void removeIndex(Index index)
    {
        assert index instanceof StorageAttachedIndex;
        boolean removed = indexes.remove(index);
        assert removed : "Cannot remove non-existing index " + index;
        /*
         * For the in-memory implementation we only need to unsubscribe from the tracker
         */
        if (indexes.isEmpty())
        {
            baseCfs.getTracker().unsubscribe(this);
        }
    }

    @Override
    public void invalidate()
    {
        // in case of dropping table, sstable contexts should already been removed by SSTableListChangedNotification.
        queryMetrics.release();
        stateMetrics.release();
        baseCfs.getTracker().unsubscribe(this);
    }

    @Override
    public boolean containsIndex(Index index)
    {
        return indexes.contains(index);
    }

    @Override
    public Index.Indexer indexerFor(Predicate<Index> indexSelector,
                                    DecoratedKey key,
                                    RegularAndStaticColumns columns,
                                    long nowInSec,
                                    WriteContext ctx,
                                    IndexTransaction.Type transactionType,
                                    Memtable memtable)
    {
        final Set<Index.Indexer> indexers =
                indexes.stream().filter(indexSelector)
                       .map(i -> i.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable))
                       .filter(Objects::nonNull)
                       .collect(Collectors.toSet());

        return indexers.isEmpty() ? null : new Index.Indexer()
        {
            @Override
            public void insertRow(Row row)
            {
                for (Index.Indexer indexer : indexers)
                    indexer.insertRow(row);
            }

            @Override
            public void updateRow(Row oldRow, Row newRow)
            {
                for (Index.Indexer indexer : indexers)
                    indexer.updateRow(oldRow, newRow);
            }
        };
    }

    @Override
    public StorageAttachedIndexQueryPlan queryPlanFor(RowFilter rowFilter)
    {
        return StorageAttachedIndexQueryPlan.create(baseCfs, queryMetrics, indexes, rowFilter);
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata)
    {
        return null;
    }

    @Override
    public boolean handles(IndexTransaction.Type type)
    {
        // to skip CleanupGCTransaction and IndexGCTransaction
        return type == IndexTransaction.Type.UPDATE;
    }

    @Override
    public Set<Component> getComponents()
    {
        return getComponents(indexes);
    }

    // TODO: Currently returns an empty set for in-memory only implementation
    static Set<Component> getComponents(Collection<Index> indices)
    {
        return Collections.emptySet();
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof MemtableRenewedNotification)
        {
            indexes.forEach(index -> ((StorageAttachedIndex)index).getIndexContext().renewMemtable(((MemtableRenewedNotification) notification).renewed));
        }
        else if (notification instanceof MemtableDiscardedNotification)
        {
            indexes.forEach(index -> ((StorageAttachedIndex)index).getIndexContext().discardMemtable(((MemtableDiscardedNotification) notification).memtable));
        }
    }

    /**
     * @return count of queryable indexes
     */
    public int totalQueryableIndexCount()
    {
        return Ints.checkedCast(indexes.stream().filter(baseCfs.indexManager::isIndexQueryable).count());
    }

    /**
     * @return count of indexes
     */
    public int totalIndexCount()
    {
        return indexes.size();
    }

    public TableMetadata metadata()
    {
        return baseCfs.metadata();
    }

    public ColumnFamilyStore table()
    {
        return baseCfs;
    }

    /**
     * Simulate the index going through a restart of node
     */
    @VisibleForTesting
    public void reset()
    {
        indexes.forEach(i -> ((StorageAttachedIndex) i).makeIndexNonQueryable());
    }
}
