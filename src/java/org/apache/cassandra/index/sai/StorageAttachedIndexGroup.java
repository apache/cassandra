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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.IndexGroupMetrics;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.metrics.TableStateMetrics;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

/**
 * Orchestrates building of storage-attached indices, and manages lifecycle of resources shared between them.
 */
public class StorageAttachedIndexGroup implements Index.Group, INotificationConsumer, Iterable<StorageAttachedIndex>
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final TableQueryMetrics queryMetrics;
    private final TableStateMetrics stateMetrics;
    private final IndexGroupMetrics groupMetrics;
    
    private final Set<StorageAttachedIndex> indices = new HashSet<>();
    private final ColumnFamilyStore baseCfs;

    private final SSTableContextManager contextManager;

    StorageAttachedIndexGroup(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
        this.queryMetrics = new TableQueryMetrics(baseCfs.metadata());
        this.stateMetrics = new TableStateMetrics(baseCfs.metadata(), this);
        this.groupMetrics = new IndexGroupMetrics(baseCfs.metadata(), this);
        this.contextManager = new SSTableContextManager();

        Tracker tracker = baseCfs.getTracker();
        tracker.subscribe(this);
    }

    @Nullable
    public static StorageAttachedIndexGroup getIndexGroup(ColumnFamilyStore cfs)
    {
        return (StorageAttachedIndexGroup)cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.class);
    }

    @Override
    public Set<Index> getIndexes()
    {
        return ImmutableSet.copyOf(indices);
    }

    @Override
    public void addIndex(Index index)
    {
        assert index instanceof StorageAttachedIndex;
        indices.add((StorageAttachedIndex) index);
    }

    @Override
    public void removeIndex(Index index)
    {
        assert index instanceof StorageAttachedIndex;
        boolean removed = indices.remove(index);
        assert removed : "Cannot remove non-existing index " + index;
        /*
         * per index files are dropped via {@link StorageAttachedIndex#getInvalidateTask()}
         */
        if (indices.isEmpty())
        {
            Set<Component> toRemove = new HashSet<>(IndexComponents.PER_SSTABLE_COMPONENTS);
            for (SSTableReader sstable : contextManager.sstables())
                sstable.unregisterComponents(toRemove, baseCfs.getTracker());

            deletePerSSTableFiles(baseCfs.getLiveSSTables());
            baseCfs.getTracker().unsubscribe(this);
        }
    }

    @Override
    public void invalidate()
    {
        // in case of dropping table, sstable contexts should already been removed by SSTableListChangedNotification.
        queryMetrics.release();
        groupMetrics.release();
        stateMetrics.release();
        baseCfs.getTracker().unsubscribe(this);
    }

    @Override
    public boolean supportsMultipleContains()
    {
        return true;
    }

    @Override
    public boolean containsIndex(Index index)
    {
        return index instanceof StorageAttachedIndex && indices.contains(index);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public Iterator<StorageAttachedIndex> iterator()
    {
        return indices.iterator();
    }

    @Override
    public Index.Indexer indexerFor(Predicate<Index> indexSelector,
                                    DecoratedKey key,
                                    RegularAndStaticColumns columns,
                                    int nowInSec,
                                    WriteContext ctx,
                                    IndexTransaction.Type transactionType,
                                    Memtable memtable)
    {
        final Set<Index.Indexer> indexers =
                indices.stream().filter(indexSelector)
                                .map(i -> i.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toSet());

        return indexers.isEmpty() ? null : new StorageAttachedIndex.IndexerAdapter()
        {
            @Override
            public void insertRow(Row row)
            {
                forEach(indexer -> indexer.insertRow(row));
            }

            @Override
            public void updateRow(Row oldRow, Row newRow)
            {
                forEach(indexer -> indexer.updateRow(oldRow, newRow));
            }

            private void forEach(Consumer<Index.Indexer> action)
            {
                indexers.forEach(action::accept);
            }
        };
    }

    @Override
    public StorageAttachedIndexQueryPlan queryPlanFor(RowFilter rowFilter)
    {
        return StorageAttachedIndexQueryPlan.create(baseCfs, queryMetrics, indices, rowFilter);
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata)
    {
        try
        {
            return new StorageAttachedIndexWriter(descriptor, indices, tracker, tableMetadata.params.compression);
        }
        catch (Throwable t)
        {
            String message = "Unable to create storage-attached index writer on SSTable flush. All indexes from this table are going to be marked as non-queryable and will need to be rebuilt.";
            logger.error(String.format("[%s.%s.*] %s", descriptor.ksname, descriptor.cfname, message), t);
            indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);
            return null;
        }
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
        return getComponents(indices);
    }

    static Set<Component> getComponents(Collection<StorageAttachedIndex> indices)
    {
        Set<Component> components = new HashSet<>(IndexComponents.PER_SSTABLE_COMPONENTS);
        indices.forEach(index -> components.addAll(index.getComponents()));
        return components;
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        // unfortunately, we can only check the type of notification via instanceof :(
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification notice = (SSTableAddedNotification) notification;

            // Avoid validation for index files just written following Memtable flush. ZCS streaming should
            // validate index checksum.
            boolean validate = !notice.memtable().isPresent();
            onSSTableChanged(Collections.emptySet(), notice.added, indices, validate, false);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification notice = (SSTableListChangedNotification) notification;

            // Avoid validation for index files just written during compaction.
            onSSTableChanged(notice.removed, notice.added, indices, false, false);
        }
        else if (notification instanceof MemtableRenewedNotification)
        {
            indices.forEach(index -> index.getContext().renewMemtable(((MemtableRenewedNotification) notification).renewed));
        }
        else if (notification instanceof MemtableDiscardedNotification)
        {
            indices.forEach(index -> index.getContext().discardMemtable(((MemtableDiscardedNotification) notification).memtable));
        }
    }

    void deletePerSSTableFiles(Collection<SSTableReader> sstables)
    {
        contextManager.release(sstables);
        sstables.forEach(sstableReader -> IndexComponents.deletePerSSTableIndexComponents(sstableReader.descriptor));
    }

    void dropIndexSSTables(Collection<SSTableReader> ss, StorageAttachedIndex index)
    {
        try
        {
            index.getContext().drop(ss);
        }
        catch (Throwable t)
        {
            // Mark the index non-queryable, as its view may be compromised.
            index.makeIndexNonQueryable();

            throw Throwables.unchecked(t);
        }
    }

    /**
     * This method is synchronized to avoid concurrent initialization tasks validating same per-SSTable files.
     *
     * @return the set of column indexes that were marked as non-queryable as a result of their per-SSTable index
     * files being corrupt or being unable to successfully update their views
     */
    synchronized Set<StorageAttachedIndex> onSSTableChanged(Collection<SSTableReader> removed, Iterable<SSTableReader> added,
                                                            Set<StorageAttachedIndex> indexes, boolean validate, boolean rename)
    {
        Pair<Set<SSTableContext>, Set<SSTableReader>> results = contextManager.update(removed, added, validate);

        if (!results.right.isEmpty())
        {
            results.right.forEach(sstable -> {
                IndexComponents.deletePerSSTableIndexComponents(sstable.descriptor);
                // Column indexes are invalid if their SSTable-level components are corrupted so delete
                // their associated index files and mark them non-queryable.
                indices.forEach(index -> {
                    index.deleteIndexFiles(sstable);
                    index.makeIndexNonQueryable();
                });
            });
            return indices;
        }

        Set<StorageAttachedIndex> incomplete = new HashSet<>();

        for (StorageAttachedIndex index : indexes)
        {
            Set<SSTableContext> invalid = index.getContext().onSSTableChanged(removed, results.left, validate, rename);

            if (!invalid.isEmpty())
            {
                // Delete the index files and mark the index non-queryable, as its view may be compromised,
                // and incomplete, for our callers:
                invalid.forEach(context -> index.deleteIndexFiles(context.sstable()));
                index.makeIndexNonQueryable();
                incomplete.add(index);
            }
        }
        return incomplete;
    }

    /**
     * open index files by checking number of {@link SSTableContext} and {@link SSTableIndex},
     * so transient open files during validation and files that are still open for in-flight requests will not be tracked.
     *
     * @return total number of open files for all {@link StorageAttachedIndex}es.
     */
    public int openIndexFiles()
    {
        return contextManager.openFiles() + indices.stream().mapToInt(index -> index.getContext().openPerIndexFiles()).sum();
    }

    /**
     * @return total disk usage of all per-sstable index files
     */
    public long diskUsage()
    {
        return contextManager.diskUsage();
    }

    /**
     * @return count of indexes building
     */
    public int totalIndexBuildsInProgress()
    {
        return (int) indices.stream().filter(i -> baseCfs.indexManager.isIndexBuilding(i.getIndexMetadata().name)).count();
    }

    /**
     * @return count of queryable indexes
     */
    public int totalQueryableIndexCount()
    {
        return (int) indices.stream().filter(i -> baseCfs.indexManager.isIndexQueryable(i)).count();
    }

    /**
     * @return count of indexes
     */
    public int totalIndexCount()
    {
        return indices.size();
    }

    /**
     * @return total disk usage of all per-sstable index files and per-column index files
     */
    public long totalDiskUsage()
    {
        return diskUsage() + indices.stream().flatMap(i -> i.getContext().getView().getIndexes().stream())
                                    .mapToLong(SSTableIndex::sizeOfPerColumnComponents).sum();
    }

    public TableMetadata metadata()
    {
        return baseCfs.metadata();
    }

    public ColumnFamilyStore table()
    {
        return baseCfs;
    }

    @VisibleForTesting
    public SSTableContextManager sstableContextManager()
    {
        return contextManager;
    }

    /**
     * simulate index loading on restart with index file validation validation
     */
    @VisibleForTesting
    public void unsafeReload()
    {
        contextManager.clear();
        onSSTableChanged(baseCfs.getLiveSSTables(), Collections.emptySet(), indices, false, false);
        onSSTableChanged(Collections.emptySet(), baseCfs.getLiveSSTables(), indices, true, true);
    }

    /**
     * Simulate the index going through a restart of node
     */
    @VisibleForTesting
    public void reset()
    {
        contextManager.clear();
        indices.forEach(index -> index.makeIndexNonQueryable());
        onSSTableChanged(baseCfs.getLiveSSTables(), Collections.emptySet(), indices, false, false);
    }
}
