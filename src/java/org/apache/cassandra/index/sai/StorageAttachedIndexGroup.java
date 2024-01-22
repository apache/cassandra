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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.metrics.IndexGroupMetrics;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.metrics.TableStateMetrics;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
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
@ThreadSafe
public class StorageAttachedIndexGroup implements Index.Group, INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexGroup.class);

    public static final Index.Group.Key GROUP_KEY = new Index.Group.Key(StorageAttachedIndexGroup.class);

    private final TableQueryMetrics queryMetrics;
    private final TableStateMetrics stateMetrics;
    private final IndexGroupMetrics groupMetrics;
    private final Set<StorageAttachedIndex> indexes = ConcurrentHashMap.newKeySet();
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
        return (StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(StorageAttachedIndexGroup.GROUP_KEY);
    }

    @Override
    public Set<Index> getIndexes()
    {
        return ImmutableSet.copyOf(indexes);
    }

    @Override
    public void addIndex(Index index)
    {
        assert index instanceof StorageAttachedIndex;
        indexes.add((StorageAttachedIndex) index);
    }

    @Override
    public void removeIndex(Index index)
    {
        assert index instanceof StorageAttachedIndex;
        boolean removed = indexes.remove(index);
        assert removed : "Cannot remove non-existing index " + index;
        /*
         * per index files are dropped via {@link StorageAttachedIndex#getInvalidateTask()}
         */
        if (indexes.isEmpty())
        {
            for (SSTableReader sstable : contextManager.sstables())
                sstable.unregisterComponents(IndexDescriptor.create(sstable).getLivePerSSTableComponents(), baseCfs.getTracker());
            deletePerSSTableFiles(baseCfs.getLiveSSTables());
        }
    }

    @Override
    public void invalidate()
    {
        // in case of removing last index from group, sstable contexts should already been removed by removeIndex
        queryMetrics.release();
        groupMetrics.release();
        stateMetrics.release();
        baseCfs.getTracker().unsubscribe(this);
    }

    @Override
    @SuppressWarnings("SuspiciousMethodCalls")
    public boolean containsIndex(Index index)
    {
        return indexes.contains(index);
    }

    @Override
    public boolean isSingleton()
    {
        return false;
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
        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, tableMetadata.partitioner, tableMetadata.comparator);
        try
        {
            return StorageAttachedIndexWriter.createFlushObserverWriter(indexDescriptor, indexes, tracker);
        }
        catch (Throwable t)
        {
            String message = "Unable to create storage-attached index writer on SSTable flush." +
                             " All indexes from this table are going to be marked as non-queryable and will need to be rebuilt.";
            logger.error(indexDescriptor.logMessage(message), t);
            indexes.forEach(StorageAttachedIndex::makeIndexNonQueryable);
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
        return getComponents(indexes);
    }

    private Set<Component> getComponents(Collection<StorageAttachedIndex> indices)
    {
        Set<Component> components = Version.LATEST.onDiskFormat()
                                                  .perSSTableIndexComponents(baseCfs.metadata.get().comparator.size() > 0)
                                                  .stream()
                                                  .map(Version.LATEST::makePerSSTableComponent)
                                                  .collect(Collectors.toSet());
        indices.forEach(index -> components.addAll(index.getComponents()));
        return components;
    }

    // This differs from getComponents in that it only returns index components that exist on disk.
    // It avoids errors being logged by the SSTable.readTOC method when we have an empty index.
    @VisibleForTesting
    public static Set<Component> getLiveComponents(SSTableReader sstable, Collection<StorageAttachedIndex> indices)
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
        Set<Component> components = indexDescriptor.getLivePerSSTableComponents();
        indices.forEach(index -> components.addAll(indexDescriptor.getLivePerIndexComponents(index.termType(), index.identifier())));
        return components;
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        // unfortunately, we can only check the type of notification via instanceof :(
        if (notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification notice = (SSTableAddedNotification) notification;

            // Avoid validation for index files just written following Memtable flush. Otherwise, the new SSTables have
            // come either from import, streaming, or a standalone tool, where they have also already been validated.
            onSSTableChanged(Collections.emptySet(), notice.added, indexes, IndexValidation.NONE);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification notice = (SSTableListChangedNotification) notification;

            // Avoid validation for index files just written during compaction.
            onSSTableChanged(notice.removed, notice.added, indexes, IndexValidation.NONE);
        }
        else if (notification instanceof MemtableRenewedNotification)
        {
            indexes.forEach(index -> index.memtableIndexManager().renewMemtable(((MemtableRenewedNotification) notification).renewed));
        }
        else if (notification instanceof MemtableDiscardedNotification)
        {
            indexes.forEach(index -> index.memtableIndexManager().discardMemtable(((MemtableDiscardedNotification) notification).memtable));
        }
    }

    void deletePerSSTableFiles(Collection<SSTableReader> sstables)
    {
        contextManager.release(sstables);
        sstables.forEach(sstableReader -> IndexDescriptor.create(sstableReader).deletePerSSTableIndexComponents());
    }

    void dropIndexSSTables(Collection<SSTableReader> ss, StorageAttachedIndex index)
    {
        try
        {
            index.drop(ss);
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
                                                            Set<StorageAttachedIndex> indexes, IndexValidation validation)
    {
        Pair<Set<SSTableContext>, Set<SSTableReader>> results = contextManager.update(removed, added, validation);

        if (!results.right.isEmpty())
        {
            results.right.forEach(sstable -> {
                IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
                indexDescriptor.deletePerSSTableIndexComponents();
                // Column indexes are invalid if their SSTable-level components are corrupted so delete
                // their associated index files and mark them non-queryable.
                indexes.forEach(index -> {
                    indexDescriptor.deleteColumnIndex(index.termType(), index.identifier());
                    index.makeIndexNonQueryable();
                });
            });
            return indexes;
        }

        Set<StorageAttachedIndex> incomplete = new HashSet<>();

        for (StorageAttachedIndex index : indexes)
        {
            Collection<SSTableContext> invalid = index.onSSTableChanged(removed, results.left, validation);

            if (!invalid.isEmpty())
            {
                // Delete the index files and mark the index non-queryable, as its view may be compromised,
                // and incomplete, for our callers:
                invalid.forEach(context -> context.indexDescriptor.deleteColumnIndex(index.termType(), index.identifier()));
                index.makeIndexNonQueryable();
                incomplete.add(index);
            }
        }
        return incomplete;
    }

    @Override
    public boolean validateSSTableAttachedIndexes(Collection<SSTableReader> sstables, boolean throwOnIncomplete, boolean validateChecksum)
    {
        boolean complete = true;

        for (SSTableReader sstable : sstables)
        {
            IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);

            if (indexDescriptor.isPerSSTableIndexBuildComplete())
            {
                indexDescriptor.validatePerSSTableComponents(IndexValidation.CHECKSUM, validateChecksum, true);

                for (StorageAttachedIndex index : indexes)
                {
                    if (indexDescriptor.isPerColumnIndexBuildComplete(index.identifier()))
                        indexDescriptor.validatePerIndexComponents(index.termType(), index.identifier(), IndexValidation.CHECKSUM, validateChecksum, true);
                    else if (throwOnIncomplete)
                        throw new IllegalStateException(indexDescriptor.logMessage("Incomplete per-column index build for SSTable " + sstable.descriptor.toString()));
                    else
                        complete = false;
                }
            }
            else if (throwOnIncomplete)
            {
                throw new IllegalStateException(indexDescriptor.logMessage("Incomplete per-SSTable index build" + sstable.descriptor.toString()));
            }
            else
            {
                complete = false;
            }
        }

        return complete;
    }

    /**
     * open index files by checking number of {@link SSTableContext} and {@link SSTableIndex},
     * so transient open files during validation and files that are still open for in-flight requests will not be tracked.
     *
     * @return total number of open files for all {@link StorageAttachedIndex}es.
     */
    public int openIndexFiles()
    {
        return contextManager.openFiles() + indexes.stream().mapToInt(StorageAttachedIndex::openPerColumnIndexFiles).sum();
    }

    /**
     * @return total disk usage (in bytes) of all per-sstable index files
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
        return (int) indexes.stream().filter(i -> baseCfs.indexManager.isIndexBuilding(i.getIndexMetadata().name)).count();
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

    /**
     * @return total disk usage of all per-sstable index files and per-column index files
     */
    public long totalDiskUsage()
    {
        return diskUsage() + indexes.stream().flatMap(index -> index.view().getIndexes().stream())
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
     * simulate index loading on restart with index file validation
     */
    @VisibleForTesting
    public void unsafeReload()
    {
        contextManager.clear();
        onSSTableChanged(baseCfs.getLiveSSTables(), Collections.emptySet(), indexes, IndexValidation.NONE);
        onSSTableChanged(Collections.emptySet(), baseCfs.getLiveSSTables(), indexes, IndexValidation.HEADER_FOOTER);
    }

    /**
     * Simulate the index going through a restart of node
     */
    @VisibleForTesting
    public void reset()
    {
        contextManager.clear();
        indexes.forEach(StorageAttachedIndex::makeIndexNonQueryable);
        onSSTableChanged(baseCfs.getLiveSSTables(), Collections.emptySet(), indexes, IndexValidation.NONE);
    }
}
