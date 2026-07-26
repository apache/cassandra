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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.disk.StorageAttachedIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.metrics.IndexGroupMetrics;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.metrics.TableStateMetrics;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryPlan;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableWatcher;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_TABLE_STATE_METRICS_ENABLED;

/**
 * Orchestrates building of storage-attached indices, and manages lifecycle of resources shared between them.
 */
@ThreadSafe
public class StorageAttachedIndexGroup implements Index.Group, INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final Index.Group.Key GROUP_KEY = new Index.Group.Key(StorageAttachedIndexGroup.class);

    private final TableQueryMetrics queryMetrics;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<TableStateMetrics> stateMetrics;
    private final IndexGroupMetrics groupMetrics;

    private final Set<StorageAttachedIndex> indices = ConcurrentHashMap.newKeySet();
    private final ColumnFamilyStore baseCfs;

    private final SSTableContextManager contextManager;
    private final Version version;



    StorageAttachedIndexGroup(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
        this.queryMetrics = new TableQueryMetrics(baseCfs.metadata());
        this.stateMetrics = SAI_TABLE_STATE_METRICS_ENABLED.getBoolean()
                            ? Optional.of(new TableStateMetrics(baseCfs.metadata(), this))
                            : Optional.empty();
        this.groupMetrics = new IndexGroupMetrics(baseCfs.metadata(), this);
        this.contextManager = new SSTableContextManager(baseCfs.getTracker());
        this.version = Version.current(baseCfs.keyspace.getName());

        Tracker tracker = baseCfs.getTracker();
        tracker.subscribe(this);
    }

    @Nullable
    public static StorageAttachedIndexGroup getIndexGroup(ColumnFamilyStore cfs)
    {
        return (StorageAttachedIndexGroup) cfs.indexManager.getIndexGroup(GROUP_KEY);
    }

    @Override
    public Set<StorageAttachedIndex> getIndexes()
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
            // We unregister the per-sstable components first, then we clear the context, which closes all the contexts
            // and unsure there is not more reference to it. When that's done, we can safely remove the component files
            // on disk. Note that we copy the contexts list because we're going to clear the manager, and we need to
            // make sure this does not clear the `contexts` collection below (since it exists to be used after the clear).
            Collection<SSTableContext> contexts = new ArrayList<>(contextManager.allContexts());
            contexts.forEach(context -> {
                var components = context.usedPerSSTableComponents();
                context.sstable.unregisterComponents(components.allAsCustomComponents(), baseCfs.getTracker());
            });

            contextManager.clear();

            contexts.forEach(context -> {
                SSTableWatcher.instance.onIndexDropped(baseCfs.metadata(), context.usedPerSSTableComponents().forWrite());
            });
        }
    }

    @Override
    public void invalidate()
    {
        // in case of dropping table, sstable contexts should already been removed by SSTableListChangedNotification.
        // in case of removing last index from group,  sstable contexts should already been removed by StorageAttachedIndexGroup#removeIndex
        queryMetrics.release();
        groupMetrics.release();
        stateMetrics.ifPresent(TableStateMetrics::release);
        baseCfs.getTracker().unsubscribe(this);
    }

    @Override
    public void unload()
    {
        baseCfs.getTracker().unsubscribe(this);

        contextManager.clear();
        queryMetrics.release();
        groupMetrics.release();
        stateMetrics.ifPresent(TableStateMetrics::release);
    }

    @Override
    public boolean supportsMultipleContains()
    {
        return true;
    }

    @Override
    public boolean supportsDisjunction()
    {
        return true;
    }

    @Override
    public boolean containsIndex(Index index)
    {
        return index instanceof StorageAttachedIndex && indices.contains(index);
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

            @Override
            public void removeRow(Row row)
            {
                forEach(indexer -> indexer.removeRow(row));
            }

            @Override
            public void partitionDelete(DeletionTime deletionTime)
            {
                forEach(indexer -> indexer.partitionDelete(deletionTime));
            }

            @Override
            public void rangeTombstone(RangeTombstone tombstone)
            {
                forEach(indexer -> indexer.rangeTombstone(tombstone));
            }

            private void forEach(Consumer<Index.Indexer> action)
            {
                indexers.forEach(action);
            }
        };
    }

    @Override
    public StorageAttachedIndexQueryPlan queryPlanFor(RowFilter rowFilter)
    {
        return StorageAttachedIndexQueryPlan.create(baseCfs, queryMetrics, indices, rowFilter);
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata, long keyCount)
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.empty(descriptor);
        try
        {
            return new StorageAttachedIndexWriter(indexDescriptor, tableMetadata, indices, tracker, keyCount, baseCfs.metric);
        }
        catch (Throwable t)
        {
            String message = "Unable to create storage-attached index writer on SSTable flush." +
                             " All indexes from this table are going to be marked as non-queryable and will need to be rebuilt.";
            logger.error(indexDescriptor.logMessage(message), t);
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
    public Set<Component> componentsForNewSSTable()
    {
        return IndexDescriptor.componentsForNewlyFlushedSSTable(indices, version);
    }

    @Override
    public Set<Component> activeComponents(SSTableReader sstable)
    {
        return activeComponents(descriptorFor(sstable));
    }

    /**
     * Returns the active components tracked by the supplied descriptor.
     */
    Set<Component> activeComponents(IndexDescriptor indexDescriptor)
    {
        Set<Component> components = indexDescriptor
                                    .perSSTableComponents()
                                    .all()
                                    .stream()
                                    .map(IndexComponent::asCustomComponent)
                                    .collect(Collectors.toSet());

        for (StorageAttachedIndex index : indices)
        {
            indexDescriptor.perIndexComponents(index.getIndexContext())
                           .all()
                           .stream()
                           .map(IndexComponent::asCustomComponent)
                           .forEach(components::add);
        }

        return components;
    }

    @Override
    public void validateComponents(SSTableReader sstable, boolean validateChecksum) throws CorruptIndexException
    {
        IndexDescriptor indexDescriptor = descriptorFor(sstable);
        if (!indexDescriptor.perSSTableComponents().isValid(validateChecksum))
            throw new CorruptIndexException("Failed validation of per-SSTable components", sstable.getFilename());

        for (StorageAttachedIndex index : indices)
        {
            IndexContext ctx = index.getIndexContext();
            if (!indexDescriptor.perIndexComponents(ctx).isValid(validateChecksum))
                throw new CorruptIndexException("Failed validation of per-column components for " + ctx, sstable.getFilename());
        }
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
            boolean validate = notice.fromStreaming() || notice.memtable().isEmpty();
            onSSTableChanged(Collections.emptySet(), notice.added, indices, validate);
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            SSTableListChangedNotification notice = (SSTableListChangedNotification) notification;

            // Avoid validation for index files just written during compaction.
            onSSTableChanged(notice.removed, notice.added, indices, false);
        }
        else if (notification instanceof MemtableRenewedNotification)
        {
            indices.forEach(index -> index.getIndexContext().renewMemtable(((MemtableRenewedNotification) notification).renewed));
        }
        else if (notification instanceof MemtableDiscardedNotification)
        {
            indices.forEach(index -> index.getIndexContext().discardMemtable(((MemtableDiscardedNotification) notification).memtable));
        }
    }

    void prepareIndexSSTablesForRebuild(Collection<SSTableReader> ss, StorageAttachedIndex index)
    {
        try
        {
            index.getIndexContext().prepareSSTablesForRebuild(ss);
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
    public synchronized Set<StorageAttachedIndex> onSSTableChanged(Collection<SSTableReader> removed, Iterable<SSTableReader> added,
                                                            Set<StorageAttachedIndex> indexes, boolean validate)
    {
        Optional<Set<SSTableContext>> optValid = contextManager.update(removed, added, validate, indices);
        if (optValid.isEmpty())
        {
            // This means at least one sstable had invalid per-sstable components, so mark all indexes non-queryable.
            indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);
            return indexes;
        }

        Set<StorageAttachedIndex> incomplete = new HashSet<>();

        for (StorageAttachedIndex index : indexes)
        {
            Set<SSTableContext> invalid = index.getIndexContext().onSSTableChanged(removed, optValid.get(), validate);

            if (!invalid.isEmpty())
            {
                // Mark the index non-queryable, as its view may be compromised, and incomplete, for our callers.
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
        return contextManager.openFiles() + indices.stream().mapToInt(index -> index.getIndexContext().openPerIndexFiles()).sum();
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
        return (int) indices.stream().filter(baseCfs.indexManager::isIndexQueryable).count();
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
        // Note that this only account the "active" files. That is, if we have old versions/generations or incomplete
        // build still on disk, those won't be counted. Counting only "live" data here is consistent with the fact
        // that `TableStateMetrics.diskUsagePercentageOfBaseTable` compare the number obtain from this to the base
        // table "live" disk space use. But there is certainly a small risk for being misleading, and where base
        // tables expose both a "liveDiskSpaceUsed" and "totalDiskSpaceUsed", SAI only exposes "diskUsageBytes", which
        // has we just mentioned is the "live" usage. Might be worth improving at some point.
        return diskUsage() + indices.stream().flatMap(i -> i.getIndexContext().getView().getIndexes().stream())
                                    .mapToLong(SSTableIndex::sizeOfPerColumnComponents).sum();
    }

    public TableMetadata metadata()
    {
        return baseCfs.metadata();
    }

    // Needed by CNDB
    public TableQueryMetrics queryMetrics()
    {
        return queryMetrics;
    }

    // Needed by CNDB
    public Optional<TableStateMetrics> stateMetrics()
    {
        return stateMetrics;
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
     * Returns the {@link IndexDescriptor} for the given {@link SSTableReader} (which must belong to the base table
     * of this group).
     * Note that this always return a non-null value, since all sstables must be indexed, but that descriptor could
     * be "empty" if the sstable has never had an index built yet.
     */
    public IndexDescriptor descriptorFor(SSTableReader sstable)
    {
        return contextManager.getOrLoadIndexDescriptor(sstable, indices);
    }

    /**
     * simulate index loading on restart with index file validation validation
     */
    @VisibleForTesting
    public void unsafeReload()
    {
        contextManager.clear();
        onSSTableChanged(baseCfs.getLiveSSTables(), Collections.emptySet(), indices, false);
        onSSTableChanged(Collections.emptySet(), baseCfs.getLiveSSTables(), indices, true);
    }

    /**
     * Simulate the index going through a restart of node
     */
    @VisibleForTesting
    public void reset()
    {
        contextManager.clear();
        indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);
        onSSTableChanged(baseCfs.getLiveSSTables(), Collections.emptySet(), indices, false);
    }

    /**
     * @return the minimum index version among all the per-sstable index components of this group,
     * or the current version if there are no sstable indexes.
     */
    public Version getMinVersion()
    {
        StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);
        assert indexGroup != null;
        Version minVersion = null;
        for (SSTableReader sstable : baseCfs.getLiveSSTables())
        {
            IndexDescriptor indexDescriptor = indexGroup.descriptorFor(sstable);
            assert indexDescriptor != null;

            Version version = indexDescriptor.perSSTableComponents().version();
            if (minVersion == null || version.compareTo(minVersion) < 0)
                minVersion = version;
        }
        return minVersion == null ? Version.current(baseCfs.metadata.keyspace) : minVersion;
    }
}
