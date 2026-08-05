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
import java.util.concurrent.TimeUnit;
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
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
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
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.MemtableDiscardedNotification;
import org.apache.cassandra.notifications.MemtableRenewedNotification;
import org.apache.cassandra.notifications.MemtableSwitchedNotification;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

/**
 * Orchestrates building of storage-attached indices, and manages lifecycle of resources shared between them.
 */
@ThreadSafe
public class StorageAttachedIndexGroup implements Index.Group, INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexGroup.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    // Both take their arguments in the order (keyspace, table, count, index, example descriptor). The leading argument
    // must not be a number: NoSpamLogStatement#warn is overloaded on (long nowNanos, Object...) as well as (Object...),
    // and a numeric first argument makes the call ambiguous.
    private static final String UNINDEXED_SSTABLES_WARNING =
    "{}.{} has {} live sstable(s) with no storage-attached index components while index {} is queryable, so rows in" +
    " them match no index predicate and query results are silently incomplete; e.g. {}. This is expected only while" +
    " an index build is in progress -- otherwise something produced an sstable without running the index writers," +
    " and `nodetool rebuild_index` is the repair.";

    private static final String UNINDEXED_COLUMNS_WARNING =
    "{}.{} has {} live sstable(s) with per-sstable index components but no completed per-column components for" +
    " queryable index {}, so they are absent from its view and rows in them match no predicate of that index;" +
    " e.g. {}. This is expected only while that index is being built for those sstables -- as it is for a short" +
    " while after `nodetool import` or streaming -- otherwise `nodetool rebuild_index` is the repair.";

    public static final Index.Group.Key GROUP_KEY = new Index.Group.Key(StorageAttachedIndexGroup.class);

    private final TableQueryMetrics queryMetrics;
    private final TableStateMetrics stateMetrics;
    private final IndexGroupMetrics groupMetrics;
    private final Set<StorageAttachedIndex> indexes = ConcurrentHashMap.newKeySet();
    private final ColumnFamilyStore baseCfs;

    private final SSTableContextManager contextManager;

    /**
     * Rate limiters for the two warnings above. {@link NoSpamLogger} keys its limiter on the string it is handed, so
     * warning through a shared constant message would let the first affected table claim the interval and leave every
     * other affected table silent -- and this is the only signal an operator gets for silently incomplete results.
     */
    private final NoSpamLogger.NoSpamLogStatement unindexedSSTableWarning;
    private final NoSpamLogger.NoSpamLogStatement unindexedColumnWarning;

    StorageAttachedIndexGroup(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
        this.queryMetrics = new TableQueryMetrics(baseCfs.metadata());
        this.stateMetrics = new TableStateMetrics(baseCfs.metadata(), this);
        this.groupMetrics = new IndexGroupMetrics(baseCfs.metadata(), this);
        this.contextManager = new SSTableContextManager();

        String tableName = baseCfs.metadata().keyspace + '.' + baseCfs.metadata().name;
        this.unindexedSSTableWarning = noSpamLogger.getStatement("unindexed sstables of " + tableName, UNINDEXED_SSTABLES_WARNING);
        this.unindexedColumnWarning = noSpamLogger.getStatement("unindexed columns of " + tableName, UNINDEXED_COLUMNS_WARNING);

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
                // SAI does not index deletions, as these are resolved during post-filtering.
                if (row.hasLiveData(nowInSec, false))
                    for (Index.Indexer indexer : indexers)
                        indexer.insertRow(row);
            }

            @Override
            public void updateRow(Row oldRow, Row newRow)
            {
                // SAI does not index deletions, as these are resolved during post-filtering.
                if (newRow.hasLiveData(nowInSec, false))
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
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, ILifecycleTransaction txn, TableMetadata tableMetadata)
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, tableMetadata.partitioner, tableMetadata.comparator);
        try
        {
            return StorageAttachedIndexWriter.createFlushObserverWriter(indexDescriptor, indexes, txn);
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
        else if (notification instanceof MemtableSwitchedNotification)
        {
            indexes.forEach(index -> index.memtableIndexManager().maybeInitializeMemtableIndex(((MemtableSwitchedNotification) notification).next));
        }
        else if (notification instanceof MemtableDiscardedNotification)
        {
            indexes.forEach(index -> index.memtableIndexManager().discardMemtable(((MemtableDiscardedNotification) notification).memtable));
        }
    }

    void deletePerSSTableFiles(Collection<SSTableReader> sstables)
    {
        // These sstables stay live without any per-sstable components until a build writes them again, so they have to
        // keep counting as unindexed for the whole of that window. The exception is the last index going away: nothing
        // will rebuild them then, and there is no view left for them to be missing from.
        if (indexes.isEmpty())
            contextManager.release(sstables);
        else
            contextManager.releaseUnindexed(sstables);

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

        // Only once the views of these indexes have been updated, so that sstables this call was still working
        // through are not mistaken for holes in them.
        warnOnUnindexedSSTables(indexes);

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

    @Override
    public boolean supportsL0Shards()
    {
        for (StorageAttachedIndex index : indexes)
            if (!index.supportsL0Shards())
                return false;

        // All indexes must support L0 sharding for the flush to shard at L0
        return true;
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
     * Live sstables that carry no per-sstable index components and are therefore absent from every index's view.
     * Rows in them are readable but match no index predicate.
     * <p>
     * Non-zero is normal and transient while an index build is running -- an initial build, or a rebuild, which strips
     * the components of each sstable before rewriting them. Non-zero while an index is QUERYABLE means results are
     * silently incomplete, which is why this is a gauge and not just a log line: it is the standing signal for a
     * failure mode that throws nothing and, before it existed, showed up nowhere at all.
     * <p>
     * Counts only the per-sstable half of the hole, and deliberately so. An sstable can have per-sstable components
     * and still be missing from one index's view, for want of that index's own per-column completion marker (see
     * {@code IndexViewManager#getBuiltIndexes}); that is the same silent query hole, but it is not countable from a
     * gauge poll, because a view legitimately lags the live sstable set until the owning index's build or startup task
     * has reached it -- every sstable is absent from index B's view for the whole of {@code CREATE INDEX B}, and from
     * a restarted index's view until its initialization task runs. Distinguishing that from a real hole needs to know
     * whether the view was just refreshed, which only the caller of {@link #onSSTableChanged} knows, so the per-column
     * half is reported by {@link #warnOnUnindexedSSTables} from there and is not reflected in this number.
     */
    public int unindexedSSTables()
    {
        return contextManager.incompleteSSTableCount();
    }

    /**
     * Warn -- rate limited per table, since this is reached on every sstable list change -- when live sstables are
     * missing from the view of an index that is already queryable, meaning its results are silently incomplete. Both
     * ways that happens are covered: no per-sstable components at all, and per-sstable components without this
     * index's per-column completion marker.
     * <p>
     * Deliberately not an error and not a makeIndexNonQueryable: by the time this is observed the sstable is already
     * live, so refusing to answer queries would trade silently-incomplete results for no results, and only the
     * operator can decide that. The fix is to rebuild, which the message says.
     *
     * @param updated the indexes whose views this {@link #onSSTableChanged} call has just refreshed
     */
    private void warnOnUnindexedSSTables(Set<StorageAttachedIndex> updated)
    {
        // A snapshot, since this both tests the set and reads an example out of it, while release() removes from it
        // without holding this monitor.
        Set<SSTableReader> unindexed = contextManager.incompleteSSTables();

        if (!unindexed.isEmpty())
        {
            for (StorageAttachedIndex index : indexes)
            {
                if (!baseCfs.indexManager.isIndexQueryable(index))
                    continue;

                unindexedSSTableWarning.warn(baseCfs.getKeyspaceName(), baseCfs.getTableName(), unindexed.size(),
                                             index.identifier(), unindexed.iterator().next().descriptor);
                break;
            }
        }

        // An sstable with per-sstable components can still be missing from a single index's view, for want of that
        // index's per-column completion marker, which IndexViewManager#getBuiltIndexes only notes at DEBUG. Checked
        // only for the indexes whose views were just refreshed: any other index's view predates this notification, so
        // sstables missing from it may simply be work that has not reached it yet (an index still building, or another
        // index's initialization task at startup), which would make this fire on every second CREATE INDEX.
        for (StorageAttachedIndex index : updated)
        {
            if (!baseCfs.indexManager.isIndexQueryable(index))
                continue;

            View view = index.view();
            Descriptor example = null;
            int missing = 0;

            for (SSTableReader sstable : contextManager.sstables())
            {
                // Compacted sstables are left out of every view by design; their contexts go away with the
                // notification that retires them.
                if (sstable.isMarkedCompacted() || view.containsSSTable(sstable))
                    continue;

                missing++;
                example = sstable.descriptor;
            }

            if (missing > 0)
            {
                unindexedColumnWarning.warn(baseCfs.getKeyspaceName(), baseCfs.getTableName(), missing,
                                            index.identifier(), example);
                break;
            }
        }
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
