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
package org.apache.cassandra.index;

import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.FutureTask;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index.IndexBuildingSupport;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.CleanupTransaction;
import org.apache.cassandra.index.transactions.CompactionTransaction;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.*;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.FORCE_DEFAULT_INDEXING_PAGE_SIZE;
import static org.apache.cassandra.utils.ExecutorUtils.awaitTermination;
import static org.apache.cassandra.utils.ExecutorUtils.shutdown;

/**
 * Handles the core maintenance functionality associated with indexes: adding/removing them to or from
 * a table, (re)building during bootstrap or other streaming operations, flushing, reloading metadata
 * and so on.
 * <br><br>
 * The Index interface defines a number of methods which return {@code Callable<?>}. These are primarily the
 * management tasks for an index implementation. Most of them are currently executed in a blocking
 * fashion via submission to SIM's blockingExecutor. This provides the desired behaviour in pretty
 * much all cases, as tasks like flushing an index needs to be executed synchronously to avoid potentially
 * deadlocking on the FlushWriter or PostFlusher. Several of these {@code Callable<?>} returning methods on Index could
 * then be defined with as void and called directly from SIM (rather than being run via the executor service).
 * Separating the task defintion from execution gives us greater flexibility though, so that in future, for example,
 * if the flush process allows it we leave open the possibility of executing more of these tasks asynchronously.
 * <br><br>
 * The primary exception to the above is the Callable returned from Index#addIndexedColumn. This may
 * involve a significant effort, building a new index over any existing data. We perform this task asynchronously;
 * as it is called as part of a schema update, which we do not want to block for a long period. Building non-custom
 * indexes is performed on the CompactionManager.
 * <br><br>
 * This class also provides instances of processors which listen to updates to the base table and forward to
 * registered Indexes the info required to keep those indexes up to date.
 * There are two variants of these processors, each with a factory method provided by SIM:
 * IndexTransaction: deals with updates generated on the regular write path.
 * CleanupTransaction: used when partitions are modified during compaction or cleanup operations.
 * Further details on their usage and lifecycles can be found in the interface definitions below.
 * <br><br>
 * The bestIndexFor method is used at query time to identify the most selective index of those able
 * to satisfy any search predicates defined by a ReadCommand's RowFilter. It returns a thin IndexAccessor object
 * which enables the ReadCommand to access the appropriate functions of the Index at various stages in its lifecycle.
 * e.g. the getEstimatedResultRows is required when StorageProxy calculates the initial concurrency factor for
 * distributing requests to replicas, whereas a Searcher instance is needed when the ReadCommand is executed locally on
 * a target replica.
 * <br><br>
 * Finally, this class provides a clear and safe lifecycle to manage index builds, either full rebuilds via
 * {@link this#rebuildIndexesBlocking(Set)} or builds of new sstables
 * added via {@link org.apache.cassandra.notifications.SSTableAddedNotification}s, guaranteeing
 * the following:
 * <ul>
 * <li>The initialization task and any subsequent successful (re)build mark the index as built.</li>
 * <li>If any (re)build operation fails, the index is not marked as built, and only another full rebuild can mark the
 * index as built.</li>
 * <li>Full rebuilds cannot be run concurrently with other full or sstable (re)builds.</li>
 * <li>SSTable builds can always be run concurrently with any other builds.</li>
 * </ul>
 */
public class SecondaryIndexManager implements IndexRegistry, INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexManager.class);

    // default page size (in rows) when rebuilding the index for a whole partition
    public static final int DEFAULT_PAGE_SIZE = 10000;

    /**
     * All registered indexes.
     */
    private final Map<String, Index> indexes = Maps.newConcurrentMap();

    /**
     * The indexes that had a build failure.
     */
    private final Set<String> needsFullRebuild = Sets.newConcurrentHashSet();

    /**
     * The indexes that are available for querying.
     */
    private final Set<String> queryableIndexes = Sets.newConcurrentHashSet();
    
    /**
     * The indexes that are available for writing.
     */
    private final Map<String, Index> writableIndexes = Maps.newConcurrentMap();

    /**
     * The groups of all the registered indexes
     */
    private final Map<Index.Group.Key, Index.Group> indexGroups = Maps.newConcurrentMap();

    /**
     * The count of pending index builds for each index.
     */
    private final Map<String, AtomicInteger> inProgressBuilds = Maps.newConcurrentMap();

    // executes tasks returned by Indexer#addIndexColumn which may require index(es) to be (re)built
    private static final ExecutorPlus asyncExecutor = executorFactory()
            .withJmxInternal()
            .sequential("SecondaryIndexManagement");

    // executes all blocking tasks produced by Indexers e.g. getFlushTask, getMetadataReloadTask etc
    private static final ExecutorPlus blockingExecutor = ImmediateExecutor.INSTANCE;

    /**
     * The underlying column family containing the source data for these indexes
     */
    public final ColumnFamilyStore baseCfs;
    private final Keyspace keyspace;

    public SecondaryIndexManager(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
        this.keyspace = baseCfs.keyspace;
        baseCfs.getTracker().subscribe(this);
    }

    /**
     * Drops and adds new indexes associated with the underlying CF
     */
    public void reload()
    {
        // figure out what needs to be added and dropped.
        Indexes tableIndexes = baseCfs.metadata().indexes;
        indexes.keySet()
               .stream()
               .filter(indexName -> !tableIndexes.has(indexName))
               .forEach(this::removeIndex);

        // we call add for every index definition in the collection as
        // some may not have been created here yet, only added to schema
        for (IndexMetadata tableIndex : tableIndexes)
            addIndex(tableIndex, false);
    }

    private Future<?> reloadIndex(IndexMetadata indexDef)
    {
        Index index = indexes.get(indexDef.name);
        Callable<?> reloadTask = index.getMetadataReloadTask(indexDef);
        return reloadTask == null
               ? ImmediateFuture.success(null)
               : blockingExecutor.submit(reloadTask);
    }

    private synchronized Future<Void> createIndex(IndexMetadata indexDef, boolean isNewCF)
    {
        final Index index = createInstance(indexDef);
        index.register(this);
        if (writableIndexes.put(index.getIndexMetadata().name, index) == null)
            logger.info("Index [{}] registered and writable.", index.getIndexMetadata().name);

        markIndexesBuilding(ImmutableSet.of(index), true, isNewCF);

        return buildIndex(index);
    }

    @VisibleForTesting
    public Future<Void> buildIndex(final Index index)
    {
        FutureTask<?> initialBuildTask = null;
        // if the index didn't register itself, we can probably assume that no initialization needs to happen
        if (indexes.containsKey(index.getIndexMetadata().name))
        {
            try
            {
                Callable<?> call = index.getInitializationTask();
                if (call != null)
                    initialBuildTask = new FutureTask<>(call);
            }
            catch (Throwable t)
            {
                logAndMarkIndexesFailed(Collections.singleton(index), t, true);
                throw t;
            }
        }

        // if there's no initialization, just mark as built and return:
        if (initialBuildTask == null)
        {
            markIndexBuilt(index, true);
            return ImmediateFuture.success(null);
        }

        // otherwise run the initialization task asynchronously with a callback to mark it built or failed
        final Promise<Void> initialization = new AsyncPromise<>();
        // we want to ensure we invoke this task asynchronously, so we want to add our callback before submission
        // to ensure the work is not completed before we register the callback and so it gets performed by us.
        // This is because Keyspace.open("system") can transitively attempt to open Keyspace.open("system")
        initialBuildTask.addCallback(
            success -> {
                markIndexBuilt(index, true);
                initialization.trySuccess(null);
            },
            failure -> {
                logAndMarkIndexesFailed(Collections.singleton(index), failure, true);
                initialization.tryFailure(failure);
            }
        );
        asyncExecutor.execute(initialBuildTask);

        return initialization;
    }

    /**
     * Adds and builds a index
     *
     * @param indexDef the IndexMetadata describing the index
     * @param isNewCF true if the index is added as part of a new table/columnfamily (i.e. loading a CF at startup),
     * false for all other cases (i.e. newly added index)
     */
    public synchronized Future<?> addIndex(IndexMetadata indexDef, boolean isNewCF)
    {
        if (indexes.containsKey(indexDef.name))
            return reloadIndex(indexDef);
        else
            return createIndex(indexDef, isNewCF);
    }

    /**
     * Checks if the specified index is queryable.
     *
     * @param index the index
     * @return <code>true</code> if the specified index is queryable, <code>false</code> otherwise
     */
    public boolean isIndexQueryable(Index index)
    {
        return queryableIndexes.contains(index.getIndexMetadata().name);
    }

    /**
     * Throws an {@link IndexNotAvailableException} if any of the indexes in the specified {@link Index.QueryPlan} is
     * not queryable, as it's defined by {@link #isIndexQueryable(Index)}.
     *
     * @param queryPlan a query plan
     * @throws IndexNotAvailableException if the query plan has any index that is not queryable
     */
    public void checkQueryability(Index.QueryPlan queryPlan)
    {
        for (Index index : queryPlan.getIndexes())
        {
            if (!isIndexQueryable(index))
                throw new IndexNotAvailableException(index);
        }
    }

    /**
     * Checks if the specified index is writable.
     *
     * @param index the index
     * @return <code>true</code> if the specified index is writable, <code>false</code> otherwise
     */
    public boolean isIndexWritable(Index index)
    {
        return writableIndexes.containsKey(index.getIndexMetadata().name);
    }

    /**
     * Checks if the specified index has any running build task.
     *
     * @param indexName the index name
     * @return {@code true} if the index is building, {@code false} otherwise
     */
    @VisibleForTesting
    public synchronized boolean isIndexBuilding(String indexName)
    {
        AtomicInteger counter = inProgressBuilds.get(indexName);
        return counter != null && counter.get() > 0;
    }

    public synchronized void removeIndex(String indexName)
    {
        Index removedIndex = indexes.remove(indexName);

        if (removedIndex != null)
        {
            removedIndex.unregister(this);

            markIndexRemoved(indexName);
            executeBlocking(removedIndex.getInvalidateTask(), null);
        }
    }

    public Set<IndexMetadata> getDependentIndexes(ColumnMetadata column)
    {
        if (indexes.isEmpty())
            return Collections.emptySet();

        Set<IndexMetadata> dependentIndexes = new HashSet<>();
        for (Index index : indexes.values())
            if (index.dependsOn(column))
                dependentIndexes.add(index.getIndexMetadata());

        return dependentIndexes;
    }

    /**
     * Called when dropping a Table
     */
    public void markAllIndexesRemoved()
    {
        getBuiltIndexNames().forEach(this::markIndexRemoved);
    }

    /**
     * Does a blocking full rebuild/recovery of the specifed indexes from all the sstables in the base table.
     * Note also that this method of (re)building/recovering indexes:
     * a) takes a set of index *names* rather than Indexers
     * b) marks existing indexes removed prior to rebuilding
     * c) fails if such marking operation conflicts with any ongoing index builds, as full rebuilds cannot be run
     * concurrently
     *
     * @param indexNames the list of indexes to be rebuilt
     */
    public void rebuildIndexesBlocking(Set<String> indexNames)
    {
        // Get the set of indexes that require blocking build
        Set<Index> toRebuild = indexes.values()
                                      .stream()
                                      .filter(index -> indexNames.contains(index.getIndexMetadata().name))
                                      .filter(Index::shouldBuildBlocking)
                                      .collect(Collectors.toSet());

        if (toRebuild.isEmpty())
        {
            logger.info("No defined indexes with the supplied names: {}", Joiner.on(',').join(indexNames));
            return;
        }

        // Optimistically mark the indexes as writable, so we don't miss incoming writes
        boolean needsFlush = false;
        for (Index index : toRebuild)
        {
            String name = index.getIndexMetadata().name;
            if (writableIndexes.put(name, index) == null)
            {
                logger.info("Index [{}] became writable starting recovery.", name);
                needsFlush = true;
            }
        }

        // Once we are tracking new writes, flush any memtable contents to not miss them from the sstable-based rebuild
        if (needsFlush)
            baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_STARTED);

        // Now that we are tracking new writes and we haven't left untracked contents on the memtables, we are ready to
        // index the sstables
        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
             Refs<SSTableReader> allSSTables = viewFragment.refs)
        {
            buildIndexesBlocking(allSSTables, toRebuild, true);
        }
    }

    /**
     * Checks if the specified {@link ColumnFamilyStore} is a secondary index.
     *
     * @param cfs the <code>ColumnFamilyStore</code> to check.
     * @return <code>true</code> if the specified <code>ColumnFamilyStore</code> is a secondary index,
     * <code>false</code> otherwise.
     */
    public static boolean isIndexColumnFamilyStore(ColumnFamilyStore cfs)
    {
        return isIndexColumnFamily(cfs.name);
    }

    /**
     * Checks if the specified {@link ColumnFamilyStore} is the one secondary index.
     *
     * @param cfName the name of the <code>ColumnFamilyStore</code> to check.
     * @return <code>true</code> if the specified <code>ColumnFamilyStore</code> is a secondary index,
     * <code>false</code> otherwise.
     */
    public static boolean isIndexColumnFamily(String cfName)
    {
        return cfName.contains(Directories.SECONDARY_INDEX_NAME_SEPARATOR);
    }

    /**
     * Returns the parent of the specified {@link ColumnFamilyStore}.
     *
     * @param cfs the <code>ColumnFamilyStore</code>
     * @return the parent of the specified <code>ColumnFamilyStore</code>
     */
    public static ColumnFamilyStore getParentCfs(ColumnFamilyStore cfs)
    {
        String parentCfs = getParentCfsName(cfs.name);
        return cfs.keyspace.getColumnFamilyStore(parentCfs);
    }

    /**
     * Returns the parent name of the specified {@link ColumnFamilyStore}.
     *
     * @param cfName the <code>ColumnFamilyStore</code> name
     * @return the parent name of the specified <code>ColumnFamilyStore</code>
     */
    public static String getParentCfsName(String cfName)
    {
        assert isIndexColumnFamily(cfName);
        return StringUtils.substringBefore(cfName, Directories.SECONDARY_INDEX_NAME_SEPARATOR);
    }

    /**
     * Returns the index name
     *
     * @param cfs the <code>ColumnFamilyStore</code>
     * @return the index name
     */
    public static String getIndexName(ColumnFamilyStore cfs)
    {
        return getIndexName(cfs.name);
    }

    /**
     * Returns the index name
     *
     * @param cfName the <code>ColumnFamilyStore</code> name
     * @return the index name
     */
    public static String getIndexName(String cfName)
    {
        assert isIndexColumnFamily(cfName);
        return StringUtils.substringAfter(cfName, Directories.SECONDARY_INDEX_NAME_SEPARATOR);
    }

    /**
     * Validates all index groups against the specified SSTables. 
     *
     * @param sstables SSTables for which indexes in the group should be built
     * @param throwOnIncomplete whether to throw an error if any index in the group is incomplete
     *
     * @return true if all indexes in all groups are complete and valid
     *         false if an index in any group is incomplete and {@code throwOnIncomplete} is false 
     *
     * @throws IllegalStateException if {@code throwOnIncomplete} is true and an index in any group is incomplete
     * @throws UncheckedIOException if there is a problem validating any on-disk component in any group
     */
    public boolean validateSSTableAttachedIndexes(Collection<SSTableReader> sstables, boolean throwOnIncomplete)
    {
        boolean complete = true;

        for (Index.Group group : indexGroups.values())
        {
            if (group.getIndexes().stream().anyMatch(Index::isSSTableAttached))
                complete &= group.validateSSTableAttachedIndexes(sstables, throwOnIncomplete);
        }

        return complete;
    }

    /**
     * Incrementally builds indexes for the specified SSTables in a blocking fashion.
     * <p>
     * This is similar to {@link #buildIndexesBlocking}, but it is designed to be used in cases where failure will
     * cascade through to failing the containing operation that actuates the build. (ex. streaming and SSTable import)
     * <p>
     * It does not update index build status or queryablility on failure or success and does not call
     * {@link #flushIndexesBlocking(Set, FutureCallback)}, as this is an artifact of the legacy non-SSTable-attached
     * index implementation.
     *
     * @param sstables the SSTables for which indexes must be built
     */
    public void buildSSTableAttachedIndexesBlocking(Collection<SSTableReader> sstables)
    {
        Set<Index> toBuild = indexes.values().stream().filter(Index::isSSTableAttached).collect(Collectors.toSet());

        if (toBuild.isEmpty())
            return;

        logger.info("Submitting incremental index build of {} for data in {}...",
                    commaSeparated(toBuild),
                    sstables.stream().map(SSTableReader::toString).collect(Collectors.joining(",")));

        // Group all building tasks
        Map<Index.IndexBuildingSupport, Set<Index>> byType = new HashMap<>();
        for (Index index : toBuild)
        {
            Set<Index> stored = byType.computeIfAbsent(index.getBuildTaskSupport(), i -> new HashSet<>());
            stored.add(index);
        }

        // Schedule all index building tasks with callbacks to handle success and failure
        List<Future<?>> futures = new ArrayList<>(byType.size());
        byType.forEach((buildingSupport, groupedIndexes) ->
        {
            SecondaryIndexBuilder builder = buildingSupport.getIndexBuildTask(baseCfs, groupedIndexes, sstables, false);
            AsyncPromise<Object> build = new AsyncPromise<>();
            CompactionManager.instance.submitIndexBuild(builder).addCallback(new FutureCallback<Object>()
            {
                @Override
                public void onFailure(Throwable t)
                {
                    logger.warn("Failed to incrementally build indexes {}", getIndexNames(groupedIndexes));
                    build.tryFailure(t);
                }

                @Override
                public void onSuccess(Object o)
                {
                    logger.info("Incremental index build of {} completed", getIndexNames(groupedIndexes));
                    build.trySuccess(o);
                }
            });
            futures.add(build);
        });

        // Finally wait for the index builds to finish
        FBUtilities.waitOnFutures(futures);
    }

    /**
     * Performs a blocking (re)indexing/recovery of the specified SSTables for the specified indexes.
     * <p>
     * If the index doesn't support ALL {@link Index.LoadType} it performs a recovery {@link Index#getRecoveryTaskSupport()}
     * instead of a build {@link Index#getBuildTaskSupport()}
     * 
     * @param sstables      the SSTables to be (re)indexed
     * @param indexes       the indexes to be (re)built for the specifed SSTables
     * @param isFullRebuild True if this method is invoked as a full index rebuild, false otherwise
     */
    @SuppressWarnings({"unchecked", "RedundantSuppression"})
    private void buildIndexesBlocking(Collection<SSTableReader> sstables, Set<Index> indexes, boolean isFullRebuild)
    {
        if (indexes.isEmpty())
            return;

        // Mark all indexes as building: this step must happen first, because if any index can't be marked, the whole
        // process needs to abort
        markIndexesBuilding(indexes, isFullRebuild, false);

        // Build indexes in a try/catch, so that any index not marked as either built or failed will be marked as failed:
        final Set<Index> builtIndexes = Sets.newConcurrentHashSet();
        final Set<Index> unbuiltIndexes = Sets.newConcurrentHashSet();

        // Any exception thrown during index building that could be suppressed by the finally block
        Exception accumulatedFail = null;

        try
        {
            logger.info("Submitting index {} of {} for data in {}",
                        isFullRebuild ? "recovery" : "build",
                        commaSeparated(indexes),
                        sstables.stream().map(SSTableReader::toString).collect(Collectors.joining(",")));

            // Group all building tasks
            Map<Index.IndexBuildingSupport, Set<Index>> byType = new HashMap<>();
            for (Index index : indexes)
            {
                IndexBuildingSupport buildOrRecoveryTask = isFullRebuild
                                                           ? index.getBuildTaskSupport()
                                                           : index.getRecoveryTaskSupport();
                Set<Index> stored = byType.computeIfAbsent(buildOrRecoveryTask, i -> new HashSet<>());
                stored.add(index);
            }

            // Schedule all index building tasks with a callback to mark them as built or failed
            List<Future<?>> futures = new ArrayList<>(byType.size());
            byType.forEach((buildingSupport, groupedIndexes) ->
                           {
                               SecondaryIndexBuilder builder = buildingSupport.getIndexBuildTask(baseCfs, groupedIndexes, sstables, isFullRebuild);
                               final AsyncPromise<Object> build = new AsyncPromise<>();
                               CompactionManager.instance.submitIndexBuild(builder).addCallback(new FutureCallback<Object>()
                               {
                                   @Override
                                   public void onFailure(Throwable t)
                                   {
                                       logAndMarkIndexesFailed(groupedIndexes, t, false);
                                       unbuiltIndexes.addAll(groupedIndexes);
                                       build.tryFailure(t);
                                   }

                                   @Override
                                   public void onSuccess(Object o)
                                   {
                                       groupedIndexes.forEach(i -> markIndexBuilt(i, isFullRebuild));
                                       logger.info("Index build of {} completed", getIndexNames(groupedIndexes));
                                       builtIndexes.addAll(groupedIndexes);
                                       build.trySuccess(o);
                                   }
                               });
                               futures.add(build);
                           });

            // Finally wait for the index builds to finish and flush the indexes that built successfully
            FBUtilities.waitOnFutures(futures);
        }
        catch (Exception e)
        {
            accumulatedFail = e;
            throw e;
        }
        finally
        {
            try
            {
                // Fail any indexes that couldn't be marked
                Set<Index> failedIndexes = Sets.difference(indexes, Sets.union(builtIndexes, unbuiltIndexes));
                if (!failedIndexes.isEmpty())
                {
                    logAndMarkIndexesFailed(failedIndexes, accumulatedFail, false);
                }

                // Flush all built indexes with an aynchronous callback to log the success or failure of the flush
                flushIndexesBlocking(builtIndexes, new FutureCallback<>()
                {
                    final String indexNames = StringUtils.join(builtIndexes.stream()
                                                                           .map(i -> i.getIndexMetadata().name)
                                                                           .collect(Collectors.toList()), ',');

                    @Override
                    public void onFailure(Throwable ignored)
                    {
                        logger.info("Index flush of {} failed", indexNames);
                    }

                    @Override
                    public void onSuccess(Object ignored)
                    {
                        logger.info("Index flush of {} completed", indexNames);
                    }
                });
            }
            catch (Exception e)
            {
                if (accumulatedFail != null)
                {
                    accumulatedFail.addSuppressed(e);
                }
                else
                {
                    throw e;
                }
            }
        }
    }

    private String getIndexNames(Set<Index> indexes)
    {
        List<String> indexNames = indexes.stream()
                                         .map(i -> i.getIndexMetadata().name)
                                         .collect(Collectors.toList());
        return StringUtils.join(indexNames, ',');
    }

    /**
     * Marks the specified indexes as (re)building if:
     * 1) There's no in progress rebuild of any of the given indexes.
     * 2) There's an in progress rebuild but the caller is not a full rebuild.
     * <p>
     * Otherwise, this method invocation fails, as it is not possible to run full rebuilds while other concurrent rebuilds
     * are in progress. Please note this is checked atomically against all given indexes; that is, no index will be marked
     * if even a single one fails.
     * <p>
     * Marking an index as "building" practically means:
     * 1) The index is removed from the "failed" set if this is a full rebuild.
     * 2) The index is removed from the system keyspace built indexes; this only happens if this method is not invoked
     * for a new table initialization, as in such case there's no need to remove it (it is either already not present,
     * or already present because already built).
     * <p>
     * Thread safety is guaranteed by having all methods managing index builds synchronized: being synchronized on
     * the SecondaryIndexManager instance, it means all invocations for all different indexes will go through the same
     * lock, but this is fine as the work done while holding such lock is trivial.
     * <p>
     * {@link #markIndexBuilt(Index, boolean)} or {@link #markIndexFailed(Index, boolean)} should be always called after
     * the rebuilding has finished, so that the index build state can be correctly managed and the index rebuilt.
     *
     * @param indexes the index to be marked as building
     * @param isFullRebuild {@code true} if this method is invoked as a full index rebuild, {@code false} otherwise
     * @param isNewCF {@code true} if this method is invoked when initializing a new table/columnfamily (i.e. loading a CF at startup),
     * {@code false} for all other cases (i.e. newly added index)
     */
    @VisibleForTesting
    public synchronized void markIndexesBuilding(Set<Index> indexes, boolean isFullRebuild, boolean isNewCF)
    {
        String keyspaceName = baseCfs.getKeyspaceName();

        // First step is to validate against concurrent rebuilds; it would be more optimized to do everything on a single
        // step, but we're not really expecting a very high number of indexes, and this isn't on any hot path, so
        // we're favouring readability over performance
        indexes.forEach(index ->
                        {
                            String indexName = index.getIndexMetadata().name;
                            AtomicInteger counter = inProgressBuilds.computeIfAbsent(indexName, ignored -> new AtomicInteger(0));

                            if (counter.get() > 0 && isFullRebuild)
                                throw new IllegalStateException(String.format("Cannot rebuild index %s as another index build for the same index is currently in progress.", indexName));
                        });

        // Second step is the actual marking:
        indexes.forEach(index ->
                        {
                            String indexName = index.getIndexMetadata().name;
                            AtomicInteger counter = inProgressBuilds.computeIfAbsent(indexName, ignored -> new AtomicInteger(0));

                            if (isFullRebuild)
                            {
                                needsFullRebuild.remove(indexName);
                                makeIndexNonQueryable(index, Index.Status.FULL_REBUILD_STARTED);
                            }

                            if (counter.getAndIncrement() == 0 && DatabaseDescriptor.isDaemonInitialized() && !isNewCF)
                                SystemKeyspace.setIndexRemoved(keyspaceName, indexName);
                        });
    }

    /**
     * Marks the specified index as built if there are no in progress index builds and the index is not failed.
     * {@link #markIndexesBuilding(Set, boolean, boolean)} should always be invoked before this method.
     *
     * @param index the index to be marked as built
     * @param isFullRebuild {@code true} if this method is invoked as a full index rebuild, {@code false} otherwise
     */
    private synchronized void markIndexBuilt(Index index, boolean isFullRebuild)
    {
        String indexName = index.getIndexMetadata().name;
        if (isFullRebuild)
            makeIndexQueryable(index, Index.Status.BUILD_SUCCEEDED);
        
        AtomicInteger counter = inProgressBuilds.get(indexName);
        if (counter != null)
        {
            assert counter.get() > 0;
            if (counter.decrementAndGet() == 0)
            {
                inProgressBuilds.remove(indexName);
                if (!needsFullRebuild.contains(indexName) && DatabaseDescriptor.isDaemonInitialized() && Keyspace.isInitialized())
                    SystemKeyspace.setIndexBuilt(baseCfs.getKeyspaceName(), indexName);
            }
        }
    }

    /**
     * Marks the specified index as failed.
     * {@link #markIndexesBuilding(Set, boolean, boolean)} should always be invoked before this method.
     *
     * @param index the index to be marked as built
     * @param isInitialBuild {@code true} if the index failed during its initial build, {@code false} otherwise
     */
    private synchronized void markIndexFailed(Index index, boolean isInitialBuild)
    {
        String indexName = index.getIndexMetadata().name;

        AtomicInteger counter = inProgressBuilds.get(indexName);
        if (counter != null)
        {
            assert counter.get() > 0;

            counter.decrementAndGet();

            if (DatabaseDescriptor.isDaemonInitialized())
                SystemKeyspace.setIndexRemoved(baseCfs.getKeyspaceName(), indexName);

            needsFullRebuild.add(indexName);

            if (!index.getSupportedLoadTypeOnFailure(isInitialBuild).supportsWrites() && writableIndexes.remove(indexName) != null)
                logger.info("Index [{}] became not-writable because of failed build.", indexName);

            if (!index.getSupportedLoadTypeOnFailure(isInitialBuild).supportsReads() && queryableIndexes.remove(indexName))
                logger.info("Index [{}] became not-queryable because of failed build.", indexName);
        }
    }

    private void logAndMarkIndexesFailed(Set<Index> indexes, Throwable indexBuildFailure, boolean isInitialBuild)
    {
        JVMStabilityInspector.inspectThrowable(indexBuildFailure);
        if (indexBuildFailure != null)
            logger.warn("Index build of {} failed. Please run full index rebuild to fix it.", getIndexNames(indexes), indexBuildFailure);
        else
            logger.warn("Index build of {} failed. Please run full index rebuild to fix it.", getIndexNames(indexes));
        indexes.forEach(i -> this.markIndexFailed(i, isInitialBuild));
    }

    /**
     * Marks the specified index as removed.
     *
     * @param indexName the index name
     */
    private synchronized void markIndexRemoved(String indexName)
    {
        SystemKeyspace.setIndexRemoved(baseCfs.getKeyspaceName(), indexName);
        queryableIndexes.remove(indexName);
        writableIndexes.remove(indexName);
        needsFullRebuild.remove(indexName);
        inProgressBuilds.remove(indexName);
        // remove existing indexing status
        IndexStatusManager.instance.propagateLocalIndexStatus(keyspace.getName(), indexName, Index.Status.DROPPED);
    }

    public Index getIndexByName(String indexName)
    {
        return indexes.get(indexName);
    }

    private Index createInstance(IndexMetadata indexDef)
    {
        Index newIndex;
        if (indexDef.isCustom())
        {
            assert indexDef.options != null;
            // Get the fully qualified index class name from the index metadata
            String className = indexDef.getIndexClassName();
            assert !Strings.isNullOrEmpty(className);

            try
            {
                Class<? extends Index> indexClass = FBUtilities.classForName(className, "Index");
                Constructor<? extends Index> ctor = indexClass.getConstructor(ColumnFamilyStore.class, IndexMetadata.class);
                newIndex = ctor.newInstance(baseCfs, indexDef);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
        else
        {
            newIndex = CassandraIndex.newIndex(baseCfs, indexDef);
        }
        return newIndex;
    }

    /**
     * Truncate all indexes
     */
    public void truncateAllIndexesBlocking(final long truncatedAt)
    {
        executeAllBlocking(indexes.values().stream(), (index) -> index.getTruncateTask(truncatedAt), null);
    }

    /**
     * Remove all indexes
     */
    public void dropAllIndexes(boolean dropData)
    {
        markAllIndexesRemoved();
        if (dropData)
            invalidateAllIndexesBlocking();

        // TODO: Determine whether "dropData" should guard this or be passed to Group#invalidate()
        indexGroups.forEach((key, group) -> group.invalidate());
    }

    @VisibleForTesting
    public void invalidateAllIndexesBlocking()
    {
        executeAllBlocking(indexes.values().stream(), Index::getInvalidateTask, null);
    }

    /**
     * Perform a blocking flush all indexes
     */
    public void flushAllIndexesBlocking()
    {
        flushIndexesBlocking(ImmutableSet.copyOf(indexes.values()));
    }

    /**
     * Perform a blocking flush of selected indexes
     */
    public void flushIndexesBlocking(Set<Index> indexes)
    {
        flushIndexesBlocking(indexes, null);
    }

    /**
     * Performs a blocking execution of pre-join tasks of all indexes
     */
    public void executePreJoinTasksBlocking(boolean hadBootstrap)
    {
        logger.info("Executing pre-join{} tasks for: {}", hadBootstrap ? " post-bootstrap" : "", this.baseCfs);
        executeAllBlocking(indexes.values().stream(), (index) ->
        {
            return index.getPreJoinTask(hadBootstrap);
        }, null);
    }

    private void flushIndexesBlocking(Set<Index> indexes, FutureCallback<Object> callback)
    {
        if (indexes.isEmpty())
            return;

        List<Future<?>> wait = new ArrayList<>();
        List<Index> nonCfsIndexes = new ArrayList<>();

        // for each CFS backed index, submit a flush task which we'll wait on for completion
        // for the non-CFS backed indexes, we'll flush those while we wait.
        synchronized (baseCfs.getTracker())
        {
            indexes.forEach(index ->
                            index.getBackingTable()
                                 .map(cfs -> wait.add(cfs.forceFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_COMPLETED)))
                                 .orElseGet(() -> nonCfsIndexes.add(index)));
        }

        executeAllBlocking(nonCfsIndexes.stream(), Index::getBlockingFlushTask, callback);
        FBUtilities.waitOnFutures(wait);
    }

    /**
     * Performs a blocking flush of all custom indexes
     */
    public void flushAllNonCFSBackedIndexesBlocking(Memtable baseCfsMemtable)
    {
        executeAllBlocking(indexes.values()
                                  .stream()
                                  .filter(index -> index.getBackingTable().isEmpty()),
                           index -> index.getBlockingFlushTask(baseCfsMemtable),
                           null);
    }

    /**
     * @return all indexes which are marked as built and ready to use
     */
    public List<String> getBuiltIndexNames()
    {
        Set<String> allIndexNames = new HashSet<>();
        indexes.values().stream()
               .map(i -> i.getIndexMetadata().name)
               .forEach(allIndexNames::add);
        return SystemKeyspace.getBuiltIndexes(baseCfs.getKeyspaceName(), allIndexNames);
    }

    /**
     * @return all backing Tables used by registered indexes
     */
    public Set<ColumnFamilyStore> getAllIndexColumnFamilyStores()
    {
        Set<ColumnFamilyStore> backingTables = new HashSet<>();
        indexes.values().forEach(index -> index.getBackingTable().ifPresent(backingTables::add));
        return backingTables;
    }

    /**
     * @return if there are ANY indexes registered for this table
     */
    public boolean hasIndexes()
    {
        return !indexes.isEmpty();
    }

    public void indexPartition(DecoratedKey key, Set<Index> indexes, int pageSize)
    {
        indexPartition(key, indexes, pageSize, baseCfs.metadata().regularAndStaticColumns());
    }

    /**
     * When building an index against existing data in sstables, add the given partition to the index
     *
     * @param key the key for the partition being indexed
     * @param indexes the indexes that must be updated
     * @param pageSize the number of {@link Unfiltered} objects to process in a single page
     * @param columns the columns indexed by at least one of the supplied indexes
     */
    public void indexPartition(DecoratedKey key, Set<Index> indexes, int pageSize, RegularAndStaticColumns columns)
    {
        if (logger.isTraceEnabled())
            logger.trace("Indexing partition {}", baseCfs.metadata().partitionKeyType.getString(key.getKey()));

        if (!indexes.isEmpty())
        {
            SinglePartitionReadCommand cmd = SinglePartitionReadCommand.create(baseCfs.metadata(),
                                                                               FBUtilities.nowInSeconds(),
                                                                               ColumnFilter.selection(columns),
                                                                               RowFilter.none(),
                                                                               DataLimits.NONE,
                                                                               key,
                                                                               new ClusteringIndexSliceFilter(Slices.ALL, false));

            long nowInSec = cmd.nowInSec();
            boolean readStatic = false;

            SinglePartitionPager pager = new SinglePartitionPager(cmd, null, ProtocolVersion.CURRENT);
            while (!pager.isExhausted())
            {
                try (ReadExecutionController controller = cmd.executionController();
                     WriteContext ctx = keyspace.getWriteHandler().createContextForIndexing();
                     UnfilteredPartitionIterator page = pager.fetchPageUnfiltered(baseCfs.metadata(), pageSize, controller))
                {
                    if (!page.hasNext())
                        break;

                    try (UnfilteredRowIterator partition = page.next())
                    {
                        Set<Index.Indexer> indexers = indexGroups.values().stream()
                                                             .map(g -> g.indexerFor(indexes::contains,
                                                                                    key,
                                                                                    partition.columns(),
                                                                                    nowInSec,
                                                                                    ctx,
                                                                                    IndexTransaction.Type.UPDATE,
                                                                                    null))
                                                             .filter(Objects::nonNull)
                                                             .collect(Collectors.toSet());

                        // Short-circuit empty partitions if static row is processed or isn't read
                        if (!readStatic && partition.isEmpty() && partition.staticRow().isEmpty())
                            break;

                        indexers.forEach(Index.Indexer::begin);

                        if (!readStatic)
                        {
                            if (!partition.staticRow().isEmpty())
                                indexers.forEach(indexer -> indexer.insertRow(partition.staticRow()));
                            indexers.forEach((Index.Indexer i) -> i.partitionDelete(partition.partitionLevelDeletion()));
                            readStatic = true;
                        }

                        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(partition.partitionLevelDeletion(), baseCfs.getComparator(), false);

                        while (partition.hasNext())
                        {
                            Unfiltered unfilteredRow = partition.next();

                            if (unfilteredRow.isRow())
                            {
                                Row row = (Row) unfilteredRow;
                                indexers.forEach(indexer -> indexer.insertRow(row));
                            }
                            else
                            {
                                assert unfilteredRow.isRangeTombstoneMarker();
                                RangeTombstoneMarker marker = (RangeTombstoneMarker) unfilteredRow;
                                deletionBuilder.add(marker);
                            }
                        }

                        MutableDeletionInfo deletionInfo = deletionBuilder.build();
                        if (deletionInfo.hasRanges())
                        {
                            Iterator<RangeTombstone> iter = deletionInfo.rangeIterator(false);
                            while (iter.hasNext())
                            {
                                RangeTombstone rt = iter.next();
                                indexers.forEach(indexer -> indexer.rangeTombstone(rt));
                            }
                        }

                        indexers.forEach(Index.Indexer::finish);
                    }
                }
            }
        }
    }

    /**
     * Return the page size used when indexing an entire partition
     */
    public int calculateIndexingPageSize()
    {
        if (FORCE_DEFAULT_INDEXING_PAGE_SIZE.getBoolean())
            return DEFAULT_PAGE_SIZE;

        double targetPageSizeInBytes = 32 * 1024 * 1024;
        double meanPartitionSize = baseCfs.getMeanPartitionSize();
        if (meanPartitionSize <= 0)
            return DEFAULT_PAGE_SIZE;

        int meanCellsPerPartition = baseCfs.getMeanEstimatedCellPerPartitionCount();
        if (meanCellsPerPartition <= 0)
            return DEFAULT_PAGE_SIZE;

        int columnsPerRow = baseCfs.metadata().regularColumns().size();
        if (columnsPerRow <= 0)
            return DEFAULT_PAGE_SIZE;

        int meanRowsPerPartition = meanCellsPerPartition / columnsPerRow;
        double meanRowSize = meanPartitionSize / meanRowsPerPartition;

        int pageSize = (int) Math.max(1, Math.min(DEFAULT_PAGE_SIZE, targetPageSizeInBytes / meanRowSize));

        logger.trace("Calculated page size {} for indexing {}.{} ({}/{}/{}/{})",
                     pageSize,
                     baseCfs.metadata.keyspace,
                     baseCfs.metadata.name,
                     meanPartitionSize,
                     meanCellsPerPartition,
                     meanRowsPerPartition,
                     meanRowSize);

        return pageSize;
    }

    /**
     * Delete all data from all indexes for this partition.
     * For when cleanup rips a partition out entirely.
     * <p>
     * TODO : improve cleanup transaction to batch updates and perform them async
     */
    public void deletePartition(UnfilteredRowIterator partition, long nowInSec)
    {
        if (!handles(IndexTransaction.Type.CLEANUP))
            return;

        // we need to acquire memtable lock because secondary index deletion may
        // cause a race (see CASSANDRA-3712). This is done internally by the
        // index transaction when it commits
        CleanupTransaction indexTransaction = newCleanupTransaction(partition.partitionKey(),
                                                                    partition.columns(),
                                                                    nowInSec);
        indexTransaction.start();
        indexTransaction.onPartitionDeletion(DeletionTime.build(FBUtilities.timestampMicros(), nowInSec));
        indexTransaction.commit();

        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();
            if (unfiltered.kind() != Unfiltered.Kind.ROW)
                continue;

            indexTransaction = newCleanupTransaction(partition.partitionKey(),
                                                     partition.columns(),
                                                     nowInSec);
            indexTransaction.start();
            indexTransaction.onRowDelete((Row) unfiltered);
            indexTransaction.commit();
        }
    }

    /**
     * Called at query time to choose which (if any) of the registered index implementations to use for a given query.
     * <p>
     * This is a two step processes, firstly compiling the set of searchable indexes then choosing the one which reduces
     * the search space the most.
     * <p>
     * In the first phase, if the command's RowFilter contains any custom index expressions, the indexes that they
     * specify are automatically included. Following that, the registered indexes are filtered to include only those
     * which support the standard expressions in the RowFilter.
     * <p>
     * The filtered set then sorted by selectivity, as reported by the Index implementations' getEstimatedResultRows
     * method.
     * <p>
     * Implementation specific validation of the target expression, either custom or standard, by the selected
     * index should be performed in the searcherFor method to ensure that we pick the right index regardless of
     * the validity of the expression.
     * <p>
     * This method is only called once during the lifecycle of a ReadCommand and the result is
     * cached for future use when obtaining a Searcher, getting the index's underlying CFS for
     * ReadOrderGroup, or an estimate of the result size from an average index query.
     *
     * @param rowFilter RowFilter of the command to be executed
     * @return the best available index query plan for the row filter, or {@code null} if none of the registered indexes
     * can support the command.
     */
    public Index.QueryPlan getBestIndexQueryPlanFor(RowFilter rowFilter)
    {
        if (indexes.isEmpty() || rowFilter.isEmpty())
            return null;

        for (RowFilter.Expression expression : rowFilter)
        {
            if (expression.isCustom())
            {
                // Only a single custom expression is allowed per query and, if present,
                // we want to always favour the index specified in such an expression
                RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression) expression;
                logger.trace("Command contains a custom index expression, using target index {}", customExpression.getTargetIndex().name);
                Tracing.trace("Command contains a custom index expression, using target index {}", customExpression.getTargetIndex().name);
                Index.Group group = getIndexGroup(customExpression.getTargetIndex());
                return group == null ? null : group.queryPlanFor(rowFilter);
            }
        }

        Set<Index.QueryPlan> queryPlans = indexGroups.values()
                                                     .stream()
                                                     .map(g -> g.queryPlanFor(rowFilter))
                                                     .filter(Objects::nonNull)
                                                     .collect(Collectors.toSet());

        if (queryPlans.isEmpty())
        {
            logger.trace("No applicable indexes found");
            Tracing.trace("No applicable indexes found");
            return null;
        }

        // find the best plan
        Index.QueryPlan selected = queryPlans.size() == 1
                                   ? Iterables.getOnlyElement(queryPlans)
                                   : queryPlans.stream()
                                               .min(Comparator.naturalOrder())
                                               .orElseThrow(() -> new AssertionError("Could not select most selective index"));

        // pay for an additional threadlocal get() rather than build the strings unnecessarily
        if (Tracing.isTracing())
        {
            Tracing.trace("Index mean cardinalities are {}. Scanning with {}.",
                          queryPlans.stream()
                                    .map(p -> commaSeparated(p.getIndexes()) + ':' + p.getEstimatedResultRows())
                                    .collect(Collectors.joining(",")),
                          commaSeparated(selected.getIndexes()));
        }
        return selected;
    }

    private static String commaSeparated(Collection<Index> indexes)
    {
        return indexes.stream().map(i -> i.getIndexMetadata().name).collect(Collectors.joining(","));
    }

    public Optional<Index> getBestIndexFor(RowFilter.Expression expression)
    {
        return indexes.values().stream().filter((i) -> i.supportsExpression(expression.column(), expression.operator())).findFirst();
    }

    public <T extends Index> Set<T> getBestIndexFor(RowFilter.Expression expression, Class<T> indexType)
    {
        return indexes.values()
                      .stream()
                      .filter(i -> indexType.isInstance(i) && i.supportsExpression(expression.column(), expression.operator()))
                      .map(indexType::cast)
                      .collect(Collectors.toSet());
    }

    /**
     * Called at write time to ensure that values present in the update
     * are valid according to the rules of all registered indexes which
     * will process it. The partition key as well as the clustering and
     * cell values for each row in the update may be checked by index
     * implementations
     *
     * @param update PartitionUpdate containing the values to be validated by registered Index implementations
     */
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        for (Index index : indexes.values())
            index.validate(update);
    }

    /*
     * IndexRegistry methods
     */
    @Override
    public void registerIndex(Index index, Index.Group.Key groupKey, Supplier<Index.Group> groupSupplier)
    {
        String name = index.getIndexMetadata().name;
        indexes.put(name, index);
        logger.trace("Registered index {}", name);

        // instantiate and add the index group if it hasn't been already added
        Index.Group group = indexGroups.computeIfAbsent(groupKey, k -> groupSupplier.get());

        // add the created index to its group if it is not a singleton group
        group.addIndex(index);
    }

    @Override
    public void unregisterIndex(Index removed, Index.Group.Key groupKey)
    {
        Index.Group group = indexGroups.get(groupKey);
        if (group != null && group.containsIndex(removed))
        {
            // Remove the index from non-singleton groups...
            group.removeIndex(removed);

            // if the group is a singleton or there are no more indexes left in the group, remove it
            if (group.isSingleton() || group.getIndexes().isEmpty())
            {
                Index.Group removedGroup = indexGroups.remove(groupKey);
                if (removedGroup != null)
                    removedGroup.invalidate();
            }
        }
    }

    public Index getIndex(IndexMetadata metadata)
    {
        return indexes.get(metadata.name);
    }

    public Collection<Index> listIndexes()
    {
        return ImmutableSet.copyOf(indexes.values());
    }

    public Set<Index.Group> listIndexGroups()
    {
        return ImmutableSet.copyOf(indexGroups.values());
    }

    public Index.Group getIndexGroup(Index.Group.Key key)
    {
        return indexGroups.get(key);
    }

    /**
     * Returns the {@link Index.Group} the specified index belongs to, as specified during registering with
     * {@link #registerIndex(Index, Index.Group.Key, Supplier)}.
     *
     * @param metadata the index metadata
     * @return the group the index belongs to, or {@code null} if the index is not registered or if it hasn't been
     * associated to any group
     */
    @Nullable
    public Index.Group getIndexGroup(IndexMetadata metadata)
    {
        Index index = getIndex(metadata);
        return index == null ? null : getIndexGroup(index);
    }

    @VisibleForTesting
    public boolean needsFullRebuild(String index)
    {
        return needsFullRebuild.contains(index);
    }

    public Index.Group getIndexGroup(Index index)
    {
        return indexGroups.values().stream().filter(g -> g.containsIndex(index)).findAny().orElse(null);
    }

    /*
     * Handling of index updates.
     * Implementations of the various IndexTransaction interfaces, for keeping indexes in sync with base data
     * during updates, compaction and cleanup. Plus factory methods for obtaining transaction instances.
     */

    /**
     * Transaction for updates on the write path.
     */
    public UpdateTransaction newUpdateTransaction(PartitionUpdate update, WriteContext ctx, long nowInSec, Memtable memtable)
    {
        if (!hasIndexes())
            return UpdateTransaction.NO_OP;

        Index.Indexer[] indexers = listIndexGroups().stream()
                                          .map(g -> g.indexerFor(writableIndexSelector(),
                                                                 update.partitionKey(),
                                                                 update.columns(),
                                                                 nowInSec,
                                                                 ctx,
                                                                 IndexTransaction.Type.UPDATE,
                                                                 memtable))
                                          .filter(Objects::nonNull)
                                          .toArray(Index.Indexer[]::new);

        return indexers.length == 0 ? UpdateTransaction.NO_OP : new WriteTimeTransaction(indexers);
    }

    private Predicate<Index> writableIndexSelector()
    {
        return index -> writableIndexes.containsKey(index.getIndexMetadata().name);
    }

    /**
     * Transaction for use when merging rows during compaction
     */
    public CompactionTransaction newCompactionTransaction(DecoratedKey key,
                                                          RegularAndStaticColumns regularAndStaticColumns,
                                                          int versions,
                                                          long nowInSec)
    {
        // the check for whether there are any registered indexes is already done in CompactionIterator
        return new IndexGCTransaction(key, regularAndStaticColumns, keyspace, versions, nowInSec, listIndexGroups(), writableIndexSelector());
    }

    /**
     * Transaction for use when removing partitions during cleanup
     */
    public CleanupTransaction newCleanupTransaction(DecoratedKey key,
                                                    RegularAndStaticColumns regularAndStaticColumns,
                                                    long nowInSec)
    {
        if (!hasIndexes())
            return CleanupTransaction.NO_OP;

        return new CleanupGCTransaction(key, regularAndStaticColumns, keyspace, nowInSec, listIndexGroups(), writableIndexSelector());
    }

    /**
     * @param type index transaction type
     * @return true if at least one of the indexes will be able to handle given index transaction type
     */
    public boolean handles(IndexTransaction.Type type)
    {
        for (Index.Group group : indexGroups.values())
        {
            if (group.handles(type))
                return true;
        }
        return false;
    }

    /**
     * A single use transaction for processing a partition update on the regular write path
     */
    private static final class WriteTimeTransaction implements UpdateTransaction
    {
        private final Index.Indexer[] indexers;

        private WriteTimeTransaction(Index.Indexer... indexers)
        {
            // don't allow null indexers, if we don't need any use a NullUpdater object
            for (Index.Indexer indexer : indexers) assert indexer != null;
            this.indexers = indexers;
        }

        public void start()
        {
            for (Index.Indexer indexer : indexers)
                indexer.begin();
        }

        public void onPartitionDeletion(DeletionTime deletionTime)
        {
            for (Index.Indexer indexer : indexers)
                indexer.partitionDelete(deletionTime);
        }

        public void onRangeTombstone(RangeTombstone tombstone)
        {
            for (Index.Indexer indexer : indexers)
                indexer.rangeTombstone(tombstone);
        }

        public void onInserted(Row row)
        {
            for (Index.Indexer indexer : indexers)
                indexer.insertRow(row);
        }

        public void onUpdated(Row existing, Row updated)
        {
            final Row.Builder toRemove = BTreeRow.sortedBuilder();
            toRemove.newRow(existing.clustering());
            toRemove.addPrimaryKeyLivenessInfo(existing.primaryKeyLivenessInfo());
            toRemove.addRowDeletion(existing.deletion());
            final Row.Builder toInsert = BTreeRow.sortedBuilder();
            toInsert.newRow(updated.clustering());
            toInsert.addPrimaryKeyLivenessInfo(updated.primaryKeyLivenessInfo());
            toInsert.addRowDeletion(updated.deletion());
            // diff listener collates the columns to be added & removed from the indexes
            RowDiffListener diffListener = new RowDiffListener()
            {
                public void onPrimaryKeyLivenessInfo(int i, Clustering<?> clustering, LivenessInfo merged, LivenessInfo original)
                {
                }

                public void onDeletion(int i, Clustering<?> clustering, Row.Deletion merged, Row.Deletion original)
                {
                }

                public void onComplexDeletion(int i, Clustering<?> clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original)
                {
                }

                public void onCell(int i, Clustering<?> clustering, Cell<?> merged, Cell<?> original)
                {
                    if (merged != null && !merged.equals(original))
                        toInsert.addCell(merged);

                    if (merged == null || (original != null && shouldCleanupOldValue(original, merged)))
                        toRemove.addCell(original);
                }
            };
            Rows.diff(diffListener, updated, existing);
            Row oldRow = toRemove.build();
            Row newRow = toInsert.build();
            for (Index.Indexer indexer : indexers)
                indexer.updateRow(oldRow, newRow);
        }

        public void commit()
        {
            for (Index.Indexer indexer : indexers)
                indexer.finish();
        }

        private <V1, V2> boolean shouldCleanupOldValue(Cell<V1> oldCell, Cell<V2> newCell)
        {
            // If either the value or timestamp is different, then we
            // should delete from the index. If not, then we can infer that
            // at least one of the cells is an ExpiringColumn and that the
            // difference is in the expiry time. In this case, we don't want to
            // delete the old value from the index as the tombstone we insert
            // will just hide the inserted value.
            // Completely identical cells (including expiring columns with
            // identical ttl & localExpirationTime) will not get this far due
            // to the oldCell.equals(newCell) in StandardUpdater.update
            return !Cells.valueEqual(oldCell, newCell) || oldCell.timestamp() != newCell.timestamp();
        }
    }

    /**
     * A single-use transaction for updating indexes for a single partition during compaction where the only
     * operation is to merge rows
     * TODO : make this smarter at batching updates so we can use a single transaction to process multiple rows in
     * a single partition
     */
    private static final class IndexGCTransaction implements CompactionTransaction
    {
        private final DecoratedKey key;
        private final RegularAndStaticColumns columns;
        private final Keyspace keyspace;
        private final int versions;
        private final long nowInSec;
        private final Collection<Index.Group> indexGroups;
        private final Predicate<Index> writableIndexSelector;

        private Row[] rows;

        private IndexGCTransaction(DecoratedKey key,
                                   RegularAndStaticColumns columns,
                                   Keyspace keyspace,
                                   int versions,
                                   long nowInSec,
                                   Collection<Index.Group> indexGroups,
                                   Predicate<Index> writableIndexSelector)
        {
            this.key = key;
            this.columns = columns;
            this.keyspace = keyspace;
            this.versions = versions;
            this.indexGroups = indexGroups;
            this.nowInSec = nowInSec;
            this.writableIndexSelector = writableIndexSelector;
        }

        public void start()
        {
            if (versions > 0)
                rows = new Row[versions];
        }

        public void onRowMerge(Row merged, Row... versions)
        {
            // Diff listener constructs rows representing deltas between the merged and original versions
            // These delta rows are then passed to registered indexes for removal processing
            final Row.Builder[] builders = new Row.Builder[versions.length];
            RowDiffListener diffListener = new RowDiffListener()
            {
                public void onPrimaryKeyLivenessInfo(int i, Clustering<?> clustering, LivenessInfo merged, LivenessInfo original)
                {
                    if (original != null && (merged == null || !merged.isLive(nowInSec)))
                        getBuilder(i, clustering).addPrimaryKeyLivenessInfo(original);
                }

                public void onDeletion(int i, Clustering<?> clustering, Row.Deletion merged, Row.Deletion original)
                {
                }

                public void onComplexDeletion(int i, Clustering<?> clustering, ColumnMetadata column, DeletionTime merged, DeletionTime original)
                {
                }

                public void onCell(int i, Clustering<?> clustering, Cell<?> merged, Cell<?> original)
                {
                    if (original != null && (merged == null || !merged.isLive(nowInSec)))
                        getBuilder(i, clustering).addCell(original);
                }

                private Row.Builder getBuilder(int index, Clustering<?> clustering)
                {
                    if (builders[index] == null)
                    {
                        builders[index] = BTreeRow.sortedBuilder();
                        builders[index].newRow(clustering);
                    }
                    return builders[index];
                }
            };

            Rows.diff(diffListener, merged, versions);

            for (int i = 0; i < builders.length; i++)
                if (builders[i] != null)
                    rows[i] = builders[i].build();
        }

        public void commit()
        {
            if (rows == null)
                return;

            try (WriteContext ctx = keyspace.getWriteHandler().createContextForIndexing())
            {
                for (Index.Group group : indexGroups)
                {
                    Index.Indexer indexer = group.indexerFor(writableIndexSelector, key, columns, nowInSec, ctx, Type.COMPACTION, null);
                    if (indexer == null)
                        continue;

                    indexer.begin();
                    for (Row row : rows)
                        if (row != null)
                            indexer.removeRow(row);
                    indexer.finish();
                }
            }
        }
    }

    /**
     * A single-use transaction for updating indexes for a single partition during cleanup, where
     * partitions and rows are only removed
     * TODO : make this smarter at batching updates so we can use a single transaction to process multiple rows in
     * a single partition
     */
    private static final class CleanupGCTransaction implements CleanupTransaction
    {
        private final DecoratedKey key;
        private final RegularAndStaticColumns columns;
        private final Keyspace keyspace;
        private final long nowInSec;
        private final Collection<Index.Group> indexGroups;
        private final Predicate<Index> writableIndexSelector;

        private Row row;
        private DeletionTime partitionDelete;

        private CleanupGCTransaction(DecoratedKey key,
                                     RegularAndStaticColumns columns,
                                     Keyspace keyspace,
                                     long nowInSec,
                                     Collection<Index.Group> indexGroups,
                                     Predicate<Index> writableIndexSelector)
        {
            this.key = key;
            this.columns = columns;
            this.keyspace = keyspace;
            this.indexGroups = indexGroups;
            this.nowInSec = nowInSec;
            this.writableIndexSelector = writableIndexSelector;
        }

        public void start()
        {
        }

        public void onPartitionDeletion(DeletionTime deletionTime)
        {
            partitionDelete = deletionTime;
        }

        public void onRowDelete(Row row)
        {
            this.row = row;
        }

        public void commit()
        {
            if (row == null && partitionDelete == null)
                return;

            try (WriteContext ctx = keyspace.getWriteHandler().createContextForIndexing())
            {
                for (Index.Group group : indexGroups)
                {
                    Index.Indexer indexer = group.indexerFor(writableIndexSelector, key, columns, nowInSec, ctx, Type.CLEANUP, null);
                    if (indexer == null)
                        continue;

                    indexer.begin();

                    if (partitionDelete != null)
                        indexer.partitionDelete(partitionDelete);

                    if (row != null)
                        indexer.removeRow(row);

                    indexer.finish();
                }
            }
        }
    }

    private void executeBlocking(Callable<?> task, FutureCallback callback)
    {
        if (null != task)
        {
            Future<?> f = blockingExecutor.submit(task);
            if (callback != null) f.addCallback(callback);
            FBUtilities.waitOnFuture(f);
        }
    }

    private void executeAllBlocking(Stream<Index> indexers, Function<Index, Callable<?>> function, FutureCallback callback)
    {
        if (function == null)
        {
            logger.error("failed to flush indexes: {} because flush task is missing.", indexers);
            return;
        }

        List<Future<?>> waitFor = new ArrayList<>();
        indexers.forEach(indexer ->
                         {
                             Callable<?> task = function.apply(indexer);
                             if (null != task)
                             {
                                 Future<?> f = blockingExecutor.submit(task);
                                 if (callback != null) f.addCallback(callback);
                                 waitFor.add(f);
                             }
                         });
        FBUtilities.waitOnFutures(waitFor);
    }

    public void handleNotification(INotification notification, Object sender)
    {
        if (!indexes.isEmpty() && notification instanceof SSTableAddedNotification)
        {
            SSTableAddedNotification notice = (SSTableAddedNotification) notification;

            // SSTables asociated to a memtable come from a flush, so their contents have already been indexed
            if (notice.memtable().isEmpty())
                buildIndexesBlocking(Lists.newArrayList(notice.added),
                                     indexes.values()
                                            .stream()
                                            .filter(Index::shouldBuildBlocking)
                                            .filter(i -> !i.isSSTableAttached())
                                            .collect(Collectors.toSet()),
                                     false);
        }
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        shutdown(asyncExecutor, blockingExecutor);
        awaitTermination(timeout, units, asyncExecutor, blockingExecutor);
    }

    public void makeIndexNonQueryable(Index index, Index.Status status)
    {
        if (status == Index.Status.BUILD_SUCCEEDED)
            throw new IllegalStateException("Index cannot be marked non-queryable with status " + status);

        String name = index.getIndexMetadata().name;
        if (indexes.get(name) == index)
        {
            IndexStatusManager.instance.propagateLocalIndexStatus(keyspace.getName(), name, status);
            if (!index.isQueryable(status))
                queryableIndexes.remove(name);
        }
    }

    public void makeIndexQueryable(Index index, Index.Status status)
    {
        if (status != Index.Status.BUILD_SUCCEEDED)
            throw new IllegalStateException("Index cannot be marked queryable with status " + status);

        String name = index.getIndexMetadata().name;
        if (indexes.get(name) == index)
        {
            IndexStatusManager.instance.propagateLocalIndexStatus(keyspace.getName(), name, status);
            if (index.isQueryable(status))
            {
                if (queryableIndexes.add(name))
                    logger.info("Index [{}] became queryable after successful build.", name);
            }

            if (writableIndexes.put(name, index) == null)
                logger.info("Index [{}] became writable after successful build.", name);
        }
    }
}
