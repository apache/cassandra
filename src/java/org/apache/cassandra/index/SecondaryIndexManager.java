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

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.transactions.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Handles the core maintenance functionality associated with indexes: adding/removing them to or from
 * a table, (re)building during bootstrap or other streaming operations, flushing, reloading metadata
 * and so on.
 *
 * The Index interface defines a number of methods which return {@code Callable<?>}. These are primarily the
 * management tasks for an index implementation. Most of them are currently executed in a blocking
 * fashion via submission to SIM's blockingExecutor. This provides the desired behaviour in pretty
 * much all cases, as tasks like flushing an index needs to be executed synchronously to avoid potentially
 * deadlocking on the FlushWriter or PostFlusher. Several of these {@code Callable<?>} returning methods on Index could
 * then be defined with as void and called directly from SIM (rather than being run via the executor service).
 * Separating the task defintion from execution gives us greater flexibility though, so that in future, for example,
 * if the flush process allows it we leave open the possibility of executing more of these tasks asynchronously.
 *
 * The primary exception to the above is the Callable returned from Index#addIndexedColumn. This may
 * involve a significant effort, building a new index over any existing data. We perform this task asynchronously;
 * as it is called as part of a schema update, which we do not want to block for a long period. Building non-custom
 * indexes is performed on the CompactionManager.
 *
 * This class also provides instances of processors which listen to updates to the base table and forward to
 * registered Indexes the info required to keep those indexes up to date.
 * There are two variants of these processors, each with a factory method provided by SIM:
 *      IndexTransaction: deals with updates generated on the regular write path.
 *      CleanupTransaction: used when partitions are modified during compaction or cleanup operations.
 * Further details on their usage and lifecycles can be found in the interface definitions below.
 *
 * Finally, the bestIndexFor method is used at query time to identify the most selective index of those able
 * to satisfy any search predicates defined by a ReadCommand's RowFilter. It returns a thin IndexAccessor object
 * which enables the ReadCommand to access the appropriate functions of the Index at various stages in its lifecycle.
 * e.g. the getEstimatedResultRows is required when StorageProxy calculates the initial concurrency factor for
 * distributing requests to replicas, whereas a Searcher instance is needed when the ReadCommand is executed locally on
 * a target replica.
 */
public class SecondaryIndexManager implements IndexRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(SecondaryIndexManager.class);

    // default page size (in rows) when rebuilding the index for a whole partition
    public static final int DEFAULT_PAGE_SIZE = 10000;

    private Map<String, Index> indexes = Maps.newConcurrentMap();

    /**
     * The indexes that are ready to server requests.
     */
    private Set<String> builtIndexes = Sets.newConcurrentHashSet();

    // executes tasks returned by Indexer#addIndexColumn which may require index(es) to be (re)built
    private static final ExecutorService asyncExecutor =
        new JMXEnabledThreadPoolExecutor(1,
                                         StageManager.KEEPALIVE,
                                         TimeUnit.SECONDS,
                                         new LinkedBlockingQueue<>(),
                                         new NamedThreadFactory("SecondaryIndexManagement"),
                                         "internal");

    // executes all blocking tasks produced by Indexers e.g. getFlushTask, getMetadataReloadTask etc
    private static final ExecutorService blockingExecutor = MoreExecutors.newDirectExecutorService();

    /**
     * The underlying column family containing the source data for these indexes
     */
    public final ColumnFamilyStore baseCfs;

    public SecondaryIndexManager(ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
    }


    /**
     * Drops and adds new indexes associated with the underlying CF
     */
    public void reload()
    {
        // figure out what needs to be added and dropped.
        Indexes tableIndexes = baseCfs.metadata.getIndexes();
        indexes.keySet()
               .stream()
               .filter(indexName -> !tableIndexes.has(indexName))
               .forEach(this::removeIndex);

        // we call add for every index definition in the collection as
        // some may not have been created here yet, only added to schema
        for (IndexMetadata tableIndex : tableIndexes)
            addIndex(tableIndex);
    }

    private Future<?> reloadIndex(IndexMetadata indexDef)
    {
        Index index = indexes.get(indexDef.name);
        Callable<?> reloadTask = index.getMetadataReloadTask(indexDef);
        return reloadTask == null
               ? Futures.immediateFuture(null)
               : blockingExecutor.submit(reloadTask);
    }

    private Future<?> createIndex(IndexMetadata indexDef)
    {
        Index index = createInstance(indexDef);
        index.register(this);

        // if the index didn't register itself, we can probably assume that no initialization needs to happen
        final Callable<?> initialBuildTask = indexes.containsKey(indexDef.name)
                                           ? index.getInitializationTask()
                                           : null;
        if (initialBuildTask == null)
        {
            // We need to make sure that the index is marked as built in the case where the initialBuildTask
            // does not need to be run (if the index didn't register itself or if the base table was empty).
            markIndexBuilt(indexDef.name);
            return Futures.immediateFuture(null);
        }
        return asyncExecutor.submit(index.getInitializationTask());
    }

    /**
     * Adds and builds a index
     * @param indexDef the IndexMetadata describing the index
     */
    public synchronized Future<?> addIndex(IndexMetadata indexDef)
    {
        if (indexes.containsKey(indexDef.name))
            return reloadIndex(indexDef);
        else
            return createIndex(indexDef);
    }

    /**
     * Checks if the specified index is queryable.
     *
     * @param index the index
     * @return <code>true</code> if the specified index is queryable, <code>false</code> otherwise
     */
    public boolean isIndexQueryable(Index index)
    {
        return builtIndexes.contains(index.getIndexMetadata().name);
    }

    public synchronized void removeIndex(String indexName)
    {
        Index index = unregisterIndex(indexName);
        if (null != index)
        {
            markIndexRemoved(indexName);
            executeBlocking(index.getInvalidateTask());
        }
    }


    public Set<IndexMetadata> getDependentIndexes(ColumnDefinition column)
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
    * Does a full, blocking rebuild of the indexes specified by columns from the sstables.
    * Caller must acquire and release references to the sstables used here.
    * Note also that only this method of (re)building indexes:
    *   a) takes a set of index *names* rather than Indexers
    *   b) marks exsiting indexes removed prior to rebuilding
    *
    * @param sstables the data to build from
    * @param indexNames the list of indexes to be rebuilt
    */
    public void rebuildIndexesBlocking(Collection<SSTableReader> sstables, Set<String> indexNames)
    {
        Set<Index> toRebuild = indexes.values().stream()
                                               .filter(index -> indexNames.contains(index.getIndexMetadata().name))
                                               .filter(Index::shouldBuildBlocking)
                                               .collect(Collectors.toSet());
        if (toRebuild.isEmpty())
        {
            logger.info("No defined indexes with the supplied names: {}", Joiner.on(',').join(indexNames));
            return;
        }

        toRebuild.forEach(indexer -> markIndexRemoved(indexer.getIndexMetadata().name));

        buildIndexesBlocking(sstables, toRebuild);

        toRebuild.forEach(indexer -> markIndexBuilt(indexer.getIndexMetadata().name));
    }

    public void buildAllIndexesBlocking(Collection<SSTableReader> sstables)
    {
        buildIndexesBlocking(sstables, indexes.values()
                                              .stream()
                                              .filter(Index::shouldBuildBlocking)
                                              .collect(Collectors.toSet()));
    }

    // For convenience, may be called directly from Index impls
    public void buildIndexBlocking(Index index)
    {
        if (index.shouldBuildBlocking())
        {
            try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL));
                 Refs<SSTableReader> sstables = viewFragment.refs)
            {
                buildIndexesBlocking(sstables, Collections.singleton(index));
                markIndexBuilt(index.getIndexMetadata().name);
            }
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

    private void buildIndexesBlocking(Collection<SSTableReader> sstables, Set<Index> indexes)
    {
        if (indexes.isEmpty())
            return;

        logger.info("Submitting index build of {} for data in {}",
                    indexes.stream().map(i -> i.getIndexMetadata().name).collect(Collectors.joining(",")),
                    sstables.stream().map(SSTableReader::toString).collect(Collectors.joining(",")));

        Map<Index.IndexBuildingSupport, Set<Index>> byType = new HashMap<>();
        for (Index index : indexes)
        {
            Set<Index> stored = byType.computeIfAbsent(index.getBuildTaskSupport(), i -> new HashSet<>());
            stored.add(index);
        }

        List<Future<?>> futures = byType.entrySet()
                                        .stream()
                                        .map((e) -> e.getKey().getIndexBuildTask(baseCfs, e.getValue(), sstables))
                                        .map(CompactionManager.instance::submitIndexBuild)
                                        .collect(Collectors.toList());

        FBUtilities.waitOnFutures(futures);

        flushIndexesBlocking(indexes);
        logger.info("Index build of {} complete",
                    indexes.stream().map(i -> i.getIndexMetadata().name).collect(Collectors.joining(",")));
    }

    /**
     * Marks the specified index as build.
     * <p>This method is public as it need to be accessible from the {@link Index} implementations</p>
     * @param indexName the index name
     */
    public void markIndexBuilt(String indexName)
    {
        builtIndexes.add(indexName);
        if (DatabaseDescriptor.isDaemonInitialized())
            SystemKeyspace.setIndexBuilt(baseCfs.keyspace.getName(), indexName);
    }

    /**
     * Marks the specified index as removed.
     * <p>This method is public as it need to be accessible from the {@link Index} implementations</p>
     * @param indexName the index name
     */
    public void markIndexRemoved(String indexName)
    {
        SystemKeyspace.setIndexRemoved(baseCfs.keyspace.getName(), indexName);
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
            String className = indexDef.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
            assert ! Strings.isNullOrEmpty(className);
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
        executeAllBlocking(indexes.values().stream(), (index) -> index.getTruncateTask(truncatedAt));
    }

    /**
     * Remove all indexes
     */
    public void invalidateAllIndexesBlocking()
    {
        markAllIndexesRemoved();
        executeAllBlocking(indexes.values().stream(), Index::getInvalidateTask);
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
                     .map(cfs -> wait.add(cfs.forceFlush()))
                     .orElseGet(() -> nonCfsIndexes.add(index)));
        }

        executeAllBlocking(nonCfsIndexes.stream(), Index::getBlockingFlushTask);
        FBUtilities.waitOnFutures(wait);
    }

    /**
     * Performs a blocking flush of all custom indexes
     */
    public void flushAllNonCFSBackedIndexesBlocking()
    {
        executeAllBlocking(indexes.values()
                                  .stream()
                                  .filter(index -> !index.getBackingTable().isPresent()),
                           Index::getBlockingFlushTask);
    }

    /**
     * Performs a blocking execution of pre-join tasks of all indexes
     */
    public void executePreJoinTasksBlocking(boolean hadBootstrap)
    {
        logger.info("Executing pre-join{} tasks for: {}", hadBootstrap ? " post-bootstrap" : "", this.baseCfs);
        executeAllBlocking(indexes.values().stream(), (index) -> {
            return index.getPreJoinTask(hadBootstrap);
        });
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
        return SystemKeyspace.getBuiltIndexes(baseCfs.keyspace.getName(), allIndexNames);
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

    /**
     * When building an index against existing data in sstables, add the given partition to the index
     */
    public void indexPartition(DecoratedKey key, Set<Index> indexes, int pageSize)
    {
        if (logger.isTraceEnabled())
            logger.trace("Indexing partition {}", baseCfs.metadata.getKeyValidator().getString(key.getKey()));

        if (!indexes.isEmpty())
        {
            SinglePartitionReadCommand cmd = SinglePartitionReadCommand.fullPartitionRead(baseCfs.metadata,
                                                                                          FBUtilities.nowInSeconds(),
                                                                                          key);
            int nowInSec = cmd.nowInSec();
            boolean readStatic = false;

            SinglePartitionPager pager = new SinglePartitionPager(cmd, null, ProtocolVersion.CURRENT);
            while (!pager.isExhausted())
            {
                try (ReadExecutionController controller = cmd.executionController();
                     OpOrder.Group writeGroup = Keyspace.writeOrder.start();
                     UnfilteredPartitionIterator page = pager.fetchPageUnfiltered(baseCfs.metadata, pageSize, controller))
                {
                    if (!page.hasNext())
                        break;

                    try (UnfilteredRowIterator partition = page.next()) {
                        Set<Index.Indexer> indexers = indexes.stream()
                                                             .map(index -> index.indexerFor(key,
                                                                                            partition.columns(),
                                                                                            nowInSec,
                                                                                            writeGroup,
                                                                                            IndexTransaction.Type.UPDATE))
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
        if (Boolean.getBoolean("cassandra.force_default_indexing_page_size"))
            return DEFAULT_PAGE_SIZE;

        double targetPageSizeInBytes = 32 * 1024 * 1024;
        double meanPartitionSize = baseCfs.getMeanPartitionSize();
        if (meanPartitionSize <= 0)
            return DEFAULT_PAGE_SIZE;

        int meanCellsPerPartition = baseCfs.getMeanColumns();
        if (meanCellsPerPartition <= 0)
            return DEFAULT_PAGE_SIZE;

        int columnsPerRow = baseCfs.metadata.partitionColumns().regulars.size();
        if (columnsPerRow <= 0)
            return DEFAULT_PAGE_SIZE;

        int meanRowsPerPartition = meanCellsPerPartition / columnsPerRow;
        double meanRowSize = meanPartitionSize / meanRowsPerPartition;

        int pageSize = (int) Math.max(1, Math.min(DEFAULT_PAGE_SIZE, targetPageSizeInBytes / meanRowSize));

        logger.trace("Calculated page size {} for indexing {}.{} ({}/{}/{}/{})",
                     pageSize,
                     baseCfs.metadata.ksName,
                     baseCfs.metadata.cfName,
                     meanPartitionSize,
                     meanCellsPerPartition,
                     meanRowsPerPartition,
                     meanRowSize);

        return pageSize;
    }

    /**
     * Delete all data from all indexes for this partition.
     * For when cleanup rips a partition out entirely.
     *
     * TODO : improve cleanup transaction to batch updates and perform them async
     */
    public void deletePartition(UnfilteredRowIterator partition, int nowInSec)
    {
        // we need to acquire memtable lock because secondary index deletion may
        // cause a race (see CASSANDRA-3712). This is done internally by the
        // index transaction when it commits
        CleanupTransaction indexTransaction = newCleanupTransaction(partition.partitionKey(),
                                                                    partition.columns(),
                                                                    nowInSec);
        indexTransaction.start();
        indexTransaction.onPartitionDeletion(new DeletionTime(FBUtilities.timestampMicros(), nowInSec));
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
            indexTransaction.onRowDelete((Row)unfiltered);
            indexTransaction.commit();
        }
    }

    /**
     * Called at query time to choose which (if any) of the registered index implementations to use for a given query.
     *
     * This is a two step processes, firstly compiling the set of searchable indexes then choosing the one which reduces
     * the search space the most.
     *
     * In the first phase, if the command's RowFilter contains any custom index expressions, the indexes that they
     * specify are automatically included. Following that, the registered indexes are filtered to include only those
     * which support the standard expressions in the RowFilter.
     *
     * The filtered set then sorted by selectivity, as reported by the Index implementations' getEstimatedResultRows
     * method.
     *
     * Implementation specific validation of the target expression, either custom or standard, by the selected
     * index should be performed in the searcherFor method to ensure that we pick the right index regardless of
     * the validity of the expression.
     *
     * This method is only called once during the lifecycle of a ReadCommand and the result is
     * cached for future use when obtaining a Searcher, getting the index's underlying CFS for
     * ReadOrderGroup, or an estimate of the result size from an average index query.
     *
     * @param rowFilter RowFilter of the command to be executed
     * @return an Index instance, ready to use during execution of the command, or null if none
     * of the registered indexes can support the command.
     */
    public Index getBestIndexFor(RowFilter rowFilter)
    {
        if (indexes.isEmpty() || rowFilter.isEmpty())
            return null;

        Set<Index> searchableIndexes = new HashSet<>();
        for (RowFilter.Expression expression : rowFilter)
        {
            if (expression.isCustom())
            {
                // Only a single custom expression is allowed per query and, if present,
                // we want to always favour the index specified in such an expression
                RowFilter.CustomExpression customExpression = (RowFilter.CustomExpression)expression;
                logger.trace("Command contains a custom index expression, using target index {}", customExpression.getTargetIndex().name);
                Tracing.trace("Command contains a custom index expression, using target index {}", customExpression.getTargetIndex().name);
                return indexes.get(customExpression.getTargetIndex().name);
            }
            else if (!expression.isUserDefined())
            {
                indexes.values().stream()
                       .filter(index -> index.supportsExpression(expression.column(), expression.operator()))
                       .forEach(searchableIndexes::add);
            }
        }

        if (searchableIndexes.isEmpty())
        {
            logger.trace("No applicable indexes found");
            Tracing.trace("No applicable indexes found");
            return null;
        }

        Index selected = searchableIndexes.size() == 1
                         ? Iterables.getOnlyElement(searchableIndexes)
                         : searchableIndexes.stream()
                                            .min((a, b) -> Longs.compare(a.getEstimatedResultRows(),
                                                                         b.getEstimatedResultRows()))
                                            .orElseThrow(() -> new AssertionError("Could not select most selective index"));

        // pay for an additional threadlocal get() rather than build the strings unnecessarily
        if (Tracing.isTracing())
        {
            Tracing.trace("Index mean cardinalities are {}. Scanning with {}.",
                          searchableIndexes.stream().map(i -> i.getIndexMetadata().name + ':' + i.getEstimatedResultRows())
                                           .collect(Collectors.joining(",")),
                          selected.getIndexMetadata().name);
        }
        return selected;
    }

    public Optional<Index> getBestIndexFor(RowFilter.Expression expression)
    {
        return indexes.values().stream().filter((i) -> i.supportsExpression(expression.column(), expression.operator())).findFirst();
    }

    /**
     * Called at write time to ensure that values present in the update
     * are valid according to the rules of all registered indexes which
     * will process it. The partition key as well as the clustering and
     * cell values for each row in the update may be checked by index
     * implementations
     * @param update PartitionUpdate containing the values to be validated by registered Index implementations
     * @throws InvalidRequestException
     */
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {
        for (Index index : indexes.values())
            index.validate(update);
    }

    /**
     * IndexRegistry methods
     */
    public void registerIndex(Index index)
    {
        String name = index.getIndexMetadata().name;
        indexes.put(name, index);
        logger.trace("Registered index {}", name);
    }

    public void unregisterIndex(Index index)
    {
        unregisterIndex(index.getIndexMetadata().name);
    }

    private Index unregisterIndex(String name)
    {
        Index removed = indexes.remove(name);
        builtIndexes.remove(name);
        logger.trace(removed == null ? "Index {} was not registered" : "Removed index {} from registry",
                     name);
        return removed;
    }

    public Index getIndex(IndexMetadata metadata)
    {
        return indexes.get(metadata.name);
    }

    public Collection<Index> listIndexes()
    {
        return ImmutableSet.copyOf(indexes.values());
    }

    /**
     * Handling of index updates.
     * Implementations of the various IndexTransaction interfaces, for keeping indexes in sync with base data
     * during updates, compaction and cleanup. Plus factory methods for obtaining transaction instances.
     */

    /**
     * Transaction for updates on the write path.
     */
    public UpdateTransaction newUpdateTransaction(PartitionUpdate update, OpOrder.Group opGroup, int nowInSec)
    {
        if (!hasIndexes())
            return UpdateTransaction.NO_OP;

        Index.Indexer[] indexers = indexes.values().stream()
                                          .map(i -> i.indexerFor(update.partitionKey(),
                                                                 update.columns(),
                                                                 nowInSec,
                                                                 opGroup,
                                                                 IndexTransaction.Type.UPDATE))
                                          .filter(Objects::nonNull)
                                          .toArray(Index.Indexer[]::new);

        return indexers.length == 0 ? UpdateTransaction.NO_OP : new WriteTimeTransaction(indexers);
    }

    /**
     * Transaction for use when merging rows during compaction
     */
    public CompactionTransaction newCompactionTransaction(DecoratedKey key,
                                                          PartitionColumns partitionColumns,
                                                          int versions,
                                                          int nowInSec)
    {
        // the check for whether there are any registered indexes is already done in CompactionIterator
        return new IndexGCTransaction(key, partitionColumns, versions, nowInSec, listIndexes());
    }

    /**
     * Transaction for use when removing partitions during cleanup
     */
    public CleanupTransaction newCleanupTransaction(DecoratedKey key,
                                                    PartitionColumns partitionColumns,
                                                    int nowInSec)
    {
        if (!hasIndexes())
            return CleanupTransaction.NO_OP;

        return new CleanupGCTransaction(key, partitionColumns, nowInSec, listIndexes());
    }

    /**
     * A single use transaction for processing a partition update on the regular write path
     */
    private static final class WriteTimeTransaction implements UpdateTransaction
    {
        private final Index.Indexer[] indexers;

        private WriteTimeTransaction(Index.Indexer...indexers)
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
                public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
                {
                }

                public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original)
                {
                }

                public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
                {
                }

                public void onCell(int i, Clustering clustering, Cell merged, Cell original)
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

        private boolean shouldCleanupOldValue(Cell oldCell, Cell newCell)
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
            return !oldCell.value().equals(newCell.value()) || oldCell.timestamp() != newCell.timestamp();
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
        private final PartitionColumns columns;
        private final int versions;
        private final int nowInSec;
        private final Collection<Index> indexes;

        private Row[] rows;

        private IndexGCTransaction(DecoratedKey key,
                                   PartitionColumns columns,
                                   int versions,
                                   int nowInSec,
                                   Collection<Index> indexes)
        {
            this.key = key;
            this.columns = columns;
            this.versions = versions;
            this.indexes = indexes;
            this.nowInSec = nowInSec;
        }

        public void start()
        {
            if (versions > 0)
                rows = new Row[versions];
        }

        public void onRowMerge(Row merged, Row...versions)
        {
            // Diff listener constructs rows representing deltas between the merged and original versions
            // These delta rows are then passed to registered indexes for removal processing
            final Row.Builder[] builders = new Row.Builder[versions.length];
            RowDiffListener diffListener = new RowDiffListener()
            {
                public void onPrimaryKeyLivenessInfo(int i, Clustering clustering, LivenessInfo merged, LivenessInfo original)
                {
                    if (original != null && (merged == null || !merged.isLive(nowInSec)))
                        getBuilder(i, clustering).addPrimaryKeyLivenessInfo(original);
                }

                public void onDeletion(int i, Clustering clustering, Row.Deletion merged, Row.Deletion original)
                {
                }

                public void onComplexDeletion(int i, Clustering clustering, ColumnDefinition column, DeletionTime merged, DeletionTime original)
                {
                }

                public void onCell(int i, Clustering clustering, Cell merged, Cell original)
                {
                    if (original != null && (merged == null || !merged.isLive(nowInSec)))
                        getBuilder(i, clustering).addCell(original);
                }

                private Row.Builder getBuilder(int index, Clustering clustering)
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

            for(int i = 0; i < builders.length; i++)
                if (builders[i] != null)
                    rows[i] = builders[i].build();
        }

        public void commit()
        {
            if (rows == null)
                return;

            try (OpOrder.Group opGroup = Keyspace.writeOrder.start())
            {
                for (Index index : indexes)
                {
                    Index.Indexer indexer = index.indexerFor(key, columns, nowInSec, opGroup, Type.COMPACTION);
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
        private final PartitionColumns columns;
        private final int nowInSec;
        private final Collection<Index> indexes;

        private Row row;
        private DeletionTime partitionDelete;

        private CleanupGCTransaction(DecoratedKey key,
                                     PartitionColumns columns,
                                     int nowInSec,
                                     Collection<Index> indexes)
        {
            this.key = key;
            this.columns = columns;
            this.indexes = indexes;
            this.nowInSec = nowInSec;
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

            try (OpOrder.Group opGroup = Keyspace.writeOrder.start())
            {
                for (Index index : indexes)
                {
                    Index.Indexer indexer = index.indexerFor(key, columns, nowInSec, opGroup, Type.CLEANUP);
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

    private static void executeBlocking(Callable<?> task)
    {
        if (null != task)
            FBUtilities.waitOnFuture(blockingExecutor.submit(task));
    }

    private static void executeAllBlocking(Stream<Index> indexers, Function<Index, Callable<?>> function)
    {
        if (function == null)
        {
            logger.error("failed to flush indexes: {} because flush task is missing.", indexers);
            return;
        }

        List<Future<?>> waitFor = new ArrayList<>();
        indexers.forEach(indexer -> {
            Callable<?> task = function.apply(indexer);
            if (null != task)
                waitFor.add(blockingExecutor.submit(task));
        });
        FBUtilities.waitOnFutures(waitFor);
    }

    @VisibleForTesting
    public static void shutdownExecutors() throws InterruptedException
    {
        ExecutorService[] executors = new ExecutorService[]{ asyncExecutor, blockingExecutor };
        for (ExecutorService executor : executors)
            executor.shutdown();
        for (ExecutorService executor : executors)
            executor.awaitTermination(60, TimeUnit.SECONDS);
    }
}
