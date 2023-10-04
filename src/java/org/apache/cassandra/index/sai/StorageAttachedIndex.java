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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture; // checkstyle: permit this import
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures; // checkstyle: permit this import
import com.google.common.util.concurrent.ListenableFuture; // checkstyle: permit this import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.SecondaryIndexBuilder;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class StorageAttachedIndex implements Index
{
    public static final String NAME = "sai";

    @VisibleForTesting
    public static final String ANALYSIS_ON_KEY_COLUMNS_MESSAGE = "Analysis options are not supported on primary key columns, but found ";
    
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndex.class);

    private static class StorageAttachedIndexBuildingSupport implements IndexBuildingSupport
    {
        @Override
        public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs,
                                                       Set<Index> indexes,
                                                       Collection<SSTableReader> sstablesToRebuild,
                                                       boolean isFullRebuild)
        {
            NavigableMap<SSTableReader, Set<StorageAttachedIndex>> sstables = new TreeMap<>(Comparator.comparing(s -> s.descriptor.id, SSTableIdFactory.COMPARATOR));
            StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

            assert group != null : "Index group does not exist for table " + cfs.keyspace + '.' + cfs.name;

            indexes.stream()
                   .filter((i) -> i instanceof StorageAttachedIndex)
                   .forEach((i) ->
                            {
                                StorageAttachedIndex sai = (StorageAttachedIndex) i;
                                IndexContext indexContext = ((StorageAttachedIndex) i).getIndexContext();

                                // If this is not a full manual index rebuild we can skip SSTables that already have an
                                // attached index. Otherwise, we override any pre-existent index.
                                Collection<SSTableReader> ss = sstablesToRebuild;
                                if (!isFullRebuild)
                                {
                                    ss = sstablesToRebuild.stream()
                                                          .filter(s -> !IndexDescriptor.create(s).isPerColumnIndexBuildComplete(indexContext))
                                                          .collect(Collectors.toList());
                                }

                                group.dropIndexSSTables(ss, sai);

                                ss.forEach(sstable -> sstables.computeIfAbsent(sstable, ignore -> new HashSet<>()).add(sai));
                            });

            return new StorageAttachedIndexBuilder(group, sstables, isFullRebuild, false);
        }
    }

    // Used to build indexes on newly added SSTables:
    private static final StorageAttachedIndexBuildingSupport INDEX_BUILDER_SUPPORT = new StorageAttachedIndexBuildingSupport();

    private static final Set<String> VALID_OPTIONS = ImmutableSet.of(IndexTarget.TARGET_OPTION_NAME,
                                                                     IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                                     NonTokenizingOptions.CASE_SENSITIVE,
                                                                     NonTokenizingOptions.NORMALIZE,
                                                                     NonTokenizingOptions.ASCII);

    public static final Set<CQL3Type> SUPPORTED_TYPES = ImmutableSet.of(CQL3Type.Native.ASCII, CQL3Type.Native.BIGINT, CQL3Type.Native.DATE,
                                                                        CQL3Type.Native.DOUBLE, CQL3Type.Native.FLOAT, CQL3Type.Native.INT,
                                                                        CQL3Type.Native.SMALLINT, CQL3Type.Native.TEXT, CQL3Type.Native.TIME,
                                                                        CQL3Type.Native.TIMESTAMP, CQL3Type.Native.TIMEUUID, CQL3Type.Native.TINYINT,
                                                                        CQL3Type.Native.UUID, CQL3Type.Native.VARCHAR, CQL3Type.Native.INET,
                                                                        CQL3Type.Native.VARINT, CQL3Type.Native.DECIMAL, CQL3Type.Native.BOOLEAN);

    private static final Set<Class<? extends IPartitioner>> ILLEGAL_PARTITIONERS =
            ImmutableSet.of(OrderPreservingPartitioner.class, LocalPartitioner.class, ByteOrderedPartitioner.class, RandomPartitioner.class);

    private final ColumnFamilyStore baseCfs;
    private final IndexContext indexContext;

    // Tracks whether we've started the index build on initialization.
    private volatile boolean initBuildStarted = false;

    // Tracks whether the index has been invalidated due to removal, a table drop, etc.
    private volatile boolean valid = true;

    public StorageAttachedIndex(ColumnFamilyStore baseCfs, IndexMetadata config)
    {
        this.baseCfs = baseCfs;
        TableMetadata tableMetadata = baseCfs.metadata();
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(tableMetadata, config);
        this.indexContext = new IndexContext(tableMetadata.keyspace,
                                             tableMetadata.name,
                                             tableMetadata.partitionKeyType,
                                             tableMetadata.partitioner,
                                             tableMetadata.comparator,
                                             target.left,
                                             target.right,
                                             config);
    }

    /**
     * Used via reflection in {@link IndexMetadata}
     */
    @SuppressWarnings({ "unused" })
    public static Map<String, String> validateOptions(Map<String, String> options, TableMetadata metadata)
    {
        Map<String, String> unknown = new HashMap<>(2);

        for (Map.Entry<String, String> option : options.entrySet())
        {
            if (!VALID_OPTIONS.contains(option.getKey()))
            {
                unknown.put(option.getKey(), option.getValue());
            }
        }

        if (!unknown.isEmpty())
        {
            return unknown;
        }

        if (ILLEGAL_PARTITIONERS.contains(metadata.partitioner.getClass()))
        {
            throw new InvalidRequestException("Storage-attached index does not support the following IPartitioner implementations: " + ILLEGAL_PARTITIONERS);
        }

        String targetColumn = options.get(IndexTarget.TARGET_OPTION_NAME);

        if (targetColumn == null)
        {
            throw new InvalidRequestException("Missing target column");
        }

        if (targetColumn.split(",").length > 1)
        {
            throw new InvalidRequestException("A storage-attached index cannot be created over multiple columns: " + targetColumn);
        }

        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(metadata, targetColumn);

        if (target == null)
        {
            throw new InvalidRequestException("Failed to retrieve target column for: " + targetColumn);
        }

        // In order to support different index targets on non-frozen map, ie. KEYS, VALUE, ENTRIES, we need to put index
        // name as part of index file name instead of column name. We only need to check that the target is different
        // between indexes. This will only allow indexes in the same column with a different IndexTarget.Type.
        //
        // Note that: "metadata.indexes" already includes current index
        if (metadata.indexes.stream().filter(index -> index.getIndexClassName().equals(StorageAttachedIndex.class.getName()))
                            .map(index -> TargetParser.parse(metadata, index.options.get(IndexTarget.TARGET_OPTION_NAME)))
                            .filter(Objects::nonNull).filter(t -> t.equals(target)).count() > 1)
        {
            throw new InvalidRequestException("Cannot create more than one storage-attached index on the same column: " + target.left);
        }

        AbstractType<?> type = TypeUtil.cellValueType(target.left, target.right);

        // If we are indexing map entries we need to validate the subtypes
        if (TypeUtil.isComposite(type))
        {
            for (AbstractType<?> subType : type.subTypes())
            {
                if (!SUPPORTED_TYPES.contains(subType.asCQL3Type()) && !TypeUtil.isFrozen(subType))
                    throw new InvalidRequestException("Unsupported type: " + subType.asCQL3Type());
            }
        }
        else if (!SUPPORTED_TYPES.contains(type.asCQL3Type()) && !TypeUtil.isFrozen(type))
        {
            throw new InvalidRequestException("Unsupported type: " + type.asCQL3Type());
        }

        Map<String, String> analysisOptions = AbstractAnalyzer.getAnalyzerOptions(options);
        if (target.left.isPrimaryKeyColumn() && !analysisOptions.isEmpty())
        {
            throw new InvalidRequestException(ANALYSIS_ON_KEY_COLUMNS_MESSAGE + new CqlBuilder().append(analysisOptions));
        }
        AbstractAnalyzer.fromOptions(type, analysisOptions);

        return Collections.emptyMap();
    }

    @Override
    public void register(IndexRegistry registry)
    {
        // index will be available for writes
        registry.registerIndex(this, StorageAttachedIndexGroup.GROUP_KEY, () -> new StorageAttachedIndexGroup(baseCfs));
    }

    @Override
    public void unregister(IndexRegistry registry)
    {
        registry.unregisterIndex(this, StorageAttachedIndexGroup.GROUP_KEY);
    }

    @Override
    public IndexMetadata getIndexMetadata()
    {
        return indexContext.getIndexMetadata();
    }

    @Override
    public Callable<?> getInitializationTask()
    {
        // New storage-attached indexes will be available for queries after on disk index data are built.
        // Memtable data will be indexed via flushing triggered by schema change
        // We only want to validate the index files if we are starting up
        IndexValidation validation = StorageService.instance.isStarting() ? IndexValidation.HEADER_FOOTER : IndexValidation.NONE;
        return () -> startInitialBuild(baseCfs, validation).get();
    }

    private Future<?> startInitialBuild(ColumnFamilyStore baseCfs, IndexValidation validation)
    {
        if (baseCfs.indexManager.isIndexQueryable(this))
        {
            logger.debug(indexContext.logMessage("Skipping validation and building in initialization task, as pre-join has already made the storage-attached index queryable..."));
            initBuildStarted = true;
            return CompletableFuture.completedFuture(null);
        }

        // stop in-progress compaction tasks to prevent compacted sstable not being indexed.
        logger.debug(indexContext.logMessage("Stopping active compactions to make sure all sstables are indexed after initial build."));
        CompactionManager.instance.interruptCompactionFor(Collections.singleton(baseCfs.metadata()),
                                                          ssTableReader -> true,
                                                          true);

        // Force another flush to make sure on disk index is generated for memtable data before marking it queryable.
        // In the case of offline scrub, there are no live memtables.
        if (!baseCfs.getTracker().getView().liveMemtables.isEmpty())
        {
            baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.INDEX_BUILD_STARTED);
        }

        // It is now safe to flush indexes directly from flushing Memtables.
        initBuildStarted = true;

        StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);

        assert indexGroup != null : "Index group does not exist for table " + baseCfs.keyspace + '.' + baseCfs.name;

        List<SSTableReader> nonIndexed = findNonIndexedSSTables(baseCfs, indexGroup, validation);

        if (nonIndexed.isEmpty())
        {
            return CompletableFuture.completedFuture(null);
        }

        // split sorted sstables into groups with similar size and build each group in separate compaction thread
        List<List<SSTableReader>> groups = groupBySize(nonIndexed, DatabaseDescriptor.getConcurrentIndexBuilders());
        List<ListenableFuture<?>> futures = new ArrayList<>();

        for (List<SSTableReader> group : groups)
        {
            SortedMap<SSTableReader, Set<StorageAttachedIndex>> current = new TreeMap<>(Comparator.comparing(s -> s.descriptor.id, SSTableIdFactory.COMPARATOR));
            group.forEach(sstable -> current.put(sstable, Collections.singleton(this)));

            futures.add(CompactionManager.instance.submitIndexBuild(new StorageAttachedIndexBuilder(indexGroup, current, false, true)));
        }

        logger.info(indexContext.logMessage("Submitting {} parallel initial index builds over {} total sstables..."), futures.size(), nonIndexed.size());
        return Futures.allAsList(futures);
    }

    /**
     * Splits SSTables into groups of similar overall size.
     *
     * @param toRebuild a list of SSTables to split (Note that this list will be sorted in place!)
     * @param parallelism an upper bound on the number of groups
     *
     * @return a {@link List} of SSTable groups, each represented as a {@link List} of {@link SSTableReader}
     */
    @VisibleForTesting
    public static List<List<SSTableReader>> groupBySize(List<SSTableReader> toRebuild, int parallelism)
    {
        List<List<SSTableReader>> groups = new ArrayList<>();

        toRebuild.sort(Comparator.comparingLong(SSTableReader::onDiskLength).reversed());
        Iterator<SSTableReader> sortedSSTables = toRebuild.iterator();
        double dataPerCompactor = toRebuild.stream().mapToLong(SSTableReader::onDiskLength).sum() * 1.0 / parallelism;

        while (sortedSSTables.hasNext())
        {
            long sum = 0;
            List<SSTableReader> current = new ArrayList<>();

            while (sortedSSTables.hasNext() && sum < dataPerCompactor)
            {
                SSTableReader sstable = sortedSSTables.next();
                sum += sstable.onDiskLength();
                current.add(sstable);
            }

            assert !current.isEmpty();
            groups.add(current);
        }

        return groups;
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    @Override
    public Callable<?> getBlockingFlushTask()
    {
        return null; // storage-attached indexes are flushed alongside memtable
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return () ->
        {
            // mark index as invalid, in-progress SSTableIndexWriters will abort
            valid = false;

            // in case of dropping table, SSTable indexes should already been removed by SSTableListChangedNotification.
            Set<Component> toRemove = getComponents();
            for (SSTableIndex sstableIndex : indexContext.getView().getIndexes())
                sstableIndex.getSSTable().unregisterComponents(toRemove, baseCfs.getTracker());

            indexContext.invalidate();
            return null;
        };
    }

    @Override
    public Callable<?> getPreJoinTask(boolean hadBootstrap)
    {
        /*
         * During bootstrap, streamed SSTable are already built for existing indexes via {@link StorageAttachedIndexBuildingSupport}
         * from {@link org.apache.cassandra.streaming.StreamReceiveTask.OnCompletionRunnable}.
         *
         * For indexes created during bootstrapping, we don't have to block bootstrap for them.
         */

        return this::startPreJoinTask;
    }

    public boolean isInitBuildStarted()
    {
        return initBuildStarted;
    }

    public BooleanSupplier isIndexValid()
    {
        return () -> valid;
    }

    private Future<?> startPreJoinTask()
    {
        try
        {
            if (baseCfs.indexManager.isIndexQueryable(this))
            {
                logger.debug(indexContext.logMessage("Skipping validation in pre-join task, as the initialization task has already made the index queryable..."));
                baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
                return null;
            }

            StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(baseCfs);

            assert indexGroup != null : "Index group does not exist for table";

            Collection<SSTableReader> nonIndexed = findNonIndexedSSTables(baseCfs, indexGroup, IndexValidation.HEADER_FOOTER);

            if (nonIndexed.isEmpty())
            {
                // If the index is complete, mark it queryable before the node starts accepting requests:
                baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
            }
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Failed in pre-join task!"), t);
        }

        return null;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt)
    {
        /*
         * index files will be removed as part of base sstable lifecycle in
         * {@link LogTransaction#delete(java.io.File)} asynchronously.
         */
        return null;
    }

    @Override
    public boolean shouldBuildBlocking()
    {
        return true;
    }

    @Override
    public boolean isSSTableAttached()
    {
        return true;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    @Override
    public boolean dependsOn(ColumnMetadata column)
    {
        return indexContext.getDefinition().compareTo(column) == 0;
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return dependsOn(column) && indexContext.supports(operator);
    }

    @Override
    public boolean filtersMultipleContains()
    {
        return false;
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        // it should be executed from the SAI query plan, this is only used by the singleton index query plan
        throw new UnsupportedOperationException();
    }

    @Override
    public long getEstimatedResultRows()
    {
        throw new UnsupportedOperationException("Use StorageAttachedIndexQueryPlan#getEstimatedResultRows() instead.");
    }

    @Override
    public boolean isQueryable(Status status)
    {
        // consider unknown status as queryable, because gossip may not be up-to-date for newly joining nodes.
        return status == Status.BUILD_SUCCEEDED || status == Status.UNKNOWN;
    }

    @Override
    public void validate(PartitionUpdate update) throws InvalidRequestException
    {}

    /**
     * This method is called by the startup tasks to find SSTables that don't have indexes. The method is
     * synchronized so that the view is unchanged between validation and the selection of non-indexed SSTables.
     *
     * @return a list SSTables without attached indexes
     */
    private synchronized List<SSTableReader> findNonIndexedSSTables(ColumnFamilyStore baseCfs, StorageAttachedIndexGroup group, IndexValidation validation)
    {
        Set<SSTableReader> sstables = baseCfs.getLiveSSTables();

        // Initialize the SSTable indexes w/ valid existing components...
        assert group != null : "Missing index group on " + baseCfs.name;
        group.onSSTableChanged(Collections.emptyList(), sstables, Collections.singleton(this), validation);

        // ...then identify and rebuild the SSTable indexes that are missing.
        List<SSTableReader> nonIndexed = new ArrayList<>();
        View view = indexContext.getView();

        for (SSTableReader sstable : sstables)
        {
            // An SSTable is considered not indexed if:
            //   1. The current view does not contain the SSTable
            //   2. The SSTable is not marked compacted
            //   3. The column index does not have a completion marker
            if (!view.containsSSTable(sstable) && !sstable.isMarkedCompacted() &&
                !IndexDescriptor.create(sstable).isPerColumnIndexBuildComplete(indexContext))
            {
                nonIndexed.add(sstable);
            }
        }

        return nonIndexed;
    }

    @Override
    public Searcher searcherFor(ReadCommand command) throws InvalidRequestException
    {
        // searchers should be created from the query plan, this is only used by the singleton index query plan
        throw new UnsupportedOperationException();
    }

    @Override
    public SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker)
    {
        // flush observers should be created from the index group, this is only used by the singleton index group
        throw new UnsupportedOperationException("Storage-attached index flush observers should never be created directly.");
    }

    @Override
    public Set<Component> getComponents()
    {
        return Version.LATEST.onDiskFormat()
                             .perColumnIndexComponents(indexContext)
                             .stream()
                             .map(c -> Version.LATEST.makePerIndexComponent(c, indexContext))
                             .collect(Collectors.toSet());
    }

    @Override
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              long nowInSec,
                              WriteContext writeContext,
                              IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        if (transactionType == IndexTransaction.Type.UPDATE)
        {
            return new UpdateIndexer(key, memtable, writeContext);
        }

        // we are only interested in the data from Memtable
        // everything else is going to be handled by SSTableWriter observers
        return null;
    }

    @Override
    public IndexBuildingSupport getBuildTaskSupport()
    {
        return INDEX_BUILDER_SUPPORT;
    }

    public IndexContext getIndexContext()
    {
        return indexContext;
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s.%s", baseCfs.keyspace.getName(), baseCfs.name, getIndexMetadata() == null ? "?" : getIndexMetadata());
    }

    /**
     * Removes this index from the {@code SecondaryIndexManager}'s set of queryable indexes.
     */
    public void makeIndexNonQueryable()
    {
        baseCfs.indexManager.makeIndexNonQueryable(this, Status.BUILD_FAILED);
        logger.warn(indexContext.logMessage("Storage-attached index is no longer queryable. Please restart this node to repair it."));
    }

    private class UpdateIndexer implements Index.Indexer
    {
        private final DecoratedKey key;
        private final Memtable memtable;
        private final WriteContext writeContext;

        UpdateIndexer(DecoratedKey key, Memtable memtable, WriteContext writeContext)
        {
            this.key = key;
            this.memtable = memtable;
            this.writeContext = writeContext;
        }

        @Override
        public void insertRow(Row row)
        {
            adjustMemtableSize(indexContext.getMemtableIndexManager().index(key, row, memtable),
                               CassandraWriteContext.fromContext(writeContext).getGroup());
        }

        @Override
        public void updateRow(Row oldRow, Row newRow)
        {
            insertRow(newRow);
        }

        void adjustMemtableSize(long additionalSpace, OpOrder.Group opGroup)
        {
            memtable.markExtraOnHeapUsed(additionalSpace, opGroup);
        }
    }
}
