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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture; //checkstyle: permit this import
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures; //checkstyle: permit this import
import com.google.common.util.concurrent.ListenableFuture; //checkstyle: permit this import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
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
import org.apache.cassandra.db.marshal.FloatType;
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
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.memory.MemtableIndexManager;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.view.IndexViewManager;
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
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_FROZEN_TERM_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_STRING_TERM_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_MAX_VECTOR_TERM_SIZE;
import static org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig.MAX_TOP_K;

public class StorageAttachedIndex implements Index
{
    public static final String NAME = "sai";
    
    public static final String VECTOR_USAGE_WARNING = "SAI ANN indexes on vector columns are experimental and are not recommended for production use.\n" +
                                                      "They don't yet support SELECT queries with:\n" +
                                                      " * Consistency level higher than ONE/LOCAL_ONE.\n" +
                                                      " * Paging.\n" +
                                                      " * No LIMIT clauses.\n" +
                                                      " * PER PARTITION LIMIT clauses.\n" +
                                                      " * GROUP BY clauses.\n" +
                                                      " * Aggregation functions.\n" +
                                                      " * Filters on columns without a SAI index.";

    public static final String VECTOR_NON_FLOAT_ERROR = "SAI ANN indexes are only allowed on vector columns with float elements";
    public static final String VECTOR_1_DIMENSION_COSINE_ERROR = "Cosine similarity is not supported for single-dimension vectors";
    public static final String VECTOR_MULTIPLE_DATA_DIRECTORY_ERROR = "SAI ANN indexes are not allowed on multiple data directories";

    @VisibleForTesting
    public static final String ANALYSIS_ON_KEY_COLUMNS_MESSAGE = "Analysis options are not supported on primary key columns, but found ";

    public static final String ANN_LIMIT_ERROR = "Use of ANN OF in an ORDER BY clause requires a LIMIT that is not greater than %s. LIMIT was %s";

    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndex.class);

    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final long MAX_STRING_TERM_SIZE = SAI_MAX_STRING_TERM_SIZE.getSizeInBytes();
    public static final long MAX_FROZEN_TERM_SIZE = SAI_MAX_FROZEN_TERM_SIZE.getSizeInBytes();
    public static final long MAX_VECTOR_TERM_SIZE = SAI_MAX_VECTOR_TERM_SIZE.getSizeInBytes();
    public static final String TERM_OVERSIZE_MESSAGE = "Can't add term of column %s to index for key: %s, term size %s " +
                                                       "max allowed size %s, use analyzed = true (if not yet set) for that column.";

    // Used to build indexes on newly added SSTables:
    private static final StorageAttachedIndexBuildingSupport INDEX_BUILDER_SUPPORT = new StorageAttachedIndexBuildingSupport();

    private static final Set<String> VALID_OPTIONS = ImmutableSet.of(IndexTarget.TARGET_OPTION_NAME,
                                                                     IndexTarget.CUSTOM_INDEX_OPTION_NAME,
                                                                     IndexWriterConfig.MAXIMUM_NODE_CONNECTIONS,
                                                                     IndexWriterConfig.CONSTRUCTION_BEAM_WIDTH,
                                                                     IndexWriterConfig.SIMILARITY_FUNCTION,
                                                                     IndexWriterConfig.OPTIMIZE_FOR,
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
    private final IndexMetadata indexMetadata;
    private final IndexTermType indexTermType;
    private final IndexIdentifier indexIdentifier;
    private final IndexViewManager viewManager;
    private final ColumnQueryMetrics columnQueryMetrics;
    private final IndexWriterConfig indexWriterConfig;
    @Nullable private final AbstractAnalyzer.AnalyzerFactory analyzerFactory;
    private final PrimaryKey.Factory primaryKeyFactory;
    private final MemtableIndexManager memtableIndexManager;
    private final IndexMetrics indexMetrics;
    private final long maxTermSize;

    // Tracks whether we've started the index build on initialization.
    private volatile boolean initBuildStarted = false;

    // Tracks whether the index has been invalidated due to removal, a table drop, etc.
    private volatile boolean valid = true;

    public StorageAttachedIndex(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
    {
        this.baseCfs = baseCfs;
        this.indexMetadata = indexMetadata;
        TableMetadata tableMetadata = baseCfs.metadata();
        Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(tableMetadata, indexMetadata);
        indexTermType = IndexTermType.create(target.left, tableMetadata.partitionKeyColumns(), target.right);
        indexIdentifier = new IndexIdentifier(baseCfs.getKeyspaceName(), baseCfs.getTableName(), indexMetadata.name);
        primaryKeyFactory = new PrimaryKey.Factory(tableMetadata.partitioner, tableMetadata.comparator);
        indexWriterConfig = IndexWriterConfig.fromOptions(indexMetadata.name, indexTermType, indexMetadata.options);
        viewManager = new IndexViewManager(this);
        columnQueryMetrics = indexTermType.isLiteral() ? new ColumnQueryMetrics.TrieIndexMetrics(indexIdentifier)
                                                       : new ColumnQueryMetrics.BalancedTreeIndexMetrics(indexIdentifier);
        analyzerFactory = AbstractAnalyzer.fromOptions(indexTermType, indexMetadata.options);
        memtableIndexManager = new MemtableIndexManager(this);
        indexMetrics = new IndexMetrics(this, memtableIndexManager);
        maxTermSize = indexTermType.isVector() ? MAX_VECTOR_TERM_SIZE
                                               : (indexTermType.isFrozen() ? MAX_FROZEN_TERM_SIZE : MAX_STRING_TERM_SIZE);
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

        Map<String, String> analysisOptions = AbstractAnalyzer.getAnalyzerOptions(options);
        if (target.left.isPrimaryKeyColumn() && !analysisOptions.isEmpty())
        {
            throw new InvalidRequestException(ANALYSIS_ON_KEY_COLUMNS_MESSAGE + new CqlBuilder().append(analysisOptions));
        }

        IndexTermType indexTermType = IndexTermType.create(target.left, metadata.partitionKeyColumns(), target.right);
        AbstractAnalyzer.fromOptions(indexTermType, analysisOptions);
        IndexWriterConfig config = IndexWriterConfig.fromOptions(null, indexTermType, options);

        // If we are indexing map entries we need to validate the subtypes
        if (indexTermType.isComposite())
        {
            for (IndexTermType subType : indexTermType.subTypes())
            {
                if (!SUPPORTED_TYPES.contains(subType.asCQL3Type()) && !subType.isFrozen())
                    throw new InvalidRequestException("Unsupported type: " + subType.asCQL3Type());
            }
        }
        else if (!SUPPORTED_TYPES.contains(indexTermType.asCQL3Type()) && !indexTermType.isFrozen())
        {
            throw new InvalidRequestException("Unsupported type: " + indexTermType.asCQL3Type());
        }
        // If this is a vector type we need to validate it for the current vector index constraints
        else if (indexTermType.isVector())
        {
            if (!(indexTermType.vectorElementType() instanceof FloatType))
                throw new InvalidRequestException(VECTOR_NON_FLOAT_ERROR);

            if (indexTermType.vectorDimension() == 1 && config.getSimilarityFunction() == VectorSimilarityFunction.COSINE)
                throw new InvalidRequestException(VECTOR_1_DIMENSION_COSINE_ERROR);

            if (DatabaseDescriptor.getRawConfig().data_file_directories.length > 1)
                throw new InvalidRequestException(VECTOR_MULTIPLE_DATA_DIRECTORY_ERROR);

            ClientWarn.instance.warn(VECTOR_USAGE_WARNING);
        }

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
        return indexMetadata;
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
            for (SSTableIndex sstableIndex : view().getIndexes())
                sstableIndex.getSSTable().unregisterComponents(toRemove, baseCfs.getTracker());

            viewManager.invalidate();
            if (analyzerFactory != null)
                analyzerFactory.close();
            columnQueryMetrics.release();
            memtableIndexManager.invalidate();
            indexMetrics.release();
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

    @Override
    public Callable<?> getTruncateTask(long truncatedAt)
    {
        /*
         * index files will be removed as part of base sstable lifecycle in {@link LogTransaction#delete(java.io.File)}
         * asynchronously, but we need to mark the index queryable because if the truncation is during the initial
         * build of the index it won't get marked queryable by the build.
         */
        return () -> {
            logger.info(indexIdentifier.logMessage("Making index queryable during table truncation"));
            baseCfs.indexManager.makeIndexQueryable(this, Status.BUILD_SUCCEEDED);
            return null;
        };
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
        return indexTermType.dependsOn(column);
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return dependsOn(column) && indexTermType.supports(operator);
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
    public Comparator<ByteBuffer> getPostQueryOrdering(Restriction restriction, QueryOptions options)
    {
        // For now, only support ANN
        assert restriction instanceof SingleColumnRestriction.AnnRestriction;

        Preconditions.checkState(indexTermType.isVector());

        SingleColumnRestriction.AnnRestriction annRestriction = (SingleColumnRestriction.AnnRestriction) restriction;
        VectorSimilarityFunction function = indexWriterConfig.getSimilarityFunction();

        float[] target = indexTermType.decomposeVector(annRestriction.value(options).duplicate());

        return (leftBuf, rightBuf) -> {
            float[] left = indexTermType.decomposeVector(leftBuf.duplicate());
            double scoreLeft = function.compare(left, target);

            float[] right = indexTermType.decomposeVector(rightBuf.duplicate());
            double scoreRight = function.compare(right, target);
            return Double.compare(scoreRight, scoreLeft); // descending order
        };
    }

    @Override
    public void validate(ReadCommand command) throws InvalidRequestException
    {
        if (!indexTermType.isVector())
            return;

        // to avoid overflow of the vector graph internal data structure and avoid OOM when filtering top-k
        if (command.limits().count() > MAX_TOP_K)
            throw new InvalidRequestException(String.format(ANN_LIMIT_ERROR, MAX_TOP_K, command.limits().count()));
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
    {
        DecoratedKey key = update.partitionKey();
        for (Row row : update)
            validateMaxTermSizeForRow(key, row, true);
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
                             .perColumnIndexComponents(indexTermType)
                             .stream()
                             .map(c -> Version.LATEST.makePerIndexComponent(c, indexIdentifier))
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

    /**
     * @return A set of SSTables which have attached to them invalid index components.
     */
    public Collection<SSTableContext> onSSTableChanged(Collection<SSTableReader> oldSSTables, Collection<SSTableContext> newSSTables, IndexValidation validation)
    {
        return viewManager.update(oldSSTables, newSSTables, validation);
    }

    public void drop(Collection<SSTableReader> sstablesToRebuild)
    {
        viewManager.drop(sstablesToRebuild);
    }

    public MemtableIndexManager memtableIndexManager()
    {
        return memtableIndexManager;
    }

    public View view()
    {
        return viewManager.view();
    }

    public IndexTermType termType()
    {
        return indexTermType;
    }

    public IndexIdentifier identifier()
    {
        return indexIdentifier;
    }

    public PrimaryKey.Factory keyFactory()
    {
        return primaryKeyFactory;
    }

    @VisibleForTesting
    public ColumnFamilyStore baseCfs()
    {
        return baseCfs;
    }

    public IndexWriterConfig indexWriterConfig()
    {
        return indexWriterConfig;
    }

    public boolean hasAnalyzer()
    {
        return analyzerFactory != null;
    }

    /**
     * Returns an {@link AbstractAnalyzer} for use by write and query paths to transform
     * literal values.
     */
    public AbstractAnalyzer analyzer()
    {
        assert analyzerFactory != null : "Index does not support string analysis";
        return analyzerFactory.create();
    }

    public IndexMetrics indexMetrics()
    {
        return indexMetrics;
    }

    public ColumnQueryMetrics columnQueryMetrics()
    {
        return columnQueryMetrics;
    }

    public boolean isInitBuildStarted()
    {
        return initBuildStarted;
    }

    public BooleanSupplier isIndexValid()
    {
        return () -> valid;
    }

    public boolean hasClustering()
    {
        return baseCfs.getComparator().size() > 0;
    }

    /**
     * @return the number of indexed rows in this index (aka. a pair of term and rowId)
     */
    public long cellCount()
    {
        return view().getIndexes()
                     .stream()
                     .mapToLong(SSTableIndex::getRowCount)
                     .sum();
    }

    /**
     * @return total number of per-index open files
     */
    public int openPerColumnIndexFiles()
    {
        return viewManager.view().size() * Version.LATEST.onDiskFormat().openFilesPerColumnIndex();
    }

    /**
     * @return the total size (in bytes) of per-column index components
     */
    public long diskUsage()
    {
        return view().getIndexes()
                     .stream()
                     .mapToLong(SSTableIndex::sizeOfPerColumnComponents)
                     .sum();
    }

    /**
     * @return the total memory usage (in bytes) of per-column index on-disk data structure
     */
    public long indexFileCacheSize()
    {
        return view().getIndexes()
                     .stream()
                     .mapToLong(SSTableIndex::indexFileCacheSize)
                     .sum();
    }

    /**
     * Removes this index from the {@code SecondaryIndexManager}'s set of queryable indexes.
     */
    public void makeIndexNonQueryable()
    {
        baseCfs.indexManager.makeIndexNonQueryable(this, Status.BUILD_FAILED);
        logger.warn(indexIdentifier.logMessage("Storage-attached index is no longer queryable. Please restart this node to repair it."));
    }

    /**
     * Validate maximum term size for given row
     */
    public void validateMaxTermSizeForRow(DecoratedKey key, Row row, boolean sendClientWarning)
    {
        AbstractAnalyzer analyzer = hasAnalyzer() ? analyzer() : null;
        if (indexTermType.isNonFrozenCollection())
        {
            Iterator<ByteBuffer> bufferIterator = indexTermType.valuesOf(row, FBUtilities.nowInSeconds());
            while (bufferIterator != null && bufferIterator.hasNext())
                validateMaxTermSizeForCell(analyzer, key, bufferIterator.next(), sendClientWarning);
        }
        else
        {
            ByteBuffer value = indexTermType.valueOf(key, row, FBUtilities.nowInSeconds());
            validateMaxTermSizeForCell(analyzer, key, value, sendClientWarning);
        }
    }

    private void validateMaxTermSizeForCell(AbstractAnalyzer analyzer, DecoratedKey key, @Nullable ByteBuffer cellBuffer, boolean sendClientWarning)
    {
        if (cellBuffer == null || cellBuffer.remaining() == 0)
            return;

        // analyzer should not return terms that are larger than the origin value.
        if (cellBuffer.remaining() <= maxTermSize)
            return;

        if (analyzer != null)
        {
            analyzer.reset(cellBuffer.duplicate());
            while (analyzer.hasNext())
                validateMaxTermSize(key, analyzer.next(), sendClientWarning);
        }
        else
        {
            validateMaxTermSize(key, cellBuffer.duplicate(), sendClientWarning);
        }
    }

    /**
     * Validate maximum term size for given term
     * @return true if given term is valid; otherwise false.
     */
    public boolean validateMaxTermSize(DecoratedKey key, ByteBuffer term, boolean sendClientWarning)
    {
        if (term.remaining() > maxTermSize)
        {
            String message = indexIdentifier.logMessage(String.format(TERM_OVERSIZE_MESSAGE,
                                                        indexTermType.columnName(),
                                                        key,
                                                        FBUtilities.prettyPrintMemory(term.remaining()),
                                                        FBUtilities.prettyPrintMemory(maxTermSize)));

            if (sendClientWarning)
                ClientWarn.instance.warn(message);

            noSpamLogger.warn(message);
            return false;
        }

        return true;
    }

    @Override
    public String toString()
    {
        return indexIdentifier.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof StorageAttachedIndex))
            return false;

        StorageAttachedIndex other = (StorageAttachedIndex) obj;

        return Objects.equals(indexTermType, other.indexTermType) &&
               Objects.equals(indexMetadata, other.indexMetadata) &&
               Objects.equals(baseCfs.getComparator(), other.baseCfs.getComparator());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexTermType, indexMetadata, baseCfs.getComparator());
    }

    private Future<?> startInitialBuild(ColumnFamilyStore baseCfs, IndexValidation validation)
    {
        if (baseCfs.indexManager.isIndexQueryable(this))
        {
            logger.debug(indexIdentifier.logMessage("Skipping validation and building in initialization task, as pre-join has already made the storage-attached index queryable..."));
            initBuildStarted = true;
            return CompletableFuture.completedFuture(null);
        }

        // stop in-progress compaction tasks to prevent compacted sstable not being indexed.
        logger.debug(indexIdentifier.logMessage("Stopping active compactions to make sure all sstables are indexed after initial build."));
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

        logger.info(indexIdentifier.logMessage("Submitting {} parallel initial index builds over {} total sstables..."), futures.size(), nonIndexed.size());
        return Futures.allAsList(futures);
    }

    private Future<?> startPreJoinTask()
    {
        try
        {
            if (baseCfs.indexManager.isIndexQueryable(this))
            {
                logger.debug(indexIdentifier.logMessage("Skipping validation in pre-join task, as the initialization task has already made the index queryable..."));
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
            logger.error(indexIdentifier.logMessage("Failed in pre-join task!"), t);
        }

        return null;
    }

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
        View view = viewManager.view();

        for (SSTableReader sstable : sstables)
        {
            // An SSTable is considered not indexed if:
            //   1. The current view does not contain the SSTable
            //   2. The SSTable is not marked compacted
            //   3. The column index does not have a completion marker
            if (!view.containsSSTable(sstable) && !sstable.isMarkedCompacted() &&
                !IndexDescriptor.create(sstable).isPerColumnIndexBuildComplete(indexIdentifier))
            {
                nonIndexed.add(sstable);
            }
        }

        return nonIndexed;
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
            adjustMemtableSize(memtableIndexManager.index(key, row, memtable),
                               CassandraWriteContext.fromContext(writeContext).getGroup());
        }

        @Override
        public void updateRow(Row oldRow, Row newRow)
        {
            adjustMemtableSize(memtableIndexManager.update(key, oldRow, newRow, memtable),
                               CassandraWriteContext.fromContext(writeContext).getGroup());
        }

        void adjustMemtableSize(long additionalSpace, OpOrder.Group opGroup)
        {
            // The memtable will assert if we try and reduce its memory usage so, for now, just don't tell it.
            if (additionalSpace >= 0)
                memtable.markExtraOnHeapUsed(additionalSpace, opGroup);
        }
    }
}
