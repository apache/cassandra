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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.IndexViewManager;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Manage metadata for each column index.
 */
public class IndexContext
{
    private static final Logger logger = LoggerFactory.getLogger(IndexContext.class);

    private static final Set<AbstractType<?>> EQ_ONLY_TYPES =
            ImmutableSet.of(UTF8Type.instance, AsciiType.instance, BooleanType.instance, UUIDType.instance);

    public static final String ENABLE_SEGMENT_COMPACTION_OPTION_NAME = "enable_segment_compaction";

    private final AbstractType<?> partitionKeyType;
    private final ClusteringComparator clusteringComparator;

    private final String keyspace;
    private final String table;
    private final ColumnMetadata column;
    private final IndexTarget.Type indexType;
    private final AbstractType<?> validator;
    private final Memtable.Owner owner;

    // Config can be null if the column context is "fake" (i.e. created for a filtering expression).
    private final IndexMetadata config;

    private final ConcurrentMap<Memtable, MemtableIndex> liveMemtables = new ConcurrentHashMap<>();

    private final IndexViewManager viewManager;
    private final IndexMetrics indexMetrics;
    private final ColumnQueryMetrics columnQueryMetrics;
    private final IndexWriterConfig indexWriterConfig;
    private final AbstractAnalyzer.AnalyzerFactory analyzerFactory;
    private final AbstractAnalyzer.AnalyzerFactory queryAnalyzerFactory;
    private final PrimaryKey.Factory primaryKeyFactory;

    private final boolean segmentCompactionEnabled;

    public IndexContext(@Nonnull String keyspace,
                        @Nonnull String table,
                        @Nonnull AbstractType<?> partitionKeyType,
                        @Nonnull ClusteringComparator clusteringComparator,
                        @Nonnull ColumnMetadata column,
                        @Nonnull IndexTarget.Type indexType,
                        IndexMetadata config,
                        @Nonnull Memtable.Owner owner)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.partitionKeyType = partitionKeyType;
        this.clusteringComparator = clusteringComparator;
        this.column = column;
        this.indexType = indexType;
        this.config = config;
        this.viewManager = new IndexViewManager(this);
        this.indexMetrics = new IndexMetrics(this);
        this.validator = TypeUtil.cellValueType(column, indexType);
        this.owner = owner;

        this.columnQueryMetrics = isLiteral() ? new ColumnQueryMetrics.TrieIndexMetrics(keyspace, table, getIndexName())
                                              : new ColumnQueryMetrics.BKDIndexMetrics(keyspace, table, getIndexName());

        this.primaryKeyFactory = Version.LATEST.onDiskFormat().primaryKeyFactory(clusteringComparator);

        if (config != null)
        {
            String fullIndexName = String.format("%s.%s.%s", this.keyspace, this.table, this.config.name);
            this.indexWriterConfig = IndexWriterConfig.fromOptions(fullIndexName, validator, config.options);
            this.analyzerFactory = AbstractAnalyzer.fromOptions(getValidator(), config.options);
            this.queryAnalyzerFactory = AbstractAnalyzer.hasQueryAnalyzer(config.options)
                                        ? AbstractAnalyzer.fromOptionsQueryAnalyzer(getValidator(), config.options)
                                        : this.analyzerFactory;
            this.segmentCompactionEnabled = Boolean.parseBoolean(config.options.getOrDefault(ENABLE_SEGMENT_COMPACTION_OPTION_NAME, "false"));
        }
        else
        {
            this.indexWriterConfig = IndexWriterConfig.emptyConfig();
            this.analyzerFactory = AbstractAnalyzer.fromOptions(getValidator(), Collections.EMPTY_MAP);
            this.queryAnalyzerFactory = this.analyzerFactory;
            this.segmentCompactionEnabled = true;
        }

        logger.debug(logMessage("Initialized index context with index writer config: {}"), indexWriterConfig);
    }

    public AbstractType<?> keyValidator()
    {
        return partitionKeyType;
    }

    public PrimaryKey.Factory keyFactory()
    {
        return primaryKeyFactory;
    }

    public ClusteringComparator comparator()
    {
        return clusteringComparator;
    }

    public IndexMetrics getIndexMetrics()
    {
        return indexMetrics;
    }

    public ColumnQueryMetrics getColumnQueryMetrics()
    {
        return columnQueryMetrics;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getTable()
    {
        return table;
    }

    public Memtable.Owner owner()
    {
        return owner;
    }

    public void index(DecoratedKey key, Row row, Memtable memtable, OpOrder.Group opGroup)
    {
        MemtableIndex current = liveMemtables.get(memtable);

        // VSTODO this is obsolete once we no longer support Java 8
        // We expect the relevant IndexMemtable to be present most of the time, so only make the
        // call to computeIfAbsent() if it's not. (see https://bugs.openjdk.java.net/browse/JDK-8161372)
        MemtableIndex target = (current != null)
                               ? current
                               : liveMemtables.computeIfAbsent(memtable, mt -> MemtableIndex.createIndex(this));

        long start = System.nanoTime();

        if (isNonFrozenCollection())
        {
            Iterator<ByteBuffer> bufferIterator = getValuesOf(row, FBUtilities.nowInSeconds());
            if (bufferIterator != null)
            {
                while (bufferIterator.hasNext())
                {
                    ByteBuffer value = bufferIterator.next();
                    target.index(key, row.clustering(), value, memtable, opGroup);
                }
            }
        }
        else
        {
            ByteBuffer value = getValueOf(key, row, FBUtilities.nowInSeconds());
            target.index(key, row.clustering(), value, memtable, opGroup);
        }
        indexMetrics.memtableIndexWriteLatency.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    }

    public void update(DecoratedKey key, Row oldRow, Row newRow, Memtable memtable, OpOrder.Group opGroup)
    {
        if (!isVector())
        {
            index(key, newRow, memtable, opGroup);
            return;
        }

        MemtableIndex target = liveMemtables.get(memtable);
        if (target == null)
            return;

        ByteBuffer oldValue = getValueOf(key, oldRow, FBUtilities.nowInSeconds());
        ByteBuffer newValue = getValueOf(key, newRow, FBUtilities.nowInSeconds());
        target.update(key, oldRow.clustering(), oldValue, newValue, memtable, opGroup);
    }

    public void renewMemtable(Memtable renewed)
    {
        for (Memtable memtable : liveMemtables.keySet())
        {
            // remove every index but the one that corresponds to the post-truncate Memtable
            if (renewed != memtable)
            {
                liveMemtables.remove(memtable);
            }
        }
    }

    public void discardMemtable(Memtable discarded)
    {
        liveMemtables.remove(discarded);
    }

    public MemtableIndex getPendingMemtableIndex(LifecycleNewTracker tracker)
    {
        return liveMemtables.keySet().stream()
                            .filter(m -> tracker.equals(m.getFlushTransaction()))
                            .findFirst()
                            .map(liveMemtables::get)
                            .orElse(null);
    }

    public List<Pair<Memtable, RangeIterator<PrimaryKey>>> iteratorsForSearch(QueryContext queryContext, Expression expression, AbstractBounds<PartitionPosition> keyRange, int limit) {
        return liveMemtables.entrySet()
                                   .stream()
                                   .map(e -> Pair.create(e.getKey(), e.getValue().search(queryContext, expression, keyRange, limit))).collect(Collectors.toList());
    }

    public RangeIterator<PrimaryKey> reorderMemtable(Memtable memtable, QueryContext context, RangeIterator<PrimaryKey> iterator, Expression exp, int limit)
    {
        var index = liveMemtables.get(memtable);
        return index.limitToTopResults(context, iterator, exp, limit);
    }

    public long liveMemtableWriteCount()
    {
        return liveMemtables.values().stream().mapToLong(MemtableIndex::writeCount).sum();
    }

    public long estimatedOnHeapMemIndexMemoryUsed()
    {
        return liveMemtables.values().stream().mapToLong(MemtableIndex::estimatedOnHeapMemoryUsed).sum();
    }

    public long estimatedOffHeapMemIndexMemoryUsed()
    {
        return liveMemtables.values().stream().mapToLong(MemtableIndex::estimatedOffHeapMemoryUsed).sum();
    }

    /**
     * @return A set of SSTables which have attached to them invalid index components.
     */
    public Set<SSTableContext> onSSTableChanged(Collection<SSTableReader> oldSSTables, Collection<SSTableContext> newSSTables, boolean validate)
    {
        return viewManager.update(oldSSTables, newSSTables, validate);
    }

    public ColumnMetadata getDefinition()
    {
        return column;
    }

    public AbstractType<?> getValidator()
    {
        return validator;
    }

    public boolean isNonFrozenCollection()
    {
        return TypeUtil.isNonFrozenCollection(column.type);
    }

    public boolean isFrozen()
    {
        return TypeUtil.isFrozen(column.type);
    }

    public String getColumnName()
    {
        return column.name.toString();
    }

    public String getIndexName()
    {
        return this.config == null ? null : config.name;
    }

    public AbstractAnalyzer.AnalyzerFactory getAnalyzerFactory()
    {
        return analyzerFactory;
    }

    public AbstractAnalyzer.AnalyzerFactory getQueryAnalyzerFactory()
    {
        return queryAnalyzerFactory;
    }

    public IndexWriterConfig getIndexWriterConfig()
    {
        return indexWriterConfig;
    }

    public View getView()
    {
        return viewManager.getView();
    }

    /**
     * @return total number of per-index open files
     */
    public int openPerIndexFiles()
    {
        return viewManager.getView().size() * Version.LATEST.onDiskFormat().openFilesPerIndex(this);
    }

    public void drop(Collection<SSTableReader> sstablesToRebuild)
    {
        viewManager.drop(sstablesToRebuild);
    }

    public boolean isIndexed()
    {
        return config != null;
    }

    /**
     * Called when index is dropped. Mark all {@link SSTableIndex} as released and per-column index files
     * will be removed when in-flight queries completed and {@code obsolete} is true.
     *
     * @param obsolete true if index files should be deleted after invalidate; false otherwise.
     */
    public void invalidate(boolean obsolete)
    {
        liveMemtables.clear();
        viewManager.invalidate(obsolete);
        indexMetrics.release();
        columnQueryMetrics.release();

        analyzerFactory.close();
        if (queryAnalyzerFactory != analyzerFactory)
        {
            queryAnalyzerFactory.close();
        }
    }

    @VisibleForTesting
    public ConcurrentMap<Memtable, MemtableIndex> getLiveMemtables()
    {
        return liveMemtables;
    }

    public boolean supports(Operator op)
    {
        if (op.isLike() || op == Operator.LIKE) return false;

        // ANN is only supported against vectors, and vector indexes only support ANN
        if (column.type instanceof VectorType)
            return op == Operator.ANN;
        if (op == Operator.ANN)
            return false;

        Expression.Op operator = Expression.Op.valueOf(op);

        if (isNonFrozenCollection())
        {
            if (indexType == IndexTarget.Type.KEYS) return operator == Expression.Op.CONTAINS_KEY;
            if (indexType == IndexTarget.Type.VALUES) return operator == Expression.Op.CONTAINS_VALUE;
            return indexType == IndexTarget.Type.KEYS_AND_VALUES && operator == Expression.Op.EQ;
        }

        if (indexType == IndexTarget.Type.FULL)
            return operator == Expression.Op.EQ;

        AbstractType<?> validator = getValidator();

        if (operator == Expression.Op.IN)
            return true;

        if (operator != Expression.Op.EQ && EQ_ONLY_TYPES.contains(validator)) return false;

        // RANGE only applicable to non-literal indexes
        return (operator != null) && !(TypeUtil.isLiteral(validator) && operator == Expression.Op.RANGE);
    }

    public ByteBuffer getValueOf(DecoratedKey key, Row row, int nowInSecs)
    {
        if (row == null)
            return null;

        switch (column.kind)
        {
            case PARTITION_KEY:
                if (key == null)
                    return null;
                return partitionKeyType instanceof CompositeType
                       ? CompositeType.extractComponent(key.getKey(), column.position())
                       : key.getKey();
            case CLUSTERING:
                // skip indexing of static clustering when regular column is indexed
                return row.isStatic() ? null : row.clustering().bufferAt(column.position());

            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                Cell cell = row.getCell(column);
                return cell == null || !cell.isLive(nowInSecs) ? null : cell.buffer();

            default:
                return null;
        }
    }

    public Iterator<ByteBuffer> getValuesOf(Row row, int nowInSecs)
    {
        if (row == null)
            return null;

        switch (column.kind)
        {
            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                return TypeUtil.collectionIterator(validator, row.getComplexColumnData(column), column, indexType, nowInSecs);

            default:
                return null;
        }
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("columnName", getColumnName())
                          .add("indexName", getIndexName())
                          .toString();
    }

    public boolean isLiteral()
    {
        return TypeUtil.isLiteral(getValidator());
    }

    public boolean isVector()
    {
        //VSTODO probably move this down to TypeUtils eventually
        return getValidator().isVector();
    }

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexContext))
            return false;

        IndexContext other = (IndexContext) obj;

        return Objects.equals(column, other.column) &&
               Objects.equals(indexType, other.indexType) &&
               Objects.equals(config, other.config) &&
               Objects.equals(partitionKeyType, other.partitionKeyType) &&
               Objects.equals(clusteringComparator, other.clusteringComparator);
    }

    public int hashCode()
    {
        return Objects.hash(column, indexType, config, partitionKeyType, clusteringComparator);
    }

    /**
     * A helper method for constructing consistent log messages for specific column indexes.
     *
     * Example: For the index "idx" in keyspace "ks" on table "tb", calling this method with the raw message
     * "Flushing new index segment..." will produce...
     *
     * "[ks.tb.idx] Flushing new index segment..."
     *
     * @param message The raw content of a logging message, without information identifying it with an index.
     *
     * @return A log message with the proper keyspace, table and index name prepended to it.
     */
    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.%s] %s", keyspace, table, config == null ? "?" : config.name, message);
    }

    /**
     * @return the indexes that are built on the given SSTables on the left and corrupted indexes'
     * corresponding contexts on the right
     */
    public Pair<Set<SSTableIndex>, Set<SSTableContext>> getBuiltIndexes(Collection<SSTableContext> sstableContexts, boolean validate)
    {
        Set<SSTableIndex> valid = new HashSet<>(sstableContexts.size());
        Set<SSTableContext> invalid = new HashSet<>();

        for (SSTableContext context : sstableContexts)
        {
            if (context.sstable.isMarkedCompacted())
                continue;

            if (!context.indexDescriptor.isPerIndexBuildComplete(this))
            {
                logger.debug(logMessage("An on-disk index build for SSTable {} has not completed."), context.descriptor());
                continue;
            }

            if (context.indexDescriptor.isIndexEmpty(this))
            {
                logger.debug(logMessage("No on-disk index was built for SSTable {} because the SSTable " +
                                                "had no indexable rows for the index."), context.descriptor());
                continue;
            }

            try
            {
                if (validate)
                {
                    if (!context.indexDescriptor.validatePerIndexComponents(this))
                    {
                        logger.warn(logMessage("Invalid per-column component for SSTable {}"), context.descriptor());
                        invalid.add(context);
                        continue;
                    }
                }

                SSTableIndex index = new SSTableIndex(context, this);
                logger.debug(logMessage("Successfully created index for SSTable {}."), context.descriptor());

                // Try to add new index to the set, if set already has such index, we'll simply release and move on.
                // This covers situation when SSTable collection has the same SSTable multiple
                // times because we don't know what kind of collection it actually is.
                if (!valid.add(index))
                {
                    index.release();
                }
            }
            catch (Throwable e)
            {
                logger.warn(logMessage("Failed to update per-column components for SSTable {}"), context.descriptor(), e);
                invalid.add(context);
            }
        }

        return Pair.create(valid, invalid);
    }

    /**
     * @return the number of indexed rows in this index (aka. pair of term and rowId)
     */
    public long getCellCount()
    {
        return getView().getIndexes()
                        .stream()
                        .mapToLong(SSTableIndex::getRowCount)
                        .sum();
    }

    /**
     * @return the total size (in bytes) of per-column index components
     */
    public long diskUsage()
    {
        return getView().getIndexes()
                        .stream()
                        .mapToLong(SSTableIndex::sizeOfPerColumnComponents)
                        .sum();
    }

    /**
     * @return the total memory usage (in bytes) of per-column index on-disk data structure
     */
    public long indexFileCacheSize()
    {
        return getView().getIndexes()
                        .stream()
                        .mapToLong(SSTableIndex::indexFileCacheSize)
                        .sum();
    }

    public IndexFeatureSet indexFeatureSet()
    {
        IndexFeatureSet.Accumulator accumulator = new IndexFeatureSet.Accumulator();
        getView().getIndexes().stream().map(SSTableIndex::indexFeatureSet).forEach(set -> accumulator.accumulate(set));
        return accumulator.complete();
    }

    /**
     * Returns true if index segments should be compacted into one segment after building the index.
     *
     * By default, this option is set to true. A user is able to override this by setting
     * <code>enable_segment_compaction</code> to false in the index options.
     * This is an expert-only option.
     * Disabling compaction improves performance of writes at the expense of significantly reducing performance
     * of read queries. A user should never turn compaction off on a production system
     * unless diagnosing a performance issue.
     */
    public boolean isSegmentCompactionEnabled()
    {
        return this.segmentCompactionEnabled;
    }
}
