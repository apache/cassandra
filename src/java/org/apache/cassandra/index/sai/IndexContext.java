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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.vector.VectorValidation;
import org.apache.cassandra.index.sai.iterators.KeyRangeAntiJoinIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.MemtableKeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.AbstractMetrics;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.IndexViewManager;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_INDEX_READS_DISABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_INDEX_METRICS_ENABLED;
import static org.apache.cassandra.config.CassandraRelevantProperties.VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR;
import static org.apache.cassandra.index.sai.plan.QueryController.INDEX_VERSION_DOES_NOT_SUPPORT_BM25;

/**
 * Manage metadata for each column index.
 */
public class IndexContext
{
    private static final Logger logger = LoggerFactory.getLogger(IndexContext.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final int MAX_STRING_TERM_SIZE = Integer.getInteger("cassandra.sai.max_string_term_size_kb", 8) * 1024;
    public static final int MAX_FROZEN_TERM_SIZE = Integer.getInteger("cassandra.sai.max_frozen_term_size_kb", 8) * 1024;
    public static final int MAX_VECTOR_TERM_SIZE = Integer.getInteger("cassandra.sai.max_vector_term_size_kb", 16) * 1024;
    public static final int MAX_ANALYZED_SIZE = Integer.getInteger("cassandra.sai.max_analyzed_size_kb", 8) * 1024;
    private static final String TERM_OVERSIZE_LOG_MESSAGE =
    "Can't add term of column {} to index for key: {}, term size {} max allowed size {}.";
    private static final String TERM_OVERSIZE_ERROR_MESSAGE =
    "Term of column %s exceeds the byte limit for index. Term size %s. Max allowed size %s.";

    private static final String ANALYZED_TERM_OVERSIZE_LOG_MESSAGE =
    "Term's analyzed size for column {} exceeds the cumulative limit for index. Max allowed size {}.";
    private static final String ANALYZED_TERM_OVERSIZE_ERROR_MESSAGE =
    "Term's analyzed size for column %s exceeds the cumulative limit for index. Max allowed size %s.";

    private static final Set<AbstractType<?>> EQ_ONLY_TYPES =
            ImmutableSet.of(UTF8Type.instance, AsciiType.instance, BooleanType.instance, UUIDType.instance);

    public static final String ENABLE_SEGMENT_COMPACTION_OPTION_NAME = "enable_segment_compaction";

    private final AbstractType<?> partitionKeyType;
    private final ClusteringComparator clusteringComparator;

    private final String keyspace;
    private final String table;
    private final TableId tableId;
    private final ColumnMetadata column;
    private final IndexTarget.Type indexType;
    private final AbstractType<?> validator;
    private final ColumnFamilyStore cfs;

    // Config can be null if the column context is "fake" (i.e. created for a filtering expression).
    private final IndexMetadata config;
    private final VectorSimilarityFunction vectorSimilarityFunction;

    private final ConcurrentMap<Memtable, MemtableIndex> liveMemtables = new ConcurrentHashMap<>();

    private final IndexViewManager viewManager;
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<IndexMetrics> indexMetrics;
    private final ColumnQueryMetrics columnQueryMetrics;
    private final IndexWriterConfig indexWriterConfig;
    private final boolean isAnalyzed;
    private final boolean hasEuclideanSimilarityFunc;
    private final AbstractAnalyzer.AnalyzerFactory analyzerFactory;
    private final AbstractAnalyzer.AnalyzerFactory queryAnalyzerFactory;
    private final PrimaryKey.Factory primaryKeyFactory;

    private final Version version;
    private final int maxTermSize;

    private volatile boolean dropped = false;

    public IndexContext(@Nonnull String keyspace,
                        @Nonnull String table,
                        @Nonnull TableId tableId,
                        @Nonnull AbstractType<?> partitionKeyType,
                        @Nonnull ClusteringComparator clusteringComparator,
                        @Nonnull ColumnMetadata column,
                        @Nonnull IndexTarget.Type indexType,
                        IndexMetadata config,
                        @Nonnull ColumnFamilyStore cfs)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
        this.partitionKeyType = partitionKeyType;
        this.clusteringComparator = clusteringComparator;
        this.column = column;
        this.indexType = indexType;
        this.config = config;
        this.viewManager = new IndexViewManager(this);
        this.validator = TypeUtil.cellValueType(column, indexType);
        this.cfs = cfs;
        this.version = Version.current(keyspace);
        this.primaryKeyFactory = version.onDiskFormat().newPrimaryKeyFactory(clusteringComparator);

        String columnName = column.name.toString();

        if (config != null)
        {
            String fullIndexName = String.format("%s.%s.%s", this.keyspace, this.table, this.config.name);
            this.indexWriterConfig = IndexWriterConfig.fromOptions(fullIndexName, validator, config.options);
            this.isAnalyzed = AbstractAnalyzer.isAnalyzed(config.options);
            this.analyzerFactory = AbstractAnalyzer.fromOptions(columnName, validator, config.options);
            this.queryAnalyzerFactory = AbstractAnalyzer.hasQueryAnalyzer(config.options)
                                        ? AbstractAnalyzer.fromOptionsQueryAnalyzer(validator, config.options)
                                        : this.analyzerFactory;
            this.vectorSimilarityFunction = indexWriterConfig.getSimilarityFunction();
            this.hasEuclideanSimilarityFunc = vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN;

            this.indexMetrics = SAI_INDEX_METRICS_ENABLED.getBoolean() ? Optional.of(new IndexMetrics(this)) : Optional.empty();
            this.columnQueryMetrics = isVector() ? new ColumnQueryMetrics.VectorIndexMetrics(keyspace, table, getIndexName()) :
                                      isLiteral() ? new ColumnQueryMetrics.TrieIndexMetrics(keyspace, table, getIndexName())
                                                  : new ColumnQueryMetrics.BKDIndexMetrics(keyspace, table, getIndexName());

        }
        else
        {
            this.indexWriterConfig = IndexWriterConfig.emptyConfig();
            this.isAnalyzed = AbstractAnalyzer.isAnalyzed(Collections.EMPTY_MAP);
            this.analyzerFactory = AbstractAnalyzer.fromOptions(columnName, validator, Collections.EMPTY_MAP);
            this.queryAnalyzerFactory = this.analyzerFactory;
            this.vectorSimilarityFunction = null;
            this.hasEuclideanSimilarityFunc = false;

            // null config indicates a "fake" index context. As such, it won't actually be used for indexing/accessing
            // data, leaving these metrics unused. This also eliminates the overhead of creating these metrics on the
            // query path.
            this.indexMetrics = Optional.empty();
            this.columnQueryMetrics = null;
        }

        this.maxTermSize = isVector() ? MAX_VECTOR_TERM_SIZE
                                      : isAnalyzed ? MAX_ANALYZED_SIZE
                                                   : isFrozen() ? MAX_FROZEN_TERM_SIZE : MAX_STRING_TERM_SIZE;


        logger.debug(logMessage("Initialized index context with index writer config: {}"), indexWriterConfig);
    }

    public Version version()
    {
        return version;
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

    public Optional<IndexMetrics> getIndexMetrics()
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

    public TableId getTableId()
    {
        return tableId;
    }

    public ColumnFamilyStore columnFamilyStore()
    {
        return cfs;
    }

    public IPartitioner getPartitioner()
    {
        return cfs.getPartitioner();
    }

    public MemtableIndex initializeMemtableIndex(Memtable memtable)
    {
        return liveMemtables.computeIfAbsent(memtable, mt -> MemtableIndex.createIndex(this, mt));
    }

    public void index(DecoratedKey key, Row row, Memtable memtable, OpOrder.Group opGroup)
    {
        MemtableIndex target = initializeMemtableIndex(memtable);

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
        indexMetrics.flatMap(metrics -> metrics.memtableIndexWriteLatency).ifPresent(timer ->
                timer.update(System.nanoTime() - start, TimeUnit.NANOSECONDS));
    }

    /**
     * Validate maximum term size for given row. Throw an exception when invalid.
     */
    public void validateMaxTermSizeForRow(DecoratedKey key, Row row)
    {
        AbstractAnalyzer analyzer = getAnalyzerFactory().create();
        if (isNonFrozenCollection())
        {
            Iterator<ByteBuffer> bufferIterator = getValuesOf(row, FBUtilities.nowInSeconds());
            while (bufferIterator != null && bufferIterator.hasNext())
                validateMaxTermSizeForCell(analyzer, key, bufferIterator.next());
        }
        else
        {
            ByteBuffer value = getValueOf(key, row, FBUtilities.nowInSeconds());
            validateMaxTermSizeForCell(analyzer, key, value);
        }
    }

    private void validateMaxTermSizeForCell(AbstractAnalyzer analyzer, DecoratedKey key, @Nullable ByteBuffer cellBuffer)
    {
        if (cellBuffer == null || cellBuffer.remaining() == 0)
            return;

        analyzer.reset(cellBuffer);
        try
        {
            if (analyzer.transformValue())
            {
                if (!validateCumulativeAnalyzedTermLimit(key, analyzer))
                {
                    var error = String.format(ANALYZED_TERM_OVERSIZE_ERROR_MESSAGE,
                            column.name, FBUtilities.prettyPrintMemory(maxTermSize));
                    throw new InvalidRequestException(error);
                }
            }
            else
            {
                while (analyzer.hasNext())
                {
                    var size = analyzer.next().remaining();
                    if (!validateMaxTermSize(key, size))
                    {
                        var error = String.format(TERM_OVERSIZE_ERROR_MESSAGE,
                                column.name,
                                FBUtilities.prettyPrintMemory(size),
                                FBUtilities.prettyPrintMemory(maxTermSize));
                        throw new InvalidRequestException(error);
                    }
                }
            }
        }
        finally
        {
            analyzer.end();
        }
    }


    /**
     * Validate maximum term size for given term
     * @return true if given term is valid; otherwise false.
     */
    public boolean validateMaxTermSize(DecoratedKey key, ByteBuffer term)
    {
        return validateMaxTermSize(key, term.remaining());
    }

    private boolean validateMaxTermSize(DecoratedKey key, int termSize)
    {
        if (termSize > maxTermSize)
        {
            noSpamLogger.warn(logMessage(TERM_OVERSIZE_LOG_MESSAGE),
                    getColumnName(),
                    keyValidator().getString(key.getKey()),
                    FBUtilities.prettyPrintMemory(termSize),
                    FBUtilities.prettyPrintMemory(maxTermSize));
            return false;
        }

        return true;
    }

    private boolean validateCumulativeAnalyzedTermLimit(DecoratedKey key, AbstractAnalyzer analyzer)
    {
        int bytesCount = 0;
        // VSTODO anayzer.hasNext copies the byteBuffer, but we don't need that here.
        while (analyzer.hasNext())
        {
            final ByteBuffer token = analyzer.next();
            bytesCount += token.remaining();
            if (bytesCount > maxTermSize)
            {
                noSpamLogger.warn(logMessage(ANALYZED_TERM_OVERSIZE_LOG_MESSAGE),
                        getColumnName(),
                        keyValidator().getString(key.getKey()),
                        FBUtilities.prettyPrintMemory(maxTermSize));
                return false;
            }
        }
        return true;
    }

    public void update(DecoratedKey key, Row oldRow, Row newRow, Memtable memtable, OpOrder.Group opGroup)
    {
        if (version.equals(Version.AA))
        {
            // AA cannot handle updates because it indexes partition keys instead of fully qualified primary keys.
            index(key, newRow, memtable, opGroup);
            return;
        }

        MemtableIndex target = liveMemtables.get(memtable);
        if (target == null)
            return;

        // Use 0 for nowInSecs to get the value(s) from the oldRow regardless of its liveness status. To get to this point,
        // C* has already determined this is the current represntation of the oldRow in the memtable, and that means
        // we need to add the newValue to the index and remove the oldValue from it, even if it has already expired via
        // TTL.
        if (isNonFrozenCollection())
        {
            Iterator<ByteBuffer> oldValues = getValuesOf(oldRow, 0);
            Iterator<ByteBuffer> newValues = getValuesOf(newRow, FBUtilities.nowInSeconds());
            target.update(key, oldRow.clustering(), oldValues, newValues, memtable, opGroup);
        }
        else
        {
            ByteBuffer oldValue = getValueOf(key, oldRow, 0);
            ByteBuffer newValue = getValueOf(key, newRow, FBUtilities.nowInSeconds());
            target.update(key, oldRow.clustering(), oldValue, newValue, memtable, opGroup);
        }
    }

    public void renewMemtable(Memtable renewed)
    {
        // remove every index but the one that corresponds to the post-truncate Memtable
        liveMemtables.keySet().removeIf(m -> m != renewed);
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

    // Returns an iterator for NEQ, NOT_CONTAINS_KEY, NOT_CONTAINS_VALUE, which
    // 1. Either includes everything if the column type values can be truncated and
    // thus the keys cannot be matched precisely,
    // 2. or includes everything minus the keys matching the expression
    // if the column type values cannot be truncated, i.e., matching the keys is always precise.
    // (not matching precisely will lead to false negatives)
    //
    // keys k such that row(k) not contains v = (all keys) \ (keys k such that row(k) contains v)
    //
    // Note that rows in other indexes are not matched, so this can return false positives,
    // but they are not a problem as post-filtering would get rid of them.
    // The keys matched in other indexes cannot be safely subtracted
    // as indexes may contain false positives caused by deletes and updates.
    private KeyRangeIterator getNonEqIterator(QueryContext context, Collection<MemtableIndex> memtables, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        KeyRangeIterator allKeys = scanMemtable(keyRange, memtables);
        if (TypeUtil.supportsRounding(expression.validator) || !version.onDiskFormat().indexFeatureSet().isRowAware())
        {
            return allKeys;
        }
        else
        {
            Expression negExpression = expression.negated();
            KeyRangeIterator matchedKeys = searchMemtable(context, memtables, negExpression, keyRange);
            return KeyRangeAntiJoinIterator.create(allKeys, matchedKeys);
        }
    }

    public KeyRangeIterator searchMemtable(QueryContext context, Collection<MemtableIndex> memtables, Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        if (expression.getOp().isNonEquality())
        {
            return getNonEqIterator(context, memtables, expression, keyRange);
        }

        if (memtables.isEmpty())
        {
            return KeyRangeIterator.empty();
        }

        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        try
        {
            for (MemtableIndex index : memtables)
                builder.add(index.search(context, expression, keyRange));

            return builder.build();
        }
        catch (Exception ex)
        {
            FileUtils.closeQuietly(builder.ranges());
            throw ex;
        }
    }

    private KeyRangeIterator scanMemtable(AbstractBounds<PartitionPosition> keyRange, Collection<MemtableIndex> memtables)
    {
        if (memtables.isEmpty())
        {
            return KeyRangeIterator.empty();
        }

        KeyRangeIterator.Builder builder = KeyRangeUnionIterator.builder(memtables.size());

        try
        {
            for (MemtableIndex memtableIndex : memtables)
            {
                Memtable memtable = memtableIndex.getMemtable();
                KeyRangeIterator memtableIterator = new MemtableKeyRangeIterator(memtable, primaryKeyFactory, keyRange);
                builder.add(memtableIterator);
            }

            return builder.build();
        }
        catch (Exception ex)
        {
            FileUtils.closeQuietly(builder.ranges());
            throw ex;
        }
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
    public Set<SSTableContext> onSSTableChanged(Collection<SSTableReader> oldSSTables,
                                                Collection<SSTableContext> newContexts,
                                                boolean validate)
    {
        return viewManager.update(oldSSTables, newContexts, validate);
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

    public boolean isCollection()
    {
        return column.type.isCollection();
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

    public int getIntOption(String name, int defaultValue)
    {
        String value = this.config.options.get(name);
        if (value == null)
            return defaultValue;

        try
        {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            logger.error("Failed to parse index configuration {} = {} as integer", name, value);
            return defaultValue;
        }
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

    public View getReferencedView(long timeoutNanos)
    {
        var deadline = MonotonicClock.approxTime.now() + timeoutNanos;
        do
        {
            View view = viewManager.getView();
            if (view.reference())
                return view;
        } while (!MonotonicClock.approxTime.isAfter(deadline));

        return null;
    }

    /**
     * @return total number of per-index open files
     */
    public int openPerIndexFiles()
    {
        return viewManager.getView().size() * version.onDiskFormat().openFilesPerIndex(this);
    }

    public void prepareSSTablesForRebuild(Collection<SSTableReader> sstablesToRebuild)
    {
        viewManager.prepareSSTablesForRebuild(sstablesToRebuild);
    }

    public boolean isIndexed()
    {
        return config != null && !dropped;
    }

    public boolean isDropped()
    {
        return dropped;
    }

    /**
     * @return whether the column is analyzed, meaning it uses an analyzer that isn't no-op.
     */
    public boolean isAnalyzed()
    {
        return isAnalyzed;
    }

    /**
     * Called when index is dropped. Mark all {@link SSTableIndex} as released and per-column index files
     * will be removed when in-flight queries completed and {@code obsolete} is true.
     *
     * @param obsolete true if index files should be deleted after invalidate; false otherwise.
     */
    public void invalidate(boolean obsolete)
    {
        dropped = true;
        liveMemtables.clear();
        viewManager.invalidate(obsolete);
        indexMetrics.ifPresent(AbstractMetrics::release);
        if (columnQueryMetrics != null)
            columnQueryMetrics.release();

        analyzerFactory.close();
        if (queryAnalyzerFactory != analyzerFactory)
        {
            queryAnalyzerFactory.close();
        }
    }

    public ConcurrentMap<Memtable, MemtableIndex> getLiveMemtables()
    {
        return liveMemtables;
    }

    public boolean supports(Operator op)
    {
        if (op.isLike() || op == Operator.LIKE) return false;
        // Analyzed columns store the indexed result, so we are unable to compute raw equality.
        // The only supported operators are ANALYZER_MATCHES and BM25.
        if (op == Operator.ANALYZER_MATCHES) return isAnalyzed;
        // BM25 frequency calculations only work on non-collection columns because it assumes a 1:1 mapping from PrK
        // to frequency, but collections have mulitple documents.
        if (op == Operator.BM25) return isAnalyzed && !isCollection();

        // If the column is analyzed and the operator is EQ, we need to check if the analyzer supports it.
        if (op == Operator.EQ && isAnalyzed && !analyzerFactory.supportsEquals())
            return false;

        // ANN is only supported against vectors.
        // BOUNDED_ANN is only supported against vectors with a Euclidean similarity function.
        // Vector indexes only support ANN and BOUNDED_ANN
        if (column.type instanceof VectorType)
            return op == Operator.ANN || (op == Operator.BOUNDED_ANN && hasEuclideanSimilarityFunc);
        if (op == Operator.ANN || op == Operator.BOUNDED_ANN)
            return false;

        // Only regular columns can be sorted by SAI (at least for now)
        if (op == Operator.ORDER_BY_ASC || op == Operator.ORDER_BY_DESC)
            return !isCollection()
                   && column.isRegular()
                   && !isAnalyzed
                   && !(column.type instanceof InetAddressType  // Possible, but need to add decoding logic based on
                                                                 // SAI's TypeUtil.encode method.
                         || column.type instanceof DecimalType   // Currently truncates to 24 bytes
                         || column.type instanceof IntegerType); // Currently truncates to 20 bytes

        Expression.Op operator = Expression.Op.valueOf(op);
        if (isNonFrozenCollection())
        {
            if (indexType == IndexTarget.Type.KEYS)
                return operator == Expression.Op.CONTAINS_KEY
                       || operator == Expression.Op.NOT_CONTAINS_KEY;
            if (indexType == IndexTarget.Type.VALUES)
                return operator == Expression.Op.CONTAINS_VALUE
                       || operator == Expression.Op.NOT_CONTAINS_VALUE;
            return indexType == IndexTarget.Type.KEYS_AND_VALUES &&
                   (operator == Expression.Op.EQ || operator == Expression.Op.NOT_EQ || operator == Expression.Op.RANGE);
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

    public void validate(DecoratedKey key, Row row)
    {
        // Validate the size of the inserted term.
        if (VALIDATE_MAX_TERM_SIZE_AT_COORDINATOR.getBoolean())
            validateMaxTermSizeForRow(key, row);

        // Verify vector is valid.
        if (isVector())
        {
            float[] value = TypeUtil.decomposeVector(getValidator(), getValueOf(key, row, FBUtilities.nowInSeconds()));
            if (value != null)
                VectorValidation.validateIndexable(value, vectorSimilarityFunction);
        }
    }

    public void validate(RowFilter rowFilter)
    {
        // Only iterate over the top level expressions because that is where the ANN and BM25 expressions are located.
        for (RowFilter.Expression expression : rowFilter.root.expressions())
        {
            if (!expression.column().equals(column))
                continue;

            switch (expression.operator())
            {
                case ANN:
                    float[] value = TypeUtil.decomposeVector(getValidator(), expression.getIndexValue());
                    VectorValidation.validateIndexable(value, vectorSimilarityFunction);
                    return;
                case BM25:
                    if (version().onOrAfter(Version.BM25_EARLIEST))
                        return;
                    throw new InvalidRequestException(String.format(INDEX_VERSION_DOES_NOT_SUPPORT_BM25, getIndexName()));
            }
        }
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
        Set<SSTableIndex> valid = ConcurrentHashMap.newKeySet();
        Set<SSTableContext> invalid = ConcurrentHashMap.newKeySet();

        sstableContexts.stream().parallel().forEach(context -> {
            if (context.sstable.isMarkedCompacted())
                return;

            var perSSTableComponents = context.usedPerSSTableComponents();
            var perIndexComponents = perSSTableComponents.indexDescriptor().perIndexComponents(this);
            if (!perSSTableComponents.isComplete() || !perIndexComponents.isComplete())
            {
                logger.debug(logMessage("An on-disk index build for SSTable {} has not completed (per-index components={})."), context.descriptor(), perIndexComponents.all());
                return;
            }

            try
            {
                if (validate)
                {
                    if (!perIndexComponents.validateComponents(context.sstable, cfs.getTracker(), false))
                    {
                        // Note that a precise warning is already logged by the validation if there is an issue.
                        invalid.add(context);
                        return;
                    }
                }

                SSTableIndex index = new SSTableIndex(context, perIndexComponents);
                if (SAI_INDEX_READS_DISABLED.getBoolean())
                {
                    logger.debug(logMessage("Skipped loading index for SSTable {} as it's disabled by {}"), context.descriptor(), SAI_INDEX_READS_DISABLED.getKey());
                }
                else
                {
                    long count = context.primaryKeyMapFactory().count();
                    logger.debug(logMessage("Successfully loaded index for SSTable {} with {} rows."), context.descriptor(), count);
                }

                // Try to add new index to the set, if set already has such index, we'll simply release and move on.
                // This covers situation when SSTable collection has the same SSTable multiple
                // times because we don't know what kind of collection it actually is.
                if (!valid.add(index))
                    index.release();
            }
            catch (Throwable e)
            {
                logger.error(logMessage("Failed to update per-column components for SSTable {}"), context.descriptor(), e);
                invalid.add(context);
            }
        });

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
        IndexFeatureSet.Accumulator accumulator = new IndexFeatureSet.Accumulator(version);
        getView().getIndexes().stream().map(SSTableIndex::indexFeatureSet).forEach(accumulator::accumulate);
        return accumulator.complete();
    }
}
