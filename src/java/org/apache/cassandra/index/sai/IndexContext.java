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
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.memory.MemtableIndexManager;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.IndexViewManager;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Pair;

/**
 * Manages metadata for each column index.
 */
public class IndexContext
{
    private static final Logger logger = LoggerFactory.getLogger(IndexContext.class);

    private static final Set<AbstractType<?>> EQ_ONLY_TYPES = ImmutableSet.of(UTF8Type.instance,
                                                                              AsciiType.instance,
                                                                              BooleanType.instance,
                                                                              UUIDType.instance);

    private final AbstractType<?> partitionKeyType;
    private final ClusteringComparator clusteringComparator;

    private final String keyspace;
    private final String table;
    private final ColumnMetadata columnMetadata;
    private final IndexTarget.Type indexType;
    private final AbstractType<?> validator;

    // Config can be null if the column context is "fake" (i.e. created for a filtering expression).
    @Nullable
    private final IndexMetadata indexMetadata;

    private final MemtableIndexManager memtableIndexManager;

    private final IndexViewManager viewManager;
    private final IndexMetrics indexMetrics;
    private final ColumnQueryMetrics columnQueryMetrics;
    private final AbstractAnalyzer.AnalyzerFactory analyzerFactory;
    private final PrimaryKey.Factory primaryKeyFactory;

    public IndexContext(String keyspace,
                        String table,
                        AbstractType<?> partitionKeyType,
                        ClusteringComparator clusteringComparator,
                        ColumnMetadata columnMetadata,
                        IndexTarget.Type indexType,
                        @Nullable IndexMetadata indexMetadata)
    {
        this.keyspace = Objects.requireNonNull(keyspace);
        this.table = Objects.requireNonNull(table);
        this.partitionKeyType = Objects.requireNonNull(partitionKeyType);
        this.clusteringComparator = Objects.requireNonNull(clusteringComparator);
        this.columnMetadata = Objects.requireNonNull(columnMetadata);
        this.indexType = Objects.requireNonNull(indexType);
        this.validator = TypeUtil.cellValueType(columnMetadata, indexType);
        this.primaryKeyFactory = new PrimaryKey.Factory(clusteringComparator);

        this.indexMetadata = indexMetadata;
        this.memtableIndexManager = indexMetadata == null ? null : new MemtableIndexManager(this);

        this.indexMetrics = indexMetadata == null ? null : new IndexMetrics(this);
        this.viewManager = new IndexViewManager(this);
        this.columnQueryMetrics = isLiteral() ? new ColumnQueryMetrics.TrieIndexMetrics(this)
                                              : new ColumnQueryMetrics.BalancedTreeIndexMetrics(this);

        this.analyzerFactory = indexMetadata == null ? AbstractAnalyzer.fromOptions(getValidator(), Collections.emptyMap())
                                                     : AbstractAnalyzer.fromOptions(getValidator(), indexMetadata.options);
    }

    public AbstractType<?> keyValidator()
    {
        return partitionKeyType;
    }

    public PrimaryKey.Factory keyFactory()
    {
        return primaryKeyFactory;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public IndexMetrics getIndexMetrics()
    {
        return indexMetrics;
    }

    public ColumnQueryMetrics getColumnQueryMetrics()
    {
        return columnQueryMetrics;
    }

    public String getTable()
    {
        return table;
    }

    public IndexMetadata getIndexMetadata()
    {
        return indexMetadata;
    }

    /**
     * @return A set of SSTables which have attached to them invalid index components.
     */
    public Collection<SSTableContext> onSSTableChanged(Collection<SSTableReader> oldSSTables, Collection<SSTableContext> newSSTables, IndexValidation validation)
    {
        return viewManager.update(oldSSTables, newSSTables, validation);
    }

    public ColumnMetadata getDefinition()
    {
        return columnMetadata;
    }

    public AbstractType<?> getValidator()
    {
        return validator;
    }

    public boolean isNonFrozenCollection()
    {
        return TypeUtil.isNonFrozenCollection(columnMetadata.type);
    }

    public boolean isFrozen()
    {
        return TypeUtil.isFrozen(columnMetadata.type);
    }

    public String getColumnName()
    {
        return columnMetadata.name.toString();
    }

    @Nullable
    public String getIndexName()
    {
        return indexMetadata == null ? null : indexMetadata.name;
    }

    /**
     * Returns an {@link AbstractAnalyzer.AnalyzerFactory} for use by write and query paths to transform
     * literal values.
     */
    public AbstractAnalyzer.AnalyzerFactory getAnalyzerFactory()
    {
        return analyzerFactory;
    }

    public View getView()
    {
        return viewManager.getView();
    }

    public MemtableIndexManager getMemtableIndexManager()
    {
        assert memtableIndexManager != null : "Attempt to use memtable index manager on non-indexed context";

        return memtableIndexManager;
    }

    /**
     * @return total number of per-index open files
     */
    public int openPerIndexFiles()
    {
        return viewManager.getView().size() * Version.LATEST.onDiskFormat().openFilesPerColumnIndex(this);
    }

    public void drop(Collection<SSTableReader> sstablesToRebuild)
    {
        viewManager.drop(sstablesToRebuild);
    }

    public boolean isNotIndexed()
    {
        return indexMetadata == null;
    }

    /**
     * Called when index is dropped. Clear all live in-memory indexes and close
     * analyzer factories. Mark all {@link SSTableIndex} as released and per-column index files
     * will be removed when in-flight queries are completed.
     */
    public void invalidate()
    {
        viewManager.invalidate();
        analyzerFactory.close();
        if (memtableIndexManager != null)
            memtableIndexManager.invalidate();
        if (indexMetrics != null)
            indexMetrics.release();
        if (columnQueryMetrics != null)
            columnQueryMetrics.release();
    }

    public boolean supports(Operator op)
    {
        if (op == Operator.LIKE ||
            op == Operator.LIKE_CONTAINS ||
            op == Operator.LIKE_PREFIX ||
            op == Operator.LIKE_MATCHES ||
            op == Operator.LIKE_SUFFIX) return false;

        Expression.IndexOperator operator = Expression.IndexOperator.valueOf(op);

        if (isNonFrozenCollection())
        {
            if (indexType == IndexTarget.Type.KEYS) return operator == Expression.IndexOperator.CONTAINS_KEY;
            if (indexType == IndexTarget.Type.VALUES) return operator == Expression.IndexOperator.CONTAINS_VALUE;
            return indexType == IndexTarget.Type.KEYS_AND_VALUES && operator == Expression.IndexOperator.EQ;
        }

        if (indexType == IndexTarget.Type.FULL)
            return operator == Expression.IndexOperator.EQ;

        AbstractType<?> validator = getValidator();

        if (operator != Expression.IndexOperator.EQ && EQ_ONLY_TYPES.contains(validator)) return false;

        // RANGE only applicable to non-literal indexes
        return (operator != null) && !(TypeUtil.isLiteral(validator) && operator == Expression.IndexOperator.RANGE);
    }

    public ByteBuffer getValueOf(DecoratedKey key, Row row, long nowInSecs)
    {
        if (row == null)
            return null;

        switch (columnMetadata.kind)
        {
            case PARTITION_KEY:
                return partitionKeyType instanceof CompositeType
                       ? CompositeType.extractComponent(key.getKey(), columnMetadata.position())
                       : key.getKey();
            case CLUSTERING:
                // skip indexing of static clustering when regular column is indexed
                return row.isStatic() ? null : row.clustering().bufferAt(columnMetadata.position());

            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                Cell<?> cell = row.getCell(columnMetadata);
                return cell == null || !cell.isLive(nowInSecs) ? null : cell.buffer();

            default:
                return null;
        }
    }

    public Iterator<ByteBuffer> getValuesOf(Row row, long nowInSecs)
    {
        if (row == null)
            return null;

        switch (columnMetadata.kind)
        {
            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                return TypeUtil.collectionIterator(validator,
                                                   row.getComplexColumnData(columnMetadata),
                                                   columnMetadata,
                                                   indexType,
                                                   nowInSecs);

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

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexContext))
            return false;

        IndexContext other = (IndexContext) obj;

        return Objects.equals(columnMetadata, other.columnMetadata) &&
               (indexType == other.indexType) &&
               Objects.equals(indexMetadata, other.indexMetadata) &&
               Objects.equals(partitionKeyType, other.partitionKeyType) &&
               Objects.equals(clusteringComparator, other.clusteringComparator);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnMetadata, indexType, indexMetadata, partitionKeyType, clusteringComparator);
    }

    /**
     * A helper method for constructing consistent log messages for specific column indexes.
     * <p>
     * Example: For the index "idx" in keyspace "ks" on table "tb", calling this method with the raw message
     * "Flushing new index segment..." will produce...
     * <p>
     * "[ks.tb.idx] Flushing new index segment..."
     *
     * @param message The raw content of a logging message, without information identifying it with an index.
     *
     * @return A log message with the proper keyspace, table and index name prepended to it.
     */
    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.%s] %s", keyspace, table, indexMetadata == null ? "?" : indexMetadata.name, message);
    }

    /**
     * @return the indexes that are built on the given SSTables on the left and corrupted indexes'
     * corresponding contexts on the right
     */
    public Pair<Collection<SSTableIndex>, Collection<SSTableContext>> getBuiltIndexes(Collection<SSTableContext> sstableContexts, IndexValidation validation)
    {
        Set<SSTableIndex> valid = new HashSet<>(sstableContexts.size());
        Set<SSTableContext> invalid = new HashSet<>();

        for (SSTableContext sstableContext : sstableContexts)
        {
            if (sstableContext.sstable.isMarkedCompacted())
                continue;

            if (!sstableContext.indexDescriptor.isPerColumnIndexBuildComplete(this))
            {
                logger.debug(logMessage("An on-disk index build for SSTable {} has not completed."), sstableContext.descriptor());
                continue;
            }

            if (sstableContext.indexDescriptor.isIndexEmpty(this))
            {
                logger.debug(logMessage("No on-disk index was built for SSTable {} because the SSTable " +
                                        "had no indexable rows for the index."), sstableContext.descriptor());
                continue;
            }

            try
            {
                if (validation != IndexValidation.NONE)
                {
                    if (!sstableContext.indexDescriptor.validatePerIndexComponents(this, validation))
                    {
                        invalid.add(sstableContext);
                        continue;
                    }
                }

                SSTableIndex index = sstableContext.newSSTableIndex(this);
                logger.debug(logMessage("Successfully created index for SSTable {}."), sstableContext.descriptor());

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
                logger.warn(logMessage("Failed to update per-column components for SSTable {}"), sstableContext.descriptor(), e);
                invalid.add(sstableContext);
            }
        }

        return Pair.create(valid, invalid);
    }

    /**
     * @return the number of indexed rows in this index (aka. a pair of term and rowId)
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
}
