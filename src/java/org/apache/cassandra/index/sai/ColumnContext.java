/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

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
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.IndexSearcher;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sai.view.IndexViewManager;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Manage metadata for each column index.
 */
public class ColumnContext
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnContext.class);

    private static final Set<AbstractType<?>> EQ_ONLY_TYPES =
            ImmutableSet.of(UTF8Type.instance, AsciiType.instance, BooleanType.instance, UUIDType.instance);

    private final AbstractType<?> partitionKeyType;
    private final ClusteringComparator clusteringComparator;

    private final String keyspace;
    private final String table;
    private final Pair<ColumnMetadata, IndexTarget.Type> target;
    private final AbstractType<?> validator;

    // Config can be null if the column context is "fake" (i.e. created for a filtering expression).
    private final IndexMetadata config;

    private final ConcurrentMap<Memtable, MemtableIndex> liveMemtables = new ConcurrentHashMap<>();

    private final IndexViewManager viewManager;
    private final IndexMetrics indexMetrics;
    private final ColumnQueryMetrics columnQueryMetrics;
    private final IndexWriterConfig indexWriterConfig;

    public ColumnContext(TableMetadata tableMeta, IndexMetadata metadata)
    {
        this.keyspace = tableMeta.keyspace;
        this.table = tableMeta.name;
        this.partitionKeyType = tableMeta.partitionKeyType;
        this.clusteringComparator = tableMeta.comparator;
        this.target = TargetParser.parse(tableMeta, metadata);
        this.config = metadata;
        this.viewManager = new IndexViewManager(this);
        this.indexMetrics = new IndexMetrics(this, tableMeta);
        this.validator = TypeUtil.cellValueType(target);

        String fullIndexName = String.format("%s.%s.%s", this.keyspace, this.table, this.config.name);
        this.indexWriterConfig = IndexWriterConfig.fromOptions(fullIndexName, validator, config.options);
        this.columnQueryMetrics = isLiteral() ? new ColumnQueryMetrics.TrieIndexMetrics(getIndexName(), tableMeta)
                                              : new ColumnQueryMetrics.BKDIndexMetrics(getIndexName(), tableMeta);

        logger.info(logMessage("Initialized column context with index writer config: {}"),
                this.indexWriterConfig.toString());
    }

    @VisibleForTesting
    public ColumnContext(String keyspace,
                         String table,
                         AbstractType<?> partitionKeyType,
                         ClusteringComparator clusteringComparator,
                         ColumnMetadata column,
                         IndexMetadata config,
                         IndexWriterConfig indexWriterConfig)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.partitionKeyType = partitionKeyType;
        this.clusteringComparator = clusteringComparator;
        this.target = Pair.create(column, IndexTarget.Type.SIMPLE);
        this.validator = column.type;
        this.config = config;
        this.viewManager = null;
        this.indexMetrics = null;
        this.columnQueryMetrics = null;
        this.indexWriterConfig = indexWriterConfig;
    }

    public ColumnContext(TableMetadata table, ColumnMetadata column)
    {
        this.keyspace = table.keyspace;
        this.table = table.name;
        this.partitionKeyType = table.partitionKeyType;
        this.clusteringComparator = table.comparator;
        this.target = TargetParser.parse(table, column.name.toString());
        this.validator = target == null ? null : TypeUtil.cellValueType(target);
        this.config = null;
        this.viewManager = null;
        this.indexMetrics = null;
        this.columnQueryMetrics = null;
        this.indexWriterConfig = IndexWriterConfig.emptyConfig();
    }

    public AbstractType<?> keyValidator()
    {
        return partitionKeyType;
    }

    public ClusteringComparator clusteringComparator()
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

    public String getTable()
    {
        return table;
    }

    public long index(DecoratedKey key, Row row, Memtable mt)
    {
        MemtableIndex current = liveMemtables.get(mt);

        // We expect the relevant IndexMemtable to be present most of the time, so only make the
        // call to computeIfAbsent() if it's not. (see https://bugs.openjdk.java.net/browse/JDK-8161372)
        MemtableIndex target = (current != null)
                               ? current
                               : liveMemtables.computeIfAbsent(mt, memtable -> new MemtableIndex(this, mt));

        long start = System.nanoTime();

        long bytes = 0;

        if (isNonFrozenCollection())
        {
            Iterator<ByteBuffer> bufferIterator = getValuesOf(row, FBUtilities.nowInSeconds());
            if (bufferIterator != null)
            {
                while (bufferIterator.hasNext())
                {
                    ByteBuffer value = bufferIterator.next();
                    bytes += target.index(key, row.clustering(), value);
                }
            }
        }
        else
        {
            ByteBuffer value = getValueOf(key, row, FBUtilities.nowInSeconds());
            target.index(key, row.clustering(), value);
        }
        indexMetrics.memtableIndexWriteLatency.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        return bytes;
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

    public RangeIterator searchMemtable(Expression e, AbstractBounds<PartitionPosition> keyRange)
    {
        Collection<MemtableIndex> memtables = liveMemtables.values();

        if (memtables.isEmpty())
        {
            return RangeIterator.empty();
        }

        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        for (MemtableIndex index : memtables)
        {
            builder.add(index.search(e, keyRange));
        }

        return builder.build();
    }

    public long liveMemtableWriteCount()
    {
        return liveMemtables.values().stream().mapToLong(MemtableIndex::writeCount).sum();
    }

    public long estimatedMemIndexMemoryUsed()
    {
        return liveMemtables.values().stream().mapToLong(MemtableIndex::estimatedMemoryUsed).sum();
    }

    /**
     * @return A set of SSTables which have attached to them invalid index components.
     */
    public Set<SSTableContext> onSSTableChanged(Collection<SSTableReader> oldSSTables, Collection<SSTableContext> newSSTables, boolean validate, boolean rename)
    {
        return viewManager.update(oldSSTables, newSSTables, validate, rename);
    }

    public ColumnMetadata getDefinition()
    {
        return target.left;
    }

    public AbstractType<?> getValidator()
    {
        return validator;
    }

    public boolean isNonFrozenCollection()
    {
        return TypeUtil.isNonFrozenCollection(target.left.type);
    }

    public boolean isFrozenCollection()
    {
        return TypeUtil.isFrozenCollection(target.left.type);
    }

    public String getColumnName()
    {
        return target.left.name.toString();
    }

    public String getIndexName()
    {
        return this.config == null ? null : config.name;
    }

    public AbstractAnalyzer getAnalyzer()
    {
        Map<String, String> options = config != null ? config.options : Collections.emptyMap();
        return AbstractAnalyzer.fromOptions(getValidator(), options);
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
        return viewManager.getView().size() * IndexSearcher.openPerIndexFiles(getValidator());
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
     * Called when index is dropped. Mark all {@link SSTableIndex} as obsolete and per-column index files
     * will be removed when in-flight queries completed.
     */
    public void invalidate()
    {
        liveMemtables.clear();
        viewManager.invalidate();
        indexMetrics.release();
        columnQueryMetrics.release();
    }

    @VisibleForTesting
    public ConcurrentMap<Memtable, MemtableIndex> getLiveMemtables()
    {
        return liveMemtables;
    }

    public boolean supports(Operator op)
    {
        if (op.isLike() || op == Operator.LIKE) return false;

        Expression.Op operator = Expression.Op.valueOf(op);
        IndexTarget.Type type = target.right;

        if (isNonFrozenCollection())
        {
            if (type == IndexTarget.Type.KEYS) return operator == Expression.Op.CONTAINS_KEY;
            if (type == IndexTarget.Type.VALUES) return operator == Expression.Op.CONTAINS_VALUE;
            return type == IndexTarget.Type.KEYS_AND_VALUES && operator == Expression.Op.EQ;
        }

        if (type == IndexTarget.Type.FULL)
            return operator == Expression.Op.EQ;

        AbstractType<?> validator = getValidator();

        if (operator != Expression.Op.EQ && EQ_ONLY_TYPES.contains(validator)) return false;

        // RANGE only applicable to non-literal indexes
        return (operator != null) && !(TypeUtil.isLiteral(validator) && operator == Expression.Op.RANGE);
    }

    public ByteBuffer getValueOf(DecoratedKey key, Row row, int nowInSecs)
    {
        if (row == null)
            return null;

        switch (target.left.kind)
        {
            case PARTITION_KEY:
                return partitionKeyType instanceof CompositeType
                       ? CompositeType.extractComponent(key.getKey(), target.left.position())
                       : key.getKey();
            case CLUSTERING:
                // skip indexing of static clustering when regular column is indexed
                return row.isStatic() ? null : row.clustering().bufferAt(target.left.position());

            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                Cell cell = row.getCell(target.left);
                return cell == null || !cell.isLive(nowInSecs) ? null : cell.buffer();

            default:
                return null;
        }
    }

    public Iterator<ByteBuffer> getValuesOf(Row row, int nowInSecs)
    {
        if (row == null)
            return null;

        switch (target.left.kind)
        {
            // treat static cell retrieval the same was as regular
            // only if row kind is STATIC otherwise return null
            case STATIC:
                if (!row.isStatic())
                    return null;
            case REGULAR:
                return TypeUtil.collectionIterator(validator, (ComplexColumnData)row.getComplexColumnData(target.left), target, nowInSecs);

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

    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof ColumnContext))
            return false;

        ColumnContext other = (ColumnContext) obj;

        return Objects.equals(target, other.target) &&
                Objects.equals(config, other.config) &&
                Objects.equals(partitionKeyType, other.partitionKeyType) &&
                Objects.equals(clusteringComparator, other.clusteringComparator);
    }

    public int hashCode()
    {
        return Objects.hash(target, config, partitionKeyType, clusteringComparator);
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
    public Pair<Set<SSTableIndex>, Set<SSTableContext>> getBuiltIndexes(Collection<SSTableContext> sstableContexts, boolean validate, boolean rename)
    {
        Set<SSTableIndex> valid = new HashSet<>(sstableContexts.size());
        Set<SSTableContext> invalid = new HashSet<>();

        for (SSTableContext context : sstableContexts)
        {
            if (context.sstable.isMarkedCompacted())
                continue;

            if (!IndexComponents.isColumnIndexComplete(context.descriptor(), getIndexName()))
            {
                logger.debug(logMessage("An on-disk index build for SSTable {} has not completed."), context.descriptor());
                continue;
            }

            if (IndexComponents.isColumnIndexEmpty(context.descriptor(), getIndexName()))
            {
                logger.debug(logMessage("No on-disk index was built for SSTable {} because the SSTable " +
                                                "had no indexable rows for the index."), context.descriptor());
                continue;
            }

            // TODO: does the column name need to be encoded since it's being included in a filename?
            final IndexComponents components = IndexComponents.create(getIndexName(), context.sstable());

            try
            {
                if (validate)
                {
                    components.validatePerColumnComponents(isLiteral());
                }

                SSTableIndex index = new SSTableIndex(context, this, components);
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
                invalid.add(context);
                logger.warn(logMessage("Invalid per-column component for SSTable {}"), context.descriptor(), e);
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
}
