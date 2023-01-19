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
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyFactory;
import org.apache.cassandra.index.sai.utils.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Manage metadata for each column index.
 */
public class IndexContext
{
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
    private final IndexMetadata config;

    private final ConcurrentMap<Memtable, MemtableIndex> liveMemtableIndexMap;

    private final IndexMetrics indexMetrics;

    private final AbstractAnalyzer.AnalyzerFactory indexAnalyzerFactory;
    private final AbstractAnalyzer.AnalyzerFactory queryAnalyzerFactory;
    private final PrimaryKeyFactory primaryKeyFactory;

    public IndexContext(String keyspace,
                        String table,
                        AbstractType<?> partitionKeyType,
                        ClusteringComparator clusteringComparator,
                        ColumnMetadata columnMetadata,
                        IndexTarget.Type indexType,
                        @Nullable IndexMetadata config)
    {
        this.keyspace = Objects.requireNonNull(keyspace);
        this.table = Objects.requireNonNull(table);
        this.partitionKeyType = Objects.requireNonNull(partitionKeyType);
        this.clusteringComparator = Objects.requireNonNull(clusteringComparator);
        this.columnMetadata = Objects.requireNonNull(columnMetadata);
        this.indexType = Objects.requireNonNull(indexType);
        this.validator = TypeUtil.cellValueType(columnMetadata, indexType);
        this.primaryKeyFactory = PrimaryKey.factory(clusteringComparator);

        this.config = config;
        this.indexMetrics = config == null ? null : new IndexMetrics(this);
        this.liveMemtableIndexMap = config == null ? null : new ConcurrentHashMap<>();

        // We currently only support the NoOpAnalyzer
        this.indexAnalyzerFactory = AbstractAnalyzer.fromOptions(getValidator(), Collections.emptyMap());
        this.queryAnalyzerFactory = AbstractAnalyzer.fromOptions(getValidator(), Collections.emptyMap());
    }

    public AbstractType<?> keyValidator()
    {
        return partitionKeyType;
    }

    public PrimaryKeyFactory keyFactory()
    {
        return primaryKeyFactory;
    }

    public ClusteringComparator comparator()
    {
        return clusteringComparator;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getTable()
    {
        return table;
    }

    public IndexMetadata getIndexMetadata()
    {
        return config;
    }

    public long index(DecoratedKey key, Row row, Memtable mt)
    {
        assert config != null : "Attempt to index on a non-indexing context";

        MemtableIndex current = liveMemtableIndexMap.get(mt);

        // We expect the relevant IndexMemtable to be present most of the time, so only make the
        // call to computeIfAbsent() if it's not. (see https://bugs.openjdk.java.net/browse/JDK-8161372)
        MemtableIndex target = (current != null)
                               ? current
                               : liveMemtableIndexMap.computeIfAbsent(mt, memtable -> new MemtableIndex(this));

        long start = Clock.Global.nanoTime();

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
        indexMetrics.memtableIndexWriteLatency.update(Clock.Global.nanoTime() - start, TimeUnit.NANOSECONDS);
        return bytes;
    }

    public void renewMemtable(Memtable renewed)
    {
        assert liveMemtableIndexMap != null : "Attempt to renew memtable on non-indexing context";

        for (Memtable memtable : liveMemtableIndexMap.keySet())
        {
            // remove every index but the one that corresponds to the post-truncate Memtable
            if (renewed != memtable)
            {
                liveMemtableIndexMap.remove(memtable);
            }
        }
    }

    public void discardMemtable(Memtable discarded)
    {
        assert liveMemtableIndexMap != null : "Attempt to discard memtable from non-indexing context";

        liveMemtableIndexMap.remove(discarded);
    }

    public KeyRangeIterator searchMemtableIndexes(Expression e, AbstractBounds<PartitionPosition> keyRange)
    {
        assert liveMemtableIndexMap != null : "Attempt to perform search on non-indexing context";

        Collection<MemtableIndex> memtableIndexes = liveMemtableIndexMap.values();

        if (memtableIndexes.isEmpty())
        {
            return KeyRangeIterator.empty();
        }

        KeyRangeIterator.Builder builder = KeyRangeUnionIterator.builder(memtableIndexes.size());

        for (MemtableIndex memtableIndex : memtableIndexes)
        {
            builder.add(memtableIndex.search(e, keyRange));
        }

        return builder.build();
    }

    public long liveMemtableWriteCount()
    {
        assert liveMemtableIndexMap != null : "Attempt to get metrics from non-indexing context";

        return liveMemtableIndexMap.values().stream().mapToLong(MemtableIndex::writeCount).sum();
    }

    public long estimatedMemIndexMemoryUsed()
    {
        assert liveMemtableIndexMap != null : "Attempt to get metrics from non-indexing context";

        return liveMemtableIndexMap.values().stream().mapToLong(MemtableIndex::estimatedMemoryUsed).sum();
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
        return config == null ? null : config.name;
    }

    /**
     * Returns an {@code AnalyzerFactory} for use by the write path to transform incoming literal
     * during indexing. The analyzers can be tokenising or non-tokenising. Tokenising analyzers
     * will split the incoming terms into multiple terms in the index while non-tokenising analyzers
     * will not split the incoming term but will transform the term (e.g. case-insensitive)
     */
    public AbstractAnalyzer.AnalyzerFactory getIndexAnalyzerFactory()
    {
        return indexAnalyzerFactory;
    }

    /**
     * Return an {@code AnalyzerFactory} for use by the query path to transform query terms before
     * searching for them in the index. This can be the same as the indexAnalyzerFactory.
     */
    public AbstractAnalyzer.AnalyzerFactory getQueryAnalyzerFactory()
    {
        return queryAnalyzerFactory;
    }

    public boolean isIndexed()
    {
        return config != null;
    }

    /**
     * Called when index is dropped. Clear all live in-memory indexes and close
     * analyzer factories.
     */
    public void invalidate()
    {
        if (liveMemtableIndexMap != null)
            liveMemtableIndexMap.clear();
        if (indexMetrics != null)
            indexMetrics.release();
        indexAnalyzerFactory.close();
        if (queryAnalyzerFactory != indexAnalyzerFactory)
        {
            queryAnalyzerFactory.close();
        }
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
               Objects.equals(config, other.config) &&
               Objects.equals(partitionKeyType, other.partitionKeyType) &&
               Objects.equals(clusteringComparator, other.clusteringComparator);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnMetadata, indexType, config, partitionKeyType, clusteringComparator);
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
}
