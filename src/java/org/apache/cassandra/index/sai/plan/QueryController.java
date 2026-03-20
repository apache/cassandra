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

package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.CellSourceIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MessageParams;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.IndexSearchResultIterator;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.utils.MergePrimaryKeyWithScoreIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RowWithSource;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.InsertionOrderedNavigableSet;
import org.apache.cassandra.utils.Throwables;

public class QueryController
{
    // Transforms a row to include its source table, which is then used for ANN query validation.
    private final static Function<CellSourceIdentifier, Transformation<BaseRowIterator<?>>> SOURCE_TABLE_ROW_TRANSFORMER = (CellSourceIdentifier sourceTable) -> new Transformation<>()
    {
        @Override
        protected Row applyToStatic(Row row)
        {
            return new RowWithSource(row, sourceTable);
        }
        @Override
        protected Row applyToRow(Row row)
        {
            return new RowWithSource(row, sourceTable);
        }
    };

    /**
     * The maximum number of primary keys we will materialize when performing hybrid vector search. If this limit is
     * exceeded, we switch to an order-by-then-filter execution path
     */
    public static int MAX_MATERIALIZED_KEYS = CassandraRelevantProperties.SAI_VECTOR_SEARCH_MAX_MATERIALIZE_KEYS.getInt();

    final QueryContext queryContext;

    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final RowFilter indexFilter;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;
    private final PrimaryKey.Factory keyFactory;
    private final PrimaryKey firstPrimaryKey;
    private final PrimaryKey lastPrimaryKey;

    private final NavigableSet<Clustering<?>> nextClusterings;

    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           RowFilter indexFilter,
                           QueryContext queryContext)
    {
        this.cfs = cfs;
        this.command = command;
        this.queryContext = queryContext;
        this.indexFilter = indexFilter;
        this.ranges = dataRanges(command);
        DataRange first = ranges.get(0);
        DataRange last = ranges.get(ranges.size() - 1);
        this.mergeRange = ranges.size() == 1 ? first.keyRange() : first.keyRange().withNewRight(last.keyRange().right);
        this.keyFactory = new PrimaryKey.Factory(cfs.getPartitioner(), cfs.getComparator());
        this.firstPrimaryKey = keyFactory.create(mergeRange.left.getToken());
        this.lastPrimaryKey = keyFactory.create(mergeRange.right.getToken());
        this.nextClusterings = new InsertionOrderedNavigableSet<>(cfs.metadata().comparator);
    }

    public PrimaryKey.Factory primaryKeyFactory()
    {
        return keyFactory;
    }

    public PrimaryKey firstPrimaryKeyInRange()
    {
        return firstPrimaryKey;
    }

    public PrimaryKey lastPrimaryKeyInRange()
    {
        return lastPrimaryKey;
    }

    public TableMetadata metadata()
    {
        return command.metadata();
    }

    public RowFilter indexFilter()
    {
        return this.indexFilter;
    }
    
    public boolean usesStrictFiltering()
    {
        return command.rowFilter().isStrict();
    }

    /**
     * @return token ranges used in the read command
     */
    public List<DataRange> dataRanges()
    {
        return ranges;
    }

    public AbstractBounds<PartitionPosition> mergeRange()
    {
        return mergeRange;
    }

    @Nullable
    public StorageAttachedIndex indexFor(RowFilter.Expression expression)
    {
        return cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class).orElse(null);
    }

    public boolean hasAnalyzer(RowFilter.Expression expression)
    {
        StorageAttachedIndex index = indexFor(expression);
        return index != null && index.hasAnalyzer();
    }

    public UnfilteredRowIterator queryStorage(List<PrimaryKey> keys, ReadExecutionController executionController)
    {
        if (keys.isEmpty())
            throw new IllegalArgumentException("At least one primary key is required!");

        SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(cfs.metadata(),
                                                                                 command.nowInSec(),
                                                                                 command.columnFilter(),
                                                                                 RowFilter.none(),
                                                                                 DataLimits.NONE,
                                                                                 keys.get(0).partitionKey(),
                                                                                 makeFilter(keys));

        return partition.queryMemtableAndDisk(cfs, executionController);
    }

    /**
     * Get an iterator over the row(s) for this primary key. Restrict the search to the specified view. Apply the
     * {@link #SOURCE_TABLE_ROW_TRANSFORMER} so that resulting cells have the source memtable/sstable. Expect one row
     * for a fully qualified primary key or all rows within a partition for a static primary key.
     *
     * @param key primary key to fetch from storage.
     * @param executionController the executionController to use when querying storage
     * @return an iterator of rows matching the query
     */
    public UnfilteredRowIterator queryStorage(PrimaryKey key, ColumnFamilyStore.ViewFragment view, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(cfs.metadata(),
                                                                                 command.nowInSec(),
                                                                                 command.columnFilter(),
                                                                                 RowFilter.none(),
                                                                                 DataLimits.NONE,
                                                                                 key.partitionKey(),
                                                                                 makeFilter(List.of(key)));

        return partition.queryMemtableAndDisk(cfs, view, SOURCE_TABLE_ROW_TRANSFORMER, executionController);
    }

    /**
     * Build a {@link KeyRangeIterator.Builder} from the given list of {@link Expression}s.
     * <p>
     * This is achieved by creating an on-disk view of the query that maps the expressions to
     * the {@link SSTableIndex}s that will satisfy the expression.
     * <p>
     * Each {@link QueryViewBuilder.QueryExpressionView} is then passed to
     * {@link IndexSearchResultIterator#build(QueryViewBuilder.QueryExpressionView, AbstractBounds, QueryContext, boolean, Runnable)}
     * to search the in-memory indexes associated with the expression and the SSTable indexes, the results of
     * which are unioned and returned.
     * <p>
     * The results from each call to {@link IndexSearchResultIterator#build(QueryViewBuilder.QueryExpressionView, AbstractBounds, QueryContext, boolean, Runnable)}
     * are added to a {@link KeyRangeIntersectionIterator} and returned if strict filtering is allowed.
     * <p>
     * If strict filtering is not allowed, indexes are split into two groups according to the repaired status of their 
     * backing SSTables. Results from searches over the repaired group are added to a 
     * {@link KeyRangeIntersectionIterator}, which is then added, along with results from searches on the unrepaired
     * set, to a top-level {@link KeyRangeUnionIterator}, and returned. This is done to ensure that AND queries do not
     * prematurely filter out matches on un-repaired partial updates. Post-filtering must also take this into
     * account. (see {@link FilterTree#isSatisfiedBy(DecoratedKey, Row, Row)}) Note that Memtable-attached 
     * indexes are treated as part of the unrepaired set.
     */
    public KeyRangeIterator.Builder getIndexQueryResults(Collection<Expression> expressions)
    {
        // VSTODO move ANN out of expressions and into its own abstraction? That will help get generic ORDER BY support
        expressions = expressions.stream().filter(e -> e.getIndexOperator() != Expression.IndexOperator.ANN).collect(Collectors.toList());

        QueryViewBuilder.QueryView queryView = new QueryViewBuilder(expressions, mergeRange).build();
        KeyRangeIterator.Builder builder = command.rowFilter().isStrict()
                                           ? KeyRangeIntersectionIterator.builder(expressions.size(), queryView::close)
                                           : KeyRangeUnionIterator.builder(expressions.size(), queryView::close);

        try
        {
            maybeTriggerGuardrails(queryView);

            if (command.rowFilter().isStrict())
            {
                // If strict filtering is enabled, evaluate indexes for both repaired and un-repaired SSTables together.
                // This usually means we are making this local index query in the context of a user query that reads 
                // from a single replica and thus can safely perform local intersections.
                for (QueryViewBuilder.QueryExpressionView queryExpressionView : queryView.view)
                    builder.add(IndexSearchResultIterator.build(queryExpressionView, mergeRange, queryContext, true, () -> {}));
            }
            else
            {
                KeyRangeIterator.Builder repairedBuilder = KeyRangeIntersectionIterator.builder(expressions.size(), () -> {});

                for (QueryViewBuilder.QueryExpressionView queryExpressionView : queryView.view)
                {
                    Expression expression = queryExpressionView.expression;
                    // The initial sizes here reflect little more than an effort to avoid resizing for 
                    // partition-restricted searches w/ LCS:
                    List<SSTableIndex> repaired = new ArrayList<>(5);
                    List<SSTableIndex> unrepaired = new ArrayList<>(5);

                    // Split SSTable indexes into repaired and un-reparired:
                    for (SSTableIndex index : queryExpressionView.sstableIndexes)
                        if (index.getSSTable().isRepaired())
                            repaired.add(index);
                        else
                            unrepaired.add(index);

                    // Always build an iterator for the un-repaired set, given this must include Memtable indexes...  
                    IndexSearchResultIterator unrepairedIterator =
                            IndexSearchResultIterator.build(expression, queryExpressionView.memtableIndexes, unrepaired, mergeRange, queryContext, true, () -> {});

                    // ...but ignore it if our combined results are empty.
                    if (unrepairedIterator.getMaxKeys() > 0)
                    {
                        builder.add(unrepairedIterator);
                        queryContext.hasUnrepairedMatches = true;
                    }
                    else
                    {
                        // We're not going to use this, so release the resources it holds.
                        unrepairedIterator.close();
                    }

                    // ...then only add an iterator to the repaired intersection if repaired SSTable indexes exist. 
                    if (!repaired.isEmpty())
                        repairedBuilder.add(IndexSearchResultIterator.build(expression, Collections.emptyList(), repaired, mergeRange, queryContext, false, () -> {}));
                }

                if (repairedBuilder.rangeCount() > 0)
                    builder.add(repairedBuilder.build());
            }
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            builder.cleanup();
            throw t;
        }
        return builder;
    }

    void maybeTriggerGuardrails(QueryViewBuilder.QueryView queryView)
    {
        int referencedIndexes = 0;

        // We want to make sure that no individual column expression touches too many SSTable-attached indexes:
        for (QueryViewBuilder.QueryExpressionView expressionSSTables : queryView.view)
            referencedIndexes = Math.max(referencedIndexes, expressionSSTables.sstableIndexes.size());

        if (Guardrails.saiSSTableIndexesPerQuery.failsOn(referencedIndexes, null))
        {
            String msg = String.format("Query %s attempted to read from too many indexes (%s) but max allowed is %s; " +
                                       "query aborted (see sai_sstable_indexes_per_query_fail_threshold)",
                                       command.toCQLString(),
                                       referencedIndexes,
                                       Guardrails.CONFIG_PROVIDER.getOrCreate(null).getSaiSSTableIndexesPerQueryFailThreshold());
            Tracing.trace(msg);
            MessageParams.add(ParamType.TOO_MANY_REFERENCED_INDEXES_FAIL, referencedIndexes);
            throw new QueryReferencingTooManyIndexesException(msg);
        }
        else if (Guardrails.saiSSTableIndexesPerQuery.warnsOn(referencedIndexes, null))
        {
            MessageParams.add(ParamType.TOO_MANY_REFERENCED_INDEXES_WARN, referencedIndexes);
        }
    }

    /**
     * Returns whether this query is not selecting the {@link PrimaryKey}.
     * The query does not select the key if both of the following statements are false:
     *  1. The table associated with the query is not using clustering keys
     *  2. The clustering index filter for the command wants the row.
     * <p>
     *  Item 2 is important in paged queries where the {@link org.apache.cassandra.db.filter.ClusteringIndexSliceFilter} for
     *  subsequent paged queries may not select rows that are returned by the index
     *  search because that is initially partition based.
     *
     * @param key The {@link PrimaryKey} to be tested
     * @return true if the key is not selected by the query
     */
    public boolean doesNotSelect(PrimaryKey key)
    {
        return key.kind() == PrimaryKey.Kind.WIDE && !command.clusteringIndexFilter(key.partitionKey()).selects(key.clustering());
    }

    // This is an ANN only query
    public CloseableIterator<PrimaryKeyWithScore> getTopKRows(QueryViewBuilder.QueryExpressionView queryExpressionView)
    {
        assert queryExpressionView.expression.operator == Expression.IndexOperator.ANN;
        List<CloseableIterator<PrimaryKeyWithScore>> intermediateResults = new ArrayList<>();
        try
        {
            for (MemtableIndex memtableIndex : queryExpressionView.memtableIndexes)
                intermediateResults.add(memtableIndex.orderBy(queryContext, queryExpressionView.expression, mergeRange));
            for (SSTableIndex sstableIndex : queryExpressionView.sstableIndexes)
                intermediateResults.addAll(sstableIndex.orderBy(queryExpressionView.expression, mergeRange, queryContext));
            return intermediateResults.isEmpty() ? CloseableIterator.empty()
                                                 : new MergePrimaryKeyWithScoreIterator(intermediateResults);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryExpressionView.sstableIndexes.forEach(SSTableIndex::releaseQuietly);
            intermediateResults.forEach(FileUtils::closeQuietly);
            throw Throwables.cleaned(t);
        }
    }

    // This is a hybrid query. We apply all other predicates before ordering and limiting.
    public CloseableIterator<PrimaryKeyWithScore> getTopKRows(KeyRangeIterator source, QueryViewBuilder.QueryExpressionView queryExpressionView)
    {
        List<PrimaryKey> primaryKeys = materializeKeysAndCloseSource(source);
        if (primaryKeys == null)
            return getTopKRows(queryExpressionView);
        if (primaryKeys.isEmpty())
            return CloseableIterator.empty();
        return getTopKRows(primaryKeys, queryExpressionView);
    }

    private CloseableIterator<PrimaryKeyWithScore> getTopKRows(List<PrimaryKey> sourceKeys, QueryViewBuilder.QueryExpressionView queryExpressionView)
    {
        List<CloseableIterator<PrimaryKeyWithScore>> intermediateResults = new ArrayList<>();
        try
        {
            for (MemtableIndex memtableIndex : queryExpressionView.memtableIndexes)
                intermediateResults.add(memtableIndex.orderResultsBy(queryContext, sourceKeys, queryExpressionView.expression));
            for (SSTableIndex sstableIndex : queryExpressionView.sstableIndexes)
                intermediateResults.addAll(sstableIndex.orderResultsBy(queryContext, sourceKeys, queryExpressionView.expression));
            return intermediateResults.isEmpty() ? CloseableIterator.empty()
                                                 : new MergePrimaryKeyWithScoreIterator(intermediateResults);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryExpressionView.sstableIndexes.forEach(SSTableIndex::releaseQuietly);
            intermediateResults.forEach(FileUtils::closeQuietly);
            throw Throwables.cleaned(t);
        }
    }

    /**
     * Materialize the keys from the given source iterator. If there is a meaningful {@link #mergeRange}, the keys
     * are filtered to only include those within the range. Note: closes the source iterator.
     * @param source The source iterator to fully consume by materializing its keys
     * @return The list of materialized keys within the {@link #mergeRange}, or return null if source exceeded the
     * materialized keys limit.
     */
    private List<PrimaryKey> materializeKeysAndCloseSource(KeyRangeIterator source)
    {
        try (source)
        {
            // Skip to the first key (which is really just a token) in the range if it is not the minimum token
            if (!mergeRange.left.isMinimum())
                source.skipTo(firstPrimaryKey);

            if (!source.hasNext())
                return List.of();

            PrimaryKey maxToken = keyFactory.create(mergeRange.right.getToken());
            boolean hasLimitingMaxToken = !maxToken.token().isMinimum() && maxToken.compareTo(source.getMaximum()) < 0;
            List<PrimaryKey> primaryKeys = new ArrayList<>();
            int count = 0;
            while (source.hasNext())
            {
                PrimaryKey next = source.next();
                if (hasLimitingMaxToken && next.compareTo(maxToken) > 0)
                    break;
                primaryKeys.add(next);
                if (MAX_MATERIALIZED_KEYS < ++count)
                {
                    Tracing.trace("WHERE clause generated more than {} rows. Switching to ORDER BY then post filter.",  MAX_MATERIALIZED_KEYS);
                    return null;
                }
            }
            return primaryKeys;
        }
    }

    // Note: This method assumes that the selects method has already been called for the
    // key to avoid having to (potentially) call selects twice
    private ClusteringIndexFilter makeFilter(List<PrimaryKey> keys)
    {
        PrimaryKey firstKey = keys.get(0);

        assert cfs.metadata().comparator.size() == 0 && !firstKey.kind().hasClustering ||
               cfs.metadata().comparator.size() > 0 && firstKey.kind().hasClustering :
               "PrimaryKey " + firstKey + " clustering does not match table. There should be a clustering of size " + cfs.metadata().comparator.size();

        ClusteringIndexFilter clusteringIndexFilter = command.clusteringIndexFilter(firstKey.partitionKey());

        // If we have skinny partitions or the key is for a static row then we need to get the partition as
        // requested by the original query.
        if (cfs.metadata().comparator.size() == 0 || firstKey.kind() == PrimaryKey.Kind.STATIC)
        {
            return clusteringIndexFilter;
        }
        else
        {
            nextClusterings.clear();
            for (PrimaryKey key : keys)
                nextClusterings.add(key.clustering());
            return new ClusteringIndexNamesFilter(nextClusterings, clusteringIndexFilter.isReversed());
        }
    }

    /**
     * Returns the {@link DataRange} list covered by the specified {@link ReadCommand}.
     *
     * @param command a read command
     * @return the data ranges covered by {@code command}
     */
    private static List<DataRange> dataRanges(ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            return Lists.newArrayList(command.dataRange());
        }
        else if (command instanceof PartitionRangeReadCommand)
        {
            return Lists.newArrayList(command.dataRange());
        }
        else
        {
            throw new AssertionError("Unsupported read command type: " + command.getClass().getName());
        }
    }
}
