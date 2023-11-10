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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.VectorQueryContext;
import org.apache.cassandra.index.sai.disk.IndexSearchResultIterator;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeConcatIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeOrderingIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE;

public class QueryController
{
    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;
    private final RowFilter filterOperation;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;
    private final PrimaryKey.Factory keyFactory;
    private final PrimaryKey firstPrimaryKey;
    private final PrimaryKey lastPrimaryKey;
    private final int orderChunkSize;

    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           RowFilter filterOperation,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this.cfs = cfs;
        this.command = command;
        this.queryContext = queryContext;
        this.tableQueryMetrics = tableQueryMetrics;
        this.filterOperation = filterOperation;
        this.ranges = dataRanges(command);
        DataRange first = ranges.get(0);
        DataRange last = ranges.get(ranges.size() - 1);
        this.mergeRange = ranges.size() == 1 ? first.keyRange() : first.keyRange().withNewRight(last.keyRange().right);
        this.keyFactory = new PrimaryKey.Factory(cfs.getPartitioner(), cfs.getComparator());
        this.firstPrimaryKey = keyFactory.create(mergeRange.left.getToken());
        this.lastPrimaryKey = keyFactory.create(mergeRange.right.getToken());
        this.orderChunkSize = SAI_VECTOR_SEARCH_ORDER_CHUNK_SIZE.getInt();
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

    public RowFilter filterOperation()
    {
        return this.filterOperation;
    }

    /**
     * @return token ranges used in the read command
     */
    public List<DataRange> dataRanges()
    {
        return ranges;
    }

    public StorageAttachedIndex indexFor(RowFilter.Expression expression)
    {
        Set<StorageAttachedIndex> indexes = cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class);
        return indexes.isEmpty() ? null : indexes.iterator().next();
    }

    public boolean hasAnalyzer(RowFilter.Expression expression)
    {
        StorageAttachedIndex index = indexFor(expression);
        return index != null && index.hasAnalyzer();
    }

    public UnfilteredRowIterator queryStorage(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        try
        {
            SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(cfs.metadata(),
                                                                                     command.nowInSec(),
                                                                                     command.columnFilter(),
                                                                                     RowFilter.none(),
                                                                                     DataLimits.NONE,
                                                                                     key.partitionKey(),
                                                                                     makeFilter(key));

            return partition.queryMemtableAndDisk(cfs, executionController);
        }
        finally
        {
            queryContext.checkpoint();
        }
    }

    /**
     * Build a {@link KeyRangeIterator.Builder} from the given list of {@link Expression}s.
     * <p>
     * This is achieved by creating an on-disk view of the query that maps the expressions to
     * the {@link SSTableIndex}s that will satisfy the expression.
     * <p>
     * Each (expression, SSTable indexes) pair is then passed to
     * {@link IndexSearchResultIterator#build(Expression, Collection, AbstractBounds, QueryContext)}
     * to search the in-memory index associated with the expression and the SSTable indexes, the results of
     * which are unioned and returned.
     * <p>
     * The results from each call to {@link IndexSearchResultIterator#build(Expression, Collection, AbstractBounds, QueryContext)}
     * are added to a {@link KeyRangeIntersectionIterator} and returned.
     */
    public KeyRangeIterator.Builder getIndexQueryResults(Collection<Expression> expressions)
    {
        // VSTODO move ANN out of expressions and into its own abstraction? That will help get generic ORDER BY support
        expressions = expressions.stream().filter(e -> e.getIndexOperator() != Expression.IndexOperator.ANN).collect(Collectors.toList());

        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(expressions.size());

        QueryViewBuilder queryViewBuilder = new QueryViewBuilder(expressions, mergeRange);

        QueryViewBuilder.QueryView queryView = queryViewBuilder.build();

        try
        {
            for (Pair<Expression, Collection<SSTableIndex>> queryViewPair : queryView.view)
            {
                KeyRangeIterator indexIterator = IndexSearchResultIterator.build(queryViewPair.left, queryViewPair.right, mergeRange, queryContext);

                builder.add(indexIterator);
            }
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            builder.cleanup();
            queryView.referencedIndexes.forEach(SSTableIndex::releaseQuietly);
            throw t;
        }
        return builder;
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

    /**
     * Used to release all resources and record metrics when query finishes.
     */
    public void finish()
    {
        if (tableQueryMetrics != null) tableQueryMetrics.record(queryContext);
    }

    // This is an ANN only query
    public KeyRangeIterator getTopKRows(RowFilter.Expression expression)
    {
        assert expression.operator() == Operator.ANN;
        StorageAttachedIndex index = indexFor(expression);
        assert index != null;
        var planExpression = Expression.create(index).add(Operator.ANN, expression.getIndexValue().duplicate());
        // search memtable before referencing sstable indexes; otherwise we may miss newly flushed memtable index
        KeyRangeIterator memtableResults = index.memtableIndexManager().searchMemtableIndexes(queryContext, planExpression, mergeRange);

        QueryViewBuilder.QueryView queryView = new QueryViewBuilder(Collections.singleton(planExpression), mergeRange).build();

        try
        {
            List<KeyRangeIterator> sstableIntersections = queryView.view
                                                                   .stream()
                                                                   .map(this::createRowIdIterator)
                                                                   .collect(Collectors.toList());
            return IndexSearchResultIterator.build(sstableIntersections, memtableResults, queryView.referencedIndexes, queryContext);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryView.referencedIndexes.forEach(SSTableIndex::release);
            throw t;
        }
    }

    // This is a hybrid query. We apply all other predicates before ordering and limiting.
    public KeyRangeIterator getTopKRows(KeyRangeIterator source, RowFilter.Expression expression)
    {
        return new KeyRangeOrderingIterator(source, orderChunkSize, list -> this.getTopKRows(list, expression));
    }

    private KeyRangeIterator getTopKRows(List<PrimaryKey> rawSourceKeys, RowFilter.Expression expression)
    {
        VectorQueryContext vectorQueryContext = queryContext.vectorContext();
        // Filter out PKs now. Each PK is passed to every segment of the ANN index, so filtering shadowed keys
        // eagerly can save some work when going from PK to row id for on disk segments.
        // Since the result is shared with multiple streams, we use an unmodifiable list.
        var sourceKeys = rawSourceKeys.stream().filter(vectorQueryContext::shouldInclude).collect(Collectors.toList());
        StorageAttachedIndex index = indexFor(expression);
        assert index != null : "Cannot do ANN ordering on an unindexed column";
        var planExpression = Expression.create(index);
        planExpression.add(Operator.ANN, expression.getIndexValue().duplicate());

        // search memtable before referencing sstable indexes; otherwise we may miss newly flushed memtable index
        KeyRangeIterator memtableResults = index.memtableIndexManager().limitToTopResults(queryContext, sourceKeys, planExpression);
        QueryViewBuilder.QueryView queryView = new QueryViewBuilder(Collections.singleton(planExpression), mergeRange).build();

        try
        {
            List<KeyRangeIterator> sstableIntersections = queryView.view
                                                                   .stream()
                                                                   .flatMap(pair -> pair.right.stream())
                                                                   .map(idx -> {
                                                                       try
                                                                       {
                                                                           return idx.limitToTopKResults(queryContext, sourceKeys, planExpression);
                                                                       }
                                                                       catch (IOException e)
                                                                       {
                                                                           throw new UncheckedIOException(e);
                                                                       }
                                                                   })
                                                                   .collect(Collectors.toList());

            return IndexSearchResultIterator.build(sstableIntersections, memtableResults, queryView.referencedIndexes, queryContext);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryView.referencedIndexes.forEach(SSTableIndex::release);
            throw t;
        }
    }

    /**
     * Create row id iterator from different indexes' on-disk searcher of the same sstable
     */
    private KeyRangeIterator createRowIdIterator(Pair<Expression, Collection<SSTableIndex>> indexExpression)
    {
        var subIterators = indexExpression.right
                           .stream()
                           .map(index ->
                                {
                                    try
                                    {
                                        List<KeyRangeIterator> iterators = index.search(indexExpression.left, mergeRange, queryContext);
                                        // concat the result from multiple segments for the same index
                                        return KeyRangeConcatIterator.builder(iterators.size()).add(iterators).build();
                                    }
                                    catch (Throwable ex)
                                    {
                                        throw Throwables.cleaned(ex);
                                    }
                                }).collect(Collectors.toList());

        return KeyRangeUnionIterator.build(subIterators);
    }

    // Note: This method assumes that the selects method has already been called for the
    // key to avoid having to (potentially) call selects twice
    private ClusteringIndexFilter makeFilter(PrimaryKey key)
    {
        ClusteringIndexFilter clusteringIndexFilter = command.clusteringIndexFilter(key.partitionKey());

        assert cfs.metadata().comparator.size() == 0 && !key.kind().hasClustering ||
               cfs.metadata().comparator.size() > 0 && key.kind().hasClustering :
               "PrimaryKey " + key + " clustering does not match table. There should be a clustering of size " + cfs.metadata().comparator.size();

        // If we have skinny partitions or the key is for a static row then we need to get the partition as
        // requested by the original query.
        if (cfs.metadata().comparator.size() == 0 || key.kind() == PrimaryKey.Kind.STATIC)
            return clusteringIndexFilter;
        else
            return new ClusteringIndexNamesFilter(FBUtilities.singleton(key.clustering(), cfs.metadata().comparator),
                                                  clusteringIndexFilter.isReversed());
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
            SinglePartitionReadCommand cmd = (SinglePartitionReadCommand) command;
            DecoratedKey key = cmd.partitionKey();
            return Lists.newArrayList(new DataRange(new Range<>(key, key), cmd.clusteringIndexFilter()));
        }
        else if (command instanceof PartitionRangeReadCommand)
        {
            PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;
            return Lists.newArrayList(cmd.dataRange());
        }
        else
        {
            throw new AssertionError("Unsupported read command type: " + command.getClass().getName());
        }
    }
}
