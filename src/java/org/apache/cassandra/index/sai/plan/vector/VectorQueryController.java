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

package org.apache.cassandra.index.sai.plan.vector;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
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
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.SSTableRowIdKeyRangeIterator;
import org.apache.cassandra.index.sai.disk.v1.vector.CheckpointingIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeConcatIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

public class VectorQueryController implements QueryController
{
    private static final Logger logger = LoggerFactory.getLogger(VectorQueryController.class);

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

    public VectorQueryController(ColumnFamilyStore cfs,
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

    /**
     * Note: merged range may contain subrange that no longer belongs to the local node after range movement.
     * It should only be used as an optimization to reduce search space. Use {@link #dataRanges()} instead to filter data.
     *
     * @return merged token range
     */
    public AbstractBounds<PartitionPosition> mergeRange()
    {
        return mergeRange;
    }

    /**
     * @return indexed {@code ColumnContext} if index is found; otherwise return non-indexed {@code ColumnContext}.
     */
    public IndexContext getContext(RowFilter.Expression expression)
    {
        StorageAttachedIndex index = getBestIndexFor(expression);

        if (index != null)
            return index.getIndexContext();

        return new IndexContext(cfs.getKeyspaceName(),
                                cfs.getTableName(),
                                cfs.metadata().partitionKeyType,
                                cfs.getPartitioner(),
                                cfs.getComparator(),
                                expression.column(),
                                IndexTarget.Type.VALUES,
                                null);
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
     * Build a {@link KeyRangeIterator.Builder} from the given list of expressions by applying given operation (OR/AND).
     * Building of such builder involves index search, results of which are persisted in the internal resources list
     *
     * @param expressions The expressions to build range iterator from (expressions with not results are ignored).
     *
     * @return range iterator builder based on given expressions and operation type.
     */
    public KeyRangeIterator.Builder getIndexQueryResults(Collection<Expression> expressions)
    {
        // FIXME having this at the expression level means that it only gets applied to Nodes at that level;
        // moving it to ORDER BY will allow us to apply it correctly for other Node sub-trees
        var annExpressionInHybridSearch = getAnnExpressionInHybridSearch(expressions);
        boolean isAnnHybridSearch = annExpressionInHybridSearch != null;
        if (isAnnHybridSearch)
            expressions = expressions.stream().filter(e -> e != annExpressionInHybridSearch).collect(Collectors.toList());

        // search memtable before referencing sstable indexes; otherwise we may miss newly flushed memtable index
        var iteratorsByMemtable = expressions.stream()
                                             .flatMap(expr -> expr.context.getMemtableIndexManager().iteratorsForSearch(queryContext, expr, mergeRange, getLimit()).stream())
                                             .collect(Collectors.groupingBy(pair -> pair.left, Collectors.mapping(pair -> pair.right, Collectors.toList())));

        var queryView = new VectorQueryViewBuilder(expressions, mergeRange).build();
        // in case of ANN query in hybrid search, we have to reference ANN sstable indexes separately because queryView doesn't include ANN sstable indexes
        var annQueryViewInHybridSearch = isAnnHybridSearch ? new VectorQueryViewBuilder(Collections.singleton(annExpressionInHybridSearch), mergeRange).build() : null;

        try
        {
            List<KeyRangeIterator<PrimaryKey>> sstableIntersections = queryView.view.entrySet()
                                                                                 .stream()
                                                                                 .map(e -> {
                                                                                     KeyRangeIterator<Long> it = createRowIdIterator(e.getValue(), isAnnHybridSearch);
                                                                                     if (isAnnHybridSearch)
                                                                                         return reorderAndLimitBySSTableRowIds(it, e.getKey(), annQueryViewInHybridSearch);
                                                                                     var pkFactory = e.getValue().iterator().next().index.getSSTableContext().primaryKeyMapFactory;
                                                                                     return convertToPrimaryKeyIterator(pkFactory, it);
                                                                                 })
                                                                                 .collect(Collectors.toList());

            List<KeyRangeIterator<PrimaryKey>> memtableIntersections = iteratorsByMemtable.entrySet()
                                                                                       .stream()
                                                                                       .map(e -> {
                                                                                           // we need to do all the intersections at the index level, or ordering won't work
                                                                                           KeyRangeIterator<PrimaryKey> it = buildIterator(e.getValue(), isAnnHybridSearch);
                                                                                           if (isAnnHybridSearch)
                                                                                               it = reorderAndLimitBy(it, e.getKey(), annExpressionInHybridSearch);
                                                                                           return it;
                                                                                       })
                                                                                       .collect(Collectors.toList());

            Iterable<KeyRangeIterator<PrimaryKey>> allIntersections = Iterables.concat(sstableIntersections, memtableIntersections);

            queryContext.sstablesHit += queryView.referencedIndexes
                                        .stream()
                                        .map(SSTableIndex::getSSTable).collect(Collectors.toSet()).size();
            queryContext.checkpoint();
            KeyRangeIterator<PrimaryKey> union = KeyRangeUnionIterator.build(Lists.newArrayList(allIntersections));

            CheckpointingIterator<PrimaryKey> checkpointingIterator = new CheckpointingIterator<>(union,
                                                                                                  queryView.referencedIndexes,
                                                                                                  annQueryViewInHybridSearch == null ? Collections.emptySet() : annQueryViewInHybridSearch.referencedIndexes,
                                                                                                  queryContext);

            return KeyRangeUnionIterator.<PrimaryKey>builder(1).add(checkpointingIterator);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryView.referencedIndexes.forEach(SSTableIndex::release);
            // if ANN sstable indexes are referenced separately, release them
            if (annQueryViewInHybridSearch != null)
                annQueryViewInHybridSearch.referencedIndexes.forEach(SSTableIndex::release);

            throw t;
        }
    }

    private KeyRangeIterator<PrimaryKey> convertToPrimaryKeyIterator(PrimaryKeyMap.Factory pkFactory, KeyRangeIterator<Long> sstableRowIdsIterator)
    {
        try
        {
            if (sstableRowIdsIterator.getCount() <= 0)
                return KeyRangeIterator.emptyKeys();

            PrimaryKeyMap primaryKeyMap = pkFactory.newPerSSTablePrimaryKeyMap();
            return SSTableRowIdKeyRangeIterator.create(primaryKeyMap, queryContext, sstableRowIdsIterator);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private KeyRangeIterator<PrimaryKey> reorderAndLimitBy(KeyRangeIterator<PrimaryKey> original, Memtable memtable, Expression expression)
    {
        return expression.context.getMemtableIndexManager().reorderMemtable(memtable, queryContext, original, expression);
    }

    private KeyRangeIterator<PrimaryKey> reorderAndLimitBySSTableRowIds(KeyRangeIterator<Long> original, SSTableReader sstable, VectorQueryViewBuilder.QueryView annQueryView)
    {
        List<VectorQueryViewBuilder.IndexExpression> annIndexExpressions = annQueryView.view.get(sstable);
        assert annIndexExpressions.size() == 1 : "only one index is expected in ANN expression, found " + annIndexExpressions.size() + " in " + annIndexExpressions;
        VectorQueryViewBuilder.IndexExpression annIndexExpression = annIndexExpressions.get(0);

        try
        {
            return annIndexExpression.index.limitToTopResults(queryContext, original, annIndexExpression.expression);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return ann expression if expressions have one ANN index and at least one non-ANN index
     */
    private Expression getAnnExpressionInHybridSearch(Collection<Expression> expressions)
    {
        if (expressions.size() < 2)
        {
            // if there is a single expression, just run search against it even if it's ANN
            return null;
        }

        return expressions.stream().filter(e -> e.getOp() == Expression.IndexOperator.ANN).findFirst().orElse(null);
    }

    /**
     * Create row id iterator from different indexes' on-disk searcher of the same sstable
     */
    private KeyRangeIterator<Long> createRowIdIterator(List<VectorQueryViewBuilder.IndexExpression> indexExpressions, boolean hasAnn)
    {
        var subIterators = indexExpressions
                           .stream()
                           .map(ie ->
                                    {
                                        try
                                        {
                                            List<KeyRangeIterator<Long>> iterators = ie.index.searchSSTableRowIds(ie.expression, mergeRange, queryContext);
                                            // concat the result from multiple segments for the same index
                                            return KeyRangeConcatIterator.build(iterators);
                                        }
                                        catch (Throwable ex)
                                        {
                                            if (!(ex instanceof QueryCancelledException))
                                                logger.debug(ie.index.getIndexContext().logMessage(String.format("Failed search on index %s, aborting query.", ie.index.getSSTable())), ex);
                                            throw Throwables.cleaned(ex);
                                        }
                                    }).collect(Collectors.toList());

        // we need to do all the intersections at the index level, or ordering won't work
        return buildIterator(subIterators, hasAnn);
    }

    private static <T extends Comparable<T>> KeyRangeIterator<T> buildIterator(List<KeyRangeIterator<T>> subIterators, boolean isAnnHybridSearch)
    {
        KeyRangeIterator.Builder<T> builder = null;
        if (isAnnHybridSearch)
            // if it's ANN with other indexes, intersect all available indexes so result will be correct top-k
            builder = KeyRangeIntersectionIterator.<T>builder(subIterators.size(), subIterators.size());
        else
            // Otherwise, pick 2 most selective indexes for better performance
            builder = KeyRangeIntersectionIterator.<T>builder(subIterators.size());

        return builder.add(subIterators).build();
    }

    private int getLimit()
    {
        return command.limits().count();
    }

    /**
     * Returns whether this query is selecting the {@link PrimaryKey}.
     * The query selects the key if any of the following statements is true:
     *  1. The query is not row-aware
     *  2. The table associated with the query is not using clustering keys
     *  3. The clustering index filter for the command wants the row.
     *
     *  Item 3 is important in paged queries where the {@link org.apache.cassandra.db.filter.ClusteringIndexSliceFilter} for
     *  subsequent paged queries may not select rows that are returned by the index
     *  search because that is initially partition based.
     *
     * @param key The {@link PrimaryKey} to be tested
     * @return true if the key is selected by the query
     */
    public boolean doesNotSelect(PrimaryKey key)
    {
        return key.kind() == PrimaryKey.Kind.WIDE && !command.clusteringIndexFilter(key.partitionKey()).selects(key.clustering());
    }

    private StorageAttachedIndex getBestIndexFor(RowFilter.Expression expression)
    {
        return cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class).stream().findFirst().orElse(null);
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
     * Used to release all resources and record metrics when query finishes.
     */
    public void finish()
    {
        if (tableQueryMetrics != null) tableQueryMetrics.record(queryContext);
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
