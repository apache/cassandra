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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MultiRangeReadCommand;
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
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableContextManager;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.CheckpointingIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SSTableRowIdKeyRangeIterator;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

public class QueryController
{
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;
    private final RowFilter.FilterElement filterOperation;
    private final IndexFeatureSet indexFeatureSet;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;

    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           RowFilter.FilterElement filterOperation,
                           IndexFeatureSet indexFeatureSet,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this.cfs = cfs;
        this.command = command;
        this.queryContext = queryContext;
        this.tableQueryMetrics = tableQueryMetrics;
        this.filterOperation = filterOperation;
        this.indexFeatureSet = indexFeatureSet;
        this.ranges = dataRanges(command);
        DataRange first = ranges.get(0);
        DataRange last = ranges.get(ranges.size() - 1);
        this.mergeRange = ranges.size() == 1 ? first.keyRange() : first.keyRange().withNewRight(last.keyRange().right);
    }

    public TableMetadata metadata()
    {
        return command.metadata();
    }

    RowFilter.FilterElement filterOperation()
    {
        return this.filterOperation;
    }

    /**
     * @return token ranges used in the read command
     */
    List<DataRange> dataRanges()
    {
        return ranges;
    }

    /**
     * Note: merged range may contain subrange that no longer belongs to the local node after range movement.
     * It should only be used as an optimization to reduce search space. Use {@link #dataRanges()} instead to filter data.
     *
     * @return merged token range
     */
    AbstractBounds<PartitionPosition> mergeRange()
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

        return new IndexContext(cfs.metadata().keyspace,
                                cfs.metadata().name,
                                cfs.metadata().partitionKeyType,
                                cfs.metadata().comparator,
                                expression.column(),
                                IndexTarget.Type.VALUES,
                                null,
                                cfs);
    }

    public UnfilteredRowIterator getPartition(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        try
        {
            SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(cfs.metadata(),
                                                                                     command.nowInSec(),
                                                                                     command.columnFilter(),
                                                                                     RowFilter.NONE,
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
     * Build a {@link RangeIterator.Builder} from the given list of expressions by applying given operation (OR/AND).
     * Building of such builder involves index search, results of which are persisted in the internal resources list
     *
     * @param op The operation type to coalesce expressions with.
     * @param expressions The expressions to build range iterator from (expressions with not results are ignored).
     *
     * @return range iterator builder based on given expressions and operation type.
     */
    public RangeIterator<PrimaryKey> getIndexes(Operation.OperationType op, Collection<Expression> expressions)
    {
        boolean defer = op == Operation.OperationType.OR || RangeIntersectionIterator.shouldDefer(expressions.size());

        // TODO this is super clunky, should the ANN expression move to ORDER BY? something like:
        // SELECT * FROM foo ORDER BY columnname ANN OF <?> LIMIT 10
        var annExpressionInHybridSearch = getAnnExpressionInHybridSearch(expressions);
        if (annExpressionInHybridSearch != null)
            expressions = expressions.stream().filter(e -> e != annExpressionInHybridSearch).collect(Collectors.toList());

        var queryView = new QueryViewBuilder(expressions, mergeRange).build();
        Map<Memtable, List<RangeIterator<PrimaryKey>>> iteratorsByMemtable = expressions
                                                                    .stream()
                                                                    .flatMap(expr -> {
                                                                        return expr.context.iteratorsForSearch(expr, mergeRange, getLimit()).stream();
                                                                    }).collect(Collectors.groupingBy(pair -> pair.left,
                                                                                                     Collectors.mapping(pair -> pair.right, Collectors.toList())));
        try
        {

            List<RangeIterator<PrimaryKey>> sstableIntersections = queryView.view.entrySet()
                                                                                 .stream()
                                                                                 .map(e -> {
                                                                                     RangeIterator<Long> it = createRowIdIterator(op, e.getValue(), defer, annExpressionInHybridSearch != null);
                                                                                     if (annExpressionInHybridSearch != null)
                                                                                         return reorderAndLimitBySSTableRowIds(it, e.getKey(), annExpressionInHybridSearch);
                                                                                     return convertToPrimaryKeyIterator(e.getKey(), it);
                                                                                 })
                                                                                 .collect(Collectors.toList());

            List<RangeIterator<PrimaryKey>> memtableIntersections = iteratorsByMemtable.entrySet()
                                                                                       .stream()
                                                                                       .map(e -> {
                                                                                           // we need to do all the intersections at the index level, or ordering won't work
                                                                                           RangeIterator<PrimaryKey> it = buildIterator(op, e.getValue(), annExpressionInHybridSearch != null);
                                                                                           if (annExpressionInHybridSearch != null)
                                                                                               it = reorderAndLimitBy(it, e.getKey(), annExpressionInHybridSearch);
                                                                                           return it;
                                                                                       })
                                                                                       .collect(Collectors.toList());

            Iterable<RangeIterator<PrimaryKey>> allIntersections = Iterables.concat(sstableIntersections, memtableIntersections);

            queryContext.sstablesHit += queryView.referencedIndexes
                                        .stream()
                                        .map(SSTableIndex::getSSTable).collect(Collectors.toSet()).size();
            queryContext.checkpoint();
            RangeIterator<PrimaryKey> union = RangeUnionIterator.build(allIntersections);
            return new CheckpointingIterator<>(union, queryView.referencedIndexes, queryContext);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryView.referencedIndexes.forEach(SSTableIndex::release);
            throw t;
        }
    }

    private RangeIterator<PrimaryKey> convertToPrimaryKeyIterator(SSTableReader sstable, RangeIterator<Long> sstableRowIdsIterator)
    {
        try
        {
            if (sstableRowIdsIterator.getCount() <= 0)
                return RangeIterator.emptyKeys();

            SSTableContextManager contextManager = StorageAttachedIndexGroup.getIndexGroup(cfs).sstableContextManager();
            SSTableContext sstableContext = contextManager.getContext(sstable);
            PrimaryKeyMap primaryKeyMap = sstableContext.primaryKeyMapFactory.newPerSSTablePrimaryKeyMap();
            return SSTableRowIdKeyRangeIterator.create(primaryKeyMap, queryContext, sstableRowIdsIterator);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private RangeIterator<PrimaryKey> reorderAndLimitBy(RangeIterator<PrimaryKey> original, Memtable memtable, Expression expression)
    {
        return expression.context.reorderMemtable(memtable, queryContext, original, expression, getLimit());
    }

    private RangeIterator<PrimaryKey> reorderAndLimitBySSTableRowIds(RangeIterator<Long> original, SSTableReader sstable, Expression expression)
    {
        var index = expression.context.getView().getIndexes()
                                      .stream().filter(i -> i.getSSTable() == sstable).findFirst().orElseThrow();
        var sstContext = queryContext.getSSTableQueryContext(index.getSSTable());
        try
        {
            return index.reorderOneComponent(sstContext, original, expression, getLimit());
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
        var L = expressions.stream().filter(e -> e.operation == Expression.Op.ANN).collect(Collectors.toList());
        if (L.size() > 1) {
            // FIXME move this to the parser
            throw new IllegalArgumentException("Only one ANN expression is allowed");
        }
        return L.size() == 1 ? L.get(0) : null;
    }

    private RangeIterator<Long> createRowIdIterator(Operation.OperationType op, List<QueryViewBuilder.IndexExpression> indexExpressions, boolean defer, boolean hasAnn)
    {
        var subIterators = indexExpressions
                           .stream()
                           .map(ie ->
                                    {
                                        try
                                        {
                                            var sstContext = queryContext.getSSTableQueryContext(ie.index.getSSTable());
                                            List<RangeIterator<Long>> iterators = ie.index.searchSSTableRowIds(ie.expression, mergeRange, sstContext, defer, getLimit());
                                            // union the result from multiple segments for the same index
                                            return RangeUnionIterator.build(iterators);
                                        }
                                        catch (Throwable ex)
                                        {
                                            if (!(ex instanceof AbortedOperationException))
                                                logger.debug(ie.index.getIndexContext().logMessage(String.format("Failed search on index %s, aborting query.", ie.index.getSSTable())), ex);
                                            throw Throwables.cleaned(ex);
                                        }
                                    }).collect(Collectors.toList());

        // we need to do all the intersections at the index level, or ordering won't work
        return buildIterator(op, subIterators, hasAnn);
    }

    private static <T extends Comparable<T>> RangeIterator<T> buildIterator(Operation.OperationType op, List<RangeIterator<T>> subIterators, boolean hasAnn)
    {
        RangeIterator.Builder<T> builder = null;
        if (op == Operation.OperationType.OR)
            builder = RangeUnionIterator.<T>builder(subIterators.size());
        else if (hasAnn)
            // if there is ANN, intersect all available indexes so result will be correct top-k
            builder = RangeIntersectionIterator.<T>builder(subIterators.size());
        else
            // Otherwise, pick 2 most selective indexes for better performance
            builder = RangeIntersectionIterator.<T>builder();

        return builder.add(subIterators).build();
    }

    private int getLimit()
    {
        return command.limits().count();
    }

    public IndexFeatureSet indexFeatureSet()
    {
        return indexFeatureSet;
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
    public boolean selects(PrimaryKey key)
    {
        return !indexFeatureSet.isRowAware() ||
               key.hasEmptyClustering() ||
               command.clusteringIndexFilter(key.partitionKey()).selects(key.clustering());
    }

    private StorageAttachedIndex getBestIndexFor(RowFilter.Expression expression)
    {
        return cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class).orElse(null);
    }

    // Note: This method assumes that the selects method has already been called for the
    // key to avoid having to (potentially) call selects twice
    private ClusteringIndexFilter makeFilter(PrimaryKey key)
    {
        ClusteringIndexFilter clusteringIndexFilter = command.clusteringIndexFilter(key.partitionKey());

        if (!indexFeatureSet.isRowAware() || key.hasEmptyClustering())
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
        else if (command instanceof MultiRangeReadCommand)
        {
            MultiRangeReadCommand cmd = (MultiRangeReadCommand) command;
            return cmd.ranges();
        }
        else
        {
            throw new AssertionError("Unsupported read command type: " + command.getClass().getName());
        }
    }
}
