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

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.v1.vector.PrimaryKeyWithScore;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.FBUtilities;

public class StorageAttachedIndexSearcher implements Index.Searcher
{
    private static final int PARTITION_ROW_BATCH_SIZE = 100;

    private final ReadCommand command;
    private final QueryController queryController;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;

    private static final FastThreadLocal<List<PrimaryKey>> nextKeys = new FastThreadLocal<>()
    {
        @Override
        protected List<PrimaryKey> initialValue()
        {
            return new ArrayList<>(PARTITION_ROW_BATCH_SIZE);
        }
    };

    public StorageAttachedIndexSearcher(ColumnFamilyStore cfs,
                                        TableQueryMetrics tableQueryMetrics,
                                        ReadCommand command,
                                        RowFilter indexFilter,
                                        long executionQuotaMs)
    {
        this.command = command;
        this.queryContext = new QueryContext(command, executionQuotaMs);
        this.queryController = new QueryController(cfs, command, indexFilter, queryContext);
        this.tableQueryMetrics = tableQueryMetrics;
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    @Override
    public PartitionIterator filterReplicaFilteringProtection(PartitionIterator fullResponse)
    {
        for (RowFilter.Expression expression : queryController.indexFilter())
        {
            if (queryController.hasAnalyzer(expression))
                return applyIndexFilter(fullResponse, Operation.buildFilter(queryController, true), queryContext);
        }

        // if no analyzer does transformation
        return Index.Searcher.super.filterReplicaFilteringProtection(fullResponse);
    }

    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) throws RequestTimeoutException
    {
        if (!command.isTopK())
        {
            return new ResultRetriever(executionController);
        }
        else
        {
            // Need a consistent view of the memtables/sstables and their associated index, so we get the view now
            // and propagate it as needed.
            try (QueryViewBuilder.QueryView queryView = buildAnnQueryView())
            {
                queryController.maybeTriggerGuardrails(queryView);
                ScoreOrderedResultRetriever result = new ScoreOrderedResultRetriever(executionController, queryView);
                // takeTopKThenSortByPrimaryKey eagerly consumes up to k rows from the result because search must
                // produce an iterator in PrimaryKey order.
                return (UnfilteredPartitionIterator) new VectorTopKProcessor(command).takeTopKThenSortByPrimaryKey(result);
            }
        }
    }

    private QueryViewBuilder.QueryView buildAnnQueryView()
    {
        RowFilter.Expression annExpression = null;
        for (RowFilter.Expression expression : queryController.indexFilter().getExpressions())
        {
            if (expression.operator() == Operator.ANN)
            {
                if (annExpression != null)
                    throw new IllegalStateException("Multiple ANN expressions in a single query are not supported");
                annExpression = expression;
            }
        }
        if (annExpression == null)
            throw new IllegalStateException("No ANN expression found in query");

        StorageAttachedIndex index = queryController.indexFor(annExpression);
        Expression planExpression = Expression.create(index).add(Operator.ANN, annExpression.getIndexValue().duplicate());
        return new QueryViewBuilder(Collections.singleton(planExpression), queryController.mergeRange()).build();
    }

    private abstract class AbstractRetreiver extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        final FilterTree filterTree;
        final ReadExecutionController executionController;

        AbstractRetreiver(ReadExecutionController executionController)
        {
            this.executionController = executionController;
            this.filterTree = Operation.buildFilter(queryController, queryController.usesStrictFiltering());
        }

        @Override
        public TableMetadata metadata()
        {
            return queryController.metadata();
        }

    }

    private class ResultRetriever extends AbstractRetreiver
    {
        private final PrimaryKey firstPrimaryKey;
        private final PrimaryKey lastPrimaryKey;
        private final Iterator<DataRange> keyRanges;
        private final DataRange firstDataRange;
        private AbstractBounds<PartitionPosition> currentKeyRange;

        private final KeyRangeIterator resultKeyIterator;
        private final PrimaryKey.Factory keyFactory;
        private final int partitionRowBatchSize;

        private PrimaryKey lastKey;

        private ResultRetriever(ReadExecutionController executionController)
        {
            super(executionController);
            this.keyRanges = queryController.dataRanges().iterator();
            this.firstDataRange = keyRanges.next();
            this.currentKeyRange = firstDataRange.keyRange();
            this.resultKeyIterator = Operation.buildIterator(queryController);
            this.keyFactory = queryController.primaryKeyFactory();
            this.firstPrimaryKey = queryController.firstPrimaryKeyInRange();
            this.lastPrimaryKey = queryController.lastPrimaryKeyInRange();

            // Ensure we don't fetch larger batches than the provided LIMIT to avoid fetching keys we won't use: 
            this.partitionRowBatchSize = Math.min(PARTITION_ROW_BATCH_SIZE, command.limits().count());
        }

        @Override
        public UnfilteredRowIterator computeNext()
        {
            if (resultKeyIterator == null)
                return endOfData();

            // If being called for the first time, skip to the beginning of the range.
            // We can't put this code in the constructor because it may throw and the caller
            // may not be prepared for that.
            if (lastKey == null)
            {
                PrimaryKey skipTarget = firstPrimaryKey;
                ClusteringComparator comparator = command.metadata().comparator;

                // If there are no clusterings, the first data range selects an entire partitions, or we have static
                // expressions, don't bother trying to skip forward within the partition.
                if (comparator.size() > 0 && !firstDataRange.selectsAllPartition() && !command.rowFilter().hasStaticExpression())
                {
                    // Only attempt to skip if the first data range covers a single partition.
                    if (currentKeyRange.left.equals(currentKeyRange.right) && currentKeyRange.left instanceof DecoratedKey)
                    {
                        DecoratedKey decoratedKey = (DecoratedKey) currentKeyRange.left;
                        ClusteringIndexFilter filter = firstDataRange.clusteringIndexFilter(decoratedKey);

                        if (filter instanceof ClusteringIndexSliceFilter)
                        {
                            Slices slices = ((ClusteringIndexSliceFilter) filter).requestedSlices();

                            if (!slices.isEmpty())
                            {
                                ClusteringBound<?> startBound = slices.get(0).start();

                                if (!startBound.isEmpty())
                                {
                                    ByteBuffer[] rawValues = startBound.getBufferArray();

                                    if (rawValues.length == comparator.size())
                                        skipTarget = keyFactory.create(decoratedKey, Clustering.make(rawValues));
                                }
                            }
                        }
                        else if (filter instanceof ClusteringIndexNamesFilter)
                        {
                            ClusteringIndexNamesFilter namesFilter = (ClusteringIndexNamesFilter) filter;

                            if (!namesFilter.requestedRows().isEmpty())
                            {
                                Clustering<?> skipClustering = namesFilter.requestedRows().iterator().next();
                                skipTarget = keyFactory.create(decoratedKey, skipClustering);
                            }
                        }
                    }
                }

                resultKeyIterator.skipTo(skipTarget);
            }

            // Theoretically we wouldn't need this if the caller of computeNext always ran the
            // returned iterators to the completion. Unfortunately, we have no control over the caller behavior here.
            // Hence, we skip to the next partition in order to comply to the unwritten partition iterator contract
            // saying this iterator must not return the same partition twice.
            skipToNextPartition();

            UnfilteredRowIterator iterator = nextRowIterator(this::nextSelectedKeysInRange);
            return iterator != null ? iteratePartition(iterator) : endOfData();
        }

        /**
         * Tries to obtain a row iterator for the supplied keys by repeatedly calling
         * {@link ResultRetriever#queryStorageAndFilter} until it gives a non-null result.
         * The keysSupplier should return the next batch of keys with every call to get()
         * and null when there are no more keys to try.
         *
         * @return an iterator or null if all keys were tried with no success
         */
        private @Nullable UnfilteredRowIterator nextRowIterator(@Nonnull Supplier<List<PrimaryKey>> keysSupplier)
        {
            UnfilteredRowIterator iterator = null;
            while (iterator == null)
            {
                List<PrimaryKey> keys = keysSupplier.get();
                if (keys.isEmpty())
                    return null;
                iterator = queryStorageAndFilter(keys);
            }
            return iterator;
        }

        /**
         * Retrieves the next batch of primary keys (i.e. up to {@link #partitionRowBatchSize} of them) that are
         * contained by one of the query key ranges and selected by the {@link QueryController}. If the next key falls
         * out of the current key range, it skips to the next key range, and so on. If no more keys accepted by
         * the controller are available, and empty list is returned.
         *
         * @return a list of up to {@link #partitionRowBatchSize} primary keys
         */
        private List<PrimaryKey> nextSelectedKeysInRange()
        {
            List<PrimaryKey> threadLocalNextKeys = nextKeys.get();
            threadLocalNextKeys.clear();
            PrimaryKey firstKey;

            do
            {
                firstKey = nextKeyInRange();

                if (firstKey == null)
                    return Collections.emptyList();
            }
            while (queryController.doesNotSelect(firstKey) || firstKey.equals(lastKey, false));

            lastKey = firstKey;
            threadLocalNextKeys.add(firstKey);
            fillNextSelectedKeysInPartition(firstKey.partitionKey(), threadLocalNextKeys);
            return threadLocalNextKeys;
        }

        /**
         * Retrieves the next batch of primary keys (i.e. up to {@link #partitionRowBatchSize} of them) that belong to 
         * the given partition and are selected by the query controller, advancing the underlying iterator only while
         * the next key belongs to that partition.
         *
         * @return a list of up to {@link #partitionRowBatchSize} primary keys within the given partition
         */
        private List<PrimaryKey> nextSelectedKeysInPartition(DecoratedKey partitionKey)
        {
            List<PrimaryKey> threadLocalNextKeys = nextKeys.get();
            threadLocalNextKeys.clear();
            fillNextSelectedKeysInPartition(partitionKey, threadLocalNextKeys);
            return threadLocalNextKeys;
        }

        /**
         * Returns the next available key contained by one of the keyRanges.
         * If the next key falls out of the current key range, it skips to the next key range, and so on.
         * If no more keys or no more ranges are available, returns null.
         */
        private @Nullable PrimaryKey nextKeyInRange()
        {
            PrimaryKey key = nextKey();

            while (key != null && !(currentKeyRange.contains(key.partitionKey())))
            {
                if (!currentKeyRange.right.isMinimum() && currentKeyRange.right.compareTo(key.partitionKey()) <= 0)
                {
                    // currentKeyRange before the currentKey so need to move currentKeyRange forward
                    currentKeyRange = nextKeyRange();
                    if (currentKeyRange == null)
                        return null;
                }
                else
                {
                    // key either before the current range, so let's move the key forward
                    skipTo(currentKeyRange.left.getToken());
                    key = nextKey();
                }
            }
            return key;
        }

        private void fillNextSelectedKeysInPartition(DecoratedKey partitionKey, List<PrimaryKey> nextPrimaryKeys)
        {
            while (resultKeyIterator.hasNext()
                   && resultKeyIterator.peek().partitionKey().equals(partitionKey)
                   && nextPrimaryKeys.size() < partitionRowBatchSize)
            {
                PrimaryKey key = nextKey();

                if (key == null)
                    break;

                if (queryController.doesNotSelect(key) || key.equals(lastKey, false))
                    continue;

                nextPrimaryKeys.add(key);
                lastKey = key;
            }
        }

        /**
         * Gets the next key from the underlying operation.
         * Returns null if there are no more keys <= lastPrimaryKey.
         */
        private @Nullable PrimaryKey nextKey()
        {
            if (!resultKeyIterator.hasNext())
                return null;
            PrimaryKey key = resultKeyIterator.next();
            return isWithinUpperBound(key) ? key : null;
        }

        /**
         * Returns true if the key is not greater than lastPrimaryKey
         */
        private boolean isWithinUpperBound(PrimaryKey key)
        {
            return lastPrimaryKey.token().isMinimum() || lastPrimaryKey.compareTo(key, false) >= 0;
        }

        /**
         * Gets the next key range from the underlying range iterator.
         */
        private @Nullable AbstractBounds<PartitionPosition> nextKeyRange()
        {
            return keyRanges.hasNext() ? keyRanges.next().keyRange() : null;
        }

        /**
         * Convenience function to skip to a given token.
         */
        private void skipTo(@Nonnull Token token)
        {
            resultKeyIterator.skipTo(keyFactory.create(token));
        }

        /**
         * Skips to the key that belongs to a different partition than the last key we fetched.
         */
        private void skipToNextPartition()
        {
            if (lastKey == null)
                return;
            DecoratedKey lastPartitionKey = lastKey.partitionKey();
            while (resultKeyIterator.hasNext() && resultKeyIterator.peek().partitionKey().equals(lastPartitionKey))
                resultKeyIterator.next();
        }


        /**
         * Returns an iterator over the rows in the partition associated with the given iterator.
         * Initially, it retrieves the rows from the given iterator until it runs out of data.
         * Then it iterates the remaining primary keys obtained from the index in batches until the end of the 
         * partition, lazily constructing an itertor for each batch. Only one row iterator is open at a time.
         * <p>
         * The rows are retrieved in the order of primary keys provided by the underlying index.
         * The iterator is complete when the next key to be fetched belongs to different partition
         * (but the iterator does not consume that key).
         *
         * @param startIter an iterator positioned at the first row in the partition that we want to return
         */
        private @Nonnull UnfilteredRowIterator iteratePartition(@Nonnull UnfilteredRowIterator startIter)
        {
            return new AbstractUnfilteredRowIterator(startIter.metadata(),
                                                     startIter.partitionKey(),
                                                     startIter.partitionLevelDeletion(),
                                                     startIter.columns(),
                                                     startIter.staticRow(),
                                                     startIter.isReverseOrder(),
                                                     startIter.stats())
            {
                private UnfilteredRowIterator currentIter = startIter;
                private final DecoratedKey partitionKey = startIter.partitionKey();

                @Override
                protected Unfiltered computeNext()
                {
                    while (!currentIter.hasNext())
                    {
                        currentIter.close();
                        currentIter = nextRowIterator(() -> nextSelectedKeysInPartition(partitionKey));
                        if (currentIter == null)
                            return endOfData();
                    }
                    return currentIter.next();
                }

                @Override
                public void close()
                {
                    FileUtils.closeQuietly(currentIter);
                    super.close();
                }
            };
        }

        private UnfilteredRowIterator queryStorageAndFilter(List<PrimaryKey> keys)
        {
            long startTimeNanos = Clock.Global.nanoTime();

            try (UnfilteredRowIterator partition = queryController.queryStorage(keys, executionController))
            {
                queryContext.partitionsRead++;
                queryContext.checkpoint();

                List<Row> filtered = filterPartition(partition, filterTree, queryContext);

                // Note that we record the duration of the read after post-filtering, which actually
                // materializes the rows from disk.
                tableQueryMetrics.postFilteringReadLatency.update(Clock.Global.nanoTime() - startTimeNanos, TimeUnit.NANOSECONDS);

                return filtered != null
                       ? new SinglePartitionIterator(partition, partition.staticRow(), filtered.iterator())
                       : null;
            }
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(resultKeyIterator);
            if (tableQueryMetrics != null) tableQueryMetrics.record(queryContext);
        }
    }

    private static List<Row> filterPartition(UnfilteredRowIterator partition, FilterTree tree, QueryContext context)
    {
        Row staticRow = partition.staticRow();
        DecoratedKey partitionKey = partition.partitionKey();
        List<Row> matches = new ArrayList<>();
        boolean hasMatch = false;

        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();

            if (unfiltered.isRow())
            {
                context.rowsFiltered++;

                if (tree.isSatisfiedBy(partitionKey, (Row) unfiltered, staticRow))
                {
                    matches.add((Row) unfiltered);
                    hasMatch = true;
                }
            }
        }

        // We may not have any non-static row data to filter...
        if (!hasMatch)
        {
            context.rowsFiltered++;

            if (tree.isSatisfiedBy(partitionKey, staticRow, staticRow))
            {
                hasMatch = true;
            }
        }

        if (!hasMatch)
        {
            // If there are no matches, return an empty partition. If reconciliation is required at the
            // coordinator, replica filtering protection may make a second round trip to complete its view
            // of the partition.
            return null;
        }

        // Return all matches found
        return matches;
    }

    private static class SinglePartitionIterator extends AbstractUnfilteredRowIterator
    {
        private final Iterator<Row> rows;

        public SinglePartitionIterator(UnfilteredRowIterator partition, Row staticRow, Iterator<Row> rows)
        {
            super(partition.metadata(),
                  partition.partitionKey(),
                  partition.partitionLevelDeletion(),
                  partition.columns(),
                  staticRow,
                  partition.isReverseOrder(),
                  partition.stats());

            this.rows = rows;
        }

        @Override
        protected Unfiltered computeNext()
        {
            return rows.hasNext() ? rows.next() : endOfData();
        }
    }

    /**
     * A result retriever that consumes an iterator primary keys sorted by some score, materializes the row for each
     * primary key (currently, each primary key is required to be fully qualified and should only point to one row),
     * apply the filter tree to the row to test that the real row satisfies the WHERE clause, and finally tests
     * that the row is valid for the ORDER BY clause. The class performs some optimizations to avoid materializing
     * rows unnecessarily. See the class for more details.
     * <p>
     * The resulting {@link UnfilteredRowIterator} objects are not guaranteed to be in any particular order. It is
     * the responsibility of the caller to sort the results if necessary.
     */
    public class ScoreOrderedResultRetriever extends AbstractRetreiver
    {
        private final ColumnFamilyStore.ViewFragment view;
        private final List<AbstractBounds<PartitionPosition>> keyRanges;
        private final boolean coversFullRing;
        private final CloseableIterator<PrimaryKeyWithScore> scoredPrimaryKeyIterator;

        private final boolean isVectorColumnStatic;
        private final HashSet<PrimaryKey> processedKeys;
        private final Queue<UnfilteredRowIterator> pendingRows;

        // The limit requested by the query. We cannot load more than softLimit rows in bulk because we only want
        // to fetch the topk rows where k is the limit. However, we allow the iterator to fetch more rows than the
        // soft limit to avoid confusing behavior. When the softLimit is reached, the iterator will fetch one row
        // at a time.
        private final int softLimit;
        private int returnedRowCount = 0;

        private ScoreOrderedResultRetriever(ReadExecutionController executionController,
                                            QueryViewBuilder.QueryView queryView)
        {
            super(executionController);
            assert queryView.view.size() == 1;
            QueryViewBuilder.QueryExpressionView queryExpressionView = queryView.view.stream().findFirst().get();
            this.view = queryExpressionView.computeViewFragment();
            this.keyRanges = queryController.dataRanges().stream().map(DataRange::keyRange).collect(Collectors.toList());
            this.coversFullRing = keyRanges.size() == 1 && RangeUtil.coversFullRing(keyRanges.get(0));

            this.scoredPrimaryKeyIterator = Operation.buildIteratorForOrder(queryController, queryExpressionView);

            this.isVectorColumnStatic = queryExpressionView.expression.getIndexTermType().columnMetadata().isStatic();
            this.softLimit = command.limits().count();
            this.processedKeys = new HashSet<>(softLimit);
            this.pendingRows = new ArrayDeque<>(softLimit);
        }

        @Override
        public UnfilteredRowIterator computeNext()
        {
            if (pendingRows.isEmpty())
                fillPendingRows();
            returnedRowCount++;
            // Because we know ordered keys are fully qualified, we do not iterate partitions
            return !pendingRows.isEmpty() ? pendingRows.poll() : endOfData();
        }

        /**
         * Fills the pendingRows queue to generate a queue of row iterators for the supplied keys by repeatedly calling
         * {@link #readAndValidatePartition} until it gives enough non-null results.
         */
        private void fillPendingRows()
        {
            // Group PKs by source sstable/memtable
            Map<PrimaryKey, List<PrimaryKeyWithScore>> groupedKeys = new HashMap<>();
            // We always want to get at least 1. When the vector column is static, we cannot batch because we need to
            // retain the score ordering a bit longer.
            int rowsToRetrieve = isVectorColumnStatic ? 1 : Math.max(1, softLimit - returnedRowCount);
            // We want to get the first unique `rowsToRetrieve` keys to materialize
            // Don't pass the priority queue here because it is more efficient to add keys in bulk
            fillKeys(groupedKeys, rowsToRetrieve, null);
            // Sort the primary keys by PrK order, just in case that helps with cache and disk efficiency
            PriorityQueue<PrimaryKey> primaryKeyPriorityQueue = new PriorityQueue<>(groupedKeys.keySet());

            // drain groupedKeys into pendingRows
            while (!groupedKeys.isEmpty())
            {
                PrimaryKey pk = primaryKeyPriorityQueue.poll();
                List<PrimaryKeyWithScore> sourceKeys = groupedKeys.remove(pk);
                UnfilteredRowIterator partitionIterator = readAndValidatePartition(pk, sourceKeys);
                if (partitionIterator != null)
                    pendingRows.add(partitionIterator);
                else
                    // The current primaryKey did not produce a partition iterator. We know the caller will need
                    // `rowsToRetrieve` rows, so we get the next unique key and add it to the queue.
                    fillKeys(groupedKeys, 1, primaryKeyPriorityQueue);
            }
        }

        /**
         * Fills the `groupedKeys` Map with the next `count` unique primary keys that are in the keys produced by calling
         * {@link #nextSelectedKeyInRange()}. We map PrimaryKey to a list of PrimaryKeyWithScore because the same
         * primary key can be in the result set multiple times, but with different source tables.
         * @param groupedKeys the map to fill
         * @param count the number of unique PrimaryKeys to consume from the iterator
         * @param primaryKeyPriorityQueue the priority queue to add new keys to. If the queue is null, we do not add
         *                                keys to the queue.
         */
        private void fillKeys(Map<PrimaryKey, List<PrimaryKeyWithScore>> groupedKeys, int count, PriorityQueue<PrimaryKey> primaryKeyPriorityQueue)
        {
            int initialSize = groupedKeys.size();
            while (groupedKeys.size() - initialSize < count)
            {
                PrimaryKeyWithScore primaryKeyWithScore = nextSelectedKeyInRange();
                if (primaryKeyWithScore == null)
                    return;
                PrimaryKey nextPrimaryKey = primaryKeyWithScore.primaryKey();
                List<PrimaryKeyWithScore> accumulator = groupedKeys.computeIfAbsent(nextPrimaryKey, k -> new ArrayList<>());
                if (primaryKeyPriorityQueue != null && accumulator.isEmpty())
                    primaryKeyPriorityQueue.add(nextPrimaryKey);
                accumulator.add(primaryKeyWithScore);
            }
        }

        /**
         * Determine if the key is in one of the queried key ranges. We do not iterate through results in
         * {@link PrimaryKey} order, so we have to check each range.
         * @param key the key to test
         * @return true if the key is in one of the queried key ranges
         */
        private boolean isInRange(DecoratedKey key)
        {
            if (coversFullRing)
                return true;

            for (AbstractBounds<PartitionPosition> range : keyRanges)
                if (range.contains(key))
                    return true;
            return false;
        }

        /**
         * Returns the next available key contained by one of the keyRanges and selected by the queryController.
         * If the next key falls out of the current key range, it skips to the next key range, and so on.
         * If no more keys acceptd by the controller are available, returns null.
         */
        private @Nullable PrimaryKeyWithScore nextSelectedKeyInRange()
        {
            while (scoredPrimaryKeyIterator.hasNext())
            {
                PrimaryKeyWithScore key = scoredPrimaryKeyIterator.next();
                if (isInRange(key.primaryKey().partitionKey()) && !queryController.doesNotSelect(key.primaryKey()))
                    return key;
            }
            return null;
        }

        /**
         * Reads and validates a partition for a given primary key against its sources.
         * <p>
         * @param pk The primary key of the partition to read and validate
         * @param sourceKeys A list of PrimaryKeyWithScore objects associated with the primary key.
         *                   Multiple sort keys can exist for the same primary key when data comes from different
         *                   sstables or memtables.
         *
         * @return An UnfilteredRowIterator containing the validated partition data, or null if:
         *         - The key has already been processed
         *         - The partition does not pass index filters
         *         - The partition contains no valid rows
         *         - The row data does not match the index metadata for any of the provided primary keys
         */
        public UnfilteredRowIterator readAndValidatePartition(PrimaryKey pk, List<PrimaryKeyWithScore> sourceKeys)
        {
            // If we've already processed the key, we can skip it. Because the score ordered iterator does not
            // deduplicate rows, we could see dupes if a row is in the ordering index multiple times. This happens
            // in the case of dupes and of overwrites.
            if (processedKeys.contains(pk))
                return null;

            try (UnfilteredRowIterator partition = queryController.queryStorage(pk, view, executionController))
            {
                queryContext.partitionsRead++;
                queryContext.checkpoint();

                List<Row> clusters = filterPartition(partition, filterTree, queryContext);

                if (clusters == null)
                {
                    // Key counts as processed because the materialized row didn't satisfy the filter logic
                    processedKeys.add(pk);
                    return null;
                }

                Row staticRow = partition.staticRow();
                long now = FBUtilities.nowInSeconds();

                // If the pk is static, then we must check that the static row satisfies the source key's validity check.
                // Otherwise, we need to make sure that we have one row in the cluster result and then we use that
                // for checking validity.
                Row representativeRow;
                if (pk.kind() == PrimaryKey.Kind.STATIC)
                {
                    representativeRow = staticRow;
                }
                else
                {
                    if (clusters.isEmpty())
                    {
                        // Key counts as processed because the materialized row didn't satisfy the filter logic
                        processedKeys.add(pk);
                        return null;
                    }
                    representativeRow = clusters.get(0);
                    assert clusters.size() == 1 : "Expect 1 result row, but got: " + clusters.size();
                }

                // Each of sourceKeys are equal with respect to primary key equality, but they have different source tables.
                // As long as one is valid, we consider the row valid.
                for (PrimaryKeyWithScore sourceKey : sourceKeys)
                {
                    assert sourceKey.primaryKey().kind() == pk.kind();
                    if (sourceKey.isIndexDataValid(representativeRow, now))
                    {
                        processedKeys.add(pk);
                        return new SinglePartitionIterator(partition, staticRow, clusters.iterator());
                    }
                }
                // Key does not count as processed because the only thing that "failed" is the validity check on the
                // grouped source keys, and it is possible that the score ordered iterator has the same key in the
                // iterator lower. We only get here when a vector's value is updated to a more distant vector, so
                // the old value ranks high in the iterator, but isn't the current value for the materialized row.
                return null;
            }
        }

        public void close()
        {
            FileUtils.closeQuietly(scoredPrimaryKeyIterator);
        }
    }

    /**
     * Used by {@link StorageAttachedIndexSearcher#filterReplicaFilteringProtection} to filter rows for columns that
     * have transformations so won't get handled correctly by the row filter.
     */
    private static PartitionIterator applyIndexFilter(PartitionIterator response, FilterTree tree, QueryContext context)
    {
        return new PartitionIterator()
        {
            @Override
            public void close()
            {
                response.close();
            }

            @Override
            public boolean hasNext()
            {
                return response.hasNext();
            }

            @Override
            public RowIterator next()
            {
                RowIterator delegate = response.next();
                Row staticRow = delegate.staticRow();

                // If we only restrict static columns, and we pass the filter, simply pass through the delegate, as all
                // non-static rows are matches. If we fail on the filter, no rows are matches, so return nothing.
                if (!tree.restrictsNonStaticRow())
                    return tree.isSatisfiedBy(delegate.partitionKey(), staticRow, staticRow) ? delegate : null;

                return new RowIterator()
                {
                    Row next;

                    @Override
                    public TableMetadata metadata()
                    {
                        return delegate.metadata();
                    }

                    @Override
                    public boolean isReverseOrder()
                    {
                        return delegate.isReverseOrder();
                    }

                    @Override
                    public RegularAndStaticColumns columns()
                    {
                        return delegate.columns();
                    }

                    @Override
                    public DecoratedKey partitionKey()
                    {
                        return delegate.partitionKey();
                    }

                    @Override
                    public Row staticRow()
                    {
                        return staticRow;
                    }

                    @Override
                    public void close()
                    {
                        delegate.close();
                    }

                    private Row computeNext()
                    {
                        while (delegate.hasNext())
                        {
                            Row row = delegate.next();
                            context.rowsFiltered++;
                            if (tree.isSatisfiedBy(delegate.partitionKey(), row, staticRow))
                                return row;
                        }
                        return null;
                    }

                    private Row loadNext()
                    {
                        if (next == null)
                            next = computeNext();
                        return next;
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return loadNext() != null;
                    }

                    @Override
                    public Row next()
                    {
                        Row result = loadNext();
                        next = null;

                        if (result == null)
                            throw new NoSuchElementException();

                        return result;
                    }
                };
            }
        };
    }
}
