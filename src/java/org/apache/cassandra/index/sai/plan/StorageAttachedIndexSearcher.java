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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.ParallelCommandProcessor;
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
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.Pair;

public class StorageAttachedIndexSearcher implements Index.Searcher
{
    private static final Logger logger = LoggerFactory.getLogger(StorageAttachedIndexSearcher.class);

    private final ReadCommand command;
    private final QueryController controller;
    private final QueryContext queryContext;
    private final ColumnFamilyStore cfs;

    public StorageAttachedIndexSearcher(ColumnFamilyStore cfs,
                                        TableQueryMetrics tableQueryMetrics,
                                        ReadCommand command,
                                        RowFilter.FilterElement filterOperation,
                                        IndexFeatureSet indexFeatureSet,
                                        long executionQuotaMs)
    {
        this.command = command;
        this.cfs = cfs;
        this.queryContext = new QueryContext(executionQuotaMs);
        this.controller = new QueryController(cfs, command, filterOperation, indexFeatureSet, queryContext, tableQueryMetrics);
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    @Override
    public PartitionIterator filterReplicaFilteringProtection(PartitionIterator fullResponse)
    {
        for (RowFilter.Expression expression : controller.filterOperation())
        {
            AbstractAnalyzer analyzer = controller.getContext(expression).getAnalyzerFactory().create();
            try
            {
                if (analyzer.transformValue())
                    return applyIndexFilter(fullResponse, analyzeFilter(), queryContext);
            }
            finally
            {
                analyzer.end();
            }
        }

        // if no analyzer does transformation
        return Index.Searcher.super.filterReplicaFilteringProtection(fullResponse);
    }

    @Override
    public UnfilteredPartitionIterator search(ReadExecutionController executionController) throws RequestTimeoutException
    {
        // VSTODO see about switching to use an order op instead of ann
        Supplier<ResultRetriever> queryIndexes = () -> new ResultRetriever(analyze(), analyzeFilter(), controller, executionController, queryContext, command.isTopK());
        if (!command.isTopK())
            return queryIndexes.get();

        // If there are shadowed primary keys, we have to at least query twice.
        // First time to find out there are shadowed keys, second time to find out there are no more shadow keys.
        int loopsCount = 1;
        final long startShadowedKeysCount = queryContext.getShadowedPrimaryKeys().size();
        final var exactLimit = controller.getExactLimit();
        while (true)
        {
            queryContext.addShadowedKeysLoopCount(1L);
            long lastShadowedKeysCount = queryContext.getShadowedPrimaryKeys().size();
            ResultRetriever result = queryIndexes.get();
            queryContext.resetRowsMatched();
            UnfilteredPartitionIterator topK = (UnfilteredPartitionIterator) new VectorTopKProcessor(command).filter(result);
            long currentShadowedKeysCount = queryContext.getShadowedPrimaryKeys().size();
            long newShadowedKeysCount = currentShadowedKeysCount - lastShadowedKeysCount;
            logger.debug("Shadow loop iteration {}: rows matched: {}, keys shadowed: {}",
                         queryContext.shadowedKeysLoopCount(),
                         queryContext.rowsMatched(),
                         currentShadowedKeysCount);
            // Stop if no new shadowed keys found or if we already got enough rows
            if (newShadowedKeysCount == 0 || exactLimit <= queryContext.rowsMatched())
            {
                cfs.metric.incShadowedKeys(loopsCount, currentShadowedKeysCount - startShadowedKeysCount);
                if (loopsCount > 1)
                {
                    if (newShadowedKeysCount == 0)
                        Tracing.trace("No new shadowed keys after query loop {}", loopsCount);
                    else if (exactLimit <= queryContext.rowsMatched())
                        Tracing.trace("Got enough rows after query loop {}", loopsCount);
                }
                return topK;
            }
            loopsCount++;
            Tracing.trace("Found {} new shadowed keys, rerunning query (loop {})", newShadowedKeysCount, loopsCount);
        }
    }

    /**
     * Converts expressions into filter tree and reference {@link SSTableIndex}s used for query.
     *
     * @return operation
     */
    private RangeIterator analyze()
    {
        return controller.buildIterator();
    }

    /**
     * Converts expressions into filter tree (which is currently just a single AND).
     *
     * Filter tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @return root of the filter tree.
     */
    private FilterTree analyzeFilter()
    {
        return controller.buildFilter();
    }

    private static class ResultRetriever extends AbstractIterator<UnfilteredRowIterator>
                implements UnfilteredPartitionIterator, ParallelCommandProcessor
    {
        private final PrimaryKey firstPrimaryKey;
        private final PrimaryKey lastPrimaryKey;
        private final Iterator<DataRange> keyRanges;
        private AbstractBounds<PartitionPosition> currentKeyRange;

        private final RangeIterator operation;
        private final FilterTree filterTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;
        private final QueryContext queryContext;
        private final PrimaryKey.Factory keyFactory;
        private final boolean topK;

        private PrimaryKey lastKey;

        private ResultRetriever(RangeIterator operation,
                                FilterTree filterTree,
                                QueryController controller,
                                ReadExecutionController executionController,
                                QueryContext queryContext, boolean topK)
        {
            this.keyRanges = controller.dataRanges().iterator();
            this.currentKeyRange = keyRanges.next().keyRange();

            this.operation = operation;
            this.filterTree = filterTree;
            this.controller = controller;
            this.executionController = executionController;
            this.queryContext = queryContext;
            this.keyFactory = controller.primaryKeyFactory();
            this.topK = topK;

            this.firstPrimaryKey = controller.firstPrimaryKey();
            this.lastPrimaryKey = controller.lastPrimaryKey();
        }

        @Override
        public UnfilteredRowIterator computeNext()
        {
            // IMPORTANT: The correctness of the entire query pipeline relies on the fact that we consume a token
            // and materialize its keys before moving on to the next token in the flow. This sequence must not be broken
            // with toList() or similar. (Both the union and intersection flow constructs, to avoid excessive object
            // allocation, reuse their token mergers as they process individual positions on the ring.)

            if (operation == null)
                return endOfData();

            // If being called for the first time, skip to the beginning of the range.
            // We can't put this code in the constructor because it may throw and the caller
            // may not be prepared for that.
            if (lastKey == null)
                operation.skipTo(firstPrimaryKey);

            // Theoretically we wouldn't need this if the caller of computeNext always ran the
            // returned iterators to the completion. Unfortunately, we have no control over the caller behavior here.
            // Hence, we skip to the next partition in order to comply to the unwritten partition iterator contract
            // saying this iterator must not return the same partition twice.
            skipToNextPartition();

            UnfilteredRowIterator iterator = nextRowIterator(this::nextSelectedKeyInRange);
            return iterator != null
                   ? iteratePartition(iterator)
                   : endOfData();
        }

        /**
         * Tries to obtain a row iterator for one of the supplied keys by repeatedly calling
         * {@link ResultRetriever#apply} until it gives a non-null result.
         * The keySupplier should return the next key with every call to get() and
         * null when there are no more keys to try.
         *
         * @return an iterator or null if all keys were tried with no success
         */
        private @Nullable UnfilteredRowIterator nextRowIterator(@Nonnull Supplier<PrimaryKey> keySupplier)
        {
            UnfilteredRowIterator iterator = null;
            while (iterator == null)
            {
                PrimaryKey key = keySupplier.get();
                if (key == null)
                    return null;
                iterator = apply(key);
            }
            return iterator;
        }

        /**
         * Eagerly collects all the keys.
         * @return List of keys
         */
        private @Nullable List<PrimaryKey> getKeys()
        {
            List<PrimaryKey> keys = new ArrayList<>();
            while (true)
            {
                if (operation == null)
                    break;

                if (lastKey == null)
                    operation.skipTo(firstPrimaryKey);

                skipToNextPartition();

                PrimaryKey key = nextSelectedKeyInRange();
                if (key == null) break;
                if (key.equals(lastKey)) break;

                while (key != null)
                {
                    lastKey = key;
                    keys.add(key);
                    key = nextSelectedKeyInPartition(key.partitionKey());
                }
            }
            return keys;
        }

        /**
         * Returns a list of commands that need to be executed to retrieve the data.
         * @return list of (key, command) tuples
         */
        @Override
        public List<Pair<PrimaryKey, SinglePartitionReadCommand>> getUninitializedCommands()
        {
            List<PrimaryKey> keys = getKeys();
            return keys.stream()
                       .map(key -> Pair.create(key, controller.getPartitionReadCommand(key, executionController)))
                       .collect(Collectors.toList());
        }

        /**
         * Executes the given command and returns an iterator.
         */
        @Override
        public UnfilteredRowIterator commandToIterator(PrimaryKey key, SinglePartitionReadCommand command)
        {
            try (UnfilteredRowIterator partition = controller.executePartitionReadCommand(command, executionController))
            {
                queryContext.addPartitionsRead(1);
                queryContext.checkpoint();
                return applyIndexFilter(key, partition, filterTree, queryContext);
            }
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
                    // the following condition may be false if currentKeyRange.left is not inclusive,
                    // and key == currentKeyRange.left; in this case we should not try to skipTo the beginning
                    // of the range because that would be requesting the key to go backwards
                    // (in some implementations, skipTo can go backwards, and we don't want that)
                    if (currentKeyRange.left.getToken().compareTo(key.token()) > 0)
                    {
                        // key before the current range, so let's move the key forward
                        skipTo(currentKeyRange.left.getToken());
                    }
                    key = nextKey();
                }
            }
            return key;
        }

        /**
         * Returns the next available key contained by one of the keyRanges and selected by the queryController.
         * If the next key falls out of the current key range, it skips to the next key range, and so on.
         * If no more keys acceptd by the controller are available, returns null.
         */
         private @Nullable PrimaryKey nextSelectedKeyInRange()
        {
            PrimaryKey key;
            do
            {
                key = nextKeyInRange();
            }
            while (key != null && !controller.selects(key));
            return key;
        }

        /**
         * Retrieves the next primary key that belongs to the given partition and is selected by the query controller.
         * The underlying key iterator is advanced only if the key belongs to the same partition.
         * <p>
         * Returns null if:
         * <ul>
         *   <li>there are no more keys</li>
         *   <li>the next key is beyond the upper bound</li>
         *   <li>the next key belongs to a different partition</li>
         * </ul>
         * </p>
         */
        private @Nullable PrimaryKey nextSelectedKeyInPartition(DecoratedKey partitionKey)
        {
            PrimaryKey key;
            do
            {
                if (!operation.hasNext())
                    return null;
                if (!operation.peek().partitionKey().equals(partitionKey))
                    return null;

                key = nextKey();
            }
            while (key != null && !controller.selects(key));
            return key;
        }

        /**
         * Gets the next key from the underlying operation.
         * Returns null if there are no more keys <= lastPrimaryKey.
         */
        private @Nullable PrimaryKey nextKey()
        {
            if (!operation.hasNext())
                return null;
            PrimaryKey key = operation.next();
            return isWithinUpperBound(key) ? key : null;
        }

        /**
         * Returns true if the key is not greater than lastPrimaryKey
         */
        private boolean isWithinUpperBound(PrimaryKey key)
        {
            return lastPrimaryKey.token().isMinimum() || lastPrimaryKey.compareTo(key) >= 0;
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
            operation.skipTo(keyFactory.createTokenOnly(token));
        }

        /**
         * Skips to the key that belongs to a different partition than the last key we fetched.
         */
        private void skipToNextPartition()
        {
            if (lastKey == null)
                return;
            DecoratedKey lastPartitionKey = lastKey.partitionKey();
            while (operation.hasNext() && operation.peek().partitionKey().equals(lastPartitionKey))
                operation.next();
        }


        /**
         * Returns an iterator over the rows in the partition associated with the given iterator.
         * Initially, it retrieves the rows from the given iterator until it runs out of data.
         * Then it iterates the primary keys obtained from the index until the end of the partition
         * and lazily constructs new row itertors for each of the key. At a given time, only one row iterator is open.
         *
         * The rows are retrieved in the order of primary keys provided by the underlying index.
         * The iterator is complete when the next key to be fetched belongs to different partition
         * (but the iterator does not consume that key).
         *
         * @param startIter an iterator positioned at the first row in the partition that we want to return
         */
        private @Nonnull UnfilteredRowIterator iteratePartition(@Nonnull UnfilteredRowIterator startIter)
        {
            return new AbstractUnfilteredRowIterator(
                startIter.metadata(),
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
                        currentIter = nextRowIterator(() -> nextSelectedKeyInPartition(partitionKey));
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

        public UnfilteredRowIterator apply(PrimaryKey key)
        {
            // Key reads are lazy, delayed all the way to this point. Skip if we've already seen this one:
            if (key.equals(lastKey))
                return null;

            lastKey = key;

            try (UnfilteredRowIterator partition = controller.getPartition(key, executionController))
            {
                queryContext.addPartitionsRead(1);
                queryContext.checkpoint();
                return applyIndexFilter(key, partition, filterTree, queryContext);
            }
        }

        private UnfilteredRowIterator applyIndexFilter(PrimaryKey key, UnfilteredRowIterator partition, FilterTree tree, QueryContext queryContext)
        {
            Row staticRow = partition.staticRow();
            List<Unfiltered> clusters = new ArrayList<>();

            while (partition.hasNext())
            {
                Unfiltered row = partition.next();

                queryContext.addRowsFiltered(1);
                if (tree.isSatisfiedBy(key.partitionKey(), row, staticRow))
                {
                    queryContext.addRowsMatched(1);
                    clusters.add(row);
                }
            }

            if (clusters.isEmpty())
            {
                queryContext.addRowsFiltered(1);
                if (tree.isSatisfiedBy(key.partitionKey(), staticRow, staticRow))
                {
                    queryContext.addRowsMatched(1);
                    clusters.add(staticRow);
                }
            }

            /*
             * If {@code clusters} is empty, which means either all clustering row and static row pairs failed,
             *       or static row and static row pair failed. In both cases, we should not return any partition.
             * If {@code clusters} is not empty, which means either there are some clustering row and static row pairs match the filters,
             *       or static row and static row pair matches the filters. In both cases, we should return a partition with static row,
             *       and remove the static row marker from the {@code clusters} for the latter case.
             */
            if (clusters.isEmpty())
            {
                // shadowed by expired TTL or row tombstone or range tombstone
                if (topK)
                    queryContext.recordShadowedPrimaryKey(key);
                return null;
            }

            return new PartitionIterator(partition, staticRow, Iterators.filter(clusters.iterator(), u -> !((Row)u).isStatic()));
        }

        private static class PartitionIterator extends AbstractUnfilteredRowIterator
        {
            private final Iterator<Unfiltered> rows;

            public PartitionIterator(UnfilteredRowIterator partition, Row staticRow, Iterator<Unfiltered> content)
            {
                super(partition.metadata(),
                      partition.partitionKey(),
                      partition.partitionLevelDeletion(),
                      partition.columns(),
                      staticRow,
                      partition.isReverseOrder(),
                      partition.stats());

                rows = content;
            }

            @Override
            protected Unfiltered computeNext()
            {
                return rows.hasNext() ? rows.next() : endOfData();
            }
        }

        @Override
        public TableMetadata metadata()
        {
            return controller.metadata();
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(operation);
            controller.finish();
        }
    }

    /**
     * Used by {@link StorageAttachedIndexSearcher#filterReplicaFilteringProtection} to filter rows for columns that
     * have transformations so won't get handled correctly by the row filter.
     */
    @SuppressWarnings("resource")
    private static PartitionIterator applyIndexFilter(PartitionIterator response, FilterTree tree, QueryContext queryContext)
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
                            queryContext.addRowsFiltered(1);
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
