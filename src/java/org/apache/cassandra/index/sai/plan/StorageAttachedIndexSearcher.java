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

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
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
import org.apache.cassandra.utils.AbstractIterator;

public class StorageAttachedIndexSearcher implements Index.Searcher
{
    private final ReadCommand command;
    private final QueryController controller;
    private final QueryContext queryContext;
    private final PrimaryKey.Factory keyFactory;

    public StorageAttachedIndexSearcher(ColumnFamilyStore cfs,
                                        TableQueryMetrics tableQueryMetrics,
                                        ReadCommand command,
                                        RowFilter.FilterElement filterOperation,
                                        IndexFeatureSet indexFeatureSet,
                                        long executionQuotaMs)
    {
        this.command = command;
        this.queryContext = new QueryContext(executionQuotaMs);
        this.controller = new QueryController(cfs, command, filterOperation, indexFeatureSet, queryContext, tableQueryMetrics);
        this.keyFactory = PrimaryKey.factory(cfs.metadata().comparator, indexFeatureSet);
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
        return new ResultRetriever(analyze(), analyzeFilter(), controller, executionController, queryContext, keyFactory);
    }

    /**
     * Converts expressions into filter tree and reference {@link SSTableIndex}s used for query.
     *
     * @return operation
     */
    private RangeIterator analyze()
    {
        return Operation.buildIterator(controller);
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
        return Operation.buildFilter(controller);
    }

    private static class ResultRetriever extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final PrimaryKey startPrimaryKey;
        private final PrimaryKey lastPrimaryKey;
        private final Iterator<DataRange> keyRanges;
        private AbstractBounds<PartitionPosition> current;

        private final RangeIterator operation;
        private final FilterTree filterTree;
        private final QueryController controller;
        private final ReadExecutionController executionController;
        private final QueryContext queryContext;
        private final PrimaryKey.Factory keyFactory;

        private PrimaryKey currentKey = null;
        private PrimaryKey lastKey;

        private ResultRetriever(RangeIterator operation,
                                FilterTree filterTree,
                                QueryController controller,
                                ReadExecutionController executionController,
                                QueryContext queryContext,
                                PrimaryKey.Factory keyFactory)
        {
            this.keyRanges = controller.dataRanges().iterator();
            this.current = keyRanges.next().keyRange();

            this.operation = operation;
            this.filterTree = filterTree;
            this.controller = controller;
            this.executionController = executionController;
            this.queryContext = queryContext;
            this.keyFactory = keyFactory;

            this.startPrimaryKey = keyFactory.createTokenOnly(controller.mergeRange().left.getToken());
            this.lastPrimaryKey = keyFactory.createTokenOnly(controller.mergeRange().right.getToken());
        }

        @Override
        public UnfilteredRowIterator computeNext()
        {
            if (operation == null)
                return endOfData();

            operation.skipTo(startPrimaryKey);
            if (!operation.hasNext())
                return endOfData();
            currentKey = operation.next().loadDeferred();

            // IMPORTANT: The correctness of the entire query pipeline relies on the fact that we consume a token
            // and materialize its keys before moving on to the next token in the flow. This sequence must not be broken
            // with toList() or similar. (Both the union and intersection flow constructs, to avoid excessive object
            // allocation, reuse their token mergers as they process individual positions on the ring.)
            while (true)
            {
                if (!lastPrimaryKey.token().isMinimum() && lastPrimaryKey.compareTo(currentKey) < 0)
                    return endOfData();

                while (current != null)
                {
                    // exists in the current range
                    if (current.contains(currentKey.partitionKey()))
                    {
                        // The current read operation wants this key
                        if (controller.selects(currentKey))
                        {
                            UnfilteredRowIterator partition = apply(currentKey);
                            if (partition != null)
                                return partition;
                        }
                        break;
                    }
                    // bigger than current range
                    else if (!current.right.isMinimum() && current.right.compareTo(currentKey.partitionKey()) <= 0)
                    {
                        if (keyRanges.hasNext())
                            current = keyRanges.next().keyRange();
                        else
                            return endOfData();
                    }
                    // smaller than current range
                    else
                    {
                        // we already knew that key is not included in "current" abstract bounds,
                        // so "left" may have the same partition position as "key" when "left" is exclusive.
                        assert current.left.compareTo(currentKey.partitionKey()) >= 0;
                        operation.skipTo(keyFactory.createTokenOnly(current.left.getToken()));
                        break;
                    }
                }
                if (!operation.hasNext())
                    return endOfData();
                currentKey = operation.next().loadDeferred();
            }
        }

        public UnfilteredRowIterator apply(PrimaryKey key)
        {
            // Key reads are lazy, delayed all the way to this point. Skip if we've already seen this one:
            if (key.equals(lastKey))
                return null;

            lastKey = key;

            try (UnfilteredRowIterator partition = controller.getPartition(key, executionController))
            {
                queryContext.partitionsRead++;

                return applyIndexFilter(key, partition, filterTree, queryContext);
            }
        }

        private static UnfilteredRowIterator applyIndexFilter(PrimaryKey key, UnfilteredRowIterator partition, FilterTree tree, QueryContext queryContext)
        {
            Row staticRow = partition.staticRow();
            List<Unfiltered> clusters = new ArrayList<>();

            while (partition.hasNext())
            {
                Unfiltered row = partition.next();

                queryContext.rowsFiltered++;
                if (tree.isSatisfiedBy(key.partitionKey(), row, staticRow))
                {
                    clusters.add(row);
                }
            }

            if (clusters.isEmpty())
            {
                queryContext.rowsFiltered++;
                if (tree.isSatisfiedBy(key.partitionKey(), staticRow, staticRow))
                {
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
                    boolean hasNext;
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

                    @Override
                    public boolean hasNext()
                    {
                        while (hasNext = delegate.hasNext())
                        {
                            next = delegate.next();
                            queryContext.rowsFiltered++;
                            if (tree.isSatisfiedBy(delegate.partitionKey(), next, staticRow))
                                return true;
                        }
                        return false;
                    }

                    @Override
                    public Row next()
                    {
                        if (!hasNext)
                            throw new NoSuchElementException();
                        return next;
                    }
                };
            }
        };
    }
}
