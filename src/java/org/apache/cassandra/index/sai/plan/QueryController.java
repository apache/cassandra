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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

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
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.IndexSearchResultIterator;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class QueryController
{
    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;
    private final RowFilter filterOperation;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;

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
    }

    public TableMetadata metadata()
    {
        return command.metadata();
    }

    RowFilter filterOperation()
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
     * @return indexed {@code IndexContext} if index is found; otherwise return non-indexed {@code IndexContext}.
     */
    public IndexContext getContext(RowFilter.Expression expression)
    {
        Set<StorageAttachedIndex> indexes = cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class);

        return indexes.isEmpty() ? new IndexContext(cfs.metadata().keyspace,
                                                    cfs.metadata().name,
                                                    cfs.metadata().partitionKeyType,
                                                    cfs.metadata().comparator,
                                                    expression.column(),
                                                    IndexTarget.Type.VALUES,
                                                    null)
                                 : indexes.iterator().next().getIndexContext();
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
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder(expressions.size());

        QueryViewBuilder queryViewBuilder = new QueryViewBuilder(expressions, mergeRange);

        Collection<Pair<Expression, Collection<SSTableIndex>>> queryView = queryViewBuilder.build();

        try
        {
            for (Pair<Expression, Collection<SSTableIndex>> queryViewPair : queryView)
            {
                @SuppressWarnings({"resource", "RedundantSuppression"}) // RangeIterators are closed by releaseIndexes
                KeyRangeIterator index = IndexSearchResultIterator.build(queryViewPair.left, queryViewPair.right, mergeRange, queryContext);

                builder.add(index);
            }
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            builder.cleanup();
            queryView.forEach(pair -> pair.right.forEach(SSTableIndex::releaseQuietly));
            throw t;
        }
        return builder;
    }

    /**
     * Returns whether this query is not selecting the {@link PrimaryKey}.
     * The query does not select the key if both of the following statements are false:
     *  1. The table associated with the query is not using clustering keys
     *  2. The clustering index filter for the command wants the row.
     *
     *  Item 2 is important in paged queries where the {@link org.apache.cassandra.db.filter.ClusteringIndexSliceFilter} for
     *  subsequent paged queries may not select rows that are returned by the index
     *  search because that is initially partition based.
     *
     * @param key The {@link PrimaryKey} to be tested
     * @return true if the key is not selected by the query
     */
    public boolean doesNotSelect(PrimaryKey key)
    {
        return !key.hasEmptyClustering() && !command.clusteringIndexFilter(key.partitionKey()).selects(key.clustering());
    }

    // Note: This method assumes that the selects method has already been called for the
    // key to avoid having to (potentially) call selects twice
    private ClusteringIndexFilter makeFilter(PrimaryKey key)
    {
        ClusteringIndexFilter clusteringIndexFilter = command.clusteringIndexFilter(key.partitionKey());

        if (key.hasEmptyClustering())
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
