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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.schema.TableMetadata;

public class StorageAttachedIndexQueryPlan implements Index.QueryPlan
{
    private final ColumnFamilyStore cfs;
    private final TableQueryMetrics queryMetrics;
    private final RowFilter postIndexFilter;
    private final RowFilter filterOperation;
    private final Set<Index> indexes;
    private final boolean isTopK;

    private StorageAttachedIndexQueryPlan(ColumnFamilyStore cfs,
                                          TableQueryMetrics queryMetrics,
                                          RowFilter postIndexFilter,
                                          RowFilter filterOperation,
                                          ImmutableSet<Index> indexes)
    {
        this.cfs = cfs;
        this.queryMetrics = queryMetrics;
        this.postIndexFilter = postIndexFilter;
        this.filterOperation = filterOperation;
        this.indexes = indexes;
        this.isTopK = indexes.stream().anyMatch(i -> i instanceof StorageAttachedIndex && ((StorageAttachedIndex) i).termType().isVector());
    }

    @Nullable
    public static StorageAttachedIndexQueryPlan create(ColumnFamilyStore cfs,
                                                       TableQueryMetrics queryMetrics,
                                                       Set<StorageAttachedIndex> indexes,
                                                       RowFilter rowFilter)
    {
        ImmutableSet.Builder<Index> selectedIndexesBuilder = ImmutableSet.builder();

        RowFilter preIndexFilter = rowFilter;
        RowFilter postIndexFilter = rowFilter;

        for (RowFilter.Expression expression : rowFilter)
        {
            // we ignore any expressions here (currently IN and user-defined expressions) where we don't have a way to
            // translate their #isSatifiedBy method, they will be included in the filter returned by QueryPlan#postIndexQueryFilter()
            //
            // Note: For both the pre- and post-filters we need to check that the expression exists before removing it
            // because the without method assert if the expression doesn't exist. This can be the case if we are given
            // a duplicate expression - a = 1 and a = 1. The without method removes all instances of the expression.
            if (expression.operator().isIN() || expression.isUserDefined())
            {
                if (preIndexFilter.getExpressions().contains(expression))
                    preIndexFilter = preIndexFilter.without(expression);
                continue;
            }
            if (postIndexFilter.getExpressions().contains(expression))
                postIndexFilter = postIndexFilter.without(expression);

            for (StorageAttachedIndex index : indexes)
            {
                if (index.supportsExpression(expression.column(), expression.operator()))
                {
                    selectedIndexesBuilder.add(index);
                }
            }
        }

        ImmutableSet<Index> selectedIndexes = selectedIndexesBuilder.build();
        if (selectedIndexes.isEmpty())
            return null;

        return new StorageAttachedIndexQueryPlan(cfs, queryMetrics, postIndexFilter, preIndexFilter, selectedIndexes);
    }

    @Override
    public Set<Index> getIndexes()
    {
        return indexes;
    }

    @Override
    public long getEstimatedResultRows()
    {
        // this is temporary (until proper QueryPlan is integrated into Cassandra)
        // and allows us to priority storage-attached indexes if any in the query since they
        // are going to be more efficient, to query and intersect, than built-in indexes.
        return Long.MIN_VALUE;
    }

    @Override
    public boolean shouldEstimateInitialConcurrency()
    {
        return false;
    }

    @Override
    public Index.Searcher searcherFor(ReadCommand command)
    {
        return new StorageAttachedIndexSearcher(cfs,
                                                queryMetrics,
                                                command,
                                                filterOperation,
                                                DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    /**
     * Called on coordinator after merging replica responses before returning to client
     */
    @Override
    public Function<PartitionIterator, PartitionIterator> postProcessor(ReadCommand command)
    {
        if (!isTopK())
            return partitions -> partitions;

        // in case of top-k query, filter out rows that are not actually global top-K
        return partitions -> (PartitionIterator) new VectorTopKProcessor(command).filter(partitions);
    }

    /**
     * @return a filter with all the expressions that are user-defined or for a non-indexed partition key column
     * <p>
     * (currently index on partition columns is not supported, see {@link StorageAttachedIndex#validateOptions(Map, TableMetadata)})
     */
    @Override
    public RowFilter postIndexQueryFilter()
    {
        return postIndexFilter;
    }

    @Override
    public boolean isTopK()
    {
        return isTopK;
    }
}
