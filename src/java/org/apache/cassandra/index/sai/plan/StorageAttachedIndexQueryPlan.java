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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.schema.TableMetadata;

public class StorageAttachedIndexQueryPlan implements Index.QueryPlan
{
    private final ColumnFamilyStore cfs;
    private final TableQueryMetrics queryMetrics;
    private final RowFilter postIndexFilter;
    private final List<RowFilter.Expression> expressions;
    private final Set<Index> indexes;

    private StorageAttachedIndexQueryPlan(ColumnFamilyStore cfs,
                                          TableQueryMetrics queryMetrics,
                                          RowFilter postIndexFilter,
                                          List<RowFilter.Expression> expressions,
                                          ImmutableSet<Index> indexes)
    {
        this.cfs = cfs;
        this.queryMetrics = queryMetrics;
        this.postIndexFilter = postIndexFilter;
        this.expressions = expressions;
        this.indexes = indexes;
    }

    @Nullable
    public static StorageAttachedIndexQueryPlan create(ColumnFamilyStore cfs,
                                                       TableQueryMetrics queryMetrics,
                                                       Set<StorageAttachedIndex> indexes,
                                                       RowFilter rowFilter)
    {
        ImmutableSet.Builder<Index> selectedIndexesBuilder = ImmutableSet.builder();
        List<RowFilter.Expression> acceptedExpressions = new ArrayList<>();

        for (RowFilter.Expression expression : rowFilter.getExpressions())
        {
            // we ignore user-defined expressions here because we don't have a way to translate their #isSatifiedBy
            // method, they will be included in the filter returned by QueryPlan#postIndexQueryFilter()
            if (expression.isUserDefined())
                continue;

            acceptedExpressions.add(expression);
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

        /*
         * postIndexFilter comprised by those expressions in the read command row filter that can't be handled by
         * {@link FilterTree#satisfiedBy(Unfiltered, Row, boolean)}. That includes expressions targeted
         * at {@link RowFilter.UserExpression}s like those used by RLAC.
         */
        RowFilter postIndexFilter = rowFilter.restrict(e -> e.isUserDefined());
        return new StorageAttachedIndexQueryPlan(cfs, queryMetrics, postIndexFilter, acceptedExpressions, selectedIndexes);
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
        return new StorageAttachedIndexSearcher(cfs, queryMetrics, command, expressions, DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    /**
     * @return a filter with all the expressions that are user-defined or for a non-indexed partition key column
     *
     * (currently index on partition columns is not supported, see {@link StorageAttachedIndex#validateOptions(Map, TableMetadata)})
     */
    @Override
    public RowFilter postIndexQueryFilter()
    {
        return postIndexFilter;
    }

    @Override
    public boolean supportsMultiRangeReadCommand()
    {
        return true;
    }
}
