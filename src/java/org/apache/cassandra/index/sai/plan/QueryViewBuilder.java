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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Pair;

/**
 * Build a query specific view of the on-disk indexes for a query. This will return a
 * {@link Collection} of {@link Expression} and {@link SSTableIndex}s that represent
 * the on-disk data for a query.
 * <p>
 * The query view will include all the indexed expressions even if they don't have any
 * on-disk data. This in necessary because the query view is used to query in-memory
 * data as well as the attached on-disk indexes.
 */
public class QueryViewBuilder
{
    private final Collection<Expression> expressions;
    private final AbstractBounds<PartitionPosition> range;

    QueryViewBuilder(Collection<Expression> expressions, AbstractBounds<PartitionPosition> range)
    {
        this.expressions = expressions;
        this.range = range;
    }

    /**
     * Acquire references to all the SSTableIndexes required to query the expressions.
     * <p>
     * Will retry if the active sstables change concurrently.
     */
    protected Map<SSTableIndex, Collection<Expression>> build()
    {
        Set<String> indexNames = new TreeSet<>();
        try
        {
            while (true)
            {
                List<SSTableIndex> referencedIndexes = new ArrayList<>();
                boolean failed = false;

                Map<SSTableIndex, Collection<Expression>> view = getQueryView(expressions);

                for (var index : view.keySet())
                {
                    indexNames.add(index.getIndexContext().getIndexName());

                    if (index.reference())
                        referencedIndexes.add(index);
                    else
                    {
                        failed = true;
                        break;
                    }

                    if (failed)
                        break;
                }

                if (failed)
                    referencedIndexes.forEach(SSTableIndex::releaseQuietly);
                else
                    return view;
            }
        }
        finally
        {
            Tracing.trace("Querying storage-attached indexes {}", indexNames);
        }
    }

    /**
     * Group the expressions and corresponding indexes by sstable
     */
    private Map<SSTableIndex, Collection<Expression>> getQueryView(Collection<Expression> expressions)
    {
        Map<SSTableIndex, Collection<Expression>> queryView = new HashMap<>();

        // Collect unique SSTableReaders from each expression's view.
        Set<SSTableReader> uniqueSSTableReaders = expressions.stream()
                                                             .filter(expr -> !expr.context.isNotIndexed())
                                                             .flatMap(expr -> expr.context.getView().getIndexes().stream().map(SSTableIndex::getSSTable))
                                                             .collect(Collectors.toSet());
        for (SSTableReader sstable : uniqueSSTableReaders)
        {
            Pair<Expression, SSTableIndex> mostSelective = calculateMostSelective(expressions, sstable);

            // Iterate over each expression.
            for (Expression expression : expressions)
            {
                // Non-index column query should only act as FILTER BY for satisfiedBy(Row) method
                // because otherwise it likely to go through the whole index.
                if (expression.context.isNotIndexed())
                    continue;

                // If we didn't get a most selective expression then none of the
                // expressions select anything so, add an empty entry for the
                // expression. We need the empty entry because we may have in-memory
                // data for the expression
                if (mostSelective == null)
                {
                    queryView.computeIfAbsent(mostSelective.right, k -> new ArrayList<>()).add(expression);
                    continue;
                }

                // If this expression is the most selective then just add it to the
                // query view
                if (expression.equals(mostSelective.left))
                {
                    queryView.computeIfAbsent(mostSelective.right, k -> new ArrayList<>()).add(expression);
                    continue;
                }

                // Finally, we select the sstable index corresponding to this expression and sstable
                // if it has overlapping keys with the most select sstable index, and
                // and has a term range that is satisfied by the expression.
                View view = expression.context.getView();
                view.match(expression).stream()
                    .filter(index -> index.getSSTable().equals(sstable))
                    .filter(index -> sstableIndexOverlaps(index, Collections.singleton(mostSelective.right)))
                    .findFirst()
                    .ifPresent(index -> {
                        queryView.computeIfAbsent(index, k -> new ArrayList<>()).add(expression);
                    });
            }
        }

        return queryView;
    }

    private boolean sstableIndexOverlaps(SSTableIndex sstableIndex, Collection<SSTableIndex> sstableIndexes)
    {
        return sstableIndexes.stream().anyMatch(index -> index.bounds().contains(sstableIndex.bounds().left) ||
                                                         index.bounds().contains(sstableIndex.bounds().right));
    }

    // The purpose of this method is to calculate the most selective expression. This is the
    // expression with the most sstable indexes that match the expression by term and lie
    // within the key range being queried.
    //
    // The result can be null. This indicates that none of the expressions select any
    // sstable indexes.
    private Pair<Expression, SSTableIndex> calculateMostSelective(Collection<Expression> expressions, SSTableReader sstable)
    {
        Expression mostSelectiveExpression = null;
        SSTableIndex mostSelectiveIndex = null;

        for (Expression expression : expressions)
        {
            if (expression.context.isNotIndexed())
                continue;

            View view = expression.context.getView();

            SSTableIndex index = null;
            if (expression.context.isVector())
                index = view.getIndexes().stream()
                            .filter(idx -> idx.getSSTable().equals(sstable))
                            .findFirst()
                            .orElse(null);
            else
                index = selectIndexesInRange(view.match(expression)).stream()
                                                                    .filter(idx -> idx.getSSTable().equals(sstable))
                                                                    .findFirst()
                                                                    .orElse(null);

            if (index == null)
                continue;

            if (mostSelectiveExpression == null || mostSelectiveIndex == null || SSTableIndex.COMPARATOR.compare(mostSelectiveIndex, index) > 0)
            {
                mostSelectiveIndex = index;
                mostSelectiveExpression = expression;
            }
        }

        return mostSelectiveExpression == null ? null : Pair.create(mostSelectiveExpression, mostSelectiveIndex);
    }

    private List<SSTableIndex> selectIndexesInRange(List<SSTableIndex> indexes)
    {
        return indexes.stream().filter(this::indexInRange).collect(Collectors.toList());
    }

    private boolean indexInRange(SSTableIndex index)
    {
        SSTableReader sstable = index.getSSTable();
        return range.left.compareTo(sstable.last) <= 0 && (range.right.isMinimum() || sstable.first.compareTo(range.right) <= 0);
    }
}
