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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tracing.Tracing;

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

    public static class QueryView
    {
        public final Map<SSTableReader, List<IndexExpression>> view;
        public final Set<SSTableIndex> referencedIndexes;

        public QueryView(Map<SSTableReader, List<IndexExpression>> view, Set<SSTableIndex> referencedIndexes)
        {
            this.view = view;
            this.referencedIndexes = referencedIndexes;
        }
    }

    /**
     * Acquire references to all the SSTableIndexes required to query the expressions.
     * <p>
     * Will retry if the active sstables change concurrently.
     */
    protected QueryView build()
    {
        Set<SSTableIndex> referencedIndexes = new HashSet<>();
        AtomicBoolean failed = new AtomicBoolean();
        try
        {
            while (true)
            {
                referencedIndexes.clear();
                failed.set(false);

                Map<SSTableReader, List<IndexExpression>> view = getQueryView(expressions);
                view.values()
                .stream()
                .flatMap(expressions -> expressions.stream().map(e -> e.index))
                .forEach(index ->
                {
                    if (referencedIndexes.contains(index))
                        return;
                    if (index.reference())
                        referencedIndexes.add(index);
                    else
                        failed.set(true);
                });

                if (failed.get())
                    referencedIndexes.forEach(SSTableIndex::release);
                else
                    return new QueryView(view, referencedIndexes);
            }
        }
        finally
        {
            if (Tracing.isTracing())
            {
                var groupedIndexes = referencedIndexes.stream().collect(
                    Collectors.groupingBy(i -> i.getIndexContext().getIndexName(), Collectors.counting()));
                var summary = groupedIndexes.entrySet().stream()
                                            .map(e -> String.format("%s (%s sstables)", e.getKey(), e.getValue()))
                                            .collect(Collectors.joining(", "));
                Tracing.trace("Querying storage-attached indexes {}", summary);
            }
        }
    }

    public static class IndexExpression
    {
        public final SSTableIndex index;
        public final Expression expression;

        public IndexExpression(SSTableIndex index, Expression expression)
        {
            this.index = index;
            this.expression = expression;
        }
    }

    /**
     * Group the expressions and corresponding indexes by sstable
     */
    private Map<SSTableReader, List<IndexExpression>> getQueryView(Collection<Expression> expressions)
    {
        Map<SSTableReader, List<IndexExpression>> queryView = new HashMap<>();

        for (Expression expression : expressions)
        {
            // Non-index column query should only act as FILTER BY for satisfiedBy(Row) method
            // because otherwise it likely to go through the whole index.
            if (!expression.context.isIndexed())
                continue;

            // Finally, we select the sstable index corresponding to this expression and sstable
            // if it has overlapping keys with the most select sstable index, and
            // and has a term range that is satisfied by the expression.
            View view = expression.context.getView();
            for (var index: view.match(expression))
            {
                if (indexInRange(index))
                    queryView.computeIfAbsent(index.getSSTable(), k -> new ArrayList<>()).add(new IndexExpression(index, expression));
            }
        }

        return queryView;
    }

    // I've removed the concept of "most selective index" since we don't actually have per-sstable
    // statistics on that; it looks like it was only used to check bounds overlap, so computing
    // an actual global bounds should be an improvement.  But computing global bounds as an intersection
    // of individual bounds is messy because you can end up with more than one range.
    private boolean indexInRange(SSTableIndex index)
    {
        SSTableReader sstable = index.getSSTable();
        return range.left.compareTo(sstable.last) <= 0 && (range.right.isMinimum() || sstable.first.compareTo(range.right) <= 0);
    }
}
