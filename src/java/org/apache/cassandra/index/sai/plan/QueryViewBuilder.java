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
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
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

    public static class QueryView
    {
        public final Collection<Pair<Expression, Collection<SSTableIndex>>> view;
        public final Set<SSTableIndex> referencedIndexes;

        public QueryView(Collection<Pair<Expression, Collection<SSTableIndex>>> view, Set<SSTableIndex> referencedIndexes)
        {
            this.view = view;
            this.referencedIndexes = referencedIndexes;
        }
    }

    protected QueryView build()
    {
        Set<SSTableIndex> referencedIndexes = new HashSet<>();
        while (true)
        {
            referencedIndexes.clear();
            boolean failed = false;

            Collection<Pair<Expression, Collection<SSTableIndex>>> view = getQueryView(expressions);
            for (SSTableIndex index : view.stream().map(pair -> pair.right).flatMap(Collection::stream).collect(Collectors.toList()))
            {
                if (index.reference())
                    referencedIndexes.add(index);
                else
                    failed = true;
            }

            if (failed)
                referencedIndexes.forEach(SSTableIndex::release);
            else
                return new QueryView(view, referencedIndexes);
        }
    }

    private Collection<Pair<Expression, Collection<SSTableIndex>>> getQueryView(Collection<Expression> expressions)
    {
        // first let's determine the most selective expression
        Pair<Expression, Collection<SSTableIndex>> mostSelective = calculateMostSelective(expressions);

        List<Pair<Expression, Collection<SSTableIndex>>> queryView = new ArrayList<>();

        for (Expression expression : expressions)
        {
            // Non-index column query should only act as FILTER BY for satisfiedBy(Row) method
            // because otherwise it likely to go through the whole index.
            if (expression.isNotIndexed())
                continue;

            // If we didn't get a most selective expression then none of the
            // expressions select anything so, add an empty entry for the
            // expression. We need the empty entry because we may have in-memory
            // data for the expression
            if (mostSelective == null)
            {
                queryView.add(Pair.create(expression, Collections.emptyList()));
                continue;
            }

            // If this expression is the most selective then just add it to the
            // query view
            if (expression.equals(mostSelective.left))
            {
                queryView.add(mostSelective);
                continue;
            }

            // Finally, we select all the sstable indexes for this expression that
            // have overlapping keys with the sstable indexes of the most selective
            // and have a term range that is satisfied by the expression.
            View view = expression.getIndex().view();
            Set<SSTableIndex> indexes = new TreeSet<>(SSTableIndex.COMPARATOR);
            indexes.addAll(view.match(expression)
                               .stream()
                               .filter(index -> sstableIndexOverlaps(index, mostSelective.right))
                               .collect(Collectors.toList()));
            queryView.add(Pair.create(expression, indexes));
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
    private Pair<Expression, Collection<SSTableIndex>> calculateMostSelective(Collection<Expression> expressions)
    {
        Expression mostSelectiveExpression = null;
        NavigableSet<SSTableIndex> mostSelectiveIndexes = null;

        for (Expression expression : expressions)
        {
            if (expression.isNotIndexed())
                continue;

            View view = expression.getIndex().view();

            NavigableSet<SSTableIndex> indexes = new TreeSet<>(SSTableIndex.COMPARATOR);
            indexes.addAll(selectIndexesInRange(view.match(expression)));

            if (indexes.isEmpty())
                continue;

            if (mostSelectiveExpression == null || mostSelectiveIndexes.size() > indexes.size())
            {
                mostSelectiveIndexes = indexes;
                mostSelectiveExpression = expression;
            }
        }

        return mostSelectiveExpression == null ? null : Pair.create(mostSelectiveExpression, mostSelectiveIndexes);
    }

    private List<SSTableIndex> selectIndexesInRange(Collection<SSTableIndex> indexes)
    {
        return indexes.stream().filter(this::indexInRange).collect(Collectors.toList());
    }

    private boolean indexInRange(SSTableIndex index)
    {
        SSTableReader sstable = index.getSSTable();
        return range.left.compareTo(sstable.getLast()) <= 0 && (range.right.isMinimum() || sstable.getFirst().compareTo(range.right) <= 0);
    }
}
