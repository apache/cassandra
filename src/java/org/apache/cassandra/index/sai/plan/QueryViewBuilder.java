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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
        List<Pair<Expression, Collection<SSTableIndex>>> queryView = new ArrayList<>();

        for (Expression expression : expressions)
        {
            // Non-index column query should only act as FILTER BY for satisfiedBy(Row) method
            // because otherwise it likely to go through the whole index.
            if (expression.isNotIndexed())
                continue;

            // Select all the sstable indexes that have a term range that is satisfied by this expression and 
            // overlap with the key range being queried.
            View view = expression.getIndex().view();
            queryView.add(Pair.create(expression, selectIndexesInRange(view.match(expression))));
        }

        return queryView;
    }

    private List<SSTableIndex> selectIndexesInRange(Collection<SSTableIndex> indexes)
    {
        return indexes.stream().filter(this::indexInRange).sorted(SSTableIndex.COMPARATOR).collect(Collectors.toList());
    }

    private boolean indexInRange(SSTableIndex index)
    {
        SSTableReader sstable = index.getSSTable();
        return range.left.compareTo(sstable.getLast()) <= 0 && (range.right.isMinimum() || sstable.getFirst().compareTo(range.right) <= 0);
    }
}
