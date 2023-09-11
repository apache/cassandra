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

package org.apache.cassandra.index.sai.plan.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.SSTableRowIdKeyRangeIterator;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.vector.CheckpointingIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIntersectionIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexQueryController;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.postings.PostingListIntersection;
import org.apache.cassandra.index.sai.postings.PostingListUnion;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;

public class VectorQueryController extends StorageAttachedIndexQueryController
{
    private static final Logger logger = LoggerFactory.getLogger(VectorQueryController.class);

    public VectorQueryController(ColumnFamilyStore cfs,
                                 ReadCommand command,
                                 RowFilter filterOperation,
                                 QueryContext queryContext,
                                 TableQueryMetrics tableQueryMetrics)
    {
        super(cfs, command, filterOperation, queryContext, tableQueryMetrics);
    }

    /**
     * Build a {@link KeyRangeIterator.Builder} from the given list of expressions by applying given operation (OR/AND).
     * Building of such builder involves index search, results of which are persisted in the internal resources list
     *
     * @param expressions The expressions to build range iterator from (expressions with not results are ignored).
     *
     * @return range iterator builder based on given expressions and operation type.
     */
    public KeyRangeIterator.Builder getIndexQueryResults(Collection<Expression> expressions)
    {
        // FIXME having this at the expression level means that it only gets applied to Nodes at that level;
        // moving it to ORDER BY will allow us to apply it correctly for other Node sub-trees
        var annExpressionInHybridSearch = getAnnExpressionInHybridSearch(expressions);
        boolean isAnnHybridSearch = annExpressionInHybridSearch != null;
        if (isAnnHybridSearch)
            expressions = expressions.stream().filter(e -> e != annExpressionInHybridSearch).collect(Collectors.toList());

        // search memtable before referencing sstable indexes; otherwise we may miss newly flushed memtable index
        var iteratorsByMemtable = expressions.stream()
                                             .flatMap(expr -> expr.context.getMemtableIndexManager().iteratorsForSearch(queryContext, expr, mergeRange).stream())
                                             .collect(Collectors.groupingBy(pair -> pair.left, Collectors.mapping(pair -> pair.right, Collectors.toList())));

        VectorQueryViewBuilder.QueryView queryView = new VectorQueryViewBuilder(expressions, mergeRange).build();
        // in case of ANN query in hybrid search, we have to reference ANN sstable indexes separately because queryView doesn't include ANN sstable indexes
        VectorQueryViewBuilder.QueryView annQueryViewInHybridSearch = isAnnHybridSearch ? new VectorQueryViewBuilder(Collections.singleton(annExpressionInHybridSearch), mergeRange).build() : null;

        try
        {
            List<KeyRangeIterator> sstableIntersections = new ArrayList<>(queryView.view.size());
            for (Map.Entry<SSTableReader, List<VectorQueryViewBuilder.IndexExpression>> viewEntry : queryView.view.entrySet())
            {
                PostingList postings = createRowIdIterator(viewEntry.getValue(), isAnnHybridSearch);
                if (isAnnHybridSearch)
                    sstableIntersections.add(reorderAndLimitBySSTableRowIds(postings, viewEntry.getKey(), annQueryViewInHybridSearch));
                else
                {
                    PrimaryKeyMap.Factory pkFactory = viewEntry.getValue().iterator().next().index.getSSTableContext().primaryKeyMapFactory;
                    sstableIntersections.add(convertToPrimaryKeyIterator(pkFactory, postings));
                }
            }

            List<KeyRangeIterator> memtableIntersections = iteratorsByMemtable.entrySet()
                                                                              .stream()
                                                                              .map(e -> {
                                                                                  // we need to do all the intersections at the index level, or ordering won't work
                                                                                  KeyRangeIterator it = buildIterator(e.getValue(), isAnnHybridSearch);
                                                                                  if (isAnnHybridSearch)
                                                                                      it = reorderAndLimitBy(it, e.getKey(), annExpressionInHybridSearch);
                                                                                  return it;
                                                                              })
                                                                              .collect(Collectors.toList());

            Iterable<KeyRangeIterator> allIntersections = Iterables.concat(sstableIntersections, memtableIntersections);

            queryContext.sstablesHit += queryView.referencedIndexes
                                        .stream()
                                        .map(SSTableIndex::getSSTable).collect(Collectors.toSet()).size();
            queryContext.checkpoint();
            KeyRangeIterator union = KeyRangeUnionIterator.build(Lists.newArrayList(allIntersections));

            CheckpointingIterator checkpointingIterator = new CheckpointingIterator(union,
                                                                                    queryView.referencedIndexes,
                                                                                    annQueryViewInHybridSearch == null ? Collections.emptySet() : annQueryViewInHybridSearch.referencedIndexes,
                                                                                    queryContext);

            return KeyRangeUnionIterator.builder(1).add(checkpointingIterator);
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            queryView.referencedIndexes.forEach(SSTableIndex::release);
            // if ANN sstable indexes are referenced separately, release them
            if (annQueryViewInHybridSearch != null)
                annQueryViewInHybridSearch.referencedIndexes.forEach(SSTableIndex::release);

            throw t;
        }
    }

    private KeyRangeIterator convertToPrimaryKeyIterator(PrimaryKeyMap.Factory pkFactory, PostingList postingList)
    {
        try
        {
            if (postingList.size() <= 0)
                return KeyRangeIterator.empty();

            PrimaryKeyMap primaryKeyMap = pkFactory.newPerSSTablePrimaryKeyMap();
            return SSTableRowIdKeyRangeIterator.create(primaryKeyMap, queryContext, postingList);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private KeyRangeIterator reorderAndLimitBy(KeyRangeIterator original, Memtable memtable, Expression expression)
    {
        return expression.context.getMemtableIndexManager().reorderMemtable(memtable, queryContext, original, expression);
    }

    private KeyRangeIterator reorderAndLimitBySSTableRowIds(PostingList original, SSTableReader sstable, VectorQueryViewBuilder.QueryView annQueryView)
    {
        List<VectorQueryViewBuilder.IndexExpression> annIndexExpressions = annQueryView.view.get(sstable);
        assert annIndexExpressions.size() == 1 : "only one index is expected in ANN expression, found " + annIndexExpressions.size() + " in " + annIndexExpressions;
        VectorQueryViewBuilder.IndexExpression annIndexExpression = annIndexExpressions.get(0);

        try
        {
            return annIndexExpression.index.limitToTopResults(queryContext, original, annIndexExpression.expression);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return ann expression if expressions have one ANN index and at least one non-ANN index
     */
    private Expression getAnnExpressionInHybridSearch(Collection<Expression> expressions)
    {
        if (expressions.size() < 2)
        {
            // if there is a single expression, just run search against it even if it's ANN
            return null;
        }

        return expressions.stream().filter(e -> e.getOp() == Expression.IndexOperator.ANN).findFirst().orElse(null);
    }

    /**
     * Create row id iterator from different indexes' on-disk searcher of the same sstable
     */
    private PostingList createRowIdIterator(List<VectorQueryViewBuilder.IndexExpression> indexExpressions, boolean hasAnn)
    {
        List<PostingList> subIterators = new ArrayList<>(indexExpressions.size());
        for (VectorQueryViewBuilder.IndexExpression indexExpression : indexExpressions)
        {
            try
            {
                List<PostingList> iterators = indexExpression.index.search(indexExpression.expression, mergeRange, queryContext);
                // concat the result from multiple segments for the same index
                subIterators.add(MergePostingList.merge(iterators));
            }
            catch (Throwable ex)
            {
                if (!(ex instanceof QueryCancelledException))
                    logger.debug(indexExpression.index.getIndexContext().logMessage(String.format("Failed search on index %s, aborting query.",
                                                                                                  indexExpression.index.getSSTable())), ex);
                throw Throwables.cleaned(ex);
            }
        }

        // we need to do all the intersections at the index level, or ordering won't work
        return hasAnn ? PostingListIntersection.createFromPostings(subIterators) : PostingListUnion.createFromPostings(subIterators);
    }

    private static KeyRangeIterator buildIterator(List<KeyRangeIterator> subIterators, boolean isAnnHybridSearch)
    {
        KeyRangeIterator.Builder builder;
        if (isAnnHybridSearch)
            // if it's ANN with other indexes, intersect all available indexes so result will be correct top-k
            builder = KeyRangeIntersectionIterator.builder(subIterators.size(), subIterators.size());
        else
            // Otherwise, pick 2 most selective indexes for better performance
            builder = KeyRangeIntersectionIterator.builder(subIterators.size());

        return builder.add(subIterators).build();
    }
}
