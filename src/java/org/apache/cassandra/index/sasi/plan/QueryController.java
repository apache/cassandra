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
package org.apache.cassandra.index.sasi.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import static java.lang.Long.min;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.TermIterator;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.conf.view.View;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.exceptions.TimeQuotaExceededException;
import org.apache.cassandra.index.sasi.plan.Operation.OperationType;
import org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;



public class QueryController
{
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    private final long executionQuota;
    private final long executionStart;

    private final long indexIgnoreTokenCountThreshold;
    private final double indexIgnoreTokenRatioThreshold = 0.1d;

    private final ColumnFamilyStore cfs;
    private final PartitionRangeReadCommand command;
    private final DataRange range;
    private final Map<Collection<Expression>, List<RangeIterator<Long, Token>>> resources = new HashMap<>();

    public QueryController(ColumnFamilyStore cfs, PartitionRangeReadCommand command, long timeQuotaMs)
    {
        this.cfs = cfs;
        this.command = command;
        this.range = command.dataRange();
        this.executionQuota = TimeUnit.MILLISECONDS.toNanos(timeQuotaMs);
        this.executionStart = System.nanoTime();
        if (command.limits().count() != DataLimits.NO_LIMIT)
            this.indexIgnoreTokenCountThreshold = min(command.limits().count(), 10000);
        else
            this.indexIgnoreTokenCountThreshold = 10000;
    }

    public boolean isForThrift()
    {
        return command.isForThrift();
    }

    public CFMetaData metadata()
    {
        return command.metadata();
    }

    public Collection<RowFilter.Expression> getExpressions()
    {
        return command.rowFilter().getExpressions();
    }

    public DataRange dataRange()
    {
        return command.dataRange();
    }

    public AbstractType<?> getKeyValidator()
    {
        return cfs.metadata.getKeyValidator();
    }

    public ColumnIndex getIndex(RowFilter.Expression expression)
    {
        Optional<Index> index = cfs.indexManager.getBestIndexFor(expression);
        return index.isPresent() ? ((SASIIndex) index.get()).getIndex() : null;
    }


    public UnfilteredRowIterator getPartition(DecoratedKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new NullPointerException();
        try
        {
            SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(command.isForThrift(),
                                                                                     cfs.metadata,
                                                                                     command.nowInSec(),
                                                                                     command.columnFilter(),
                                                                                     command.rowFilter().withoutExpressions(),
                                                                                     DataLimits.NONE,
                                                                                     key,
                                                                                     command.clusteringIndexFilter(key));

            return partition.queryMemtableAndDisk(cfs, executionController);
        }
        finally
        {
            checkpoint();
        }
    }

    /**
     * Build a range iterator from the given list of expressions by applying given operation (OR/AND).
     * Building of such iterator involves index search, results of which are persisted in the internal resources list
     * and can be released later via {@link QueryController#releaseIndexes(Operation)}.
     *
     * @param op The operation type to coalesce expressions with.
     * @param expressions The expressions to build range iterator from (expressions with not results are ignored).
     *
     * @return The range builder based on given expressions and operation type.
     */
    public RangeIterator.Builder<Long, Token> getIndexes(OperationType op, Collection<Expression> expressions)
    {
        if (resources.containsKey(expressions))
            throw new IllegalArgumentException("Can't process the same expressions multiple times.");

        List<RangeIterator<Long, Token>> indexes = new ArrayList<>();
        long minCount = Long.MAX_VALUE;

        // First execute the search on every index.
        for (Pair<Expression, Set<SSTableIndex>> e : getWeightedViews(op, expressions))
        {
            logger.trace("Searching {}", e);

            @SuppressWarnings("resource") // RangeIterators are closed by releaseIndexes
            RangeIterator<Long, Token> index = TermIterator.build(e.left, e.right);
            logger.trace("Found {} tokens", index == null ? 0 : index.getCount());

            if (index == null) {
                minCount = 0;
                continue;
            }

            // Keep track of the index that returned the least amount of results.
            if (index.getCount() < minCount)
                minCount = index.getCount();

            indexes.add(index);

            // If we found an index that already returned less results than the user
            // specified limit, it's probably faster to stop here because we already
            // ordered the expressions by cardinality.
            logger.trace("{} {} {}", e, command.limits(), index.getCount());
            if (op == OperationType.AND &&
                    command.limits().count() != DataLimits.NO_LIMIT &&
                    index.getCount() < command.limits().count()) {
                logger.trace("We found less than {} tokens. Stopping.",
			     command.limits().count());
                break;
            }
        }

        RangeIterator.Builder<Long, Token> builder = op == OperationType.OR
                ? RangeUnionIterator.<Long, Token>builder()
                : RangeIntersectionIterator.<Long, Token>builder();


        for (RangeIterator<Long, Token> index : indexes) {
            if (op == OperationType.AND) {
                long tokenCount = index == null ? 0 : index.getCount();
                float ratio = minCount == 0 ? 0 : (float) minCount / tokenCount;
                // If we are doing intersections between indexes we can enable some
                // additional optimizations.

                // See CASSANDRA-12915 for details. OnDiskIndexIterator can be rather
                // inefficient and in some cases it is faster to rely on post-filtering.
                logger.trace("count: {}, ratio: {}, threshold: {}",
                        tokenCount, ratio, indexIgnoreTokenCountThreshold);
                if (tokenCount > indexIgnoreTokenCountThreshold && ratio < indexIgnoreTokenRatioThreshold) {
                    logger.trace("Skipping");
                    continue;
                }
            }
            if (index != null)
                builder.add(index);
        }

        resources.put(expressions, indexes);
        return builder;
    }

    public void checkpoint()
    {
        if ((System.nanoTime() - executionStart) >= executionQuota)
            throw new TimeQuotaExceededException();
    }

    public void releaseIndexes(Operation operation)
    {
        if (operation.expressions != null)
            releaseIndexes(resources.remove(operation.expressions.values()));
    }

    private void releaseIndexes(List<RangeIterator<Long, Token>> indexes)
    {
        if (indexes == null)
            return;

        indexes.forEach(FileUtils::closeQuietly);
    }

    public void finish()
    {
        resources.values().forEach(this::releaseIndexes);
    }

    /**
     * Returns a list of Pair<Expression, Set<SSTableIndex>> ordered by guessed cardinality. The first
     * element of this list is likely to have a higher cardinality and be able to filter out more elements.
     * @param op
     * @param expressions
     * @return A sorted list of expression and associated sstables.
     */
    private List<Pair<Expression, Set<SSTableIndex>>> getWeightedViews(OperationType op,
                                                                       Collection<Expression> expressions) {
        Map<Expression, Set<SSTableIndex>> views = getView(op, expressions);

        // Generate a list of expression sorted by score.
        List<Pair<Expression, Long>> expressionScores = new ArrayList<>();
        for (Map.Entry<Expression, Set<SSTableIndex>> e : views.entrySet()) {
            Expression expression = e.getKey();

            long estimatedRowCount = 0;

            // Prioritize operations that are likely to make a good use of the index.
            if (expression.getOp() == Expression.Op.EQ
                || expression.getOp() == Expression.Op.PREFIX) {
                for (SSTableIndex index : e.getValue())
                    estimatedRowCount += index.getEstimatedResultRows();
            }
            // Let's assume that prefix will return way more values so let's reduce its score.
            if (expression.getOp() == Expression.Op.PREFIX)
                estimatedRowCount = (long)Math.sqrt((double)estimatedRowCount);

            expressionScores.add(Pair.create(expression, estimatedRowCount));
            logger.trace("Estimated row count for {}: {}", expression.toString(), estimatedRowCount);
        }
        expressionScores.sort(((a, b) -> b.right.compareTo(a.right)));

        // Now move that back in something more useful to the caller.
        List<Pair<Expression, Set<SSTableIndex>>> sortedViews = new ArrayList<>();
        for (Pair<Expression, Long> e : expressionScores)
            sortedViews.add(Pair.create(e.left, views.get(e.left)));

        return sortedViews;
    }

    private Map<Expression, Set<SSTableIndex>> getView(OperationType op, Collection<Expression> expressions)
    {
        // first let's determine the primary expression if op is AND
        Pair<Expression, Set<SSTableIndex>> primary = (op == OperationType.AND) ? calculatePrimary(expressions) : null;

        Map<Expression, Set<SSTableIndex>> indexes = new HashMap<>();

        logger.trace("Expressions: {}, Op: {}", expressions.toString(), op);
        for (Expression e : expressions)
        {
            // NO_EQ and non-index column query should only act as FILTER BY for satisfiedBy(Row) method
            // because otherwise it likely to go through the whole index.
            if (!e.isIndexed() || e.getOp() == Expression.Op.NOT_EQ)
                continue;

            // primary expression, we'll have to add as is
            if (primary != null && e.equals(primary.left))
            {
                indexes.put(primary.left, primary.right);
                continue;
            }

            View view = e.index.getView();
            if (view == null)
                continue;

            Set<SSTableIndex> readers = new HashSet<>();
            if (primary != null && primary.right.size() > 0)
            {
                for (SSTableIndex index : primary.right)
                    readers.addAll(view.match(index.minKey(), index.maxKey()));
            }
            else
            {
                readers.addAll(applyScope(view.match(e)));
            }

            indexes.put(e, readers);
        }
        logger.trace("Final view: {}", indexes);

        return indexes;
    }

    private Pair<Expression, Set<SSTableIndex>> calculatePrimary(Collection<Expression> expressions)
    {
        Expression expression = null;
        Set<SSTableIndex> primaryIndexes = Collections.emptySet();

        for (Expression e : expressions)
        {
            if (!e.isIndexed())
                continue;

            View view = e.index.getView();
            if (view == null)
                continue;

            Set<SSTableIndex> indexes = applyScope(view.match(e));
            if (expression == null || primaryIndexes.size() > indexes.size())
            {
                primaryIndexes = indexes;
                expression = e;
            }
        }

        return expression == null ? null : Pair.create(expression, primaryIndexes);
    }

    private Set<SSTableIndex> applyScope(Set<SSTableIndex> indexes)
    {
        return Sets.filter(indexes, index -> {
            SSTableReader sstable = index.getSSTable();
            return range.startKey().compareTo(sstable.last) <= 0 && (range.stopKey().isMinimum() || sstable.first.compareTo(range.stopKey()) <= 0);
        });
    }
}
