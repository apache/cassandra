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

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
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
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

public class QueryController
{
    private final long executionQuota;
    private final long executionStart;

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

            return partition.queryMemtableAndDisk(cfs, executionController.baseReadOpOrderGroup());
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

        RangeIterator.Builder<Long, Token> builder = op == OperationType.OR
                                                ? RangeUnionIterator.<Long, Token>builder()
                                                : RangeIntersectionIterator.<Long, Token>builder();

        List<RangeIterator<Long, Token>> perIndexUnions = new ArrayList<>();

        for (Map.Entry<Expression, Set<SSTableIndex>> e : getView(op, expressions).entrySet())
        {
            RangeIterator<Long, Token> index = TermIterator.build(e.getKey(), e.getValue());

            if (index == null)
                continue;

            builder.add(index);
            perIndexUnions.add(index);
        }

        resources.put(expressions, perIndexUnions);
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

    private Map<Expression, Set<SSTableIndex>> getView(OperationType op, Collection<Expression> expressions)
    {
        // first let's determine the primary expression if op is AND
        Pair<Expression, Set<SSTableIndex>> primary = (op == OperationType.AND) ? calculatePrimary(expressions) : null;

        Map<Expression, Set<SSTableIndex>> indexes = new HashMap<>();
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
            if (primaryIndexes.size() > indexes.size())
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
