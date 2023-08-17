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
package org.apache.cassandra.index.sai.disk;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.iterators.KeyRangeAntiJoinIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

public class IndexSearchResultIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSearchResultIterator.class);

    private final QueryContext context;
    private final KeyRangeIterator union;
    private final Collection<SSTableIndex> referencedIndexes;

    private IndexSearchResultIterator(KeyRangeIterator union, Collection<SSTableIndex> referencedIndexes, QueryContext queryContext)
    {
        super(union.getMinimum(), union.getMaximum(), union.getCount());

        this.union = union;
        this.referencedIndexes = referencedIndexes;
        this.context = queryContext;
    }

    /**
     * Builds a new {@link IndexSearchResultIterator} that wraps a {@link KeyRangeUnionIterator} over the
     * results of searching the {@link org.apache.cassandra.index.sai.memory.MemtableIndex} and the {@link SSTableIndex}es.
     */
    @SuppressWarnings({"resource", "RedundantSuppression"})
    public static IndexSearchResultIterator build(Expression expression,
                                                  Collection<SSTableIndex> sstableIndexes,
                                                  AbstractBounds<PartitionPosition> keyRange,
                                                  QueryContext queryContext)
    {
        KeyRangeIterator keyIterator = buildKeyIterator(expression, sstableIndexes, keyRange, queryContext);

        // For NOT CONTAINS or NOT CONTAINS KEY it is not enought to just return the primary keys
        // for values not matching the value being queried.
        //
        // keys k such that row(k) not contains v =
        // (keys k such that row(k) contains x != v || row(k) empty) \ (keys k such that row(k) contains v)
        //
        if (expression.getOp() == Expression.IndexOperator.NOT_CONTAINS_KEY
            || expression.getOp() == Expression.IndexOperator.NOT_CONTAINS_VALUE)
        {
            Expression negExpression = expression.negated();
            KeyRangeIterator negIterator = buildKeyIterator(negExpression, sstableIndexes, keyRange, queryContext);
            keyIterator = KeyRangeAntiJoinIterator.create(keyIterator, negIterator);
        }

        return new IndexSearchResultIterator(keyIterator, sstableIndexes, queryContext);
    }

    private static KeyRangeIterator buildKeyIterator(Expression expression, Collection<SSTableIndex> sstableIndexes, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext)
    {
        List<KeyRangeIterator> subIterators = new ArrayList<>(1 + sstableIndexes.size());

        KeyRangeIterator memtableIterator = expression.context.getMemtableIndexManager().searchMemtableIndexes(expression, keyRange);
        if (memtableIterator != null)
            subIterators.add(memtableIterator);

        for (SSTableIndex sstableIndex : sstableIndexes)
        {
            try
            {
                queryContext.checkpoint();
                queryContext.sstablesHit++;

                if (sstableIndex.isReleased())
                    throw new IllegalStateException(sstableIndex.getIndexContext().logMessage("Index was released from the view during the query"));

                List<KeyRangeIterator> segmentIterators = sstableIndex.search(expression, keyRange, queryContext);

                if (!segmentIterators.isEmpty())
                    subIterators.addAll(segmentIterators);
            }
            catch (Throwable e)
            {
                if (!(e instanceof QueryCancelledException))
                    logger.debug(sstableIndex.getIndexContext().logMessage(String.format("Failed search an index %s, aborting query.", sstableIndex.getSSTable())), e);

                throw Throwables.cleaned(e);
            }
        }

        return KeyRangeUnionIterator.build(subIterators);
    }

    protected PrimaryKey computeNext()
    {
        try
        {
            return union.hasNext() ? union.next() : endOfData();
        }
        finally
        {
            context.checkpoint();
        }
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        try
        {
            union.skipTo(nextKey);
        }
        finally
        {
            context.checkpoint();
        }
    }

    public void close()
    {
        FileUtils.closeQuietly(union);
        referencedIndexes.forEach(IndexSearchResultIterator::releaseQuietly);
        referencedIndexes.clear();
    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(index.getIndexContext().logMessage(String.format("Failed to release index on SSTable %s", index.getSSTable())), e);
        }
    }
}
