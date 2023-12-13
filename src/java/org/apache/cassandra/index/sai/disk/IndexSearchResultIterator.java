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
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.index.sai.QueryContext;
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
    public static IndexSearchResultIterator build(Expression expression,
                                                  Collection<SSTableIndex> sstableIndexes,
                                                  AbstractBounds<PartitionPosition> keyRange,
                                                  QueryContext queryContext)
    {
        List<KeyRangeIterator> subIterators = new ArrayList<>(1 + sstableIndexes.size());

        KeyRangeIterator memtableIterator = expression.getIndex().memtableIndexManager().searchMemtableIndexes(queryContext, expression, keyRange);
        if (memtableIterator != null)
            subIterators.add(memtableIterator);

        for (SSTableIndex sstableIndex : sstableIndexes)
        {
            try
            {
                queryContext.checkpoint();
                queryContext.sstablesHit++;

                if (sstableIndex.isReleased())
                    throw new IllegalStateException(sstableIndex.getIndexIdentifier().logMessage("Index was released from the view during the query"));

                List<KeyRangeIterator> indexIterators = sstableIndex.search(expression, keyRange, queryContext);

                if (!indexIterators.isEmpty())
                    subIterators.addAll(indexIterators);
            }
            catch (Throwable e)
            {
                if (!(e instanceof QueryCancelledException))
                    logger.debug(sstableIndex.getIndexIdentifier().logMessage(String.format("Failed search an index %s, aborting query.", sstableIndex.getSSTable())), e);

                throw Throwables.cleaned(e);
            }
        }

        KeyRangeIterator union = KeyRangeUnionIterator.build(subIterators);
        return new IndexSearchResultIterator(union, sstableIndexes, queryContext);
    }

    public static IndexSearchResultIterator build(List<KeyRangeIterator> sstableIntersections,
                                                  KeyRangeIterator memtableResults,
                                                  Set<SSTableIndex> referencedIndexes,
                                                  QueryContext queryContext)
    {
        queryContext.sstablesHit += referencedIndexes
                                    .stream()
                                    .map(SSTableIndex::getSSTable).collect(Collectors.toSet()).size();
        queryContext.checkpoint();
        KeyRangeIterator union = KeyRangeUnionIterator.builder(sstableIntersections.size() + 1)
                                                      .add(sstableIntersections)
                                                      .add(memtableResults)
                                                      .build();
        return new IndexSearchResultIterator(union, referencedIndexes, queryContext);
    }

    protected PrimaryKey computeNext()
    {
        return union.hasNext() ? union.next() : endOfData();
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        union.skipTo(nextKey);
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
            logger.error(index.getIndexIdentifier().logMessage(String.format("Failed to release index on SSTable %s", index.getSSTable())), e);
        }
    }
}
