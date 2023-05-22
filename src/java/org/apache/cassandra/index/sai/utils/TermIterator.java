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
package org.apache.cassandra.index.sai.utils;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.io.util.FileUtils;

public class TermIterator<T extends Comparable> extends RangeIterator<T>
{
    private static final Logger logger = LoggerFactory.getLogger(TermIterator.class);

    private final QueryContext context;

    private final RangeIterator<T> union;
    private final Set<SSTableIndex> referencedIndexes;

    private TermIterator(RangeIterator<T> union, Set<SSTableIndex> referencedIndexes, QueryContext queryContext)
    {
        super(union.getMinimum(), union.getMaximum(), union.getCount());

        this.union = union;
        this.referencedIndexes = referencedIndexes;
        this.context = queryContext;
    }

    @SuppressWarnings("resource")
// FIXME left unmerged since I'm focusing on the AND case which is handled by QueryController now
//    public static TermIterator build(final Expression e, Set<SSTableIndex> perSSTableIndexes, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, int limit, boolean defer)
//    {
//        final List<RangeIterator> tokens = new ArrayList<>(1 + perSSTableIndexes.size());;
//
//        RangeIterator memtableIterator = e.context.searchMemtable(e, keyRange, limit);
//        if (memtableIterator != null)
//            tokens.add(memtableIterator);
//
//        for (final SSTableIndex index : perSSTableIndexes)
//        {
//            try
//            {
//                queryContext.checkpoint();
//                queryContext.incSstablesHit();
//                assert !index.isReleased();
//
//                SSTableQueryContext context = queryContext.getSSTableQueryContext(index.getSSTable());
//                List<RangeIterator> keyIterators = index.search(e, keyRange, context, defer, limit);
//
//                if (keyIterators == null || keyIterators.isEmpty())
//                    continue;
//
//                tokens.addAll(keyIterators);
//            }
//            catch (Throwable e1)
//            {
//                if (logger.isDebugEnabled() && !(e1 instanceof AbortedOperationException))
//                    logger.debug(String.format("Failed search an index %s, skipping.", index.getSSTable()), e1);
//
//                throw Throwables.cleaned(e1);
//            }
//        }
//
//        RangeIterator ranges = RangeUnionIterator.build(tokens);
//        return new TermIterator(ranges, perSSTableIndexes, queryContext);
//    }

    protected T computeNext()
    {
        try
        {
            return union.hasNext() ? union.next() : (T) endOfData();
        }
        finally
        {
            context.checkpoint();
        }
    }

    protected void performSkipTo(T nextKey)
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
        referencedIndexes.forEach(TermIterator::releaseQuietly);
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
            logger.error(String.format("Failed to release index %s", index.getSSTable()), e);
        }
    }
}
