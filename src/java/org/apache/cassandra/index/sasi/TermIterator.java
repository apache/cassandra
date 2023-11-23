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
package org.apache.cassandra.index.sasi;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;

import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder.Mode.CONTAINS;
import static org.apache.cassandra.index.sasi.plan.Expression.Op.PREFIX;
import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;

public class TermIterator extends RangeIterator<Long, Token>
{
    private static final Logger logger = LoggerFactory.getLogger(TermIterator.class);

    private static final FastThreadLocal<ExecutorService> SEARCH_EXECUTOR = new FastThreadLocal<ExecutorService>()
    {
        public ExecutorService initialValue()
        {
            final String currentThread = Thread.currentThread().getName();
            final int concurrencyFactor = DatabaseDescriptor.searchConcurrencyFactor();

            logger.info("Search Concurrency Factor is set to {} for {}", concurrencyFactor, currentThread);

            return (concurrencyFactor <= 1)
                    ? ImmediateExecutor.INSTANCE
                    : executorFactory().pooled(currentThread + "-SEARCH-", concurrencyFactor);
        }
    };

    private final Expression expression;

    private final RangeIterator<Long, Token> union;
    private final Set<SSTableIndex> referencedIndexes;

    private TermIterator(Expression e,
                         RangeIterator<Long, Token> union,
                         Set<SSTableIndex> referencedIndexes)
    {
        super(union.getMinimum(), union.getMaximum(), union.getCount());

        this.expression = e;
        this.union = union;
        this.referencedIndexes = referencedIndexes;
    }

    public static TermIterator build(final Expression e, Set<SSTableIndex> perSSTableIndexes)
    {
        final List<RangeIterator<Long, Token>> tokens = new CopyOnWriteArrayList<>();
        final AtomicLong tokenCount = new AtomicLong(0);

        RangeIterator<Long, Token> memtableIterator = e.index.searchMemtable(e);
        if (memtableIterator != null)
        {
            tokens.add(memtableIterator);
            tokenCount.addAndGet(memtableIterator.getCount());
        }

        final Set<SSTableIndex> referencedIndexes = new CopyOnWriteArraySet<>();

        try
        {
            final CountDownLatch latch = newCountDownLatch(perSSTableIndexes.size());
            final ExecutorService searchExecutor = SEARCH_EXECUTOR.get();

            for (final SSTableIndex index : perSSTableIndexes)
            {
                if (e.getOp() == PREFIX &&
                    index.mode() == CONTAINS && !index.hasMarkedPartials())
                    throw new UnsupportedOperationException(format("The index %s has not yet been upgraded " +
                                                                          "to support prefix queries in CONTAINS mode. " +
                                                                          "Wait for compaction or rebuild the index.",
                                                                          index.getPath()));


                if (!index.reference())
                {
                    latch.decrement();
                    continue;
                }

                // add to referenced right after the reference was acquired,
                // that helps to release index if something goes bad inside of the search
                referencedIndexes.add(index);

                searchExecutor.submit((Runnable) () -> {
                    try
                    {
                        e.checkpoint();

                        RangeIterator<Long, Token> keyIterator = index.search(e);
                        if (keyIterator == null)
                        {
                            releaseIndex(referencedIndexes, index);
                            return;
                        }

                        tokens.add(keyIterator);
                        tokenCount.getAndAdd(keyIterator.getCount());
                    }
                    catch (Throwable e1)
                    {
                        releaseIndex(referencedIndexes, index);

                        if (logger.isDebugEnabled())
                            logger.debug(format("Failed search an index %s, skipping.", index.getPath()), e1);
                    }
                    finally
                    {
                        latch.decrement();
                    }
                });
            }

            latch.awaitUninterruptibly();

            // checkpoint right away after all indexes complete search because we might have crossed the quota
            e.checkpoint();

            RangeIterator<Long, Token> ranges = RangeUnionIterator.build(tokens);
            return new TermIterator(e, ranges, referencedIndexes);
        }
        catch (Throwable ex)
        {
            // if execution quota was exceeded while opening indexes or something else happened
            // local (yet to be tracked) indexes should be released first before re-throwing exception
            referencedIndexes.forEach(TermIterator::releaseQuietly);

            throw ex;
        }
    }

    protected Token computeNext()
    {
        try
        {
            return union.hasNext() ? union.next() : endOfData();
        }
        finally
        {
            expression.checkpoint();
        }
    }

    protected void performSkipTo(Long nextToken)
    {
        try
        {
            union.skipTo(nextToken);
        }
        finally
        {
            expression.checkpoint();
        }
    }

    public void close()
    {
        FileUtils.closeQuietly(union);
        referencedIndexes.forEach(TermIterator::releaseQuietly);
        referencedIndexes.clear();
    }

    private static void releaseIndex(Set<SSTableIndex> indexes, SSTableIndex index)
    {
        indexes.remove(index);
        releaseQuietly(index);
    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(String.format("Failed to release index %s", index.getPath()), e);
        }
    }
}
