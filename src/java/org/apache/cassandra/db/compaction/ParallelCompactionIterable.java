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
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;

import com.google.common.base.Functions;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.ICountableColumnIterator;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.utils.*;

/**
 * A class to run compaction taking advantage of multiple-core processes:
 *
 * One Deserializer thread per input sstable performs read + deserialize (a row at a time).
 * The resulting ColumnFamilies are added to a queue, which is fed to the merge Reducer.
 *
 * The merge Reducer creates MergeTasks on a thread-per-core Executor, and returns AsyncPrecompactedRow objects.
 *
 * The main complication is in handling larger-than-memory rows.  When one is encountered, no further deserialization
 * is done until that row is merged and written -- creating a pipeline stall, as it were.  Thus, this is intended
 * to be useful with mostly-in-memory row sizes, but preserves correctness in the face of occasional exceptions.
 */
public class ParallelCompactionIterable extends AbstractCompactionIterable
{
    private static final Logger logger = LoggerFactory.getLogger(ParallelCompactionIterable.class);

    private final int maxInMemorySize;

    public ParallelCompactionIterable(OperationType type, List<ICompactionScanner> scanners, CompactionController controller)
    {
        this(type, scanners, controller, DatabaseDescriptor.getInMemoryCompactionLimit() / scanners.size());
    }

    public ParallelCompactionIterable(OperationType type, List<ICompactionScanner> scanners, CompactionController controller, int maxInMemorySize)
    {
        super(controller, type, scanners);
        this.maxInMemorySize = maxInMemorySize;
    }

    public CloseableIterator<AbstractCompactedRow> iterator()
    {
        List<CloseableIterator<RowContainer>> sources = new ArrayList<CloseableIterator<RowContainer>>(scanners.size());
        for (ICompactionScanner scanner : scanners)
            sources.add(new Deserializer(scanner, maxInMemorySize));
        return new Unwrapper(MergeIterator.get(sources, RowContainer.comparator, new Reducer()), controller);
    }

    private static class Unwrapper extends AbstractIterator<AbstractCompactedRow> implements CloseableIterator<AbstractCompactedRow>
    {
        private final CloseableIterator<CompactedRowContainer> reducer;
        private final CompactionController controller;

        public Unwrapper(CloseableIterator<CompactedRowContainer> reducer, CompactionController controller)
        {
            this.reducer = reducer;
            this.controller = controller;
        }

        protected AbstractCompactedRow computeNext()
        {
            if (!reducer.hasNext())
                return endOfData();

            CompactedRowContainer container = reducer.next();
            AbstractCompactedRow compactedRow;
            try
            {
                compactedRow = container.future == null
                             ? container.row
                             : new PrecompactedRow(container.key, container.future.get());
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }

            if (compactedRow.isEmpty())
            {
                controller.invalidateCachedRow(compactedRow.key);
                return null;
            }
            else
            {
                // If the raw is cached, we call removeDeleted on it to have/ coherent query returns. However it would look
                // like some deleted columns lived longer than gc_grace + compaction. This can also free up big amount of
                // memory on long running instances
                controller.invalidateCachedRow(compactedRow.key);
                return compactedRow;
            }
        }

        public void close() throws IOException
        {
            reducer.close();
        }
    }

    private class Reducer extends MergeIterator.Reducer<RowContainer, CompactedRowContainer>
    {
        private final List<RowContainer> rows = new ArrayList<RowContainer>();
        private int row = 0;

        private final ThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                                                                                     Integer.MAX_VALUE,
                                                                                     TimeUnit.MILLISECONDS,
                                                                                     new SynchronousQueue<Runnable>(),
                                                                                     new NamedThreadFactory("CompactionReducer"));

        public void reduce(RowContainer current)
        {
            rows.add(current);
        }

        protected CompactedRowContainer getReduced()
        {
            assert rows.size() > 0;

            CompactedRowContainer compacted = getCompactedRow(rows);
            rows.clear();
            if ((row++ % 1000) == 0)
            {
                long n = 0;
                for (ICompactionScanner scanner : scanners)
                    n += scanner.getCurrentPosition();
                bytesRead = n;
                controller.mayThrottle(bytesRead);
            }
            return compacted;
        }

        public CompactedRowContainer getCompactedRow(List<RowContainer> rows)
        {
            boolean inMemory = true;
            for (RowContainer container : rows)
            {
                if (container.row == null)
                {
                    inMemory = false;
                    break;
                }
            }

            if (inMemory)
            {
                // caller will re-use rows List, so make ourselves a copy
                List<Row> rawRows = new ArrayList<Row>(rows.size());
                for (RowContainer rowContainer : rows)
                    rawRows.add(rowContainer.row);
                return new CompactedRowContainer(rows.get(0).getKey(), executor.submit(new MergeTask(rawRows)));
            }

            List<ICountableColumnIterator> iterators = new ArrayList<ICountableColumnIterator>(rows.size());
            for (RowContainer container : rows)
                iterators.add(container.row == null ? container.wrapper : new DeserializedColumnIterator(container.row));
            return new CompactedRowContainer(new LazilyCompactedRow(controller, iterators));
        }

        public void close()
        {
            executor.shutdown();
        }

        private class MergeTask implements Callable<ColumnFamily>
        {
            private final List<Row> rows;

            public MergeTask(List<Row> rows)
            {
                this.rows = rows;
            }

            public ColumnFamily call() throws Exception
            {
                ColumnFamily cf = null;
                for (Row row : rows)
                {
                    ColumnFamily thisCF = row.cf;
                    if (cf == null)
                    {
                        cf = thisCF;
                    }
                    else
                    {
                        // addAll is ok even if cf is an ArrayBackedSortedColumns
                        SecondaryIndexManager.Updater indexer = controller.cfs.indexManager.updaterFor(row.key, false);
                        cf.addAllWithSizeDelta(thisCF, HeapAllocator.instance, Functions.<IColumn>identity(), indexer);
                    }
                }

                return PrecompactedRow.removeDeletedAndOldShards(rows.get(0).key, controller, cf);
            }
        }

        private class DeserializedColumnIterator implements ICountableColumnIterator
        {
            private final Row row;
            private Iterator<IColumn> iter;

            public DeserializedColumnIterator(Row row)
            {
                this.row = row;
                iter = row.cf.iterator();
            }

            public ColumnFamily getColumnFamily()
            {
                return row.cf;
            }

            public DecoratedKey getKey()
            {
                return row.key;
            }

            public int getColumnCount()
            {
                return row.cf.getColumnCount();
            }

            public void reset()
            {
                iter = row.cf.iterator();
            }

            public void close() throws IOException {}

            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public OnDiskAtom next()
            {
                return iter.next();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static class Deserializer extends AbstractIterator<RowContainer> implements CloseableIterator<RowContainer>
    {
        private final LinkedBlockingQueue<RowContainer> queue = new LinkedBlockingQueue<RowContainer>(1);
        private static final RowContainer finished = new RowContainer((Row) null);
        private Condition condition;
        private final ICompactionScanner scanner;

        public Deserializer(ICompactionScanner ssts, final int maxInMemorySize)
        {
            this.scanner = ssts;
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws Exception
                {
                    while (true)
                    {
                        if (condition != null)
                            condition.await();

                        if (!scanner.hasNext())
                        {
                            queue.put(finished);
                            break;
                        }

                        SSTableIdentityIterator iter = (SSTableIdentityIterator) scanner.next();
                        if (iter.dataSize > maxInMemorySize)
                        {
                            logger.debug("parallel lazy deserialize from " + iter.getPath());
                            condition = new SimpleCondition();
                            queue.put(new RowContainer(new NotifyingSSTableIdentityIterator(iter, condition)));
                        }
                        else
                        {
                            logger.debug("parallel eager deserialize from " + iter.getPath());
                            queue.put(new RowContainer(new Row(iter.getKey(), iter.getColumnFamilyWithColumns(TreeMapBackedSortedColumns.factory()))));
                        }
                    }
                }
            };
            new Thread(runnable, "Deserialize " + scanner.getBackingFiles()).start();
        }

        protected RowContainer computeNext()
        {
            RowContainer container;
            try
            {
                container = queue.take();
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            return container == finished ? endOfData() : container;
        }

        public void close() throws IOException
        {
            scanner.close();
        }
    }

    /**
     * a wrapper around SSTII that notifies the given condition when it is closed
     */
    private static class NotifyingSSTableIdentityIterator implements ICountableColumnIterator
    {
        private final SSTableIdentityIterator wrapped;
        private final Condition condition;

        public NotifyingSSTableIdentityIterator(SSTableIdentityIterator wrapped, Condition condition)
        {
            this.wrapped = wrapped;
            this.condition = condition;
        }

        public ColumnFamily getColumnFamily()
        {
            return wrapped.getColumnFamily();
        }

        public DecoratedKey getKey()
        {
            return wrapped.getKey();
        }

        public int getColumnCount()
        {
            return wrapped.getColumnCount();
        }

        public void reset()
        {
            wrapped.reset();
        }

        public void close() throws IOException
        {
            wrapped.close();
            condition.signal();
        }

        public boolean hasNext()
        {
            return wrapped.hasNext();
        }

        public OnDiskAtom next()
        {
            return wrapped.next();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class RowContainer
    {
        // either row is not null, or wrapper is not null.  But not both.
        public final Row row;
        public final NotifyingSSTableIdentityIterator wrapper;
        public static final Comparator<RowContainer> comparator = new Comparator<RowContainer>()
        {
            public int compare(RowContainer o1, RowContainer o2)
            {
                return o1.getKey().compareTo(o2.getKey());
            }
        };

        private RowContainer(Row row)
        {
            this.row = row;
            wrapper = null;
        }

        public RowContainer(NotifyingSSTableIdentityIterator wrapper)
        {
            this.wrapper = wrapper;
            row = null;
        }

        public DecoratedKey getKey()
        {
            return row == null ? wrapper.getKey() : row.key;
        }
    }

    private static class CompactedRowContainer
    {
        public final DecoratedKey key;
        /** either "future" or "row" will be not-null, but not both at once. */
        public final Future<ColumnFamily> future;
        public final LazilyCompactedRow row;

        private CompactedRowContainer(DecoratedKey key, Future<ColumnFamily> future)
        {
            this.key = key;
            this.future = future;
            row = null;
        }

        private CompactedRowContainer(LazilyCompactedRow row)
        {
            this.row = row;
            future = null;
            key = null;
        }
    }
}
