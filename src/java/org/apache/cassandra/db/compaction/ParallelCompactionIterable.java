package org.apache.cassandra.db.compaction;
/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.ICountableColumnIterator;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableScanner;
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
    private static Logger logger = LoggerFactory.getLogger(ParallelCompactionIterable.class);

    private final List<SSTableScanner> scanners;
    private final int maxInMemorySize;

    public ParallelCompactionIterable(OperationType type, Iterable<SSTableReader> sstables, CompactionController controller) throws IOException
    {
        this(type, getScanners(sstables), controller, DatabaseDescriptor.getInMemoryCompactionLimit() / Iterables.size(sstables));
    }

    public ParallelCompactionIterable(OperationType type, Iterable<SSTableReader> sstables, CompactionController controller, int maxInMemorySize) throws IOException
    {
        this(type, getScanners(sstables), controller, maxInMemorySize);
    }

    protected ParallelCompactionIterable(OperationType type, List<SSTableScanner> scanners, CompactionController controller, int maxInMemorySize)
    {
        super(controller, type);
        this.scanners = scanners;
        this.maxInMemorySize = maxInMemorySize;
    }

    public CloseableIterator<AbstractCompactedRow> iterator()
    {
        List<CloseableIterator<RowContainer>> sources = new ArrayList<CloseableIterator<RowContainer>>();
        for (SSTableScanner scanner : scanners)
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
                controller.removeDeletedInCache(compactedRow.key);
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
                for (SSTableScanner scanner : scanners)
                    n += scanner.getFilePointer();
                bytesRead = n;
                throttle.throttle(bytesRead);
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
                return new CompactedRowContainer(rows.get(0).getKey(), executor.submit(new MergeTask(new ArrayList<RowContainer>(rows))));

            List<ICountableColumnIterator> iterators = new ArrayList<ICountableColumnIterator>();
            for (RowContainer container : rows)
                iterators.add(container.row == null ? container.wrapper : new DeserializedColumnIterator(container.row));
            return new CompactedRowContainer(new LazilyCompactedRow(controller, iterators));
        }

        private class MergeTask implements Callable<ColumnFamily>
        {
            private final List<RowContainer> rows;

            public MergeTask(List<RowContainer> rows)
            {
                this.rows = rows;
            }

            public ColumnFamily call() throws Exception
            {
                ColumnFamily cf = null;
                for (RowContainer container : rows)
                {
                    ColumnFamily thisCF = container.row.cf;
                    if (cf == null)
                    {
                        cf = thisCF;
                    }
                    else
                    {
                        // addAll is ok even if cf is an ArrayBackedSortedColumns
                        cf.addAll(thisCF, HeapAllocator.instance);
                    }
                }

                return PrecompactedRow.removeDeletedAndOldShards(rows.get(0).getKey(), controller, cf);
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

            public IColumn next()
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
        private final SSTableScanner scanner;

        public Deserializer(SSTableScanner ssts, final int maxInMemorySize)
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
                            queue.put(new RowContainer(new Row(iter.getKey(), iter.getColumnFamilyWithColumns())));
                        }
                    }
                }
            };
            new Thread(runnable, "Deserialize " + scanner.sstable).start();
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

        public IColumn next()
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
