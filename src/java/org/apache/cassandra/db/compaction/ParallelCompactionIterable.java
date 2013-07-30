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

import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
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
        return new Unwrapper(MergeIterator.get(sources, RowContainer.comparator, new Reducer()));
    }

    private static class Unwrapper extends AbstractIterator<AbstractCompactedRow> implements CloseableIterator<AbstractCompactedRow>
    {
        private final CloseableIterator<CompactedRowContainer> reducer;

        public Unwrapper(CloseableIterator<CompactedRowContainer> reducer)
        {
            this.reducer = reducer;
        }

        protected AbstractCompactedRow computeNext()
        {
            if (!reducer.hasNext())
                return endOfData();

            CompactedRowContainer container = reducer.next();
            AbstractCompactedRow compactedRow;
            compactedRow = container.future == null
                         ? container.row
                         : new PrecompactedRow(container.key, FBUtilities.waitOnFuture(container.future));

            return compactedRow;
        }

        public void close() throws IOException
        {
            reducer.close();
        }
    }

    private class Reducer extends MergeIterator.Reducer<RowContainer, CompactedRowContainer>
    {
        private final List<RowContainer> rows = new ArrayList<RowContainer>();

        private final ThreadPoolExecutor executor = new DebuggableThreadPoolExecutor(FBUtilities.getAvailableProcessors(),
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

            ParallelCompactionIterable.this.updateCounterFor(rows.size());
            CompactedRowContainer compacted = getCompactedRow(rows);
            rows.clear();
            long n = 0;
            for (ICompactionScanner scanner : scanners)
                n += scanner.getCurrentPosition();
            bytesRead = n;
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

            List<OnDiskAtomIterator> iterators = new ArrayList<OnDiskAtomIterator>(rows.size());
            for (RowContainer container : rows)
                iterators.add(container.row == null ? container.wrapper : new DeserializedColumnIterator(container.row));
            return new CompactedRowContainer(new LazilyCompactedRow(controller, iterators));
        }

        public void close()
        {
            executor.shutdown();
        }

        /**
         * Merges a set of in-memory rows
         */
        private class MergeTask implements Callable<ColumnFamily>
        {
            private final List<Row> rows;

            public MergeTask(List<Row> rows)
            {
                this.rows = rows;
            }

            public ColumnFamily call() throws Exception
            {
                final ColumnFamily returnCF = ArrayBackedSortedColumns.factory.create(controller.cfs.metadata);

                List<CloseableIterator<Column>> data = new ArrayList<CloseableIterator<Column>>(rows.size());
                for (Row row : rows)
                {
                    returnCF.delete(row.cf);
                    data.add(FBUtilities.closeableIterator(row.cf.iterator()));
                }

                PrecompactedRow.merge(returnCF, data, controller.cfs.indexManager.updaterFor(rows.get(0).key));
                return PrecompactedRow.removeDeletedAndOldShards(rows.get(0).key, controller, returnCF);
            }
        }

        private class DeserializedColumnIterator implements OnDiskAtomIterator
        {
            private final Row row;
            private final Iterator<Column> iter;

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
        private final ICompactionScanner scanner;

        public Deserializer(ICompactionScanner ssts, final int maxInMemorySize)
        {
            this.scanner = ssts;
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws Exception
                {
                    SimpleCondition condition = null;
                    while (true)
                    {
                        if (condition != null)
                        {
                            condition.await();
                            condition = null;
                        }
                        if (!scanner.hasNext())
                        {
                            queue.put(finished);
                            break;
                        }

                        SSTableIdentityIterator iter = (SSTableIdentityIterator) scanner.next();
                        if (iter.dataSize > maxInMemorySize)
                        {
                            logger.debug("parallel lazy deserialize from {}", iter.getPath());
                            condition = new SimpleCondition();
                            queue.put(new RowContainer(new NotifyingSSTableIdentityIterator(iter, condition)));
                        }
                        else
                        {
                            logger.debug("parallel eager deserialize from {}", iter.getPath());
                            queue.put(new RowContainer(new Row(iter.getKey(), iter.getColumnFamilyWithColumns(ArrayBackedSortedColumns.factory))));
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
    private static class NotifyingSSTableIdentityIterator implements OnDiskAtomIterator
    {
        private final SSTableIdentityIterator wrapped;
        private final SimpleCondition condition;

        public NotifyingSSTableIdentityIterator(SSTableIdentityIterator wrapped, SimpleCondition condition)
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

        public void close() throws IOException
        {
            try
            {
                wrapped.close();
            }
            finally
            {
                condition.signalAll();
            }
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
