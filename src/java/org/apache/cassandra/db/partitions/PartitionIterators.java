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
package org.apache.cassandra.db.partitions;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;

import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.NoSpamLogger;

public abstract class PartitionIterators
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionIterators.class);

    private PartitionIterators() {}

    @SuppressWarnings("resource") // The created resources are returned right away
    public static RowIterator getOnlyElement(final PartitionIterator iter, SinglePartitionReadQuery query)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        RowIterator toReturn = iter.hasNext()
                             ? iter.next()
                             : EmptyIterators.row(query.metadata(),
                                                  query.partitionKey(),
                                                  query.clusteringIndexFilter().isReversed());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole PartitionIterator.
        class Close extends Transformation
        {
            public void onPartitionClose()
            {
                // asserting this only now because it bothers UnfilteredPartitionIterators.Serializer (which might be used
                // under the provided DataIter) if hasNext() is called before the previously returned iterator hasn't been fully consumed.
                boolean hadNext = iter.hasNext();
                iter.close();
                assert !hadNext;
            }
        }
        return Transformation.apply(toReturn, new Close());
    }

    @SuppressWarnings("resource") // The created resources are returned right away
    public static PartitionIterator concat(final List<PartitionIterator> iterators)
    {
        if (iterators.size() == 1)
            return iterators.get(0);

        class Extend implements MorePartitions<PartitionIterator>
        {
            int i = 0;
            public PartitionIterator moreContents()
            {
                if (i >= iterators.size())
                    return null;
                return iterators.get(i++);
            }
        }

        return MorePartitions.extend(EmptyIterators.partition(), new Extend());
    }

    public static PartitionIterator singletonIterator(RowIterator iterator)
    {
        return new SingletonPartitionIterator(iterator);
    }

    public static void consume(PartitionIterator iterator)
    {
        while (iterator.hasNext())
        {
            try (RowIterator partition = iterator.next())
            {
                while (partition.hasNext())
                    partition.next();
            }
        }
    }

    /**
     * Consumes all rows in the next partition of the provided partition iterator.
     */
    public static void consumeNext(PartitionIterator iterator)
    {
        if (iterator.hasNext())
        {
            try (RowIterator partition = iterator.next())
            {
                while (partition.hasNext())
                    partition.next();
            }
        }
    }

    /**
     * Wraps the provided iterator so it logs the returned rows for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    @SuppressWarnings("resource") // The created resources are returned right away
    public static PartitionIterator loggingIterator(PartitionIterator iterator, final String id)
    {
        class Logger extends Transformation<RowIterator>
        {
            public RowIterator applyToPartition(RowIterator partition)
            {
                return RowIterators.loggingIterator(partition, id);
            }
        }
        return Transformation.apply(iterator, new Logger());
    }

    /**
     * Wraps the provided iterator to run a specified action on close. Note that the action will be
     * run even if closure of the provided iterator throws an exception.
     */
    public static PartitionIterator doOnClose(PartitionIterator delegate, Runnable action)
    {
        return new PartitionIterator()
        {
            public void close()
            {
                try
                {
                    delegate.close();
                }
                finally
                {
                    action.run();
                }
            }

            public boolean hasNext()
            {
                return delegate.hasNext();
            }

            public RowIterator next()
            {
                return delegate.next();
            }
        };
    }

    /**
     * Wraps the provided iterator to run a specified actions whenever a new partition or row is iterated over.
     * The resulting iterator is tolerant to the provided actions throwing exceptions.
     * The actions are allowed to fail and won't stop iteration, but that fact will be logged on ERROR level.
     *
     * The wrapper iterators do not delegate Object class methods to the wrapped ones (PartitionIterator and RowIterator)
     *
     * @param delegate the iterator to wrap
     * @param onPartition the action to run when a new partition is iterated over
     * @param onStaticRow the action to run when the partition has a static row
     * @param onRow the action to run when a new row is iterated over
     */
    public static PartitionIterator filteredRowTrackingIterator(PartitionIterator delegate,
                                                                Consumer<DecoratedKey> onPartition,
                                                                Consumer<Row> onStaticRow,
                                                                Consumer<Row> onRow)
    {
        return new PartitionIterator()
        {
            public void close()
            {
                delegate.close();
            }

            public boolean hasNext()
            {
                return delegate.hasNext();
            }

            public RowIterator next()
            {
                RowIterator next = delegate.next();
                try
                {
                    onPartition.accept(next.partitionKey());
                    if (!next.staticRow().isEmpty())
                    {
                        onStaticRow.accept(next.staticRow());
                    }
                }
                catch (Throwable t)
                {
                    NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 60, TimeUnit.SECONDS,
                                     "Tracking callback for read rows failed on new partition {}", next.partitionKey(), t);
                }
                return new RowTrackingIterator(next, onRow);
            }
        };
    }


    private static class SingletonPartitionIterator extends AbstractIterator<RowIterator> implements PartitionIterator
    {
        private final RowIterator iterator;
        private boolean returned;

        private SingletonPartitionIterator(RowIterator iterator)
        {
            this.iterator = iterator;
        }

        protected RowIterator computeNext()
        {
            if (returned)
                return endOfData();

            returned = true;
            return iterator;
        }

        public void close()
        {
            iterator.close();
        }
    }

    private static class RowTrackingIterator implements RowIterator
    {
        private final RowIterator delegate;
        private final Consumer<Row> onRow;

        RowTrackingIterator(RowIterator delegate, Consumer<Row> onRow)
        {
            this.delegate = delegate;
            this.onRow = onRow;
        }

        @Override
        public TableMetadata metadata()
        {
            return delegate.metadata();
        }

        @Override
        public boolean isReverseOrder()
        {
            return delegate.isReverseOrder();
        }

        @Override
        public RegularAndStaticColumns columns()
        {
            return delegate.columns();
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return delegate.partitionKey();
        }

        @Override
        public Row staticRow()
        {
            return delegate.staticRow();
        }

        @Override
        public boolean hasNext()
        {
            return delegate.hasNext();
        }

        @Override
        public Row next()
        {
            Row next = delegate.next();
            try
            {
                onRow.accept(next);
            }
            catch (Throwable t)
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 60, TimeUnit.SECONDS,
                                 "Tracking callback for read rows failed on row {}", next, t);

            }
            return next;
        }

        @Override
        public void remove()
        {
            delegate.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super Row> action)
        {
            delegate.forEachRemaining(action);
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public boolean isEmpty()
        {
            return delegate.isEmpty();
        }
    }

}
