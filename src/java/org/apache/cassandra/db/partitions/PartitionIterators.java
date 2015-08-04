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
import java.security.MessageDigest;

import org.apache.cassandra.utils.AbstractIterator;

import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.FileUtils;

public abstract class PartitionIterators
{
    private PartitionIterators() {}

    public static final PartitionIterator EMPTY = new PartitionIterator()
    {
        public boolean hasNext()
        {
            return false;
        }

        public RowIterator next()
        {
            throw new NoSuchElementException();
        }

        public void remove()
        {
        }

        public void close()
        {
        }
    };

    @SuppressWarnings("resource") // The created resources are returned right away
    public static RowIterator getOnlyElement(final PartitionIterator iter, SinglePartitionReadCommand<?> command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        RowIterator toReturn = iter.hasNext()
                             ? iter.next()
                             : RowIterators.emptyIterator(command.metadata(),
                                                          command.partitionKey(),
                                                          command.clusteringIndexFilter().isReversed());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole PartitionIterator.
        return new WrappingRowIterator(toReturn)
        {
            public void close()
            {
                try
                {
                    super.close();
                }
                finally
                {
                    // asserting this only now because it bothers UnfilteredPartitionIterators.Serializer (which might be used
                    // under the provided DataIter) if hasNext() is called before the previously returned iterator hasn't been fully consumed.
                    assert !iter.hasNext();

                    iter.close();
                }
            }
        };
    }

    @SuppressWarnings("resource") // The created resources are returned right away
    public static PartitionIterator concat(final List<PartitionIterator> iterators)
    {
        if (iterators.size() == 1)
            return iterators.get(0);

        return new PartitionIterator()
        {
            private int idx = 0;

            public boolean hasNext()
            {
                while (idx < iterators.size())
                {
                    if (iterators.get(idx).hasNext())
                        return true;

                    ++idx;
                }
                return false;
            }

            public RowIterator next()
            {
                if (!hasNext())
                    throw new NoSuchElementException();
                return iterators.get(idx).next();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            public void close()
            {
                FileUtils.closeQuietly(iterators);
            }
        };
    }

    public static void digest(PartitionIterator iterator, MessageDigest digest)
    {
        while (iterator.hasNext())
        {
            try (RowIterator partition = iterator.next())
            {
                RowIterators.digest(partition, digest);
            }
        }
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
     * Wraps the provided iterator so it logs the returned rows for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    @SuppressWarnings("resource") // The created resources are returned right away
    public static PartitionIterator loggingIterator(PartitionIterator iterator, final String id)
    {
        return new WrappingPartitionIterator(iterator)
        {
            public RowIterator next()
            {
                return RowIterators.loggingIterator(super.next(), id);
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
}
