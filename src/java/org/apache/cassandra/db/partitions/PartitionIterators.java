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

import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.utils.AbstractIterator;

import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.rows.*;

public abstract class PartitionIterators
{
    private PartitionIterators() {}

    @SuppressWarnings("resource") // The created resources are returned right away
    public static RowIterator getOnlyElement(final PartitionIterator iter, SinglePartitionReadCommand command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        RowIterator toReturn = iter.hasNext()
                             ? iter.next()
                             : EmptyIterators.row(command.metadata(),
                                                  command.partitionKey(),
                                                  command.clusteringIndexFilter().isReversed());

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
            int i = 1;
            public PartitionIterator moreContents()
            {
                if (i >= iterators.size())
                    return null;
                return iterators.get(i++);
            }
        }
        return MorePartitions.extend(iterators.get(0), new Extend());
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
        class Logger extends Transformation<RowIterator>
        {
            public RowIterator applyToPartition(RowIterator partition)
            {
                return RowIterators.loggingIterator(partition, id);
            }
        }
        return Transformation.apply(iterator, new Logger());
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
