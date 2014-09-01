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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public class ArrayBackedPartition extends AbstractPartitionData
{
    protected ArrayBackedPartition(CFMetaData metadata,
                                   DecoratedKey partitionKey,
                                   DeletionTime deletionTime,
                                   PartitionColumns columns,
                                   int initialRowCapacity,
                                   boolean sortable)
    {
        super(metadata, partitionKey, deletionTime, columns, initialRowCapacity, sortable);
    }

    /**
     * Creates an {@code ArrayBackedPartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator to gather in memory.
     * @return the created partition.
     */
    public static ArrayBackedPartition create(UnfilteredRowIterator iterator)
    {
        return create(iterator, 4);
    }

    /**
     * Creates an {@code ArrayBackedPartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator to gather in memory.
     * @param initialRowCapacity sizing hint (in rows) to use for the created partition. It should ideally
     * correspond or be a good estimation of the number or rows in {@code iterator}.
     * @return the created partition.
     */
    public static ArrayBackedPartition create(UnfilteredRowIterator iterator, int initialRowCapacity)
    {
        ArrayBackedPartition partition = new ArrayBackedPartition(iterator.metadata(),
                                                                  iterator.partitionKey(),
                                                                  iterator.partitionLevelDeletion(),
                                                                  iterator.columns(),
                                                                  initialRowCapacity,
                                                                  iterator.isReverseOrder());

        partition.staticRow = iterator.staticRow().takeAlias();

        Writer writer = partition.new Writer(true);
        RangeTombstoneCollector markerCollector = partition.new RangeTombstoneCollector(iterator.isReverseOrder());

        copyAll(iterator, writer, markerCollector, partition);

        return partition;
    }

    protected static void copyAll(UnfilteredRowIterator iterator, Writer writer, RangeTombstoneCollector markerCollector, ArrayBackedPartition partition)
    {
        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                ((Row) unfiltered).copyTo(writer);
            else
                ((RangeTombstoneMarker) unfiltered).copyTo(markerCollector);
        }

        // A Partition (or more precisely AbstractPartitionData) always assumes that its data is in clustering
        // order. So if we've just added them in reverse clustering order, reverse them.
        if (iterator.isReverseOrder())
            partition.reverse();
    }
}
