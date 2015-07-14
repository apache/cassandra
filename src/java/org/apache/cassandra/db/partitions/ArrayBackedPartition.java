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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;

public class ArrayBackedPartition extends AbstractThreadUnsafePartition
{
    private final Row staticRow;
    private final DeletionInfo deletionInfo;
    private final EncodingStats stats;

    protected ArrayBackedPartition(CFMetaData metadata,
                                   DecoratedKey partitionKey,
                                   PartitionColumns columns,
                                   Row staticRow,
                                   List<Row> rows,
                                   DeletionInfo deletionInfo,
                                   EncodingStats stats)
    {
        super(metadata, partitionKey, columns, rows);
        this.staticRow = staticRow;
        this.deletionInfo = deletionInfo;
        this.stats = stats;
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
        return create(iterator, 16);
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
        CFMetaData metadata = iterator.metadata();
        boolean reversed = iterator.isReverseOrder();

        List<Row> rows = new ArrayList<>(initialRowCapacity);
        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(iterator.partitionLevelDeletion(), metadata.comparator, reversed);

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                rows.add((Row)unfiltered);
            else
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
        }

        if (reversed)
            Collections.reverse(rows);

        return new ArrayBackedPartition(metadata, iterator.partitionKey(), iterator.columns(), iterator.staticRow(), rows, deletionBuilder.build(), iterator.stats());
    }

    protected boolean canHaveShadowedData()
    {
        // We only create instances from UnfilteredRowIterator that don't have shadowed data
        return false;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public DeletionInfo deletionInfo()
    {
        return deletionInfo;
    }

    public EncodingStats stats()
    {
        return stats;
    }
}
