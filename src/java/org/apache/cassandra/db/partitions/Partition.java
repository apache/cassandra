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

import java.util.Iterator;
import java.util.NavigableSet;

import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * In-memory representation of a Partition.
 *
 * Note that most of the storage engine works through iterators (UnfilteredPartitionIterator) to
 * avoid "materializing" a full partition/query response in memory as much as possible,
 * and so Partition objects should be use as sparingly as possible. There is a couple
 * of cases where we do need to represent partition in-memory (memtables and row cache).
 */
public interface Partition
{
    public TableMetadata metadata();

    public DecoratedKey partitionKey();
    public DeletionTime partitionLevelDeletion();

    public RegularAndStaticColumns columns();

    public EncodingStats stats();

    /**
     * Whether the partition object has no informations at all, including any deletion informations.
     */
    public boolean isEmpty();

    /**
     * Whether the partition object has any rows, excluding the static row.
     * This may be false but partition still be non-empty if it has a deletion or a non-empty static row.
     */
    boolean hasRows();

    /**
     * Returns the number of rows in this partition, excluding the static row.
     */
    int rowCount();

    /**
     * Returns an iterator over the rows of this partition excluding the static row.
     */
    Iterator<Row> rowIterator();

    /**
     * Returns the collection of rows of this partition excluding the static row as an iterable.
     */
    default Iterable<Row> rows()
    {
        return this::rowIterator;
    }

    Row staticRow();

    /**
     * Returns the last non-static row in the partition.
     */
    Row lastRow();

    /**
     * Returns the row corresponding to the provided clustering, or null if there is not such row.
     *
     * @param clustering clustering key to search
     * @return row corresponding to the clustering, it's either null or non-empty row.
     */
    public @Nullable Row getRow(Clustering<?> clustering);

    /**
     * Returns an UnfilteredRowIterator over all the rows/RT contained by this partition.
     */
    public UnfilteredRowIterator unfilteredIterator();

    /**
     * Returns an UnfilteredRowIterator over the rows/RT contained by this partition
     * selected by the provided slices.
     */
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed);

    /**
     * Returns an UnfilteredRowIterator over the rows/RT contained by this partition
     * selected by the provided clusterings.
     */
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, NavigableSet<Clustering<?>> clusteringsInQueryOrder, boolean reversed);

    static String toString(Partition p)
    {
        return toString(p, true);
    }

    static String toString(Partition p, boolean includeFullDetails)
    {
        StringBuilder sb = new StringBuilder();
        if (includeFullDetails)
        {
            sb.append(String.format("[%s.%s] key=%s partition_deletion=%s columns=%s",
                                    p.metadata().keyspace,
                                    p.metadata().name,
                                    p.metadata().partitionKeyType.getString(p.partitionKey().getKey()),
                                    p.partitionLevelDeletion(),
                                    p.columns()));
        }
        else
        {
            sb.append("key=").append(p.metadata().partitionKeyType.getString(p.partitionKey().getKey()));
        }

        if (p.staticRow() != Rows.EMPTY_STATIC_ROW)
            sb.append("\n    ").append(p.staticRow().toString(p.metadata(), includeFullDetails));

        try (UnfilteredRowIterator iter = p.unfilteredIterator())
        {
            while (iter.hasNext())
                sb.append("\n    ").append(iter.next().toString(p.metadata(), includeFullDetails));
        }
        return sb.toString();
    }
}
