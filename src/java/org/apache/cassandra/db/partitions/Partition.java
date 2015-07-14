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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.utils.SearchIterator;

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
    public CFMetaData metadata();
    public DecoratedKey partitionKey();
    public DeletionTime partitionLevelDeletion();

    public PartitionColumns columns();

    public EncodingStats stats();

    /**
     * Whether the partition object has no informations at all, including any deletion informations.
     */
    public boolean isEmpty();

    /**
     * Returns the row corresponding to the provided clustering, or null if there is not such row.
     */
    public Row getRow(Clustering clustering);

    /**
     * Returns an iterator that allows to search specific rows efficiently.
     */
    public SearchIterator<Clustering, Row> searchIterator(ColumnFilter columns, boolean reversed);

    /**
     * Returns an UnfilteredRowIterator over all the rows/RT contained by this partition.
     */
    public UnfilteredRowIterator unfilteredIterator();

    /**
     * Returns an UnfilteredRowIterator over the rows/RT contained by this partition
     * selected by the provided slices.
     */
    public UnfilteredRowIterator unfilteredIterator(ColumnFilter columns, Slices slices, boolean reversed);
}
