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

package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;

/**
 * Common data access interface for sstables and memtables.
 */
public interface UnfilteredSource
{
    /**
     * Returns a row iterator for the given partition, applying the specified row and column filters.
     *
     * @param key the partition key
     * @param slices the row ranges to return
     * @param columnFilter filter to apply to all returned partitions
     * @param reversed true if the content should be returned in reverse order
     * @param listener a listener used to handle internal read events
     */
    UnfilteredRowIterator rowIterator(DecoratedKey key,
                                      Slices slices,
                                      ColumnFilter columnFilter,
                                      boolean reversed,
                                      SSTableReadsListener listener);

    default UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        return rowIterator(key, Slices.ALL, ColumnFilter.NONE, false, SSTableReadsListener.NOOP_LISTENER);
    }

    /**
     * Returns a partition iterator for the given data range.
     *
     * @param columnFilter filter to apply to all returned partitions
     * @param dataRange the partition and clustering range queried
     * @param listener a listener used to handle internal read events
     */
    UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter,
                                                  DataRange dataRange,
                                                  SSTableReadsListener listener);

    /** Minimum timestamp of all stored data */
    long getMinTimestamp();

    /** Minimum local deletion time in the memtable */
    long getMinLocalDeletionTime();
}
