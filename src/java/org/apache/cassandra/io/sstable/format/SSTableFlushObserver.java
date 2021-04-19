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
package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;

/**
 * Observer for events in the lifecycle of writing out an sstable.
 */
public interface SSTableFlushObserver
{
    /**
     * Called before writing any data to the sstable.
     */
    void begin();

    /**
     * Called when a new partition in being written to the sstable,
     * but before any cells are processed (see {@link #nextUnfilteredCluster(Unfiltered)}).
     *
     * @param key The key being appended to SSTable.
     * @param position The position of the key in the component preferred for reading keys
     */
    void startPartition(DecoratedKey key, long position);

    /**
     * Called when the deletion time of a partition is written to the sstable.
     *
     * Will be preceded by a call to {@link #startPartition(DecoratedKey, long)},
     * and the deletion time should be assumed to belong to that partition.
     *
     * @param deletionTime the partition-level deletion time being written to the SSTable
     * @param position the position of the written deletion time in the data file,
     * as required by {@link SSTableReader#partitionLevelDeletionAt(long)}
     */
    void partitionLevelDeletion(DeletionTime deletionTime, long position);

    /**
     * Called when the static row of a partition is written to the sstable.
     *
     * Will be preceded by a call to {@link #startPartition(DecoratedKey, long)},
     * and the static row should be assumed to belong to that partition.
     *
     * @param staticRow the static row being written to the SSTable
     * @param position the position of the written static row in the data file,
     * as required by {@link SSTableReader#staticRowAt(long, ColumnFilter)}
     */
    void staticRow(Row staticRow, long position);

    /**
     * Called after an unfiltered is written to the sstable.
     *
     * Will be preceded by a call to {@link #startPartition(DecoratedKey, long)},
     * and the unfiltered should be assumed to belong to that partition.
     *
     * Implementations overriding {@link #nextUnfilteredCluster(Unfiltered, long)} shouldn't implement this method
     * since only one of the two methods is required.
     *
     * @param unfiltered the unfiltered being written to the SSTable
     */
    default void nextUnfilteredCluster(Unfiltered unfiltered)
    {
    }

    /**
     * Called after an unfiltered is written to the sstable.
     *
     * Will be preceded by a call to {@link #startPartition(DecoratedKey, long)},
     * and the unfiltered should be assumed to belong to that partition.
     *
     * Implementations overriding {@link #nextUnfilteredCluster(Unfiltered)} shouldn't implement this method
     * since only one of the two methods is required.
     *
     * @param unfiltered the unfiltered being written to the SSTable
     * @param position the position of the written unfiltered in the data file,
     * as required by {@link SSTableReader#clusteringAt(long)}
     * and {@link SSTableReader#unfilteredAt(long, ColumnFilter)}
     */
    default void nextUnfilteredCluster(Unfiltered unfiltered, long position)
    {
        nextUnfilteredCluster(unfiltered);
    }

    /**
     * Called when all data is written to the file and it's ready to be finished up.
     */
    void complete();

    /**
     * Clean up resources on error. There should be no side effects if called multiple times.
     */
    default void abort(Throwable accumulator) {}
}