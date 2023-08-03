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
package org.apache.cassandra.io.sstable;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.SSTableReader;

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
     * @param key                the key being appended to SSTable.
     * @param keyPosition        the position of the key in the SSTable data file
     * @param keyPositionForSASI SSTable format specific key position for storage attached indexes, it can be
     *                           in data file or in some index file. It is the same position as returned by
     *                           {@link KeyReader#keyPositionForSecondaryIndex()} for the same format, and the same
     *                           position as expected by {@link SSTableReader#keyAtPositionFromSecondaryIndex(long)}.
     */
    void startPartition(DecoratedKey key, long keyPosition, long keyPositionForSASI);

    /**
     * Called when a static row is being written to the sstable. If static columns are present in the table, it is called
     * after {@link #startPartition(DecoratedKey, long, long)} and before any calls to {@link #nextUnfilteredCluster(Unfiltered)}.
     *
     * @param staticRow static row appended to the sstable, can be empty, may not be {@code null}
     */
    void staticRow(Row staticRow);

    /**
     * Called after an unfiltered is written to the sstable.
     *
     * Will be preceded by a call to {@link #startPartition(DecoratedKey, long, long)},
     * and the unfiltered should be assumed to belong to that partition.
     *
     * @param unfiltered the unfiltered being written to the SSTable
     */
    void nextUnfilteredCluster(Unfiltered unfiltered);

    /**
     * Called when all data is written to the file and it's ready to be finished up.
     */
    void complete();

    /**
     * Clean up resources on error. There should be no side effects if called multiple times.
     */
    @SuppressWarnings("unused")
    default void abort(Throwable accumulator) {}
}