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

import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener.SelectionReason;

/**
 * Listener for receiving notifications associated with reading SSTables.
 */
public interface SSTableReadsListener
{
    /**
     * The reasons for skipping an SSTable
     */
    enum SkippingReason
    {
        BLOOM_FILTER,
        MIN_MAX_KEYS,
        PARTITION_INDEX_LOOKUP,
        INDEX_ENTRY_NOT_FOUND;
    }

    /**
     * The reasons for selecting an SSTable
     */
    enum SelectionReason
    {
        KEY_CACHE_HIT,
        INDEX_ENTRY_FOUND;
    }

    /**
     * Listener that does nothing.
     */
    static final SSTableReadsListener NOOP_LISTENER = new SSTableReadsListener() {};

    /**
     * Handles notification that the specified SSTable has been skipped during a single partition query.
     *
     * @param sstable the SSTable reader
     * @param reason the reason for which the SSTable has been skipped
     */
    default void onSSTableSkipped(SSTableReader sstable, SkippingReason reason)
    {
    }

    /**
     * Handles notification that the specified SSTable has been selected during a single partition query.
     *
     * @param sstable the SSTable reader
     * @param indexEntry the index entry
     * @param reason the reason for which the SSTable has been selected
     */
    default void onSSTableSelected(SSTableReader sstable, RowIndexEntry<?> indexEntry, SelectionReason reason)
    {
    }

    /**
     * Handles notification that the specified SSTable is being scanned during a partition range query.
     *
     * @param sstable the SSTable reader of the SSTable being scanned.
     */
    default void onScanningStarted(SSTableReader sstable)
    {
    }
}
