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

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tracing.Tracing;

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
        BLOOM_FILTER("Bloom filter allows skipping sstable {}"),
        MIN_MAX_KEYS("Check against min and max keys allows skipping sstable {}"),
        PARTITION_INDEX_LOOKUP("Partition index lookup allows skipping sstable {}"),
        INDEX_ENTRY_NOT_FOUND("Partition index lookup complete (bloom filter false positive) for sstable {}");

        private final String message;

        SkippingReason(String message)
        {
            this.message = message;
        }

        public void trace(Descriptor descriptor)
        {
            Tracing.trace(message, descriptor.id);
        }
    }

    /**
     * The reasons for selecting an SSTable
     */
    enum SelectionReason
    {
        KEY_CACHE_HIT("Key cache hit for sstable {}, size = {}"),
        INDEX_ENTRY_FOUND("Partition index found for sstable {}, size = {}");

        private final String message;

        SelectionReason(String message)
        {
            this.message = message;
        }

        public void trace(Descriptor descriptor, AbstractRowIndexEntry entry)
        {
            Tracing.trace(message, descriptor.id, entry.blockCount());
        }
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
    default void onSSTableSelected(SSTableReader sstable, SelectionReason reason)
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
