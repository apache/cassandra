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

package org.apache.cassandra.index.transactions;

/**
 * Base interface for the handling of index updates.
 * There are 3 types of transaction where indexes are updated to stay in sync with the base table, each represented by
 * a subinterface:
 * * {@code UpdateTransaction}
 *   Used on the regular write path and when indexing newly acquired SSTables from streaming or sideloading. This type
 *   of transaction may include both row inserts and updates to rows previously existing in the base Memtable. Instances
 *   are scoped to a single partition update and are obtained from the factory method
 *   {@code SecondaryIndexManager#newUpdateTransaction}
 *
 * * {@code CompactionTransaction}
 *   Used during compaction when stale entries which have been superceded are cleaned up from the index. As rows in a
 *   partition are merged during the compaction, index entries for any purged rows are cleaned from the index to
 *   compensate for the fact that they may not have been removed at write time if the data in the base table had been
 *   already flushed to disk (and so was processed as an insert, not an update by the UpdateTransaction). These
 *   transactions are currently scoped to a single row within a partition, but this could be improved to batch process
 *   multiple rows within a single partition.
 *
 * * @{code CleanupTransaction}
 *   During cleanup no merging is required, the only thing to do is to notify indexes of the partitions being removed,
 *   along with the rows within those partitions. Like with compaction, these transactions are currently scoped to a
 *   single row within a partition, but this could be improved with batching.
 */
public interface IndexTransaction
{
    /**
     * Used to differentiate between type of index transaction when obtaining
     * a handler from Index implementations.
     */
    public enum Type
    {
        UPDATE, COMPACTION, CLEANUP
    }

    void start();
    void commit();
}
