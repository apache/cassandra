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

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.Row;

/**
 * Performs garbage collection of index entries during a cleanup.
 *
 * Notifies registered indexers of each partition being removed and
 *
 * Compaction and Cleanup are somewhat simpler than dealing with incoming writes,
 * being only concerned with cleaning up stale index entries.
 *
 * When multiple versions of a row are compacted, the CleanupTransaction is
 * notified of the versions being merged, which it diffs against the merge result
 * and forwards to the registered Index.Indexer instances when on commit.
 *
 * Instances are currently scoped to a single row within a partition, but this could be improved to batch process
 * multiple rows within a single partition.
 */
public interface CleanupTransaction extends IndexTransaction
{

    void onPartitionDeletion(DeletionTime deletionTime);
    void onRowDelete(Row row);

    CleanupTransaction NO_OP = new CleanupTransaction()
    {
        public void start(){}
        public void onPartitionDeletion(DeletionTime deletionTime){}
        public void onRowDelete(Row row){}
        public void commit(){}
    };
}
