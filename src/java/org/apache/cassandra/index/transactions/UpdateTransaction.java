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
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.rows.Row;

/**
 * Handling of index updates on the write path.
 *
 * Instances of an UpdateTransaction are scoped to a single partition update
 * A new instance is used for every write, obtained from the
 * newUpdateTransaction(PartitionUpdate) method. Likewise, a single
 * CleanupTransaction instance is used for each partition processed during a
 * compaction or cleanup.
 *
 * We make certain guarantees about the lifecycle of each UpdateTransaction
 * instance. Namely that start() will be called before any other method, and
 * commit() will be called at the end of the update.
 * Each instance is initialized with 1..many Index.Indexer instances, one per
 * registered Index. As with the transaction itself, these are scoped to a
 * specific partition update, so implementations can be assured that all indexing
 * events they receive relate to the same logical operation.
 *
 * onPartitionDelete(), onRangeTombstone(), onInserted() and onUpdated()
 * calls may arrive in any order, but this should have no impact for the
 * Indexers being notified as any events delivered to a single instance
 * necessarily relate to a single partition.
 *
 * The typical sequence of events during a Memtable update would be:
 * start()                       -- no-op, used to notify Indexers of the start of the transaction
 * onPartitionDeletion(dt)       -- if the PartitionUpdate implies one
 * onRangeTombstone(rt)*         -- for each in the PartitionUpdate, if any
 *
 * then:
 * onInserted(row)*              -- called for each Row not already present in the Memtable
 * onUpdated(existing, updated)* -- called for any Row in the update for where a version was already present
 *                                  in the Memtable. It's important to note here that existing is the previous
 *                                  row from the Memtable and updated is the final version replacing it. It is
 *                                  *not* the incoming row, but the result of merging the incoming and existing
 *                                  rows.
 * commit()                      -- finally, finish is called when the new Partition is swapped into the Memtable
 */
public interface UpdateTransaction extends IndexTransaction
{
    void onPartitionDeletion(DeletionTime deletionTime);
    void onRangeTombstone(RangeTombstone rangeTombstone);
    void onInserted(Row row);
    void onUpdated(Row existing, Row updated);

    UpdateTransaction NO_OP = new UpdateTransaction()
    {
        public void start(){}
        public void onPartitionDeletion(DeletionTime deletionTime){}
        public void onRangeTombstone(RangeTombstone rangeTombstone){}
        public void onInserted(Row row){}
        public void onUpdated(Row existing, Row updated){}
        public void commit(){}
    };
}
