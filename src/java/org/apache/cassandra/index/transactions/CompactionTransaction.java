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

import org.apache.cassandra.db.rows.Row;

/**
 * Performs garbage collection of stale index entries during a regular compaction.
 *
 * A CompactionTransaction is concerned with cleaning up stale index entries.
 * When multiple versions of a row are compacted, the CompactionTransaction is
 * notified of the versions being merged, which it diffs against the merge result.
 *
 * Instances are currently scoped to a single row within a partition, but this could be improved to batch process
 * multiple rows within a single partition.
 */
public interface CompactionTransaction extends IndexTransaction
{
    void onRowMerge(Row merged, Row...versions);

    CompactionTransaction NO_OP = new CompactionTransaction()
    {
        public void start(){}
        public void onRowMerge(Row merged, Row...versions){}
        public void commit(){}
    };
}
