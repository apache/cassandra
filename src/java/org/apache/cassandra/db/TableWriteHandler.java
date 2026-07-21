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

package org.apache.cassandra.db;

import org.apache.cassandra.db.partitions.PartitionUpdate;

public interface TableWriteHandler
{
    /**
     * Apply {@code update} to the table's memtable.
     *
     * @param update       the partition update to write
     * @param context      write context for the current mutation
     * @param updateIndexes whether secondary indexes should be updated.
     *                      When {@code false} this is a <em>nested</em> write (e.g. a legacy 2i index-table write
     *                      issued from {@code indexer.onInserted()} under the base table's memtable-internal locks).
     *                      Nested writes bypass the memtable pool's room-wait gate to avoid deadlocking the flush
     *                      write-barrier; as a consequence they may allocate slightly beyond the gated limit
     *                      (CASSANDRA-21019).
     */
    void write(PartitionUpdate update, WriteContext context, boolean updateIndexes);
}
