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

package org.apache.cassandra.replication;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableId;

/**
 * Tracks offsets of clean (i.e. memtable->sstable flushed) and dirty (i.e. not yet durably persisted in sstable)
 * allocations.
 *
 * Mutations in segments marked as clean do not need to be replayed.
 */
public interface SegmentStateTracker
{
    long segmentId();

    /**
     * Removes all clean (i.e. memtable -> sstable flushed) from dirty interval. If metadata tracking for all intervals of all tables
     * are clean, returns true. False otherwise.
     */
    boolean removeCleanFromDirty();
    boolean isClean();

    void markDirty(TableId tableId, long segmentId, int position);
    void markDirty(TableId metadata, CommitLogPosition ptr);

    void markClean(TableId metadata, CommitLogPosition lowerBound, CommitLogPosition upperBound);
}
