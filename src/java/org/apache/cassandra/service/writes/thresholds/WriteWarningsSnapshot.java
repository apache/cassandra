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

package org.apache.cassandra.service.writes.thresholds;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.service.thresholds.ThresholdCounter;

/**
 * Immutable snapshot of write warnings.
 * Simpler than WarningsSnapshot since writes never abort (warnings only).
 */
public class WriteWarningsSnapshot
{
    private static final WriteWarningsSnapshot EMPTY = new WriteWarningsSnapshot(ThresholdCounter.empty(), ThresholdCounter.empty());

    public final ThresholdCounter writeSize;
    public final ThresholdCounter writeTombstone;

    private WriteWarningsSnapshot(ThresholdCounter writeSize, ThresholdCounter writeTombstone)
    {
        this.writeSize = writeSize;
        this.writeTombstone = writeTombstone;
    }

    public static WriteWarningsSnapshot empty()
    {
        return EMPTY;
    }

    public static WriteWarningsSnapshot create(ThresholdCounter writeSize, ThresholdCounter writeTombstone)
    {
        if (writeSize == ThresholdCounter.empty() && writeTombstone == ThresholdCounter.empty())
            return EMPTY;
        return new WriteWarningsSnapshot(writeSize, writeTombstone);
    }

    public boolean isEmpty()
    {
        return this == EMPTY;
    }

    public WriteWarningsSnapshot merge(WriteWarningsSnapshot other)
    {
        if (other == null || other == EMPTY)
            return this;
        return WriteWarningsSnapshot.create(
        writeSize.merge(other.writeSize),
        writeTombstone.merge(other.writeTombstone)
        );
    }

    @VisibleForTesting
    public static String writeSizeWarnMessage(int nodes, long bytes)
    {
        return String.format("%d nodes detected write to large partition; estimated size is %d bytes (see write_size_warn_threshold)",
                             nodes, bytes);
    }

    @VisibleForTesting
    public static String writeTombstoneWarnMessage(int nodes, long tombstones)
    {
        return String.format("%d nodes detected write to partition with many tombstones; estimated count is %d (see write_tombstone_warn_threshold)",
                             nodes, tombstones);
    }
}