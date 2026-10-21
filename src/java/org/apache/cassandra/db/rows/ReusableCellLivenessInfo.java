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

package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.LivenessInfo;

/**
 * A mutable {@link CellLivenessInfo}, for readers that walk cells without materializing a
 * {@link Cell}.
 *
 * It does not implement {@link LivenessInfo}, so the row-liveness operations are unreachable
 * rather than merely unused. {@code supersedes} is the one that matters: it inverts the cell
 * rule, which prefers a tombstone to a live cell at the same timestamp.
 */
public class ReusableCellLivenessInfo implements CellLivenessInfo
{
    private long timestamp = LivenessInfo.NO_TIMESTAMP;
    private int ttl = Cell.NO_TTL;
    private long localDeletionTime = Cell.NO_DELETION_TIME;

    @Override
    public long timestamp()
    {
        return timestamp;
    }

    @Override
    public int ttl()
    {
        return ttl;
    }

    @Override
    public long localDeletionTime()
    {
        return localDeletionTime;
    }

    @Override
    public boolean isExpiring()
    {
        return ttl != Cell.NO_TTL;
    }

    @Override
    public boolean isTombstone()
    {
        return localDeletionTime != Cell.NO_DELETION_TIME && ttl == Cell.NO_TTL;
    }

    @Override
    public boolean isLive(long nowInSec)
    {
        return CellLivenessInfo.isLive(nowInSec, localDeletionTime, ttl);
    }

    /**
     * Whether the TTL has lapsed at {@code nowInSec}. Cell-only, and unrelated to the row contract's no-arg
     * {@code isExpired()}, which asks whether the liveness carries the view-maintenance
     * {@code EXPIRED_LIVENESS_TTL} marker. The two shared a name and an arity-only distinction before the
     * contracts were split.
     */
    public boolean isExpired(long nowInSec)
    {
        return nowInSec >= localDeletionTime;
    }

    /** Converts an expired expiring cell into the tombstone it becomes: the deletion time moves back to the
     * second the TTL started from, and the TTL is dropped. Mirrors {@code AbstractCell.purge}'s
     * {@code BufferCell.tombstone(column, timestamp(), localDeletionTime() - ttl(), path())}. */
    public void ttlToTombstone()
    {
        localDeletionTime = localDeletionTime - ttl;
        ttl = Cell.NO_TTL;
    }

    public void reset(long timestamp, int ttl, long localDeletionTime)
    {
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTime = localDeletionTime;
    }

    /**
     * Rendered into corrupt-sstable reports by {@code UnfilteredValidation.handleInvalid}, so this is a
     * diagnostic and not cosmetic: without it such a report carries an identity hash.
     */
    @Override
    public String toString()
    {
        return "ReusableCellLivenessInfo{"
               + ((timestamp == LivenessInfo.NO_TIMESTAMP && ttl == Cell.NO_TTL && localDeletionTime == Cell.NO_DELETION_TIME)
                  ? "NONE }"
                  : "timestamp=" + (timestamp == LivenessInfo.NO_TIMESTAMP ? "NO_TIMESTAMP" : timestamp) +
                    ", ttl=" + (ttl == Cell.NO_TTL ? "NO_TTL" : ttl) +
                    ", localDeletionTime=" + (localDeletionTime == Cell.NO_DELETION_TIME ? "NO_DELETION_TIME" : localDeletionTime) +
                    '}');
    }
}
