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

// TODO: maybe flatten into descriptor classes?
public class ReusableLivenessInfo implements LivenessInfo
{
    private int ttl = NO_TTL;
    private long localExpirationTime = NO_EXPIRATION_TIME;
    private long timestamp = NO_TIMESTAMP;

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
    public long localExpirationTime()
    {
        return localExpirationTime;
    }

    /**
     * "Has a TTL", as specified by {@link LivenessInfo#isExpiring()} and implemented by every other
     * liveness type ({@link LivenessInfo.ExpiringLivenessInfo}, {@link org.apache.cassandra.db.rows.AbstractCell#isExpiring()}).
     * Note this is NOT "has an expiration time": cell liveness held here can be a tombstone, which
     * carries a local deletion time with no TTL.
     */
    @Override
    public boolean isExpiring()
    {
        return ttl != NO_TTL;
    }

    @Override
    public boolean isExpired()
    {
        return ttl == EXPIRED_LIVENESS_TTL;
    }

    /**
     * The ROW contract, matching the row references: empty liveness is not live
     * ({@link LivenessInfo#EMPTY}, whose {@code isLive} is {@code !isEmpty()}), an
     * {@link #EXPIRED_LIVENESS_TTL} marker is never live ({@link LivenessInfo.ExpiredLivenessInfo}), and
     * otherwise an expiring liveness is live until its expiration second.
     *
     * It differs from the cell contract on three inputs, which is why cell liveness has its own type;
     * {@code ReusableCellLivenessInfo} carries that reading.
     *
     * The old code here read the CELL contract instead, treating an {@code EXPIRED_LIVENESS_TTL} marker
     * (view maintenance's PK-shadow tombstone) as live until its expiration second rather than never. The
     * flip is inert on this branch: the marker's {@code localExpirationTime} is stamped with
     * {@code nowInSec} when the view mutation is applied, and compaction only observes the row after that
     * mutation has been applied and flushed, so compaction-time {@code nowInSec} is never less than the
     * stamped one — the old read could never actually return {@code true} for it either. Exercised by
     * {@code MaterializedViewDifferentialCompactionTest}.
     */
    @Override
    public boolean isLive(long nowInSec)
    {
        if (isEmpty() || isExpired())
            return false;
        return !isExpiring() || nowInSec < localExpirationTime;
    }

    public void reset(long timestamp, int ttl, long localExpirationTime)
    {
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localExpirationTime = localExpirationTime;
    }

    @Override
    public String toString()
    {
        return "ReusableLivenessInfo{" + ((timestamp == NO_TIMESTAMP && ttl == NO_TTL && localExpirationTime == NO_EXPIRATION_TIME) ? "NONE }" :
               "timestamp=" + (timestamp == NO_TIMESTAMP ? "NO_TIMESTAMP" : timestamp) +
               ", ttl=" + (ttl == NO_TTL ? "NO_TTL" : ttl)  +
               ", localExpirationTime=" + (localExpirationTime == NO_EXPIRATION_TIME ? "NO_EXPIRATION_TIME" : localExpirationTime) +
               '}');
    }
}
