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

    @Override
    public boolean isExpiring()
    {
        return localExpirationTime != NO_EXPIRATION_TIME;
    }

    @Override
    public boolean isExpired()
    {
        return ttl == EXPIRED_LIVENESS_TTL;
    }

    /**
     * {@link org.apache.cassandra.db.rows.AbstractCell#isTombstone()}
     */
    public boolean isTombstone()
    {
        return localExpirationTime != NO_EXPIRATION_TIME && ttl == NO_TTL;
    }

    @Override
    public boolean isLive(long nowInSec)
    {
        return localExpirationTime == NO_EXPIRATION_TIME || ttl != NO_TTL && !isExpired(nowInSec);
    }

    public boolean isExpired(long nowInSec)
    {
        return nowInSec >= localExpirationTime;
    }

    public void ttlToTombstone()
    {
        // LET/LDT is now the time the TTL would have expired
        localExpirationTime = localExpirationTime - ttl;
        ttl = NO_TTL;
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
