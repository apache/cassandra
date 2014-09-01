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

import java.util.Objects;
import java.security.MessageDigest;

import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Base abstract class for {@code LivenessInfo} implementations.
 *
 * All {@code LivenessInfo} should extends this class unless it has a very
 * good reason not to.
 */
public abstract class AbstractLivenessInfo implements LivenessInfo
{
    public boolean hasTimestamp()
    {
        return timestamp() != NO_TIMESTAMP;
    }

    public boolean hasTTL()
    {
        return ttl() != NO_TTL;
    }

    public boolean hasLocalDeletionTime()
    {
        return localDeletionTime() != NO_DELETION_TIME;
    }

    public int remainingTTL(int nowInSec)
    {
        if (!hasTTL())
            return -1;

        int remaining = localDeletionTime() - nowInSec;
        return remaining >= 0 ? remaining : -1;
    }

    public boolean isLive(int nowInSec)
    {
        // Note that we don't rely on localDeletionTime() only because if we were to, we
        // could potentially consider a tombstone as a live cell (due to time skew). So
        // if a cell has a local deletion time and no ttl, it's a tombstone and consider
        // dead no matter what it's actual local deletion value is.
        return hasTimestamp() && (!hasLocalDeletionTime() || (hasTTL() && nowInSec < localDeletionTime()));
    }

    public void digest(MessageDigest digest)
    {
        FBUtilities.updateWithLong(digest, timestamp());
        FBUtilities.updateWithInt(digest, localDeletionTime());
        FBUtilities.updateWithInt(digest, ttl());
    }

    public void validate()
    {
        if (ttl() < 0)
            throw new MarshalException("A TTL should not be negative");
        if (localDeletionTime() < 0)
            throw new MarshalException("A local deletion time should not be negative");
        if (hasTTL() && !hasLocalDeletionTime())
            throw new MarshalException("Shoud not have a TTL without an associated local deletion time");
    }

    public int dataSize()
    {
        int size = 0;
        if (hasTimestamp())
            size += TypeSizes.NATIVE.sizeof(timestamp());
        if (hasTTL())
            size += TypeSizes.NATIVE.sizeof(ttl());
        if (hasLocalDeletionTime())
            size += TypeSizes.NATIVE.sizeof(localDeletionTime());
        return size;

    }

    public boolean supersedes(LivenessInfo other)
    {
        return timestamp() > other.timestamp();
    }

    public LivenessInfo mergeWith(LivenessInfo other)
    {
        return supersedes(other) ? this : other;
    }

    public LivenessInfo takeAlias()
    {
        return new SimpleLivenessInfo(timestamp(), ttl(), localDeletionTime());
    };

    public LivenessInfo withUpdatedTimestamp(long newTimestamp)
    {
        if (!hasTimestamp())
            return this;

        return new SimpleLivenessInfo(newTimestamp, ttl(), localDeletionTime());
    }

    public boolean isPurgeable(long maxPurgeableTimestamp, int gcBefore)
    {
        return timestamp() < maxPurgeableTimestamp && localDeletionTime() < gcBefore;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        boolean needSpace = false;
        if (hasTimestamp())
        {
            sb.append("ts=").append(timestamp());
            needSpace = true;
        }
        if (hasTTL())
        {
            sb.append(needSpace ? ' ' : "").append("ttl=").append(ttl());
            needSpace = true;
        }
        if (hasLocalDeletionTime())
            sb.append(needSpace ? ' ' : "").append("ldt=").append(localDeletionTime());
        sb.append(']');
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof LivenessInfo))
            return false;

        LivenessInfo that = (LivenessInfo)other;
        return this.timestamp() == that.timestamp()
            && this.ttl() == that.ttl()
            && this.localDeletionTime() == that.localDeletionTime();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timestamp(), ttl(), localDeletionTime());
    }
}
