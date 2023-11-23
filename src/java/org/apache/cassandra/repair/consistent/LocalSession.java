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

package org.apache.cassandra.repair.consistent;

import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.repair.SharedContext;

/**
 * Basically just a record of a local session. All of the local session logic is implemented in {@link LocalSessions}
 */
public class LocalSession extends ConsistentSession
{
    public final long startedAt;
    private volatile long lastUpdate;

    public LocalSession(Builder builder)
    {
        super(builder);
        this.startedAt = builder.startedAt;
        this.lastUpdate = builder.lastUpdate;
    }

    public long getStartedAt()
    {
        return startedAt;
    }

    public long getLastUpdate()
    {
        return lastUpdate;
    }

    public void setLastUpdate()
    {
        lastUpdate = ctx.clock().nowInSeconds();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        LocalSession session = (LocalSession) o;

        if (startedAt != session.startedAt) return false;
        return lastUpdate == session.lastUpdate;
    }

    public int hashCode()
    {
        return Objects.hash(super.hashCode(), startedAt, lastUpdate);
    }

    public String toString()
    {
        return "LocalSession{" +
               "sessionID=" + sessionID +
               ", state=" + getState() +
               ", coordinator=" + coordinator +
               ", tableIds=" + tableIds +
               ", repairedAt=" + repairedAt +
               ", ranges=" + ranges +
               ", participants=" + participants +
               ", startedAt=" + startedAt +
               ", lastUpdate=" + lastUpdate +
               '}';
    }

    public static class Builder extends AbstractBuilder
    {
        private long startedAt;
        private long lastUpdate;

        public Builder(SharedContext ctx)
        {
            super(ctx);
        }

        public Builder withStartedAt(long startedAt)
        {
            this.startedAt = startedAt;
            return this;
        }

        public Builder withLastUpdate(long lastUpdate)
        {
            this.lastUpdate = lastUpdate;
            return this;
        }

        void validate()
        {
            super.validate();
            Preconditions.checkArgument(startedAt > 0);
            Preconditions.checkArgument(lastUpdate > 0);
        }

        public LocalSession build()
        {
            validate();
            return new LocalSession(this);
        }
    }

    public static Builder builder(SharedContext ctx)
    {
        return new Builder(ctx);
    }
}
