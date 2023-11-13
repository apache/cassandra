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

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Basically just a record of a local session. All of the local session logic is implemented in {@link LocalSessions}
 */
public class LocalSession extends ConsistentSession
{
    public final int startedAt;
    private volatile int lastUpdate;

    public LocalSession(Builder builder)
    {
        super(builder);
        this.startedAt = builder.startedAt;
        this.lastUpdate = builder.lastUpdate;
    }

    public int getStartedAt()
    {
        return startedAt;
    }

    public int getLastUpdate()
    {
        return lastUpdate;
    }

    public void setLastUpdate()
    {
        lastUpdate = FBUtilities.nowInSeconds();
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
        int result = super.hashCode();
        result = 31 * result + startedAt;
        result = 31 * result + lastUpdate;
        return result;
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
        private int startedAt;
        private int lastUpdate;

        public Builder withStartedAt(int startedAt)
        {
            this.startedAt = startedAt;
            return this;
        }

        public Builder withLastUpdate(int lastUpdate)
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

    public static Builder builder()
    {
        return new Builder();
    }
}
