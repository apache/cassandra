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
package org.apache.cassandra.service.tracking;

public class MutationId
{
    /**
     * 4 byte TCM host id + 4 byte host log id packed into a long.
     * Host log ID is unique within the host, allocated
     * anew on host restart - one per token range replicated by the host,
     * persisted on allocation, unique within the host.
     */
    public final long logId;

    /**
     * 4 byte position + 4 byte timestamp packed into a long.
     * Position is incremented, the timestamp is monotonically non-decreasing.
     * The position is enough to identify the entry within a coordinator log,
     * the timestamp is added for correlation purposes.
     */
    public final long sequenceId;

    MutationId(long logId, long sequenceId)
    {
        this.logId = logId;
        this.sequenceId = sequenceId;
    }

    public int hostId()
    {
        return (int) (0xffffffffL & (logId >> 32));
    }

    public int hostLogId()
    {
        return (int) (0xffffffffL & logId);
    }

    public int position()
    {
        return (int) (0xffffffffL & (sequenceId >> 32));
    }

    public int timestamp()
    {
        return (int) (0xffffffffL & sequenceId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MutationId)) return false;
        MutationId that = (MutationId) o;
        return this.logId == that.logId && this.sequenceId == that.sequenceId;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(logId) + 31 * Long.hashCode(sequenceId);
    }

    @Override
    public String toString()
    {
        return "MutationId{" + logId + ", " + sequenceId + '}';
    }
}
