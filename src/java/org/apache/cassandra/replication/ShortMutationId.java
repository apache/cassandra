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

import java.io.IOException;
import java.io.Serializable;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.replication.CoordinatorLogId.NONE_HOST_ID;
import static org.apache.cassandra.replication.CoordinatorLogId.NONE_HOST_LOG_ID;
import static org.apache.cassandra.replication.CoordinatorLogId.NONE_LOG_ID;

/**
 * MutationId without the timestamp component. This is sufficient for uniquely identifying a mutation,
 * and for lookup in the journal and most tracking data structures.
 */
public class ShortMutationId implements Serializable, Comparable<ShortMutationId>
{
    static final int NONE_OFFSET = Integer.MIN_VALUE;

    static final ShortMutationId NONE = new ShortMutationId(NONE_LOG_ID, NONE_OFFSET);

    /** TCM host ID */
    public final int hostId;

    /**
     * Host log ID (unique within the host).
     * Allocated anew on host restart - one per token range replicated by the host.
     * Persisted on allocation, unique within the host.
     */
    public final int hostLogId;

    /**
     * 4 byte offset. Offest is incremented, is alone is sufficient to identify
     * the entry within a coordinator log.
     * MutationId adds a timestamp for correlation purposes.
     */
    public final int offset;

    public ShortMutationId(long logId, int offset)
    {
        this(CoordinatorLogId.hostId(logId), CoordinatorLogId.hostLogId(logId), offset);
    }

    public ShortMutationId(CoordinatorLogId logId, int offset)
    {
        this(logId.hostId, logId.hostLogId, offset);
    }

    private ShortMutationId(int hostId, int hostLogId, int offset)
    {
        this.hostId = hostId;
        this.hostLogId = hostLogId;
        this.offset = offset;
    }

    public ShortMutationId(MutationId mutationId)
    {
        this(mutationId.hostId(), mutationId.hostLogId(), mutationId.offset());
    }

    @Override
    public int compareTo(ShortMutationId that)
    {
        int cmp = Integer.compare(this.hostId, that.hostId);
        if (cmp != 0) return cmp;

        cmp = Integer.compare(this.hostLogId, that.hostLogId);
        if (cmp != 0) return cmp;

        return Integer.compare(this.offset, that.offset);
    }

    public int hostId()
    {
        return hostId;
    }

    public int hostLogId()
    {
        return hostLogId;
    }

    public int offset()
    {
        return offset;
    }

    public long logId()
    {
        return CoordinatorLogId.asLong(hostId, hostLogId);
    }

    public CoordinatorLogId asLogId()
    {
        return new CoordinatorLogId(hostId, hostLogId);
    }

    public boolean isNone()
    {
        return hostId == NONE_HOST_ID && hostLogId == NONE_HOST_LOG_ID && offset == NONE_OFFSET;
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof ShortMutationId)) return false;
        ShortMutationId that = (ShortMutationId) o;
        return this.logId() == that.logId() && this.offset == that.offset;
    }

    @Override
    public final int hashCode()
    {
        return Integer.hashCode(offset) + 31 * Long.hashCode(logId());
    }

    @Override
    public String toString()
    {
        return "ShortMutationId{" + hostId() + ", " + hostLogId() + ", " + offset() + '}';
    }

    public static final UnversionedSerializer<ShortMutationId> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(ShortMutationId id, DataOutputPlus out) throws IOException
        {
            out.writeLong(id.logId());
            out.writeInt(id.offset());
        }

        @Override
        public ShortMutationId deserialize(DataInputPlus in) throws IOException
        {
            long logId = in.readLong();
            int offset = in.readInt();

            return (logId == NONE_LOG_ID && offset == NONE_OFFSET)
                 ? NONE
                 : new ShortMutationId(logId, offset);
        }

        @Override
        public long serializedSize(ShortMutationId id)
        {
            return TypeSizes.sizeof(id.logId()) + TypeSizes.sizeof(id.offset());
        }
    };
}
