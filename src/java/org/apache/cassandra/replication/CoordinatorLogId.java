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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;

public class CoordinatorLogId implements Serializable
{
    private static final CoordinatorLogId NONE = new CoordinatorLogId(Integer.MIN_VALUE, Integer.MIN_VALUE);

    /** TCM host ID */
    protected final int hostId;

    /**
     * Host log ID (unique within the host).
     * Allocated anew on host restart - one per token range replicated by the host.
     * Persisted on allocation, unique within the host.
     */
    protected final int hostLogId;

    CoordinatorLogId(long id)
    {
        this(hostId(id), hostLogId(id));
    }

    CoordinatorLogId(int hostId, int hostLogId)
    {
        this.hostId = hostId;
        this.hostLogId = hostLogId;
    }

    public int hostId()
    {
        return hostId;
    }

    public int hostLogId()
    {
        return hostLogId;
    }

    public long asLong()
    {
        return asLong(hostId, hostLogId);
    }

    static long asLong(int hostId, int hostLogId)
    {
        return ((long) hostId << 32) | hostLogId;
    }

    static int hostId(long coordinatorLogId)
    {
        return (int) (coordinatorLogId >>> 32);
    }

    static int hostLogId(long coordinatorLogId)
    {
        return (int) coordinatorLogId;
    }

    public static CoordinatorLogId none()
    {
        return NONE;
    }

    static boolean isNone(int hostId, int hostLogId)
    {
        return hostId == NONE.hostId && hostLogId == NONE.hostLogId;
    }

    public boolean isNone()
    {
        return this == NONE || isNone(hostId, hostLogId);
    }

    @Override
    public String toString()
    {
        return "CoordinatorLogId{" + hostId + ", " + hostLogId + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        CoordinatorLogId logId = (CoordinatorLogId) o;
        return hostId == logId.hostId && hostLogId == logId.hostLogId;
    }

    @Override
    public int hashCode()
    {
        return Integer.hashCode(hostLogId) + 31 * Integer.hashCode(hostId);
    }

    public static final Comparator<CoordinatorLogId> comparator = (l, r) -> Long.compareUnsigned(l.asLong(), r.asLong());

    public static final IVersionedSerializer<CoordinatorLogId> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(CoordinatorLogId logId, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(logId.hostId);
            out.writeInt(logId.hostLogId);
        }

        @Override
        public CoordinatorLogId deserialize(DataInputPlus in, int version) throws IOException
        {
            int hostId = in.readInt();
            int hostLogId = in.readInt();
            if (isNone(hostId, hostLogId))
                return none();
            return new CoordinatorLogId(hostId, hostLogId);
        }

        @Override
        public long serializedSize(CoordinatorLogId logId, int version)
        {
            return TypeSizes.sizeof(logId.hostId) + TypeSizes.sizeof(logId.hostLogId);
        }
    };
}
