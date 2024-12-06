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
import java.util.Comparator;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * MutationId without the timestamp component. This is sufficient for uniquely identifying a mutation,
 * and for lookup in the journal and most tracking data structures.
 */
public class ShortMutationId extends CoordinatorLogId
{
    /**
     * 4 byte offset. Offest is incremented, is alone is sufficient to identify
     * the entry within a coordinator log.
     * MutationId adds a timestamp for correlation purposes.
     */
    protected final int offset;

    public ShortMutationId(long logId, int offset)
    {
        super(logId);
        this.offset = offset;
    }

    public ShortMutationId(CoordinatorLogId logId, int offset)
    {
        super(logId.hostId(), logId.hostLogId());
        this.offset = offset;
    }

    public long logId()
    {
        return super.asLong();
    }

    public int offset()
    {
        return offset;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof ShortMutationId)) return false;
        ShortMutationId that = (ShortMutationId) o;
        return this.logId() == that.logId() && this.offset == that.offset;
    }

    @Override
    public int hashCode()
    {
        return Integer.hashCode(offset) + 31 * super.hashCode();
    }

    @Override
    public String toString()
    {
        return "ShortMutationId{" + hostId() + ", " + hostLogId() + ", " + offset() + '}';
    }

    public static final Comparator<ShortMutationId> comparator = (l, r) -> {
        int cmp = CoordinatorLogId.comparator.compare(l, r);
        return cmp != 0 ? cmp : Integer.compare(l.offset, r.offset);
    };

    public static final IVersionedSerializer<ShortMutationId> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(ShortMutationId id, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(id.logId());
            out.writeInt(id.offset());
        }

        @Override
        public ShortMutationId deserialize(DataInputPlus in, int version) throws IOException
        {
            long logId = in.readLong();
            int offset = in.readInt();
            return new ShortMutationId(logId, offset);
        }

        @Override
        public long serializedSize(ShortMutationId id, int version)
        {
            return TypeSizes.sizeof(id.logId()) + TypeSizes.sizeof(id.offset());
        }
    };
}
