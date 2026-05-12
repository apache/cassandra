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
import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.replication.CoordinatorLogId.NONE_LOG_ID;

/**
 * Full mutation id, with the addition of timestamp component.
 * <p>
 * equals() and hashCode() are intentionally not overridden by this class, since log id and offset alone
 * are meant to uniquely identify a mutation.
 */
public class MutationId extends ShortMutationId
{
    static final int NONE_TIMESTAMP = 0;
    static final long NONE_SEQUENCE_ID = sequenceId(NONE_OFFSET, NONE_TIMESTAMP);

    static final MutationId NONE = new MutationId(NONE_LOG_ID, NONE_SEQUENCE_ID);

    /**
     * 4 byte timestamp. The timestamp is monotonically non-decreasing.
     * The offset alone is sufficient to identify the entry within a coordinator
     * log, the timestamp is added for correlation purposes.
     */
    protected final int timestamp;

    public MutationId(long logId, int offset, int timestamp)
    {
        super(logId, offset);
        this.timestamp = timestamp;
    }

    public MutationId(long logId, long sequenceId)
    {
        super(logId, offset(sequenceId));
        this.timestamp = timestamp(sequenceId);
    }

    public long sequenceId()
    {
        return sequenceId(offset, timestamp);
    }

    public int timestamp()
    {
        return timestamp;
    }

    public static long sequenceId(int offset, int timestamp)
    {
        return ((long) offset << 32) | (timestamp & 0xffffffffL);
    }

    public static int offset(long sequenceId)
    {
        return (int) (0xffffffffL & (sequenceId >> 32));
    }

    public static int timestamp(long sequenceId)
    {
        return (int) (0xffffffffL & sequenceId);
    }

    // FIXME: used in place of figuring out if we should use a mutation id or not
    public static MutationId fixme()
    {
        return none();
    }

    public static MutationId none()
    {
        return NONE;
    }

    @Override
    public boolean isNone()
    {
        return logId() == NONE_LOG_ID && sequenceId() == NONE_SEQUENCE_ID;
    }

    @Override
    public String toString()
    {
        return "MutationId{" + hostId + ", " + hostLogId + ", " + offset + ", " + timestamp + '}';
    }

    /**
     * The comparator is intentionally not overridden by this class, since log id and offset alone
     * are meant to uniquely identify a mutation, and only offset determines the order within a log.
     */
    public static final Comparator<MutationId> comparator = ShortMutationId.comparator::compare;

    public ByteBuffer toByteBuffer()
    {
        return ByteBuffer.allocate(16)
                         .putLong(logId())
                         .putLong(sequenceId())
                         .flip();
    }

    public static MutationId fromByteBuffer(ByteBuffer buffer)
    {
        if (buffer.remaining() < 16)
            throw new IllegalStateException();

        int pos = buffer.position();
        long logId = buffer.getLong(pos);
        long sequenceId = buffer.getLong(pos + 8);

        return logId == NONE_LOG_ID && sequenceId == NONE_SEQUENCE_ID
             ? NONE
             : new MutationId(logId, sequenceId);
    }

    public static class Serializer implements IVersionedSerializer<MutationId>
    {
        @Override
        public void serialize(MutationId id, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(id.logId());
            out.writeLong(id.sequenceId());
        }

        @Override
        public MutationId deserialize(DataInputPlus in, int version) throws IOException
        {
            long logId = in.readLong();
            long sequenceId = in.readLong();
            return logId == NONE_LOG_ID && sequenceId == NONE_SEQUENCE_ID
                 ? NONE
                 : new MutationId(logId, sequenceId);
        }

        @Override
        public long serializedSize(MutationId id, int version)
        {
            return TypeSizes.sizeof(id.logId()) + TypeSizes.sizeof(id.sequenceId());
        }

        public void skip(DataInputPlus in, int version) throws IOException
        {
            in.readLong();
            in.readLong();
        }
    }

    public static Serializer serializer = new Serializer();
}
