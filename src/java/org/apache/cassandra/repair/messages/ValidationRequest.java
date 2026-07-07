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
package org.apache.cassandra.repair.messages;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.replication.ValidationOffsets;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.NullableSerializer;

/**
 * ValidationRequest
 *
 * @since 2.0
 */
public class ValidationRequest extends RepairMessage
{
    public final long nowInSec;
    public final boolean dontPurgeTombstones;

    /** Non-null iff this is a tracked keyspace validation. */
    public final ValidationOffsets offset;

    public ValidationRequest(RepairJobDesc desc, long nowInSec, boolean dontPurgeTombstones)
    {
        this(desc, nowInSec, dontPurgeTombstones, null);
    }

    public ValidationRequest(RepairJobDesc desc, long nowInSec, boolean dontPurgeTombstones, ValidationOffsets offset)
    {
        super(desc);
        this.nowInSec = nowInSec;
        this.dontPurgeTombstones = dontPurgeTombstones;
        this.offset = offset;
    }

    @Override
    public String toString()
    {
        return "ValidationRequest{" +
               "nowInSec=" + nowInSec +
               ", dontPurgeTombstones=" + dontPurgeTombstones +
               (offset != null ? ", offset=" + offset : "") +
               "} " + super.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ValidationRequest that = (ValidationRequest) o;
        return nowInSec == that.nowInSec;
    }

    @Override
    public int hashCode()
    {
        return (int) nowInSec;
    }

    public static final IVersionedSerializer<ValidationRequest> serializer = new IVersionedSerializer<ValidationRequest>()
    {
        public void serialize(ValidationRequest message, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(message.desc, out, version);
            out.writeInt(version >= MessagingService.VERSION_50 ? CassandraUInt.fromLong(message.nowInSec) : (int) message.nowInSec);
            if (version >= MessagingService.VERSION_60)
                out.writeBoolean(message.dontPurgeTombstones);

            if (version >= MessagingService.VERSION_61)
                NullableSerializer.serializeNullable(message.offset, out, ValidationOffsets.serializer);
        }

        public ValidationRequest deserialize(DataInputPlus dis, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(dis, version);
            long nowInsec = version >= MessagingService.VERSION_50 ? CassandraUInt.toLong(dis.readInt()) : dis.readInt();
            boolean dontPurgeTombstones = version >= MessagingService.VERSION_60 ? dis.readBoolean() : false;

            ValidationOffsets offset = version >= MessagingService.VERSION_61
                                        ? NullableSerializer.deserializeNullable(dis, ValidationOffsets.serializer)
                                        : null;

            return new ValidationRequest(desc, nowInsec, dontPurgeTombstones, offset);
        }

        public long serializedSize(ValidationRequest message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += TypeSizes.INT_SIZE;
            size += version >= MessagingService.VERSION_60 ? TypeSizes.sizeof(message.dontPurgeTombstones) : 0;

            if (version >= MessagingService.VERSION_61)
                size += NullableSerializer.serializedNullableSize(message.offset, ValidationOffsets.serializer);

            return size;
        }
    };
}
