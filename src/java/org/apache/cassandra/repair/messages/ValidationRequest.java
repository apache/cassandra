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
import org.apache.cassandra.utils.CassandraUInt;

/**
 * ValidationRequest
 *
 * @since 2.0
 */
public class ValidationRequest extends RepairMessage
{
    public final long nowInSec;

    public ValidationRequest(RepairJobDesc desc, long nowInSec)
    {
        super(desc);
        this.nowInSec = nowInSec;
    }

    @Override
    public String toString()
    {
        return "ValidationRequest{" +
               "nowInSec=" + nowInSec +
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
        }

        public ValidationRequest deserialize(DataInputPlus dis, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(dis, version);
            long nowInsec = version >= MessagingService.VERSION_50 ? CassandraUInt.toLong(dis.readInt()) : dis.readInt();
            return new ValidationRequest(desc, nowInsec);
        }

        public long serializedSize(ValidationRequest message, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(message.desc, version);
            size += TypeSizes.INT_SIZE;
            return size;
        }
    };
}
