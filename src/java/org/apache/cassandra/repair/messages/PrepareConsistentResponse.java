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
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.UUIDSerializer;

public class PrepareConsistentResponse extends RepairMessage
{
    public final UUID parentSession;
    public final InetAddressAndPort participant;
    public final boolean success;

    public PrepareConsistentResponse(UUID parentSession, InetAddressAndPort participant, boolean success)
    {
        super(Type.CONSISTENT_RESPONSE, null);
        assert parentSession != null;
        assert participant != null;
        this.parentSession = parentSession;
        this.participant = participant;
        this.success = success;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrepareConsistentResponse that = (PrepareConsistentResponse) o;

        if (success != that.success) return false;
        if (!parentSession.equals(that.parentSession)) return false;
        return participant.equals(that.participant);
    }

    public int hashCode()
    {
        int result = parentSession.hashCode();
        result = 31 * result + participant.hashCode();
        result = 31 * result + (success ? 1 : 0);
        return result;
    }

    public static MessageSerializer serializer = new MessageSerializer<PrepareConsistentResponse>()
    {
        public void serialize(PrepareConsistentResponse response, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(response.parentSession, out, version);
            CompactEndpointSerializationHelper.instance.serialize(response.participant, out, version);
            out.writeBoolean(response.success);
        }

        public PrepareConsistentResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            return new PrepareConsistentResponse(UUIDSerializer.serializer.deserialize(in, version),
                                                 CompactEndpointSerializationHelper.instance.deserialize(in, version),
                                                 in.readBoolean());
        }

        public long serializedSize(PrepareConsistentResponse response, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(response.parentSession, version);
            size += CompactEndpointSerializationHelper.instance.serializedSize(response.participant, version);
            size += TypeSizes.sizeof(response.success);
            return size;
        }
    };
}
