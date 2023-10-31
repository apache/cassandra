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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

public class PrepareConsistentResponse extends RepairMessage
{
    public final TimeUUID parentSession;
    public final InetAddressAndPort participant;
    public final boolean success;

    public PrepareConsistentResponse(TimeUUID parentSession, InetAddressAndPort participant, boolean success)
    {
        super(null);
        assert parentSession != null;
        assert participant != null;
        this.parentSession = parentSession;
        this.participant = participant;
        this.success = success;
    }

    @Override
    public TimeUUID parentRepairSession()
    {
        return parentSession;
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

    public static final IVersionedSerializer<PrepareConsistentResponse> serializer = new IVersionedSerializer<PrepareConsistentResponse>()
    {
        public void serialize(PrepareConsistentResponse response, DataOutputPlus out, int version) throws IOException
        {
            response.parentSession.serialize(out);
            inetAddressAndPortSerializer.serialize(response.participant, out, version);
            out.writeBoolean(response.success);
        }

        public PrepareConsistentResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            return new PrepareConsistentResponse(TimeUUID.deserialize(in),
                                                 inetAddressAndPortSerializer.deserialize(in, version),
                                                 in.readBoolean());
        }

        public long serializedSize(PrepareConsistentResponse response, int version)
        {
            long size = TimeUUID.sizeInBytes();
            size += inetAddressAndPortSerializer.serializedSize(response.participant, version);
            size += TypeSizes.sizeof(response.success);
            return size;
        }
    };
}
