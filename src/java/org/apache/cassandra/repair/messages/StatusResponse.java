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
import org.apache.cassandra.repair.consistent.ConsistentSession;
import org.apache.cassandra.utils.TimeUUID;

public class StatusResponse extends RepairMessage
{
    public final TimeUUID sessionID;
    public final ConsistentSession.State state;

    public StatusResponse(TimeUUID sessionID, ConsistentSession.State state)
    {
        super(null);
        assert sessionID != null;
        assert state != null;
        this.sessionID = sessionID;
        this.state = state;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatusResponse that = (StatusResponse) o;

        if (!sessionID.equals(that.sessionID)) return false;
        return state == that.state;
    }

    public int hashCode()
    {
        int result = sessionID.hashCode();
        result = 31 * result + state.hashCode();
        return result;
    }

    public String toString()
    {
        return "StatusResponse{" +
               "sessionID=" + sessionID +
               ", state=" + state +
               '}';
    }

    public static final IVersionedSerializer<StatusResponse> serializer = new IVersionedSerializer<StatusResponse>()
    {
        public void serialize(StatusResponse msg, DataOutputPlus out, int version) throws IOException
        {
            msg.sessionID.serialize(out);
            out.writeInt(msg.state.ordinal());
        }

        public StatusResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            return new StatusResponse(TimeUUID.deserialize(in),
                                      ConsistentSession.State.valueOf(in.readInt()));
        }

        public long serializedSize(StatusResponse msg, int version)
        {
            return TimeUUID.sizeInBytes()
                   + TypeSizes.sizeof(msg.state.ordinal());
        }
    };
}
