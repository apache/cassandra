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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

public class FinalizePropose extends RepairMessage
{
    public final UUID sessionID;

    public FinalizePropose(UUID sessionID)
    {
        super(Type.FINALIZE_PROPOSE, null);
        assert sessionID != null;
        this.sessionID = sessionID;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FinalizePropose that = (FinalizePropose) o;

        return sessionID.equals(that.sessionID);
    }

    public int hashCode()
    {
        return sessionID.hashCode();
    }

    public String toString()
    {
        return "FinalizePropose{" +
               "sessionID=" + sessionID +
               '}';
    }

    public static MessageSerializer serializer = new MessageSerializer<FinalizePropose>()
    {
        public void serialize(FinalizePropose msg, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(msg.sessionID, out, version);
        }

        public FinalizePropose deserialize(DataInputPlus in, int version) throws IOException
        {
            return new FinalizePropose(UUIDSerializer.serializer.deserialize(in, version));
        }

        public long serializedSize(FinalizePropose msg, int version)
        {
            return UUIDSerializer.serializer.serializedSize(msg.sessionID, version);
        }
    };
}
