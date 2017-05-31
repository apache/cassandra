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
import java.net.InetAddress;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

public class FinalizePromise extends RepairMessage
{
    public final UUID sessionID;
    public final InetAddress participant;
    public final boolean promised;

    public FinalizePromise(UUID sessionID, InetAddress participant, boolean promised)
    {
        super(Type.FINALIZE_PROMISE, null);
        assert sessionID != null;
        assert participant != null;
        this.sessionID = sessionID;
        this.participant = participant;
        this.promised = promised;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FinalizePromise that = (FinalizePromise) o;

        if (promised != that.promised) return false;
        if (!sessionID.equals(that.sessionID)) return false;
        return participant.equals(that.participant);
    }

    public int hashCode()
    {
        int result = sessionID.hashCode();
        result = 31 * result + participant.hashCode();
        result = 31 * result + (promised ? 1 : 0);
        return result;
    }

    public static MessageSerializer serializer = new MessageSerializer<FinalizePromise>()
    {
        private TypeSerializer<InetAddress> inetSerializer = InetAddressSerializer.instance;

        public void serialize(FinalizePromise msg, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(msg.sessionID, out, version);
            ByteBufferUtil.writeWithShortLength(inetSerializer.serialize(msg.participant), out);
            out.writeBoolean(msg.promised);
        }

        public FinalizePromise deserialize(DataInputPlus in, int version) throws IOException
        {
            return new FinalizePromise(UUIDSerializer.serializer.deserialize(in, version),
                                       inetSerializer.deserialize(ByteBufferUtil.readWithShortLength(in)),
                                       in.readBoolean());
        }

        public long serializedSize(FinalizePromise msg, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(msg.sessionID, version);
            size += ByteBufferUtil.serializedSizeWithShortLength(inetSerializer.serialize(msg.participant));
            size += TypeSizes.sizeof(msg.promised);
            return size;
        }
    };
}
