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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDSerializer;

public class PrepareConsistentRequest extends RepairMessage
{
    public final UUID parentSession;
    public final InetAddress coordinator;
    public final Set<InetAddress> participants;

    public PrepareConsistentRequest(UUID parentSession, InetAddress coordinator, Set<InetAddress> participants)
    {
        super(Type.CONSISTENT_REQUEST, null);
        assert parentSession != null;
        assert coordinator != null;
        assert participants != null && !participants.isEmpty();
        this.parentSession = parentSession;
        this.coordinator = coordinator;
        this.participants = ImmutableSet.copyOf(participants);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PrepareConsistentRequest that = (PrepareConsistentRequest) o;

        if (!parentSession.equals(that.parentSession)) return false;
        if (!coordinator.equals(that.coordinator)) return false;
        return participants.equals(that.participants);
    }

    public int hashCode()
    {
        int result = parentSession.hashCode();
        result = 31 * result + coordinator.hashCode();
        result = 31 * result + participants.hashCode();
        return result;
    }

    public String toString()
    {
        return "PrepareConsistentRequest{" +
               "parentSession=" + parentSession +
               ", coordinator=" + coordinator +
               ", participants=" + participants +
               '}';
    }

    public static MessageSerializer serializer = new MessageSerializer<PrepareConsistentRequest>()
    {
        private TypeSerializer<InetAddress> inetSerializer = InetAddressSerializer.instance;

        public void serialize(PrepareConsistentRequest request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.parentSession, out, version);
            ByteBufferUtil.writeWithShortLength(inetSerializer.serialize(request.coordinator), out);
            out.writeInt(request.participants.size());
            for (InetAddress peer : request.participants)
            {
                ByteBufferUtil.writeWithShortLength(inetSerializer.serialize(peer), out);
            }
        }

        public PrepareConsistentRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID sessionId = UUIDSerializer.serializer.deserialize(in, version);
            InetAddress coordinator = inetSerializer.deserialize(ByteBufferUtil.readWithShortLength(in));
            int numPeers = in.readInt();
            Set<InetAddress> peers = new HashSet<>(numPeers);
            for (int i = 0; i < numPeers; i++)
            {
                InetAddress peer = inetSerializer.deserialize(ByteBufferUtil.readWithShortLength(in));
                peers.add(peer);
            }
            return new PrepareConsistentRequest(sessionId, coordinator, peers);
        }

        public long serializedSize(PrepareConsistentRequest request, int version)
        {
            long size = UUIDSerializer.serializer.serializedSize(request.parentSession, version);
            size += ByteBufferUtil.serializedSizeWithShortLength(inetSerializer.serialize(request.coordinator));
            size += TypeSizes.sizeof(request.participants.size());
            for (InetAddress peer : request.participants)
            {
                size += ByteBufferUtil.serializedSizeWithShortLength(inetSerializer.serialize(peer));
            }
            return size;
        }
    };
}
