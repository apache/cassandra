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
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

public class PrepareConsistentRequest extends RepairMessage
{
    public final TimeUUID parentSession;
    public final InetAddressAndPort coordinator;
    public final Set<InetAddressAndPort> participants;

    public PrepareConsistentRequest(TimeUUID parentSession, InetAddressAndPort coordinator, Set<InetAddressAndPort> participants)
    {
        super(null);
        assert parentSession != null;
        assert coordinator != null;
        assert participants != null && !participants.isEmpty();
        this.parentSession = parentSession;
        this.coordinator = coordinator;
        this.participants = ImmutableSet.copyOf(participants);
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

    public static final IVersionedSerializer<PrepareConsistentRequest> serializer = new IVersionedSerializer<PrepareConsistentRequest>()
    {
        public void serialize(PrepareConsistentRequest request, DataOutputPlus out, int version) throws IOException
        {
            request.parentSession.serialize(out);
            inetAddressAndPortSerializer.serialize(request.coordinator, out, version);
            out.writeInt(request.participants.size());
            for (InetAddressAndPort peer : request.participants)
            {
                inetAddressAndPortSerializer.serialize(peer, out, version);
            }
        }

        public PrepareConsistentRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID sessionId = TimeUUID.deserialize(in);
            InetAddressAndPort coordinator = inetAddressAndPortSerializer.deserialize(in, version);
            int numPeers = in.readInt();
            Set<InetAddressAndPort> peers = new HashSet<>(numPeers);
            for (int i = 0; i < numPeers; i++)
            {
                InetAddressAndPort peer = inetAddressAndPortSerializer.deserialize(in, version);
                peers.add(peer);
            }
            return new PrepareConsistentRequest(sessionId, coordinator, peers);
        }

        public long serializedSize(PrepareConsistentRequest request, int version)
        {
            long size = TimeUUID.sizeInBytes();
            size += inetAddressAndPortSerializer.serializedSize(request.coordinator, version);
            size += TypeSizes.sizeof(request.participants.size());
            for (InetAddressAndPort peer : request.participants)
            {
                size += inetAddressAndPortSerializer.serializedSize(peer, version);
            }
            return size;
        }
    };
}
