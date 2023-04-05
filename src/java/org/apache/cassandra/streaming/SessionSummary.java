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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

public class SessionSummary
{
    public final InetSocketAddress coordinator;
    public final InetSocketAddress peer;
    /** Immutable collection of receiving summaries */
    public final Collection<StreamSummary> receivingSummaries;
    /** Immutable collection of sending summaries*/
    public final Collection<StreamSummary> sendingSummaries;

    public SessionSummary(InetSocketAddress coordinator, InetSocketAddress peer,
                          Collection<StreamSummary> receivingSummaries,
                          Collection<StreamSummary> sendingSummaries)
    {
        assert coordinator != null;
        assert peer != null;
        assert receivingSummaries != null;
        assert sendingSummaries != null;

        this.coordinator = coordinator;
        this.peer = peer;
        this.receivingSummaries = receivingSummaries;
        this.sendingSummaries = sendingSummaries;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SessionSummary summary = (SessionSummary) o;

        if (!coordinator.equals(summary.coordinator)) return false;
        if (!peer.equals(summary.peer)) return false;
        if (!receivingSummaries.equals(summary.receivingSummaries)) return false;
        return sendingSummaries.equals(summary.sendingSummaries);
    }

    public int hashCode()
    {
        int result = coordinator.hashCode();
        result = 31 * result + peer.hashCode();
        result = 31 * result + receivingSummaries.hashCode();
        result = 31 * result + sendingSummaries.hashCode();
        return result;
    }

    public static IVersionedSerializer<SessionSummary> serializer = new IVersionedSerializer<SessionSummary>()
    {
        public void serialize(SessionSummary summary, DataOutputPlus out, int version) throws IOException
        {
            inetAddressAndPortSerializer.serialize(summary.coordinator, out, version);
            inetAddressAndPortSerializer.serialize(summary.peer, out, version);

            out.writeInt(summary.receivingSummaries.size());
            for (StreamSummary streamSummary: summary.receivingSummaries)
            {
                StreamSummary.serializer.serialize(streamSummary, out, version);
            }

            out.writeInt(summary.sendingSummaries.size());
            for (StreamSummary streamSummary: summary.sendingSummaries)
            {
                StreamSummary.serializer.serialize(streamSummary, out, version);
            }
        }

        public SessionSummary deserialize(DataInputPlus in, int version) throws IOException
        {
            InetAddressAndPort coordinator = inetAddressAndPortSerializer.deserialize(in, version);
            InetAddressAndPort peer = inetAddressAndPortSerializer.deserialize(in, version);

            int numRcvd = in.readInt();
            List<StreamSummary> receivingSummaries = new ArrayList<>(numRcvd);
            for (int i=0; i<numRcvd; i++)
            {
                receivingSummaries.add(StreamSummary.serializer.deserialize(in, version));
            }

            int numSent = in.readInt();
            List<StreamSummary> sendingSummaries = new ArrayList<>(numRcvd);
            for (int i=0; i<numSent; i++)
            {
                sendingSummaries.add(StreamSummary.serializer.deserialize(in, version));
            }

            return new SessionSummary(coordinator, peer, receivingSummaries, sendingSummaries);
        }

        public long serializedSize(SessionSummary summary, int version)
        {
            long size = 0;
            size += inetAddressAndPortSerializer.serializedSize(summary.coordinator, version);
            size += inetAddressAndPortSerializer.serializedSize(summary.peer, version);

            size += TypeSizes.sizeof(summary.receivingSummaries.size());
            for (StreamSummary streamSummary: summary.receivingSummaries)
            {
                size += StreamSummary.serializer.serializedSize(streamSummary, version);
            }
            size += TypeSizes.sizeof(summary.sendingSummaries.size());
            for (StreamSummary streamSummary: summary.sendingSummaries)
            {
                size += StreamSummary.serializer.serializedSize(streamSummary, version);
            }
            return size;
        }
    };
}
