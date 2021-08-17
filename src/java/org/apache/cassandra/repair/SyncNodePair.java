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
package org.apache.cassandra.repair;

import java.io.IOException;

import com.google.common.base.Objects;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

/**
 * SyncNodePair is used for repair message body to indicate the pair of nodes.
 *
 * @since 2.0
 */
public class SyncNodePair
{
    public static IVersionedSerializer<SyncNodePair> serializer = new NodePairSerializer();

    public final InetAddressAndPort coordinator;
    public final InetAddressAndPort peer;

    public SyncNodePair(InetAddressAndPort coordinator, InetAddressAndPort peer)
    {
        this.coordinator = coordinator;
        this.peer = peer;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SyncNodePair nodePair = (SyncNodePair) o;
        return coordinator.equals(nodePair.coordinator) && peer.equals(nodePair.peer);
    }

    @Override
    public String toString()
    {
        return coordinator.toString() + " - " + peer.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(coordinator, peer);
    }

    public static class NodePairSerializer implements IVersionedSerializer<SyncNodePair>
    {
        public void serialize(SyncNodePair nodePair, DataOutputPlus out, int version) throws IOException
        {
            inetAddressAndPortSerializer.serialize(nodePair.coordinator, out, version);
            inetAddressAndPortSerializer.serialize(nodePair.peer, out, version);
        }

        public SyncNodePair deserialize(DataInputPlus in, int version) throws IOException
        {
            InetAddressAndPort ep1 = inetAddressAndPortSerializer.deserialize(in, version);
            InetAddressAndPort ep2 = inetAddressAndPortSerializer.deserialize(in, version);
            return new SyncNodePair(ep1, ep2);
        }

        public long serializedSize(SyncNodePair nodePair, int version)
        {
            return inetAddressAndPortSerializer.serializedSize(nodePair.coordinator, version)
                   + inetAddressAndPortSerializer.serializedSize(nodePair.peer, version);
        }
    }
}
