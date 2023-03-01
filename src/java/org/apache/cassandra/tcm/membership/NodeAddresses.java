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

package org.apache.cassandra.tcm.membership;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

public class NodeAddresses
{
    public static final Serializer serializer = new Serializer();

    public final InetAddressAndPort broadcastAddress;
    public final InetAddressAndPort localAddress;
    public final InetAddressAndPort nativeAddress;

    /**
     *
     * @param broadcastAddress this comes from config if broadcast_address is set or it falls through to getLocalAddress
     *                         which either grabs the local host address or listen_address from config if set
     *                         todo; config broadcast_address can be changed by snitch (EC2MultiRegionSnitch) during runtime, handle that
     * @param localAddress this is the local host if listen_address is not set in config
     * @param nativeAddress address for clients to communicate with this node
     */
    public NodeAddresses(InetAddressAndPort broadcastAddress, InetAddressAndPort localAddress, InetAddressAndPort nativeAddress)
    {
        this.broadcastAddress = broadcastAddress;
        this.localAddress = localAddress;
        this.nativeAddress = nativeAddress;
    }

    public NodeAddresses(InetAddressAndPort address)
    {
        this(address, address, address);
    }

    @Override
    public String toString()
    {
        return "NodeAddresses{" +
               "broadcastAddress=" + broadcastAddress +
               ", localAddress=" + localAddress +
               ", nativeAddress=" + nativeAddress +
               '}';
    }

    public boolean conflictsWith(NodeAddresses other)
    {
        return broadcastAddress.equals(other.broadcastAddress) ||
               localAddress.equals(other.localAddress) ||
               nativeAddress.equals(other.nativeAddress);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof NodeAddresses)) return false;
        NodeAddresses that = (NodeAddresses) o;
        return Objects.equals(broadcastAddress, that.broadcastAddress) && Objects.equals(localAddress, that.localAddress) && Objects.equals(nativeAddress, that.nativeAddress);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(broadcastAddress, localAddress, nativeAddress);
    }

    public static NodeAddresses current()
    {
        return new NodeAddresses(FBUtilities.getBroadcastAddressAndPort(),
                                 FBUtilities.getLocalAddressAndPort(),
                                 FBUtilities.getBroadcastNativeAddressAndPort());
    }

    public static class Serializer implements MetadataSerializer<NodeAddresses>
    {
        @Override
        public void serialize(NodeAddresses t, DataOutputPlus out, Version version) throws IOException
        {
            InetAddressAndPort.MetadataSerializer.serializer.serialize(t.broadcastAddress, out, version);
            InetAddressAndPort.MetadataSerializer.serializer.serialize(t.localAddress, out, version);
            InetAddressAndPort.MetadataSerializer.serializer.serialize(t.nativeAddress, out, version);
        }

        @Override
        public NodeAddresses deserialize(DataInputPlus in, Version version) throws IOException
        {
            InetAddressAndPort broadcastAddress = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
            InetAddressAndPort localAddress = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
            InetAddressAndPort rpcAddress = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
            return new NodeAddresses(broadcastAddress, localAddress, rpcAddress);
        }

        @Override
        public long serializedSize(NodeAddresses t, Version version)
        {
            return InetAddressAndPort.MetadataSerializer.serializer.serializedSize(t.broadcastAddress, version) +
                   InetAddressAndPort.MetadataSerializer.serializer.serializedSize(t.localAddress, version) +
                   InetAddressAndPort.MetadataSerializer.serializer.serializedSize(t.nativeAddress, version);
        }
    }
}
