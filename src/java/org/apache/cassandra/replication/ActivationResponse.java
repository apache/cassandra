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

package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;

public class ActivationResponse
{
    public final Pair<InetAddressAndPort, InetAddressAndPort> syncPair;

    public ActivationResponse(Pair<InetAddressAndPort, InetAddressAndPort> syncPair)
    {
        Preconditions.checkNotNull(syncPair, "Activations require a sync node address pair");

        this.syncPair = syncPair;
    }

    @Override
    public String toString()
    {
        return "ActivationResponse{" +
               "syncPair=" + syncPair +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        ActivationResponse that = (ActivationResponse) o;
        return Objects.equals(syncPair, that.syncPair);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(syncPair);
    }

    public static final VersionedSerializer<ActivationResponse> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(ActivationResponse activate, DataOutputPlus out, Version version) throws IOException
        {
            InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(activate.syncPair.left, out, version.messagingVersion());
            InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(activate.syncPair.right, out, version.messagingVersion());
        }

        @SuppressWarnings("SuspiciousNameCombination")
        @Override
        public ActivationResponse deserialize(DataInputPlus in, Version version) throws IOException
        {
            InetAddressAndPort left = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version.messagingVersion());
            InetAddressAndPort right = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version.messagingVersion());
            return new ActivationResponse(Pair.create(left, right));
        }

        @Override
        public long serializedSize(ActivationResponse activate, Version version)
        {
            long size = 0;
            size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(activate.syncPair.left, version.messagingVersion());
            size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(activate.syncPair.right, version.messagingVersion());
            return size;
        }
    };
}
