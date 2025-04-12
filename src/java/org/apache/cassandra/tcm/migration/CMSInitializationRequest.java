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

package org.apache.cassandra.tcm.migration;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.ownership.TokenMap;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.UUIDSerializer;

public class CMSInitializationRequest
{
    public static final IVersionedSerializer<CMSInitializationRequest> defaultMessageSerializer = new Serializer(NodeVersion.CURRENT.serializationVersion());

    private static volatile Serializer serializerCache;

    public static IVersionedSerializer<CMSInitializationRequest> messageSerializer(Version version)
    {
        Serializer cached = serializerCache;
        if (cached != null && cached.serializationVersion.equals(version))
            return cached;
        cached = new Serializer(version);
        serializerCache = cached;
        return cached;
    }

    public final Initiator initiator;
    public final Directory directory;
    public final TokenMap tokenMap;
    public final UUID schemaVersion;

    public CMSInitializationRequest(InetAddressAndPort initiator, UUID initToken, ClusterMetadata metadata)
    {
        this(new Initiator(initiator, initToken), metadata.directory, metadata.tokenMap, SchemaKeyspace.calculateSchemaDigest());
    }

    public CMSInitializationRequest(Initiator initiator, Directory directory, TokenMap tokenMap, UUID schemaVersion)
    {
        this.initiator = initiator;
        this.directory = directory;
        this.tokenMap = tokenMap;
        this.schemaVersion = schemaVersion;
    }

    public static class Serializer implements IVersionedSerializer<CMSInitializationRequest>
    {
        private final Version serializationVersion;

        public Serializer(Version serializationVersion)
        {
            this.serializationVersion = serializationVersion;
        }

        @Override
        public void serialize(CMSInitializationRequest t, DataOutputPlus out, int version) throws IOException
        {
            Initiator.serializer.serialize(t.initiator, out, version);
            Directory.serializer.serialize(t.directory, out, serializationVersion);
            TokenMap.serializer.serialize(t.tokenMap, out, serializationVersion);
            UUIDSerializer.serializer.serialize(t.schemaVersion, out, version);
        }

        @Override
        public CMSInitializationRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            Initiator initiator = Initiator.serializer.deserialize(in, version);
            Directory directory = Directory.serializer.deserialize(in, serializationVersion);
            TokenMap tokenMap = TokenMap.serializer.deserialize(in, serializationVersion);
            UUID schemaVersion = UUIDSerializer.serializer.deserialize(in, version);
            return new CMSInitializationRequest(initiator, directory, tokenMap, schemaVersion);
        }

        @Override
        public long serializedSize(CMSInitializationRequest t, int version)
        {
            return Initiator.serializer.serializedSize(t.initiator, version) +
                   Directory.serializer.serializedSize(t.directory, serializationVersion) +
                   TokenMap.serializer.serializedSize(t.tokenMap, serializationVersion) +
                   UUIDSerializer.serializer.serializedSize(t.schemaVersion, version);
        }
    }

    public static class Initiator
    {
        public static final Serializer serializer = new Serializer();
        public final InetAddressAndPort endpoint;
        public final UUID initToken;

        public Initiator(InetAddressAndPort initiator, UUID initToken)
        {
            this.endpoint = initiator;
            this.initToken = initToken;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof Initiator)) return false;
            Initiator other = (Initiator) o;
            return Objects.equals(endpoint, other.endpoint) && Objects.equals(initToken, other.initToken);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(endpoint, initToken);
        }

        @Override
        public String toString()
        {
            return "Initiator{" +
                   "initiator=" + endpoint +
                   ", initToken=" + initToken +
                   '}';
        }

        public static class Serializer implements IVersionedSerializer<Initiator>
        {
            @Override
            public void serialize(Initiator t, DataOutputPlus out, int version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(t.endpoint, out, version);
                UUIDSerializer.serializer.serialize(t.initToken, out, version);
            }

            @Override
            public Initiator deserialize(DataInputPlus in, int version) throws IOException
            {
                return new Initiator(InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version),
                                     UUIDSerializer.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(Initiator t, int version)
            {
                return InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(t.endpoint, version) +
                       UUIDSerializer.serializer.serializedSize(t.initToken, version);
            }
        }
    }
}
