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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.VerboseMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class ClusterMetadataHolder
{
    public static final IVersionedSerializer<ClusterMetadataHolder> defaultMessageSerializer = new ClusterMetadataHolder.Serializer(NodeVersion.CURRENT.serializationVersion());

    private static volatile Serializer serializerCache;
    public static IVersionedSerializer<ClusterMetadataHolder> messageSerializer(Version version)
    {
        Serializer cached = serializerCache;
        if (cached != null && cached.serializationVersion.equals(version))
            return cached;
        cached = new Serializer(version);
        serializerCache = cached;
        return cached;
    }

    public final Election.Initiator coordinator;
    public final ClusterMetadata metadata;

    public ClusterMetadataHolder(Election.Initiator coordinator, ClusterMetadata metadata)
    {
        this.coordinator = coordinator;
        this.metadata = metadata;
    }

    @Override
    public String toString()
    {
        return "ClusterMetadataHolder{" +
               "coordinator=" + coordinator +
               ", epoch=" + metadata.epoch +
               '}';
    }

    private static class Serializer implements IVersionedSerializer<ClusterMetadataHolder>
    {
        private final Version serializationVersion;

        public Serializer(Version serializationVersion)
        {
            this.serializationVersion = serializationVersion;
        }

        @Override
        public void serialize(ClusterMetadataHolder t, DataOutputPlus out, int version) throws IOException
        {
            Election.Initiator.serializer.serialize(t.coordinator, out, version);
            VerboseMetadataSerializer.serialize(ClusterMetadata.serializer, t.metadata, out, serializationVersion);
        }

        @Override
        public ClusterMetadataHolder deserialize(DataInputPlus in, int version) throws IOException
        {
            Election.Initiator coordinator = Election.Initiator.serializer.deserialize(in, version);
            ClusterMetadata metadata = VerboseMetadataSerializer.deserialize(ClusterMetadata.serializer, in);
            return new ClusterMetadataHolder(coordinator, metadata);
        }

        @Override
        public long serializedSize(ClusterMetadataHolder t, int version)
        {
            return Election.Initiator.serializer.serializedSize(t.coordinator, version) +
                   VerboseMetadataSerializer.serializedSize(ClusterMetadata.serializer, t.metadata, serializationVersion);
        }
    }
}