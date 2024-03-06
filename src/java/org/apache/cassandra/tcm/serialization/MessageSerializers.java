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

package org.apache.cassandra.tcm.serialization;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.migration.ClusterMetadataHolder;

/**
 * Provides IVersionedSerializers for internode messages where the payload includes
 * elements of ClusterMetadata. Metadata elements are versioned seperately from
 * MessagingService and the appropriate version is not established based on the
 * peer receiving the messages, but is the lowest supported version of any member
 * of the cluster.
 *
 * NOTE: Serialization version here is used for convenience of serializing the message
 * on the outgoing path. Since receiving node may have a different view of
 * min serialization version, we _always_ have to either use a {@link VerboseMetadataSerializer}
 * (like {@link LogState}/ {@link Replication} or explicitly serialize the version (like {@link Commit}).
 */
public class MessageSerializers
{
    public static IVersionedSerializer<LogState> logStateSerializer()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null || metadata.directory.clusterMinVersion.serializationVersion == NodeVersion.CURRENT.serializationVersion)
            return LogState.defaultMessageSerializer;

        assert !metadata.directory.clusterMinVersion.serializationVersion().equals(NodeVersion.CURRENT.serializationVersion());
        return LogState.messageSerializer(metadata.directory.clusterMinVersion.serializationVersion());
    }

    public static IVersionedSerializer<Commit.Result> commitResultSerializer()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null || metadata.directory.clusterMinVersion.serializationVersion == NodeVersion.CURRENT.serializationVersion)
            return Commit.Result.defaultMessageSerializer;

        assert !metadata.directory.clusterMinVersion.serializationVersion().equals(NodeVersion.CURRENT.serializationVersion());
        return Commit.Result.messageSerializer(metadata.directory.clusterMinVersion.serializationVersion());
    }

    public static IVersionedSerializer<Commit> commitSerializer()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null || metadata.directory.clusterMinVersion.serializationVersion == NodeVersion.CURRENT.serializationVersion)
            return Commit.defaultMessageSerializer;

        assert !metadata.directory.clusterMinVersion.serializationVersion().equals(NodeVersion.CURRENT.serializationVersion());
        return Commit.messageSerializer(metadata.directory.clusterMinVersion.serializationVersion());
    }

    public static IVersionedSerializer<ClusterMetadataHolder> metadataHolderSerializer()
    {
        ClusterMetadata metadata = ClusterMetadata.currentNullable();
        if (metadata == null || metadata.directory.clusterMinVersion.serializationVersion == NodeVersion.CURRENT.serializationVersion)
            return ClusterMetadataHolder.defaultMessageSerializer;

        assert !metadata.directory.clusterMinVersion.serializationVersion().equals(NodeVersion.CURRENT.serializationVersion());
        return ClusterMetadataHolder.messageSerializer(metadata.directory.clusterMinVersion.serializationVersion());
    }
}
