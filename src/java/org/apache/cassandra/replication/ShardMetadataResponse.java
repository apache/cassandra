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

import javax.annotation.Nullable;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * Response to a {@link ShardMetadataRequest}, carrying the shard metadata
 * (epoch, range, write participants) for a coordinator log,
 * or null if the log is unknown to the responder.
 */
public final class ShardMetadataResponse
{
    @Nullable
    public final ShardMetadata metadata;

    ShardMetadataResponse(ShardMetadata metadata)
    {
        this.metadata = metadata;
    }

    public static final VersionedSerializer<ShardMetadataResponse> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(ShardMetadataResponse response, DataOutputPlus out, Version version) throws IOException
        {
            ShardMetadata.nullableSerializer.serialize(response.metadata, out, version);
        }

        @Override
        public ShardMetadataResponse deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new ShardMetadataResponse(ShardMetadata.nullableSerializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(ShardMetadataResponse response, Version version)
        {
            return ShardMetadata.nullableSerializer.serializedSize(response.metadata, version);
        }
    };
}
