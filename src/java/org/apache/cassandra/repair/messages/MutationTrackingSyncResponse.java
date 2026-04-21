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
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.Offsets;
import org.apache.cassandra.utils.CollectionSerializers;

/**
 * Response from a participant to a {@link MutationTrackingSyncRequest}. Contains the
 * participant's current witnessed offsets for each shard overlapping the requested ranges.
 * These offsets are captured after the request is received, establishing a happens-before
 * relationship with the repair start.
 */
public class MutationTrackingSyncResponse extends RepairMessage
{
    /** Per-shard witnessed offsets: shard range -> (logId -> offsets) */
    public final Map<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>> offsetsByShard;

    public MutationTrackingSyncResponse(RepairJobDesc desc,
                                        Map<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>> offsetsByShard)
    {
        super(desc);
        Objects.requireNonNull(offsetsByShard);
        this.offsetsByShard = offsetsByShard;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof MutationTrackingSyncResponse))
            return false;
        MutationTrackingSyncResponse other = (MutationTrackingSyncResponse) o;
        return Objects.equals(desc, other.desc)
               && Objects.equals(offsetsByShard, other.offsetsByShard);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(desc, offsetsByShard);
    }

    @Override
    public String toString()
    {
        return "MutationTrackingSyncResponse{" +
               "desc=" + desc +
               ", shardCount=" + offsetsByShard.size() +
               '}';
    }

    private static final IVersionedSerializer<Map<CoordinatorLogId, Offsets.Immutable>> offsetsMapSerializer =
        CollectionSerializers.newMapSerializer(CoordinatorLogId.serializer, Offsets.serializer);

    @SuppressWarnings("unchecked")
    public static final IVersionedSerializer<MutationTrackingSyncResponse> serializer = new IVersionedSerializer<>()
    {
        public void serialize(MutationTrackingSyncResponse response, DataOutputPlus out, int version) throws IOException
        {
            RepairJobDesc.serializer.serialize(response.desc, out, version);
            CollectionSerializers.serializeMap((Map<AbstractBounds<Token>, Map<CoordinatorLogId, Offsets.Immutable>>) (Map<?, ?>) response.offsetsByShard,
                                              out, version, Range.tokenSerializer, offsetsMapSerializer);
        }

        public MutationTrackingSyncResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            RepairJobDesc desc = RepairJobDesc.serializer.deserialize(in, version);
            Map<AbstractBounds<Token>, Map<CoordinatorLogId, Offsets.Immutable>> raw =
                CollectionSerializers.deserializeMap(in, version, Range.tokenSerializer, offsetsMapSerializer);
            Map<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>> offsetsByShard =
                (Map<Range<Token>, Map<CoordinatorLogId, Offsets.Immutable>>) (Map<?, ?>) raw;
            return new MutationTrackingSyncResponse(desc, offsetsByShard);
        }

        public long serializedSize(MutationTrackingSyncResponse response, int version)
        {
            long size = RepairJobDesc.serializer.serializedSize(response.desc, version);
            size += CollectionSerializers.serializedMapSize((Map<AbstractBounds<Token>, Map<CoordinatorLogId, Offsets.Immutable>>) (Map<?, ?>) response.offsetsByShard,
                                                           version, Range.tokenSerializer, offsetsMapSerializer);
            return size;
        }
    };
}
