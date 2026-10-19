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
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.CollectionSerializers;

public class BroadcastLogOffsets
{
    private final ShardMetadata shardMetadata;
    private final List<Offsets.Immutable> replicatedOffsets;
    private final boolean durable;

    public BroadcastLogOffsets(
        String keyspace, long sinceEpoch, Range<Token> range, Participants participants,
        List<Offsets.Immutable> offsets, boolean durable)
    {
        this(new ShardMetadata(keyspace, sinceEpoch, range, participants), offsets, durable);
    }

    public BroadcastLogOffsets(ShardMetadata shardMetadata, List<Offsets.Immutable> offsets, boolean durable)
    {
        this.shardMetadata = shardMetadata;
        this.replicatedOffsets = offsets;
        this.durable = durable;
    }

    boolean isEmpty()
    {
        return replicatedOffsets.isEmpty();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("[");
        boolean isFirst = true;
        for (Offsets.Immutable logOffsets : replicatedOffsets)
        {
            if (!isFirst) sb.append(", ");
            sb.append('(').append(logOffsets.logId()).append(" -> ").append(logOffsets).append(')');
            isFirst = false;
        }
        sb.append(']');
        return "ShardReplicatedOffsets{" + shardMetadata + ", " + sb + ", " + durable + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof BroadcastLogOffsets)) return false;
        BroadcastLogOffsets that = (BroadcastLogOffsets) o;
        return durable == that.durable
            && Objects.equals(shardMetadata, that.shardMetadata)
            && Objects.equals(replicatedOffsets, that.replicatedOffsets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(shardMetadata, replicatedOffsets, durable);
    }

    public static final IVerbHandler<BroadcastLogOffsets> verbHandler = message -> {
        MutationTrackingService.ensureEnabled();
        BroadcastLogOffsets replicatedOffsets = message.payload;
        MutationTrackingService.instance().updateReplicatedOffsets(replicatedOffsets.shardMetadata.keyspace,
                                                                   replicatedOffsets.shardMetadata.sinceEpoch,
                                                                   replicatedOffsets.shardMetadata.range,
                                                                   replicatedOffsets.shardMetadata.participants,
                                                                   replicatedOffsets.replicatedOffsets,
                                                                   replicatedOffsets.durable,
                                                                   message.from());
    };

    public static final VersionedSerializer<BroadcastLogOffsets> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(BroadcastLogOffsets status, DataOutputPlus out, Version version) throws IOException
        {
            if (version.isAtLeast(Version.V2))
            {
                ShardMetadata.serializer.serialize(status.shardMetadata, out, version);
            }
            else
            {
                out.writeUTF(status.shardMetadata.keyspace);
                AbstractBounds.tokenSerializer.serialize(status.shardMetadata.range, out, version.messagingVersion());
            }
            CollectionSerializers.serializeList(status.replicatedOffsets, out, Offsets.serializer);
            out.writeBoolean(status.durable);
        }

        @Override
        public BroadcastLogOffsets deserialize(DataInputPlus in, Version version) throws IOException
        {
            ShardMetadata shardMetadata;
            if (version.isAtLeast(Version.V2))
            {
                shardMetadata = ShardMetadata.serializer.deserialize(in, version);
            }
            else
            {
                String keyspace = in.readUTF();
                Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
                ClusterMetadata cm = ClusterMetadata.current();
                Participants participants;
                try
                {
                    VersionedEndpoints.ForRange forRange = cm.placements.get(cm.schema.getKeyspace(keyspace).getMetadata().params.replication).writes.forRange(range);
                    participants = Participants.fromReplicas(forRange.get(), cm);
                }
                catch (Throwable t)
                {
                    participants = Participants.EMPTY;
                }
                shardMetadata = new ShardMetadata(keyspace, Epoch.EMPTY.getEpoch(), range, participants);
            }
            List<Offsets.Immutable> replicatedOffsets = CollectionSerializers.deserializeList(in, Offsets.serializer);
            boolean durable = in.readBoolean();
            return new BroadcastLogOffsets(shardMetadata, replicatedOffsets, durable);
        }

        @Override
        public long serializedSize(BroadcastLogOffsets replicatedOffsets, Version version)
        {
            long size = 0;
            if (version.isAtLeast(Version.V2))
            {
                size += ShardMetadata.serializer.serializedSize(replicatedOffsets.shardMetadata, version);
            }
            else
            {
                size += TypeSizes.sizeof(replicatedOffsets.shardMetadata.keyspace);
                size += AbstractBounds.tokenSerializer.serializedSize(replicatedOffsets.shardMetadata.range, version.messagingVersion());
            }
            size += CollectionSerializers.serializedListSize(replicatedOffsets.replicatedOffsets, Offsets.serializer);
            size += TypeSizes.sizeof(replicatedOffsets.durable);
            return size;
        }
    };
}
