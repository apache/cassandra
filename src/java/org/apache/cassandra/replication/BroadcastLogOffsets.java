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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.utils.CollectionSerializers;

public class BroadcastLogOffsets
{
    private final String keyspace;
    private final Range<Token> range;
    private final List<Offsets.Immutable> replicatedOffsets;
    private final boolean durable;

    public BroadcastLogOffsets(String keyspace, Range<Token> range, List<Offsets.Immutable> offsets, boolean durable)
    {
        this.keyspace = keyspace;
        this.range = range;
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
        return "ShardReplicatedOffsets{" + keyspace + ", " + range + ", " + sb + ", " + durable + '}';
    }

    public static final IVerbHandler<BroadcastLogOffsets> verbHandler = message -> {
        MutationTrackingService.ensureEnabled();
        BroadcastLogOffsets replicatedOffsets = message.payload;
        MutationTrackingService.instance().updateReplicatedOffsets(replicatedOffsets.keyspace,
                                                                 replicatedOffsets.range,
                                                                 replicatedOffsets.replicatedOffsets,
                                                                 replicatedOffsets.durable,
                                                                 message.from());
    };

    public static final VersionedSerializer<BroadcastLogOffsets> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(BroadcastLogOffsets status, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(status.keyspace);
            AbstractBounds.tokenSerializer.serialize(status.range, out, version.messagingVersion());
            CollectionSerializers.serializeList(status.replicatedOffsets, out, Offsets.serializer);
            out.writeBoolean(status.durable);
        }

        @Override
        public BroadcastLogOffsets deserialize(DataInputPlus in, Version version) throws IOException
        {
            String keyspace = in.readUTF();
            Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version.messagingVersion());
            List<Offsets.Immutable> replicatedOffsets = CollectionSerializers.deserializeList(in, Offsets.serializer);
            boolean durable = in.readBoolean();
            return new BroadcastLogOffsets(keyspace, range, replicatedOffsets, durable);
        }

        @Override
        public long serializedSize(BroadcastLogOffsets replicatedOffsets, Version version)
        {
            long size = 0;
            size += TypeSizes.sizeof(replicatedOffsets.keyspace);
            size += AbstractBounds.tokenSerializer.serializedSize(replicatedOffsets.range, version.messagingVersion());
            size += CollectionSerializers.serializedListSize(replicatedOffsets.replicatedOffsets, Offsets.serializer);
            size += TypeSizes.sizeof(replicatedOffsets.durable);
            return size;
        }
    };
}
