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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;

public class BroadcastLogOffsets
{
    private static final Logger logger = LoggerFactory.getLogger(BroadcastLogOffsets.class);

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
        StringBuilder sb = new StringBuilder('[');
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
        BroadcastLogOffsets replicatedOffsets = message.payload;
        logger.trace("Received replicated offsets {} from {}", replicatedOffsets, message.from());
        MutationTrackingService.instance.updateReplicatedOffsets(replicatedOffsets.keyspace,
                                                                 replicatedOffsets.range,
                                                                 replicatedOffsets.replicatedOffsets,
                                                                 replicatedOffsets.durable,
                                                                 message.from());
    };

    public static final IVersionedSerializer<BroadcastLogOffsets> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(BroadcastLogOffsets status, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(status.keyspace);
            AbstractBounds.tokenSerializer.serialize(status.range, out, version);
            out.writeInt(status.replicatedOffsets.size());
            for (Offsets.Immutable logOffsets : status.replicatedOffsets)
                Offsets.serializer.serialize(logOffsets, out, version);
            out.writeBoolean(status.durable);
        }

        @Override
        public BroadcastLogOffsets deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version);
            int count = in.readInt();
            List<Offsets.Immutable> replicatedOffsets = new ArrayList<>(count);
            for (int i = 0; i < count; ++i)
                replicatedOffsets.add(Offsets.serializer.deserialize(in, version));
            boolean durable = in.readBoolean();
            return new BroadcastLogOffsets(keyspace, range, replicatedOffsets, durable);
        }

        @Override
        public long serializedSize(BroadcastLogOffsets replicatedOffsets, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(replicatedOffsets.keyspace);
            size += AbstractBounds.tokenSerializer.serializedSize(replicatedOffsets.range, version);
            size += TypeSizes.sizeof(replicatedOffsets.replicatedOffsets.size());
            for (Offsets.Immutable logOffsets : replicatedOffsets.replicatedOffsets)
                size += Offsets.serializer.serializedSize(logOffsets, version);
            size += TypeSizes.sizeof(replicatedOffsets.durable);
            return size;
        }
    };
}
