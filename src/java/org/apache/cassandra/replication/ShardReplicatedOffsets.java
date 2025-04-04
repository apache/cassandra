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

public class ShardReplicatedOffsets
{
    private static final Logger logger = LoggerFactory.getLogger(ShardReplicatedOffsets.class);

    private final String keyspace;
    private final Range<Token> range;
    private final List<Offsets.Immutable> replicatedOffsets;

    public ShardReplicatedOffsets(String keyspace, Range<Token> range, List<Offsets.Immutable> offsets)
    {
        this.keyspace = keyspace;
        this.range = range;
        this.replicatedOffsets = offsets;
    }

    boolean isEmpty()
    {
        return replicatedOffsets.isEmpty();
    }

    @Override
    public String toString()
    {
        return "ShardReplicatedOffsets{" + keyspace + ", " + range + ", " + replicatedOffsets + '}';
    }

    public static final IVerbHandler<ShardReplicatedOffsets> verbHandler = message -> {
        ShardReplicatedOffsets replicatedOffsets = message.payload;
        logger.trace("Received replicated offsets {} from {}", replicatedOffsets, message.from());
        MutationTrackingService.instance.updateReplicatedOffsets(replicatedOffsets.keyspace,
                                                                 replicatedOffsets.range,
                                                                 replicatedOffsets.replicatedOffsets,
                                                                 message.from());
    };

    public static final IVersionedSerializer<ShardReplicatedOffsets> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(ShardReplicatedOffsets status, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(status.keyspace);
            AbstractBounds.tokenSerializer.serialize(status.range, out, version);
            out.writeInt(status.replicatedOffsets.size());
            for (Offsets.Immutable logOffsets : status.replicatedOffsets)
                Offsets.serializer.serialize(logOffsets, out, version);
        }

        @Override
        public ShardReplicatedOffsets deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            Range<Token> range = (Range<Token>) AbstractBounds.tokenSerializer.deserialize(in, IPartitioner.global(), version);
            int count = in.readInt();
            List<Offsets.Immutable> replicatedOffsets = new ArrayList<>(count);
            for (int i = 0; i < count; ++i)
                replicatedOffsets.add(Offsets.serializer.deserialize(in, version));
            return new ShardReplicatedOffsets(keyspace, range, replicatedOffsets);
        }

        @Override
        public long serializedSize(ShardReplicatedOffsets replicatedOffsets, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(replicatedOffsets.keyspace);
            size += AbstractBounds.tokenSerializer.serializedSize(replicatedOffsets.range, version);
            size += TypeSizes.sizeof(replicatedOffsets.replicatedOffsets.size());
            for (Offsets.Immutable logOffsets : replicatedOffsets.replicatedOffsets)
                size += Offsets.serializer.serializedSize(logOffsets, version);
            return size;
        }
    };
}
