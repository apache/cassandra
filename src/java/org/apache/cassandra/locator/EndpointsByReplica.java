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

package org.apache.cassandra.locator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IPartitionerDependentSerializer;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;

import java.io.IOException;
import java.util.Map;

import static org.apache.cassandra.dht.AbstractBounds.tokenSerializer;

public class EndpointsByReplica extends ReplicaMultimap<Replica, EndpointsForRange>
{
    public static final Serializer serializer = new Serializer();
    public EndpointsByReplica(Map<Replica, EndpointsForRange> map)
    {
        super(map);
    }

    public EndpointsForRange get(Replica range)
    {
        Preconditions.checkNotNull(range);
        return map.getOrDefault(range, EndpointsForRange.empty(range.range()));
    }

    public static class Builder extends ReplicaMultimap.Builder<Replica, EndpointsForRange.Builder>
    {
        @Override
        protected EndpointsForRange.Builder newBuilder(Replica replica)
        {
            return new EndpointsForRange.Builder(replica.range());
        }

        // TODO: consider all ignoreDuplicates cases
        public void putAll(Replica range, EndpointsForRange replicas, Conflict ignoreConflicts)
        {
            map.computeIfAbsent(range, r -> newBuilder(r)).addAll(replicas, ignoreConflicts);
        }

        public EndpointsByReplica build()
        {
            return new EndpointsByReplica(
                    ImmutableMap.copyOf(
                            Maps.transformValues(this.map, EndpointsForRange.Builder::build)));
        }
    }

    public static class Serializer implements IPartitionerDependentSerializer<EndpointsByReplica>
    {
        @Override
        public void serialize(EndpointsByReplica t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(t.map.size());
            for (Map.Entry<Replica, EndpointsForRange> entry : t.map.entrySet())
            {
                Replica.serializer.serialize(entry.getKey(), out, version);
                EndpointsForRange efr = entry.getValue();
                tokenSerializer.serialize(efr.range(), out, version);
                out.writeUnsignedVInt32(efr.size());
                for (Replica replica : efr)
                    Replica.serializer.serialize(replica, out, version);
            }
        }

        @Override
        public EndpointsByReplica deserialize(DataInputPlus in, IPartitioner partitioner, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            EndpointsByReplica.Builder builder = new EndpointsByReplica.Builder();
            for (int i = 0; i < size; i++)
            {
                Replica replica = Replica.serializer.deserialize(in, partitioner, version);
                Range<Token> range = (Range<Token>) tokenSerializer.deserialize(in, partitioner, version);
                int efrSize = in.readUnsignedVInt32();
                EndpointsForRange.Builder efrBuilder = new EndpointsForRange.Builder(range, efrSize);
                for (int j = 0; j < efrSize; j++)
                    efrBuilder.add(Replica.serializer.deserialize(in, partitioner, version), Conflict.NONE);
                builder.putAll(replica, efrBuilder.build(), Conflict.NONE);

            }
            return builder.build();
        }

        @Override
        public long serializedSize(EndpointsByReplica t, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(t.map.size());
            for (Map.Entry<Replica, EndpointsForRange> entry : t.map.entrySet())
            {
                size += Replica.serializer.serializedSize(entry.getKey(), version);
                EndpointsForRange efr = entry.getValue();
                size += tokenSerializer.serializedSize(efr.range(), version);
                size += TypeSizes.sizeofUnsignedVInt(efr.size());
                for (Replica replica : efr)
                    size += Replica.serializer.serializedSize(replica, version);
            }
            return size;
        }
    }
}
