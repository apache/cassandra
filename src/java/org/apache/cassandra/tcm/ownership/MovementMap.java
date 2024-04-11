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

package org.apache.cassandra.tcm.ownership;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsByReplica;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;

public class MovementMap extends ReplicationMap<EndpointsByReplica>
{
    private static final MovementMap EMPTY = new MovementMap(Collections.emptyMap());
    public static final IVersionedSerializer<MovementMap> serializer = new Serializer();

    private MovementMap(Map<ReplicationParams, EndpointsByReplica> map)
    {
        super(map);
    }

    protected EndpointsByReplica defaultValue()
    {
        return new EndpointsByReplica.Builder().build();
    }

    protected EndpointsByReplica localOnly()
    {
        throw new IllegalStateException("Cannot move local ranges");
    }

    public String toString()
    {
        return "MovementMap{" +
               "map=" + asMap() +
               '}';
    }

    public static MovementMap empty()
    {
        return EMPTY;
    }

    public Map<InetAddressAndPort, MovementMap> byEndpoint()
    {
        Map<InetAddressAndPort, Map<ReplicationParams, EndpointsByReplica.Builder>> builder = new HashMap<>();
        map.forEach((params, endpointsByReplica) -> {
            for (Replica dst : endpointsByReplica.keySet())
            {
                Map<ReplicationParams, EndpointsByReplica.Builder> cur = builder.computeIfAbsent(dst.endpoint(), k -> new HashMap<>());
                EndpointsByReplica.Builder curBuilder = cur.computeIfAbsent(params, k -> new EndpointsByReplica.Builder());
                for (Replica src : endpointsByReplica.get(dst))
                    curBuilder.put(dst, src);
            }
        });
        // and .build the builders created above:
        Map<InetAddressAndPort, MovementMap> built = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, Map<ReplicationParams, EndpointsByReplica.Builder>> entry : builder.entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            Map<ReplicationParams, EndpointsByReplica.Builder> builders = entry.getValue();

            Map<ReplicationParams, EndpointsByReplica> byParams = new HashMap<>();
            for (Map.Entry<ReplicationParams, EndpointsByReplica.Builder> builderEntry : builders.entrySet())
                byParams.put(builderEntry.getKey(), builderEntry.getValue().build());
            built.put(endpoint, new MovementMap(byParams));
        }
        return built;
    }

    public static Builder builder()
    {
        return new Builder(new HashMap<>());
    }

    public static Builder builder(int expectedSize)
    {
        return new Builder(Maps.newHashMapWithExpectedSize(expectedSize));
    }

    public static class Builder
    {
        private final Map<ReplicationParams, EndpointsByReplica> map;
        private Builder(Map<ReplicationParams, EndpointsByReplica> map)
        {
            this.map = map;
        }

        public Builder put(ReplicationParams params, EndpointsByReplica placement)
        {
            map.put(params, placement);
            return this;
        }

        public MovementMap build()
        {
            return new MovementMap(map);
        }
    }

    public static class Serializer implements IVersionedSerializer<MovementMap>
    {
        @Override
        public void serialize(MovementMap t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(t.size());
            for (Map.Entry<ReplicationParams, EndpointsByReplica> entry : t.asMap().entrySet())
            {
                ReplicationParams.messageSerializer.serialize(entry.getKey(), out, version);
                EndpointsByReplica.serializer.serialize(entry.getValue(), out, version);
            }
        }

        @Override
        public MovementMap deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            MovementMap.Builder builder = MovementMap.builder(size);
            for (int i = 0; i < size; i++)
            {
                ReplicationParams params = ReplicationParams.messageSerializer.deserialize(in, version);
                IPartitioner partitioner = params.isMeta() ? MetaStrategy.partitioner : IPartitioner.global();
                EndpointsByReplica endpointsByReplica = EndpointsByReplica.serializer.deserialize(in, partitioner, version);
                builder.put(params, endpointsByReplica);
            }
            return builder.build();
        }

        @Override
        public long serializedSize(MovementMap t, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(t.size());
            for (Map.Entry<ReplicationParams, EndpointsByReplica> entry : t.asMap().entrySet())
            {
                size += ReplicationParams.messageSerializer.serializedSize(entry.getKey(), version);
                size += EndpointsByReplica.serializer.serializedSize(entry.getValue(), version);
            }
            return size;
        }
    }
}
