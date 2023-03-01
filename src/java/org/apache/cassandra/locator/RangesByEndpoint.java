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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class RangesByEndpoint extends ReplicaMultimap<InetAddressAndPort, RangesAtEndpoint>
{
    public static RangesByEndpoint EMPTY = new RangesByEndpoint.Builder().build();

    public static final Serializer serializer = new Serializer();

    public RangesByEndpoint(Map<InetAddressAndPort, RangesAtEndpoint> map)
    {
        super(map);
    }

    public RangesAtEndpoint get(InetAddressAndPort endpoint)
    {
        Preconditions.checkNotNull(endpoint);
        return map.getOrDefault(endpoint, RangesAtEndpoint.empty(endpoint));
    }

    public static class Builder extends ReplicaMultimap.Builder<InetAddressAndPort, RangesAtEndpoint.Builder>
    {
        @Override
        protected RangesAtEndpoint.Builder newBuilder(InetAddressAndPort endpoint)
        {
            return new RangesAtEndpoint.Builder(endpoint);
        }

        public RangesByEndpoint build()
        {
            return new RangesByEndpoint(
                    ImmutableMap.copyOf(
                            Maps.transformValues(this.map, RangesAtEndpoint.Builder::build)));
        }
    }

    public static class Serializer implements MetadataSerializer<RangesByEndpoint>
    {
        public void serialize(RangesByEndpoint t, DataOutputPlus out, Version version) throws IOException
        {
            Set<Map.Entry<InetAddressAndPort, RangesAtEndpoint>> entries = t.entrySet();
            out.writeInt(entries.size());
            for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> entry : entries)
            {
                InetAddressAndPort.MetadataSerializer.serializer.serialize(entry.getKey(), out, version);
                AbstractReplicaCollection.ReplicaList replicas = entry.getValue().list;
                out.writeInt(replicas.size());
                for (Replica r : replicas)
                {
                    IPartitioner.validate(r.range());
                    Range.serializer.serialize(r.range(), out, version);
                    out.writeBoolean(r.isFull());
                }
            }
        }

        public RangesByEndpoint deserialize(DataInputPlus in, Version version) throws IOException
        {
            RangesByEndpoint.Builder builder = new Builder();
            int size = in.readInt();
            for (int i=0; i<size; i++)
            {
                InetAddressAndPort endpoint = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
                int replicas = in.readInt();
                for (int j=0; j<replicas; j++)
                {
                    Range<Token> range = Range.serializer.deserialize(in, version);
                    boolean full = in.readBoolean();
                    builder.put(endpoint, new Replica(endpoint, range, full));
                }
            }
            return builder.build();
        }

        public long serializedSize(RangesByEndpoint t, Version version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> entry : t.entrySet())
            {
                size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(entry.getKey(), version);
                size += TypeSizes.INT_SIZE;
                for (Replica r : entry.getValue().list)
                {
                    size += Range.serializer.serializedSize(r.range(), version);
                    size += TypeSizes.BOOL_SIZE;
                }
            }
            return size;
        }
    }
}
