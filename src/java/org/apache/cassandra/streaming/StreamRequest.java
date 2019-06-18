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
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

public class StreamRequest
{
    public static final IVersionedSerializer<StreamRequest> serializer = new StreamRequestSerializer();

    public final String keyspace;
    //Full replicas and transient replicas are split based on the transient status of the remote we are fetching
    //from. We preserve this distinction so on completion we can log to a system table whether we got the data transiently
    //or fully from some remote. This is an important distinction for resumable bootstrap. The Replicas in these collections
    //are local replicas (or dummy if this is triggered by repair) and don't encode the necessary information about
    //what the remote provided.
    public final RangesAtEndpoint full;
    public final RangesAtEndpoint transientReplicas;
    public final Collection<String> columnFamilies = new HashSet<>();

    public StreamRequest(String keyspace, RangesAtEndpoint full, RangesAtEndpoint transientReplicas, Collection<String> columnFamilies)
    {
        this.keyspace = keyspace;
        if (!full.endpoint().equals(transientReplicas.endpoint()))
            throw new IllegalStateException("Mismatching endpoints: " + full + ", " + transientReplicas);

        this.full = full;
        this.transientReplicas = transientReplicas;
        this.columnFamilies.addAll(columnFamilies);
    }

    public static class StreamRequestSerializer implements IVersionedSerializer<StreamRequest>
    {
        public void serialize(StreamRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(request.keyspace);
            out.writeInt(request.columnFamilies.size());

            inetAddressAndPortSerializer.serialize(request.full.endpoint(), out, version);
            serializeReplicas(request.full, out, version);
            serializeReplicas(request.transientReplicas, out, version);
            for (String cf : request.columnFamilies)
                out.writeUTF(cf);
        }

        private void serializeReplicas(RangesAtEndpoint replicas, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(replicas.size());

            for (Replica replica : replicas)
            {
                IPartitioner.validate(replica.range());
                Token.serializer.serialize(replica.range().left, out, version);
                Token.serializer.serialize(replica.range().right, out, version);
            }
        }

        public StreamRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            String keyspace = in.readUTF();
            int cfCount = in.readInt();
            InetAddressAndPort endpoint = inetAddressAndPortSerializer.deserialize(in, version);

            RangesAtEndpoint full = deserializeReplicas(in, version, endpoint, true);
            RangesAtEndpoint transientReplicas = deserializeReplicas(in, version, endpoint, false);
            List<String> columnFamilies = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                columnFamilies.add(in.readUTF());
            return new StreamRequest(keyspace, full, transientReplicas, columnFamilies);
        }

        RangesAtEndpoint deserializeReplicas(DataInputPlus in, int version, InetAddressAndPort endpoint, boolean isFull) throws IOException
        {
            int replicaCount = in.readInt();

            RangesAtEndpoint.Builder replicas = RangesAtEndpoint.builder(endpoint, replicaCount);
            for (int i = 0; i < replicaCount; i++)
            {
                //TODO, super need to review the usage of streaming vs not streaming endpoint serialization helper
                //to make sure I'm not using the wrong one some of the time, like do repair messages use the
                //streaming version?
                Token left = Token.serializer.deserialize(in, IPartitioner.global(), version);
                Token right = Token.serializer.deserialize(in, IPartitioner.global(), version);
                replicas.add(new Replica(endpoint, new Range<>(left, right), isFull));
            }
            return replicas.build();
        }

        public long serializedSize(StreamRequest request, int version)
        {
            int size = TypeSizes.sizeof(request.keyspace);
            size += TypeSizes.sizeof(request.columnFamilies.size());
            size += inetAddressAndPortSerializer.serializedSize(request.full.endpoint(), version);
            size += replicasSerializedSize(request.transientReplicas, version);
            size += replicasSerializedSize(request.full, version);
            for (String cf : request.columnFamilies)
                size += TypeSizes.sizeof(cf);
            return size;
        }

        private long replicasSerializedSize(RangesAtEndpoint replicas, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(replicas.size());

            for (Replica replica : replicas)
            {
                size += Token.serializer.serializedSize(replica.range().left, version);
                size += Token.serializer.serializedSize(replica.range().right, version);
            }
            return size;
        }

    }
}
