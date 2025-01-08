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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.Collection;

import accord.topology.Topology;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingUtils;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.utils.concurrent.Future;

public class FetchTopology
{
    private final long epoch;

    public static final IVersionedSerializer<FetchTopology> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(FetchTopology t, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(t.epoch);
        }

        @Override
        public FetchTopology deserialize(DataInputPlus in, int version) throws IOException
        {
            return new FetchTopology(in.readLong());
        }

        @Override
        public long serializedSize(FetchTopology t, int version)
        {
            return Long.BYTES;
        }
    };

    public FetchTopology(long epoch)
    {
        this.epoch = epoch;
    }

    public static class Response
    {
        private static Response UNKNOWN = new Response(-1, null) {
            public String toString()
            {
                return "UNKNOWN_TOPOLOGY{}";
            }
        };

        // TODO (required): messaging version after version patch
        public static final IVersionedSerializer<Response> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Response t, DataOutputPlus out, int version) throws IOException
            {
                if (t == UNKNOWN)
                {
                    out.writeLong(-1);
                    return;
                }
                out.writeLong(t.epoch);
                TopologySerializers.topology.serialize(t.topology, out, version);
            }

            @Override
            public Response deserialize(DataInputPlus in, int version) throws IOException
            {
                long epoch = in.readLong();
                if (epoch == -1)
                    return UNKNOWN;
                Topology topology = TopologySerializers.topology.deserialize(in, version);
                return new Response(epoch, topology);
            }

            @Override
            public long serializedSize(Response t, int version)
            {
                if (t == UNKNOWN)
                    return Long.BYTES;

                return Long.BYTES + TopologySerializers.topology.serializedSize(t.topology, version);
            }
        };

        private final long epoch;
        private final Topology topology;

        public Response(long epoch, Topology topology)
        {
            this.epoch = epoch;
            this.topology = topology;
        }
    }

    public static final IVerbHandler<FetchTopology> handler = message -> {
        long epoch = message.payload.epoch;
        Topology topology = AccordService.instance().topology().maybeGlobalForEpoch(epoch);
        if (topology == null)
            MessagingService.instance().respond(Response.UNKNOWN, message);
        else
            MessagingService.instance().respond(new Response(epoch, topology), message);
    };

    public static Future<Topology> fetch(SharedContext context, Collection<InetAddressAndPort> peers, long epoch)
    {
        FetchTopology req = new FetchTopology(epoch);
        return context.messaging().<FetchTopology, Response>sendWithRetries(Verb.ACCORD_FETCH_TOPOLOGY_REQ, req, MessagingUtils.tryAliveFirst(SharedContext.Global.instance, peers),
                                                                                          // If the epoch is already discovered, no need to retry
                                                                                          (attempt, from, failure) -> AccordService.instance().currentEpoch() < epoch,
                                                                                          MessageDelivery.RetryErrorMessage.EMPTY)
                      .map(m -> m.payload.topology);
    }
}