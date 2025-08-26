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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.topology.Topology;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.UnversionedSerializer;
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

import static accord.topology.TopologyManager.TopologyRange;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retryFetchTopology;

/**
 * Fetch Accord topologies form remote peer.
 */
public class FetchTopologies
{
    private static final Logger logger = LoggerFactory.getLogger(FetchTopologies.class);
    public String toString()
    {
        return "FetchTopology{" +
               "epoch=" + minEpoch +
               '}';
    }

    private final long minEpoch;
    private final long maxEpoch;

    public static final UnversionedSerializer<FetchTopologies> serializer = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(FetchTopologies t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(t.minEpoch);
            out.writeUnsignedVInt(t.maxEpoch);
        }

        @Override
        public FetchTopologies deserialize(DataInputPlus in) throws IOException
        {
            return new FetchTopologies(in.readUnsignedVInt(), in.readUnsignedVInt());
        }

        @Override
        public long serializedSize(FetchTopologies t)
        {
            return TypeSizes.sizeofUnsignedVInt(t.minEpoch) +
                   TypeSizes.sizeofUnsignedVInt(t.maxEpoch);
        }
    };

    public FetchTopologies(long minEpoch, long maxEpoch)
    {
        this.minEpoch = minEpoch;
        this.maxEpoch = maxEpoch;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        FetchTopologies that = (FetchTopologies) o;
        return minEpoch == that.minEpoch && maxEpoch == that.maxEpoch;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minEpoch, maxEpoch);
    }

    public static final UnversionedSerializer<TopologyRange> responseSerializer = new UnversionedSerializer<>()
    {
            @Override
            public void serialize(TopologyRange t, DataOutputPlus out) throws IOException
            {
                out.writeUnsignedVInt(t.min);
                out.writeUnsignedVInt(t.current);
                out.writeUnsignedVInt(t.firstNonEmpty);
                out.writeUnsignedVInt32(t.topologies.size());

                for (Topology topology : t.topologies)
                    TopologySerializers.compactTopology.serialize(topology, out);
            }

            @Override
            public TopologyRange deserialize(DataInputPlus in) throws IOException
            {
                long min = in.readUnsignedVInt();
                long current = in.readUnsignedVInt();
                long firstNonEmpty = in.readUnsignedVInt();
                int count = in.readUnsignedVInt32();
                List<Topology> topologies = new ArrayList<>(count);
                for (int i = 0; i < count; ++i)
                    topologies.add(TopologySerializers.compactTopology.deserialize(in));
                return new TopologyRange(min, current, firstNonEmpty, topologies);
            }

            @Override
            public long serializedSize(TopologyRange t)
            {
                long size = TypeSizes.sizeofUnsignedVInt(t.min);
                size += TypeSizes.sizeofUnsignedVInt(t.current);
                size += TypeSizes.sizeofUnsignedVInt(t.firstNonEmpty);
                size += TypeSizes.sizeofUnsignedVInt(t.topologies.size());
                for (Topology topology : t.topologies)
                    size += TopologySerializers.compactTopology.serializedSize(topology);
                return size;
            }
        };

    public static final IVerbHandler<FetchTopologies> handler = message -> {
        if (!AccordService.isSetup())
        {
            logger.debug("Accord unitialized, responding with failure to {}", message.payload);
            MessagingService.instance().respondWithFailure(RequestFailure.UNKNOWN, message);
            return;
        }

        TopologyRange topologies = AccordService.instance().topology().between(message.payload.minEpoch, message.payload.maxEpoch);
        logger.debug("Responding with {} failure to {}", topologies, message.payload);
        MessagingService.instance().respond(topologies, message);
    };

    public static Future<TopologyRange> fetch(SharedContext context, Collection<InetAddressAndPort> peers, long minEpoch, long maxEpoch)
    {
        FetchTopologies request = new FetchTopologies(minEpoch, maxEpoch);
        return context.messaging().<FetchTopologies, TopologyRange>sendWithRetries(retryFetchTopology(),
                                                                                   context.optionalTasks()::schedule,
                                                                                   Verb.ACCORD_FETCH_TOPOLOGY_REQ,
                                                                                   request,
                                                                                   MessagingUtils.tryAliveFirst(context, peers, Verb.ACCORD_FETCH_TOPOLOGY_REQ.name()),
                                                                                   (attempt, from, failure) -> true,
                                                                                   MessageDelivery.RetryErrorMessage.EMPTY)
                      .map(m -> m.payload);
    }
}