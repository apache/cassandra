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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.utils.Backoff;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static org.apache.cassandra.net.MessageDelivery.RetryErrorMessage;
import static org.apache.cassandra.net.MessageDelivery.RetryPredicate;
import static org.apache.cassandra.net.MessageDelivery.logger;

// TODO (required, efficiency): this can be simplified: we seem to always use "entire range"
public class FetchMinEpoch
{
    private static final FetchMinEpoch instance = new FetchMinEpoch();

    public static final IVersionedSerializer<FetchMinEpoch> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(FetchMinEpoch t, DataOutputPlus out, int version)
        {
        }

        @Override
        public FetchMinEpoch deserialize(DataInputPlus in, int version)
        {
            return FetchMinEpoch.instance;
        }

        @Override
        public long serializedSize(FetchMinEpoch t, int version)
        {
            return 0;
        }
    };

    public static final IVerbHandler<FetchMinEpoch> handler = message -> {
        if (AccordService.started())
        {
            Long epoch = AccordService.instance().minEpoch();
            MessagingService.instance().respond(new Response(epoch), message);
        }
        else
        {
            logger.error("Accord service is not started, resopnding with error to {}", message);
            MessagingService.instance().respondWithFailure(RequestFailure.BOOTING, message);
        }
    };

    private FetchMinEpoch()
    {
    }

    public static Future<Long> fetch(SharedContext context, Set<InetAddressAndPort> peers)
    {
        List<Future<Long>> accum = new ArrayList<>(peers.size());
        for (InetAddressAndPort peer : peers)
            accum.add(fetch(context, peer));
        // TODO (required): we are collecting only successes, but we need some threshold
        return FutureCombiner.successfulOf(accum).map(epochs -> {
            Long min = null;
            for (Long epoch : epochs)
            {
                if (epoch == null) continue;
                if (min == null) min = epoch;
                else min = Math.min(min, epoch);
            }
            return min;
        });
    }

    @VisibleForTesting
    static Future<Long> fetch(SharedContext context, InetAddressAndPort to)
    {
        Backoff backoff = Backoff.fromConfig(context, DatabaseDescriptor.getAccord().minEpochSyncRetry);
        return context.messaging().<FetchMinEpoch, Response>sendWithRetries(backoff,
                                                                            context.optionalTasks()::schedule,
                                                                            Verb.ACCORD_FETCH_MIN_EPOCH_REQ,
                                                                            FetchMinEpoch.instance,
                                                                            Iterators.cycle(to),
                                                                            RetryPredicate.ALWAYS_RETRY,
                                                                            RetryErrorMessage.EMPTY)
                      .map(m -> m.payload.minEpoch);
    }

    public static class Response
    {
        public static final IVersionedSerializer<Response> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(Response t, DataOutputPlus out, int version) throws IOException
            {
                out.writeBoolean(t.minEpoch != null);
                if (t.minEpoch != null)
                    out.writeUnsignedVInt(t.minEpoch);
            }

            @Override
            public Response deserialize(DataInputPlus in, int version) throws IOException
            {
                boolean notNull = in.readBoolean();
                return new Response(notNull ? in.readUnsignedVInt() : null);
            }

            @Override
            public long serializedSize(Response t, int version)
            {
                int size = TypeSizes.BOOL_SIZE;
                if (t.minEpoch != null)
                    size += TypeSizes.sizeofUnsignedVInt(t.minEpoch);
                return size;
            }
        };
        @Nullable
        public final Long minEpoch;

        public Response(@Nullable Long minEpoch)
        {
            this.minEpoch = minEpoch;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(minEpoch, response.minEpoch);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(minEpoch);
        }

        @Override
        public String toString()
        {
            return "Response{" +
                   "minEpoch=" + minEpoch +
                   '}';
        }
    }
}