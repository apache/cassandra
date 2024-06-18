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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.utils.Backoff;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

public class FetchMinEpoch
{
    public static final IVersionedSerializer<FetchMinEpoch> serializer = new IVersionedSerializer<>()
    {

        @Override
        public void serialize(FetchMinEpoch t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(t.ranges.size());
            for (TokenRange range : t.ranges)
                TokenRange.serializer.serialize(range, out, version);
        }

        @Override
        public FetchMinEpoch deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            List<TokenRange> ranges = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                ranges.add(TokenRange.serializer.deserialize(in, version));
            return new FetchMinEpoch(ranges);
        }

        @Override
        public long serializedSize(FetchMinEpoch t, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(t.ranges.size());
            for (TokenRange range : t.ranges)
                size += TokenRange.serializer.serializedSize(range, version);
            return size;
        }
    };
    public static final IVerbHandler<FetchMinEpoch> handler = new IVerbHandler<FetchMinEpoch>()
    {
        @Override
        public void doVerb(Message<FetchMinEpoch> message) throws IOException
        {
            Long epoch = AccordService.instance().minEpoch(message.payload.ranges);
            MessagingService.instance().respond(new Response(epoch), message);
        }
    };
    public final Collection<TokenRange> ranges;

    public FetchMinEpoch(Collection<TokenRange> ranges)
    {
        this.ranges = ranges;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchMinEpoch that = (FetchMinEpoch) o;
        return Objects.equals(ranges, that.ranges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ranges);
    }

    @Override
    public String toString()
    {
        return "FetchMinEpoch{" +
               "ranges=" + ranges +
               '}';
    }

    public static Future<Long> fetch(SharedContext context, Map<InetAddressAndPort, Set<TokenRange>> peers)
    {
        List<Future<Long>> accum = new ArrayList<>(peers.size());
        for (Map.Entry<InetAddressAndPort, Set<TokenRange>> e : peers.entrySet())
            accum.add(fetch(context, e.getKey(), e.getValue()));
        return FutureCombiner.successfulOf(accum).map(ls -> {
            Long min = null;
            for (Long l : ls)
            {
                if (l == null) continue;
                if (min == null) min = l;
                else min = Math.min(min, l);
            }
            return min;
        });
    }

    @VisibleForTesting
    static Future<Long> fetch(SharedContext context, InetAddressAndPort to, Set<TokenRange> value)
    {
        FetchMinEpoch req = new FetchMinEpoch(value);
        Backoff backoff = Backoff.fromConfig(context, DatabaseDescriptor.getAccord().minEpochSyncRetry);
        return context.messaging().<FetchMinEpoch, FetchMinEpoch.Response>sendWithRetries(backoff, context.optionalTasks()::schedule,
                                                                                          Verb.ACCORD_FETCH_MIN_EPOCH_REQ, req,
                                                                                          Iterators.cycle(to),
                                                                                          (i1, i2, i3) -> true,
                                                                                          (i1, i2, i3, i4) -> null)
                      .map(m -> m.payload.minEpoch);
    }

    public static class Response
    {
        public static final IVersionedSerializer<Response> serializer = new IVersionedSerializer<Response>()
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
