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
package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;

public class MessageIn<T>
{
    public final InetAddress from;
    public final T payload;
    public final Map<String, byte[]> parameters;
    public final MessagingService.Verb verb;
    public final int version;
    public final long constructionTime;

    private MessageIn(InetAddress from,
                      T payload,
                      Map<String, byte[]> parameters,
                      MessagingService.Verb verb,
                      int version,
                      long constructionTime)
    {
        this.from = from;
        this.payload = payload;
        this.parameters = parameters;
        this.verb = verb;
        this.version = version;
        this.constructionTime = constructionTime;
    }

    public static <T> MessageIn<T> create(InetAddress from,
                                          T payload,
                                          Map<String, byte[]> parameters,
                                          MessagingService.Verb verb,
                                          int version,
                                          long constructionTime)
    {
        return new MessageIn<>(from, payload, parameters, verb, version, constructionTime);
    }

    public static <T> MessageIn<T> create(InetAddress from,
                                          T payload,
                                          Map<String, byte[]> parameters,
                                          MessagingService.Verb verb,
                                          int version)
    {
        return new MessageIn<>(from, payload, parameters, verb, version, ApproximateTime.currentTimeMillis());
    }

    public static <T2> MessageIn<T2> read(DataInputPlus in, int version, int id) throws IOException
    {
        return read(in, version, id, ApproximateTime.currentTimeMillis());
    }

    public static <T2> MessageIn<T2> read(DataInputPlus in, int version, int id, long constructionTime) throws IOException
    {
        InetAddress from = CompactEndpointSerializationHelper.deserialize(in);

        MessagingService.Verb verb = MessagingService.verbValues[in.readInt()];
        int parameterCount = in.readInt();
        Map<String, byte[]> parameters;
        if (parameterCount == 0)
        {
            parameters = Collections.emptyMap();
        }
        else
        {
            ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
            for (int i = 0; i < parameterCount; i++)
            {
                String key = in.readUTF();
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                builder.put(key, value);
            }
            parameters = builder.build();
        }

        int payloadSize = in.readInt();
        IVersionedSerializer<T2> serializer = (IVersionedSerializer<T2>) MessagingService.instance().verbSerializers.get(verb);
        if (serializer instanceof MessagingService.CallbackDeterminedSerializer)
        {
            CallbackInfo callback = MessagingService.instance().getRegisteredCallback(id);
            if (callback == null)
            {
                // reply for expired callback.  we'll have to skip it.
                in.skipBytesFully(payloadSize);
                return null;
            }
            serializer = (IVersionedSerializer<T2>) callback.serializer;
        }
        if (payloadSize == 0 || serializer == null)
            return create(from, null, parameters, verb, version, constructionTime);

        T2 payload = serializer.deserialize(in, version);
        return MessageIn.create(from, payload, parameters, verb, version, constructionTime);
    }

    public static long readConstructionTime(InetAddress from, DataInputPlus input, long currentTime) throws IOException
    {
        // Reconstruct the message construction time sent by the remote host (we sent only the lower 4 bytes, assuming the
        // higher 4 bytes wouldn't change between the sender and receiver)
        int partial = input.readInt(); // make sure to readInt, even if cross_node_to is not enabled
        long sentConstructionTime = (currentTime & 0xFFFFFFFF00000000L) | (((partial & 0xFFFFFFFFL) << 2) >> 2);

        // Because nodes may not have their clock perfectly in sync, it's actually possible the sentConstructionTime is
        // later than the currentTime (the received time). If that's the case, as we definitively know there is a lack
        // of proper synchronziation of the clock, we ignore sentConstructionTime. We also ignore that
        // sentConstructionTime if we're told to.
        long elapsed = currentTime - sentConstructionTime;
        if (elapsed > 0)
            MessagingService.instance().metrics.addTimeTaken(from, elapsed);

        boolean useSentTime = DatabaseDescriptor.hasCrossNodeTimeout() && elapsed > 0;
        return useSentTime ? sentConstructionTime : currentTime;
    }

    /**
     * Since how long (in milliseconds) the message has lived.
     */
    public long getLifetimeInMS()
    {
        return ApproximateTime.currentTimeMillis() - constructionTime;
    }

    /**
     * Whether the message has crossed the node boundary, that is whether it originated from another node.
     *
     */
    public boolean isCrossNode()
    {
        return !from.equals(DatabaseDescriptor.getBroadcastAddress());
    }

    public Stage getMessageType()
    {
        return MessagingService.verbStages.get(verb);
    }

    public boolean doCallbackOnFailure()
    {
        return parameters.containsKey(MessagingService.FAILURE_CALLBACK_PARAM);
    }

    public boolean isFailureResponse()
    {
        return parameters.containsKey(MessagingService.FAILURE_RESPONSE_PARAM);
    }

    public boolean containsFailureReason()
    {
        return parameters.containsKey(MessagingService.FAILURE_REASON_PARAM);
    }

    public RequestFailureReason getFailureReason()
    {
        if (containsFailureReason())
        {
            try (DataInputBuffer in = new DataInputBuffer(parameters.get(MessagingService.FAILURE_REASON_PARAM)))
            {
                return RequestFailureReason.fromCode(in.readUnsignedShort());
            }
            catch (IOException ex)
            {
                throw new RuntimeException(ex);
            }
        }
        else
        {
            return RequestFailureReason.UNKNOWN;
        }
    }

    public long getTimeout()
    {
        return verb.getTimeout();
    }

    public long getSlowQueryTimeout()
    {
        return DatabaseDescriptor.getSlowQueryTimeout();
    }

    public String toString()
    {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("FROM:").append(from).append(" TYPE:").append(getMessageType()).append(" VERB:").append(verb);
        return sbuf.toString();
    }
}
