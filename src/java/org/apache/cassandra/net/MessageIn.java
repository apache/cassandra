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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The receiving node's view of a {@link MessageOut}. See documentation on {@link MessageOut} for details on the
 * serialization format.
 *
 * @param <T> The type of the payload
 */
public class MessageIn<T>
{
    public final InetAddressAndPort from;
    public final T payload;
    public final Map<ParameterType, Object> parameters;
    public final MessagingService.Verb verb;
    public final int version;
    public final long constructionTime;

    private MessageIn(InetAddressAndPort from,
                      T payload,
                      Map<ParameterType, Object> parameters,
                      Verb verb,
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

    public static <T> MessageIn<T> create(InetAddressAndPort from,
                                          T payload,
                                          Map<ParameterType, Object> parameters,
                                          Verb verb,
                                          int version,
                                          long constructionTime)
    {
        return new MessageIn<>(from, payload, parameters, verb, version, constructionTime);
    }

    public static <T> MessageIn<T> create(InetAddressAndPort from,
                                          T payload,
                                          Map<ParameterType, Object> parameters,
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
        InetAddressAndPort from = CompactEndpointSerializationHelper.instance.deserialize(in, version);

        MessagingService.Verb verb = MessagingService.Verb.fromId(in.readInt());
        Map<ParameterType, Object> parameters = readParameters(in, version);
        int payloadSize = in.readInt();
        return read(in, version, id, constructionTime, from, payloadSize, verb, parameters);
    }

    public static Map<ParameterType, Object> readParameters(DataInputPlus in, int version) throws IOException
    {
        int parameterCount = in.readInt();
        Map<ParameterType, Object> parameters;
        if (parameterCount == 0)
        {
            return Collections.emptyMap();
        }
        else
        {
            ImmutableMap.Builder<ParameterType, Object> builder = ImmutableMap.builder();
            for (int i = 0; i < parameterCount; i++)
            {
                String key = in.readUTF();
                ParameterType type = ParameterType.byName.get(key);
                if (type != null)
                {
                    byte[] value = new byte[in.readInt()];
                    in.readFully(value);
                    builder.put(type, type.serializer.deserialize(new DataInputBuffer(value), version));
                }
                else
                {
                    in.skipBytes(in.readInt());
                }
            }
            return builder.build();
        }
    }

    public static <T2> MessageIn<T2> read(DataInputPlus in, int version, int id, long constructionTime,
                                          InetAddressAndPort from, int payloadSize, Verb verb, Map<ParameterType, Object> parameters) throws IOException
    {
        IVersionedSerializer<T2> serializer = (IVersionedSerializer<T2>) MessagingService.verbSerializers.get(verb);
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

    public static long deriveConstructionTime(InetAddressAndPort from, int messageTimestamp, long currentTime)
    {
        // Reconstruct the message construction time sent by the remote host (we sent only the lower 4 bytes, assuming the
        // higher 4 bytes wouldn't change between the sender and receiver)
        long sentConstructionTime = (currentTime & 0xFFFFFFFF00000000L) | (((messageTimestamp & 0xFFFFFFFFL) << 2) >> 2);

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
        return !from.equals(FBUtilities.getBroadcastAddressAndPort());
    }

    public Stage getMessageType()
    {
        return MessagingService.verbStages.get(verb);
    }

    public boolean doCallbackOnFailure()
    {
        return parameters.containsKey(ParameterType.FAILURE_CALLBACK);
    }

    public boolean isFailureResponse()
    {
        return parameters.containsKey(ParameterType.FAILURE_RESPONSE);
    }

    public RequestFailureReason getFailureReason()
    {
        Short code = (Short)parameters.get(ParameterType.FAILURE_REASON);
        return code != null ? RequestFailureReason.fromCode(code) : RequestFailureReason.UNKNOWN;
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
