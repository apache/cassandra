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
import org.apache.cassandra.db.monitoring.ConstructionTime;
import org.apache.cassandra.db.monitoring.MonitorableImpl;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.FileUtils;

public class MessageIn<T>
{
    public final InetAddress from;
    public final T payload;
    public final Map<String, byte[]> parameters;
    public final MessagingService.Verb verb;
    public final int version;
    public final ConstructionTime constructionTime;

    private MessageIn(InetAddress from,
                      T payload,
                      Map<String, byte[]> parameters,
                      MessagingService.Verb verb,
                      int version,
                      ConstructionTime constructionTime)
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
                                          ConstructionTime constructionTime)
    {
        return new MessageIn<>(from, payload, parameters, verb, version, constructionTime);
    }

    public static <T2> MessageIn<T2> read(DataInputPlus in, int version, int id) throws IOException
    {
        return read(in, version, id, new ConstructionTime());
    }

    public static <T2> MessageIn<T2> read(DataInputPlus in, int version, int id, ConstructionTime constructionTime) throws IOException
    {
        InetAddress from = CompactEndpointSerializationHelper.deserialize(in);

        MessagingService.Verb verb = MessagingService.Verb.values()[in.readInt()];
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

    public static ConstructionTime createTimestamp()
    {
        return new ConstructionTime();
    }

    public static ConstructionTime readTimestamp(DataInputPlus input) throws IOException
    {
        // make sure to readInt, even if cross_node_to is not enabled
        int partial = input.readInt();
        if(DatabaseDescriptor.hasCrossNodeTimeout())
        {
            long timestamp = System.currentTimeMillis();
            long crossNodeTimestamp = (timestamp & 0xFFFFFFFF00000000L) | (((partial & 0xFFFFFFFFL) << 2) >> 2);
            return new ConstructionTime(crossNodeTimestamp, timestamp != crossNodeTimestamp);
        }
        else
        {
            return new ConstructionTime();
        }
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

    public long getTimeout()
    {
        return DatabaseDescriptor.getTimeout(verb);
    }

    public String toString()
    {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("FROM:").append(from).append(" TYPE:").append(getMessageType()).append(" VERB:").append(verb);
        return sbuf.toString();
    }
}
