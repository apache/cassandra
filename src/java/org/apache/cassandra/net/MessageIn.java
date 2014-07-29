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

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FileUtils;

public class MessageIn<T>
{
    private static final Logger logger = LoggerFactory.getLogger(MessageIn.class);

    public final InetAddress from;
    public final T payload;
    public final Map<String, byte[]> parameters;
    public final MessagingService.Verb verb;
    public final int version;

    private MessageIn(InetAddress from, T payload, Map<String, byte[]> parameters, MessagingService.Verb verb, int version)
    {
        this.from = from;
        this.payload = payload;
        this.parameters = parameters;
        this.verb = verb;
        this.version = version;
    }

    public static <T> MessageIn<T> create(InetAddress from, T payload, Map<String, byte[]> parameters, MessagingService.Verb verb, int version)
    {
        return new MessageIn<T>(from, payload, parameters, verb, version);
    }

    public static <T2> MessageIn<T2> read(DataInput in, int version, int id) throws IOException
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
                FileUtils.skipBytesFully(in, payloadSize);
                return null;
            }
            serializer = (IVersionedSerializer<T2>) callback.serializer;
        }
        if (payloadSize == 0 || serializer == null)
            return create(from, null, parameters, verb, version);
        T2 payload = serializer.deserialize(in, version);
        return MessageIn.create(from, payload, parameters, verb, version);
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
