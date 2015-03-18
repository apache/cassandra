/**
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.tracing.Tracing.TRACE_HEADER;
import static org.apache.cassandra.tracing.Tracing.TRACE_TYPE;
import static org.apache.cassandra.tracing.Tracing.isTracing;

public class MessageOut<T>
{
    public final InetAddress from;
    public final MessagingService.Verb verb;
    public final T payload;
    public final IVersionedSerializer<T> serializer;
    public final Map<String, byte[]> parameters;
    private long payloadSize = -1;
    private int payloadSizeVersion = -1;

    // we do support messages that just consist of a verb
    public MessageOut(MessagingService.Verb verb)
    {
        this(verb, null, null);
    }

    public MessageOut(MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer)
    {
        this(verb,
             payload,
             serializer,
             isTracing()
                 ? ImmutableMap.of(TRACE_HEADER, UUIDGen.decompose(Tracing.instance.getSessionId()),
                                   TRACE_TYPE, new byte[] { Tracing.TraceType.serialize(Tracing.instance.getTraceType()) })
                 : Collections.<String, byte[]>emptyMap());
    }

    private MessageOut(MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, Map<String, byte[]> parameters)
    {
        this(FBUtilities.getBroadcastAddress(), verb, payload, serializer, parameters);
    }

    @VisibleForTesting
    public MessageOut(InetAddress from, MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, Map<String, byte[]> parameters)
    {
        this.from = from;
        this.verb = verb;
        this.payload = payload;
        this.serializer = serializer;
        this.parameters = parameters;
    }

    public MessageOut<T> withParameter(String key, byte[] value)
    {
        ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
        builder.putAll(parameters).put(key, value);
        return new MessageOut<T>(verb, payload, serializer, builder.build());
    }

    private Stage getStage()
    {
        return MessagingService.verbStages.get(verb);
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getTimeout(verb);
    }

    public String toString()
    {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("TYPE:").append(getStage()).append(" VERB:").append(verb);
        return sbuf.toString();
    }

    public void serialize(DataOutputPlus out, int version) throws IOException
    {
        CompactEndpointSerializationHelper.serialize(from, out);

        out.writeInt(verb.ordinal());
        out.writeInt(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        long longSize = payloadSize(version);
        assert longSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        out.writeInt((int) longSize);
        if (payload != null)
            serializer.serialize(payload, out, version);
    }

    public int serializedSize(int version)
    {
        int size = CompactEndpointSerializationHelper.serializedSize(from);

        size += TypeSizes.NATIVE.sizeof(verb.ordinal());
        size += TypeSizes.NATIVE.sizeof(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            size += TypeSizes.NATIVE.sizeof(entry.getKey());
            size += TypeSizes.NATIVE.sizeof(entry.getValue().length);
            size += entry.getValue().length;
        }

        long longSize = payloadSize(version);
        assert longSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        size += TypeSizes.NATIVE.sizeof((int) longSize);
        size += longSize;
        return size;
    }

    /**
     * Calculate the size of the payload of this message for the specified protocol version
     * and memoize the result for the specified protocol version. Memoization only covers the protocol
     * version of the first invocation.
     *
     * It is not safe to call payloadSize concurrently from multiple threads unless it has already been invoked
     * once from a single thread and there is a happens before relationship between that invocation and other
     * threads concurrently invoking payloadSize.
     *
     * For instance it would be safe to invokePayload size to make a decision in the thread that created the message
     * and then hand it off to other threads via a thread-safe queue, volatile write, or synchronized/ReentrantLock.
     * @param version Protocol version to use when calculating payload size
     * @return Size of the payload of this message in bytes
     */
    public long payloadSize(int version)
    {
        if (payloadSize == -1)
        {
            payloadSize = payload == null ? 0 : serializer.serializedSize(payload, version);
            payloadSizeVersion = version;
        }
        else if (payloadSizeVersion != version)
        {
            return payload == null ? 0 : serializer.serializedSize(payload, version);
        }
        return payloadSize;
    }
}
