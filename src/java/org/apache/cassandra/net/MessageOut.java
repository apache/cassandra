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
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.tracing.Tracing.isTracing;

/**
 * Each message contains a header with several fixed fields, an optional key-value parameters section, and then
 * the message payload itself. Note: the IP address in the header may be either IPv4 (4 bytes) or IPv6 (16 bytes).
 * The diagram below shows the IPv4 address for brevity.
 *
 * <pre>
 * {@code
 *            1 1 1 1 1 2 2 2 2 2 3 3 3 3 3 4 4 4 4 4 5 5 5 5 5 6 6
 *  0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0 2
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                       PROTOCOL MAGIC                          |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Message ID                            |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                         Timestamp                             |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Addr len |           IP Address (IPv4)                       /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /           |                 Verb                              /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /           |            Parameters size                        /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /           |             Parameter data                        /
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * /                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                        Payload size                           |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                                                               /
 * /                           Payload                             /
 * /                                                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }
 * </pre>
 *
 * An individual parameter has a String key and a byte array value. The key is serialized with it's length,
 * encoded as two bytes, followed by the UTF-8 byte encoding of the string (see {@link java.io.DataOutput#writeUTF(java.lang.String)}).
 * The body is serialized with it's length, encoded as four bytes, followed by the bytes of the value.
 *
 * * @param <T> The type of the message payload.
 */
public class MessageOut<T>
{
    private static final int SERIALIZED_SIZE_VERSION_UNDEFINED = -1;

    public final InetAddress from;
    public final MessagingService.Verb verb;
    public final T payload;
    public final IVersionedSerializer<T> serializer;
    public final Map<String, byte[]> parameters;

    /**
     * Memoization of the serialized size of the just the payload.
     */
    private int payloadSerializedSize = -1;

    /**
     * Memoization of the serialized size of the entire message.
     */
    private int serializedSize = -1;

    /**
     * The internode protocol messaging version that was used to calculate the memoized serailized sizes.
     */
    private int serializedSizeVersion = SERIALIZED_SIZE_VERSION_UNDEFINED;

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
                 ? Tracing.instance.getTraceHeaders()
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

    public Stage getStage()
    {
        return MessagingService.verbStages.get(verb);
    }

    public long getTimeout()
    {
        return verb.getTimeout();
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

        out.writeInt(verb.getId());
        out.writeInt(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        if (payload != null)
        {
            int payloadSize = payloadSerializedSize >= 0
                              ? (int)payloadSerializedSize
                              : (int) serializer.serializedSize(payload, version);

            out.writeInt(payloadSize);
            serializer.serialize(payload, out, version);
        }
        else
        {
            out.writeInt(0);
        }
    }

    private Pair<Long, Long> calculateSerializedSize(int version)
    {
        long size = CompactEndpointSerializationHelper.serializedSize(from);

        size += TypeSizes.sizeof(verb.getId());
        size += TypeSizes.sizeof(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet())
        {
            size += TypeSizes.sizeof(entry.getKey());
            size += TypeSizes.sizeof(entry.getValue().length);
            size += entry.getValue().length;
        }

        long payloadSize = payload == null ? 0 : serializer.serializedSize(payload, version);
        assert payloadSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        size += TypeSizes.sizeof((int) payloadSize);
        size += payloadSize;
        return Pair.create(size, payloadSize);
    }

    /**
     * Calculate the size of this message for the specified protocol version and memoize the result for the specified
     * protocol version. Memoization only covers the protocol version of the first invocation.
     *
     * It is not safe to call this function concurrently from multiple threads unless it has already been invoked
     * once from a single thread and there is a happens before relationship between that invocation and other
     * threads concurrently invoking this function.
     *
     * For instance it would be safe to invokePayload size to make a decision in the thread that created the message
     * and then hand it off to other threads via a thread-safe queue, volatile write, or synchronized/ReentrantLock.
     *
     * @param version Protocol version to use when calculating size
     * @return Size of this message in bytes, which will be less than or equal to {@link Integer#MAX_VALUE}
     */
    public int serializedSize(int version)
    {
        if (serializedSize > 0 && serializedSizeVersion == version)
            return serializedSize;

        Pair<Long, Long> sizes = calculateSerializedSize(version);
        if (sizes.left > Integer.MAX_VALUE)
            throw new IllegalStateException("message size exceeds maximum allowed size: size = " + sizes.left);

        if (serializedSizeVersion == SERIALIZED_SIZE_VERSION_UNDEFINED)
        {
            serializedSize = sizes.left.intValue();
            payloadSerializedSize = sizes.right.intValue();
            serializedSizeVersion = version;
        }

        return sizes.left.intValue();
    }
}
