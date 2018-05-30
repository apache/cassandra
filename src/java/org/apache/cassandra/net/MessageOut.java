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

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.tracing.Tracing.isTracing;

/**
 * Each message contains a header with several fixed fields, an optional key-value parameters section, and then
 * the message payload itself. Note: the legacy IP address (pre-4.0) in the header may be either IPv4 (4 bytes)
 * or IPv6 (16 bytes). The diagram below shows the IPv4 address for brevity. In pre-4.0, the payloadSize was
 * encoded as a 4-byte integer; in 4.0 and up it is an unsigned byte (255 parameters should be enough for anyone).
 *
 * <pre>
 * {@code
 *            1 1 1 1 1 2 2 2 2 2 3
 *  0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |       PROTOCOL MAGIC          |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |        Message ID             |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |        Timestamp              |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |          Verb                 |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |ParmLen| Parameter data (var)  |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |   Payload size (vint)         |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |                               /
 * /           Payload             /
 * /                               |
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
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
    //Parameters are stored in an object array as tuples of size two
    public static final int PARAMETER_TUPLE_SIZE = 2;
    //Offset in a parameter tuple containing the type of the parameter
    public static final int PARAMETER_TUPLE_TYPE_OFFSET = 0;
    //Offset in a parameter tuple containing the actual parameter represented as a POJO
    public static final int PARAMETER_TUPLE_PARAMETER_OFFSET = 1;

    public final InetAddressAndPort from;
    public final MessagingService.Verb verb;
    public final T payload;
    public final IVersionedSerializer<T> serializer;
    //A list of tuples, first object is the ParameterType enum value,
    //the second object is the POJO to serialize
    public final List<Object> parameters;

    /**
     * Allows sender to explicitly state which connection type the message should be sent on.
     */
    public final ConnectionType connectionType;

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
             isTracing() ? Tracing.instance.getTraceHeaders() : ImmutableList.of(),
             null);
    }

    public MessageOut(MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, ConnectionType connectionType)
    {
        this(verb,
             payload,
             serializer,
             isTracing() ? Tracing.instance.getTraceHeaders() : ImmutableList.of(),
             connectionType);
    }

    private MessageOut(MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, List<Object> parameters, ConnectionType connectionType)
    {
        this(FBUtilities.getBroadcastAddressAndPort(), verb, payload, serializer, parameters, connectionType);
    }

    @VisibleForTesting
    public MessageOut(InetAddressAndPort from, MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, List<Object> parameters, ConnectionType connectionType)
    {
        this.from = from;
        this.verb = verb;
        this.payload = payload;
        this.serializer = serializer;
        this.parameters = parameters;
        this.connectionType = connectionType;
    }

    public <VT> MessageOut<T> withParameter(ParameterType type, VT value)
    {
        List<Object> newParameters = new ArrayList<>(parameters.size() + 2);
        newParameters.addAll(parameters);
        newParameters.add(type);
        newParameters.add(value);
        return new MessageOut<T>(from, verb, payload, serializer, newParameters, connectionType);
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
        if (version >= MessagingService.VERSION_40)
            serialize40(out, version);
        else
            serializePre40(out, version);
    }

    private void serialize40(DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(verb.getId());

        // serialize the headers, if any
        assert parameters.size() % PARAMETER_TUPLE_SIZE == 0;
        if (parameters.isEmpty())
        {
            out.writeVInt(0);
        }
        else
        {
            try (DataOutputBuffer buf = new DataOutputBuffer())
            {
                serializeParams(buf, version);
                out.writeUnsignedVInt(buf.getLength());
                out.write(buf.buffer());
            }
        }

        if (payload != null)
        {
            int payloadSize = payloadSerializedSize >= 0
                              ? payloadSerializedSize
                              : (int) serializer.serializedSize(payload, version);

            out.writeUnsignedVInt(payloadSize);
            serializer.serialize(payload, out, version);
        }
        else
        {
            out.writeUnsignedVInt(0);
        }
    }

    private void serializePre40(DataOutputPlus out, int version) throws IOException
    {
        CompactEndpointSerializationHelper.instance.serialize(from, out, version);
        out.writeInt(verb.getId());

        assert parameters.size() % PARAMETER_TUPLE_SIZE == 0;
        out.writeInt(parameters.size() / PARAMETER_TUPLE_SIZE);
        serializeParams(out, version);

        if (payload != null)
        {
            int payloadSize = payloadSerializedSize >= 0
                              ? payloadSerializedSize
                              : (int) serializer.serializedSize(payload, version);

            out.writeInt(payloadSize);
            serializer.serialize(payload, out, version);
        }
        else
        {
            out.writeInt(0);
        }
    }

    private void serializeParams(DataOutputPlus out, int version) throws IOException
    {
        for (int ii = 0; ii < parameters.size(); ii += PARAMETER_TUPLE_SIZE)
        {
            ParameterType type = (ParameterType)parameters.get(ii + PARAMETER_TUPLE_TYPE_OFFSET);
            out.writeUTF(type.key);
            IVersionedSerializer serializer = type.serializer;
            Object parameter = parameters.get(ii + PARAMETER_TUPLE_PARAMETER_OFFSET);

            int valueLength = Ints.checkedCast(serializer.serializedSize(parameter, version));
            if (version >= MessagingService.VERSION_40)
                out.writeUnsignedVInt(valueLength);
            else
                out.writeInt(valueLength);

            serializer.serialize(parameter, out, version);
        }
    }

    private MessageOutSizes calculateSerializedSize(int version)
    {
        return version >= MessagingService.VERSION_40
               ? calculateSerializedSize40(version)
               : calculateSerializedSizePre40(version);
    }

    private MessageOutSizes calculateSerializedSize40(int version)
    {
        long size = 0;
        size += TypeSizes.sizeof(verb.getId());

        if (parameters.isEmpty())
        {
            size += VIntCoding.computeVIntSize(0);
        }
        else
        {
            // calculate the params size independently, as we write that before the actual params block
            int paramsSize = 0;
            for (int ii = 0; ii < parameters.size(); ii += PARAMETER_TUPLE_SIZE)
            {
                ParameterType type = (ParameterType)parameters.get(ii + PARAMETER_TUPLE_TYPE_OFFSET);
                paramsSize += TypeSizes.sizeof(type.key());
                IVersionedSerializer serializer = type.serializer;
                Object parameter = parameters.get(ii + PARAMETER_TUPLE_PARAMETER_OFFSET);
                int valueLength = Ints.checkedCast(serializer.serializedSize(parameter, version));
                paramsSize += VIntCoding.computeUnsignedVIntSize(valueLength);//length prefix
                paramsSize += valueLength;
            }
            size += VIntCoding.computeUnsignedVIntSize(paramsSize);
            size += paramsSize;
        }

        long payloadSize = payload == null ? 0 : serializer.serializedSize(payload, version);
        assert payloadSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        size += VIntCoding.computeUnsignedVIntSize(payloadSize);
        size += payloadSize;
        return new MessageOutSizes(size, payloadSize);
    }

    private MessageOutSizes calculateSerializedSizePre40(int version)
    {
        long size = 0;
        size += CompactEndpointSerializationHelper.instance.serializedSize(from, version);

        size += TypeSizes.sizeof(verb.getId());
        size += TypeSizes.sizeof(parameters.size() / PARAMETER_TUPLE_SIZE);
        for (int ii = 0; ii < parameters.size(); ii += PARAMETER_TUPLE_SIZE)
        {
            ParameterType type = (ParameterType)parameters.get(ii + PARAMETER_TUPLE_TYPE_OFFSET);
            size += TypeSizes.sizeof(type.key());
            size += 4;//length prefix
            IVersionedSerializer serializer = type.serializer;
            Object parameter = parameters.get(ii + PARAMETER_TUPLE_PARAMETER_OFFSET);
            size += serializer.serializedSize(parameter, version);
        }

        long payloadSize = payload == null ? 0 : serializer.serializedSize(payload, version);
        assert payloadSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        size += TypeSizes.sizeof((int) payloadSize);
        size += payloadSize;
        return new MessageOutSizes(size, payloadSize);
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

        MessageOutSizes sizes = calculateSerializedSize(version);
        if (sizes.messageSize > Integer.MAX_VALUE)
            throw new IllegalStateException("message size exceeds maximum allowed size: size = " + sizes.messageSize);

        if (serializedSizeVersion == SERIALIZED_SIZE_VERSION_UNDEFINED)
        {
            serializedSize = Ints.checkedCast(sizes.messageSize);
            payloadSerializedSize = Ints.checkedCast(sizes.payloadSize);
            serializedSizeVersion = version;
        }

        return Ints.checkedCast(sizes.messageSize);
    }

    public Object getParameter(ParameterType type)
    {
        for (int ii = 0; ii < parameters.size(); ii += PARAMETER_TUPLE_SIZE)
        {
            if (((ParameterType)parameters.get(ii + PARAMETER_TUPLE_TYPE_OFFSET)).equals(type))
            {
                return parameters.get(ii + PARAMETER_TUPLE_PARAMETER_OFFSET);
            }
        }
        return null;
    }

    private static class MessageOutSizes
    {
        public final long messageSize;
        public final long payloadSize;

        private MessageOutSizes(long messageSize, long payloadSize)
        {
            this.messageSize = messageSize;
            this.payloadSize = payloadSize;
        }

        @Override
        public final int hashCode()
        {
            int hashCode = (int) messageSize ^ (int) (messageSize >>> 32);
            return 31 * (hashCode ^ (int) ((int) payloadSize ^ (payloadSize >>> 32)));
        }

        @Override
        public final boolean equals(Object o)
        {
            if (!(o instanceof MessageOutSizes))
                return false;
            MessageOutSizes that = (MessageOutSizes) o;
            return messageSize == that.messageSize && payloadSize == that.payloadSize;
        }
    }
}
