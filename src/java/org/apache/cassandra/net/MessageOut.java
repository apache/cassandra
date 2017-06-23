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
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import static org.apache.cassandra.tracing.Tracing.isTracing;

public class MessageOut<T>
{
    public static final int PARAMETER_TUPLE_SIZE = 2;
    public static final int PARAMETER_TUPLE_TYPE_OFFSET = 0;
    public static final int PARAMETER_TUPLE_PARAMETER_OFFSET = 1;

    public final InetAddressAndPort from;
    public final MessagingService.Verb verb;
    public final T payload;
    public final IVersionedSerializer<T> serializer;
    //A list of pairs, first object is the ParameterType enum,
    //the second object is the object to serialize
    public final List<Object> parameters;
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
                 ? Tracing.instance.getTraceHeaders()
                 : ImmutableList.of());
    }

    private MessageOut(MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, List<Object> parameters)
    {
        this(FBUtilities.getBroadcastAddressAndPort(), verb, payload, serializer, parameters);
    }

    @VisibleForTesting
    public MessageOut(InetAddressAndPort from, MessagingService.Verb verb, T payload, IVersionedSerializer<T> serializer, List<Object> parameters)
    {
        this.from = from;
        this.verb = verb;
        this.payload = payload;
        this.serializer = serializer;
        this.parameters = parameters;
    }

    public <VT> MessageOut<T> withParameter(ParameterType type, VT value)
    {
        List<Object> newParameters = new ArrayList<>(parameters.size() + 3);
        newParameters.addAll(parameters);
        newParameters.add(type);
        newParameters.add(value);
        return new MessageOut<T>(verb, payload, serializer, newParameters);
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
        CompactEndpointSerializationHelper.instance.serialize(from, out, version);

        out.writeInt(verb.getId());
        assert parameters.size() % PARAMETER_TUPLE_SIZE == 0;
        out.writeInt(parameters.size() / PARAMETER_TUPLE_SIZE);
        for (int ii = 0; ii < parameters.size(); ii += PARAMETER_TUPLE_SIZE)
        {
            ParameterType type = (ParameterType)parameters.get(ii + PARAMETER_TUPLE_TYPE_OFFSET);
            out.writeUTF(type.key());
            IVersionedSerializer serializer = type.serializer;
            Object parameter = parameters.get(ii + PARAMETER_TUPLE_PARAMETER_OFFSET);
            out.writeInt(Ints.checkedCast(serializer.serializedSize(parameter, version)));
            serializer.serialize(parameter, out, version);
        }

        if (payload != null)
        {
            try(DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
            {
                serializer.serialize(payload, dob, version);

                int size = dob.getLength();
                out.writeInt(size);
                out.write(dob.getData(), 0, size);
            }
        }
        else
        {
            out.writeInt(0);
        }
    }

    public int serializedSize(int version)
    {
        int size = 0;
        size += CompactEndpointSerializationHelper.instance.serializedSize(from, version);

        size += TypeSizes.sizeof(verb.getId());
        size += TypeSizes.sizeof(parameters.size());
        for (int ii = 0; ii < parameters.size(); ii += PARAMETER_TUPLE_SIZE)
        {
            ParameterType type = (ParameterType)parameters.get(ii + PARAMETER_TUPLE_TYPE_OFFSET);
            size += TypeSizes.sizeof(type.name());
            size += 4;//length prefix
            IVersionedSerializer serializer = type.serializer;
            Object parameter = parameters.get(ii + PARAMETER_TUPLE_PARAMETER_OFFSET);
            size += serializer.serializedSize(parameter, version);
        }

        long longSize = payloadSize(version);
        assert longSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        size += TypeSizes.sizeof((int) longSize);
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
}
