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

package org.apache.cassandra.net.async;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;

/**
 * Parses incoming messages as per the pre-4.0 internode messaging protocol.
 */
public class MessageInHandlerPre40 extends BaseMessageInHandler
{
    public static final Logger logger = LoggerFactory.getLogger(MessageInHandlerPre40.class);

    static final int PARAMETERS_SIZE_LENGTH = Integer.BYTES;
    static final int PARAMETERS_VALUE_SIZE_LENGTH = Integer.BYTES;
    static final int PAYLOAD_SIZE_LENGTH = Integer.BYTES;

    private MessageHeader messageHeader;

    MessageInHandlerPre40(InetAddressAndPort peer, int messagingVersion)
    {
        this (peer, messagingVersion, MESSAGING_SERVICE_CONSUMER);
    }

    public MessageInHandlerPre40(InetAddressAndPort peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        super(peer, messagingVersion, messageConsumer);

        assert messagingVersion < MessagingService.VERSION_40 : String.format("wrong messaging version for this handler: got %d, but expect lower than %d",
                                                                               messagingVersion, MessagingService.VERSION_40);
        state = State.READ_FIRST_CHUNK;
    }

    /**
     * For each new message coming in, builds up a {@link MessageHeader} instance incrementally. This method
     * attempts to deserialize as much header information as it can out of the incoming {@link ByteBuf}, and
     * maintains a trivial state machine to remember progress across invocations.
     */
    @SuppressWarnings("resource")
    public void handleDecode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        ByteBufDataInputPlus inputPlus = new ByteBufDataInputPlus(in);
        while (true)
        {
            switch (state)
            {
                case READ_FIRST_CHUNK:
                    MessageHeader header = readFirstChunk(in);
                    if (header == null)
                        return;
                    messageHeader = header;
                    state = State.READ_IP_ADDRESS;
                    // fall-through
                case READ_IP_ADDRESS:
                    // unfortunately, this assumes knowledge of how CompactEndpointSerializationHelper serializes data (the first byte is the size).
                    // first, check that we can actually read the size byte, then check if we can read that number of bytes.
                    // the "+ 1" is to make sure we have the size byte in addition to the serialized IP addr count of bytes in the buffer.
                    int readableBytes = in.readableBytes();
                    if (readableBytes < 1 || readableBytes < in.getByte(in.readerIndex()) + 1)
                        return;
                    messageHeader.from = CompactEndpointSerializationHelper.instance.deserialize(inputPlus, messagingVersion);
                    state = State.READ_VERB;
                    // fall-through
                case READ_VERB:
                    if (in.readableBytes() < VERB_LENGTH)
                        return;
                    messageHeader.verb = MessagingService.Verb.fromId(in.readInt());
                    state = State.READ_PARAMETERS_SIZE;
                    // fall-through
                case READ_PARAMETERS_SIZE:
                    if (in.readableBytes() < PARAMETERS_SIZE_LENGTH)
                        return;
                    messageHeader.parameterLength = in.readInt();
                    messageHeader.parameters = messageHeader.parameterLength == 0 ? Collections.emptyMap() : new EnumMap<>(ParameterType.class);
                    state = State.READ_PARAMETERS_DATA;
                    // fall-through
                case READ_PARAMETERS_DATA:
                    if (messageHeader.parameterLength > 0)
                    {
                        if (!readParameters(in, inputPlus, messageHeader.parameterLength, messageHeader.parameters))
                            return;
                    }
                    state = State.READ_PAYLOAD_SIZE;
                    // fall-through
                case READ_PAYLOAD_SIZE:
                    if (in.readableBytes() < PAYLOAD_SIZE_LENGTH)
                        return;
                    messageHeader.payloadSize = in.readInt();
                    state = State.READ_PAYLOAD;
                    // fall-through
                case READ_PAYLOAD:
                    if (in.readableBytes() < messageHeader.payloadSize)
                        return;

                    // TODO consider deserailizing the messge not on the event loop
                    MessageIn<Object> messageIn = MessageIn.read(inputPlus, messagingVersion,
                                                                 messageHeader.messageId, messageHeader.constructionTime, messageHeader.from,
                                                                 messageHeader.payloadSize, messageHeader.verb, messageHeader.parameters);

                    if (messageIn != null)
                        messageConsumer.accept(messageIn, messageHeader.messageId);

                    state = State.READ_FIRST_CHUNK;
                    messageHeader = null;
                    break;
                default:
                    throw new IllegalStateException("unknown/unhandled state: " + state);
            }
        }
    }

    /**
     * @return <code>true</code> if all the parameters have been read from the {@link ByteBuf}; else, <code>false</code>.
     */
    private boolean readParameters(ByteBuf in, ByteBufDataInputPlus inputPlus, int parameterCount, Map<ParameterType, Object> parameters) throws IOException
    {
        // makes the assumption that map.size() is a constant time function (HashMap.size() is)
        while (parameters.size() < parameterCount)
        {
            if (!canReadNextParam(in))
                return false;

            String key = inputPlus.readUTF();
            ParameterType parameterType = ParameterType.byName.get(key);
            byte[] value = new byte[inputPlus.readInt()];
            inputPlus.readFully(value);
            try (DataInputBuffer buffer = new DataInputBuffer(value))
            {
                parameters.put(parameterType, parameterType.serializer.deserialize(buffer, messagingVersion));
            }
        }

        return true;
    }

    private static boolean readParameters(DataInputPlus in, int messagingVersion, int parameterCount, Map<ParameterType, Object> parameters) throws IOException
    {
        // makes the assumption that map.size() is a constant time function (HashMap.size() is)
        while (parameters.size() < parameterCount)
        {
            String key = in.readUTF();
            ParameterType parameterType = ParameterType.byName.get(key);
            in.readInt();
            parameters.put(parameterType, parameterType.serializer.deserialize(in, messagingVersion));
        }

        return true;
    }

    static MessageIn<?> deserializePre40(DataInputPlus in, int id, int version, InetAddressAndPort from) throws IOException
    {
        assert from.equals(CompactEndpointSerializationHelper.instance.deserialize(in, version));
        MessagingService.Verb verb = MessagingService.Verb.fromId(in.readInt());

        Map<ParameterType, Object> parameters = Collections.emptyMap();
        int parameterCount = in.readInt();
        if (parameterCount != 0)
        {
            parameters = new EnumMap<>(ParameterType.class);
            readParameters(in, version, parameterCount, parameters);
        }

        Object payload = null;
        int payloadSize = in.readInt();
        if (payloadSize > 0)
        {
            IVersionedSerializer serializer = MessagingService.getVerbSerializer(verb, id);
            if (serializer == null) in.skipBytesFully(payloadSize);
            else payload = serializer.deserialize(in, version);
        }

        return new MessageIn<>(from, payload, parameters, verb, version, System.nanoTime());
    }



    /**
     * Determine if we can read the next parameter from the {@link ByteBuf}. This method will *always* set the {@code in}
     * readIndex back to where it was when this method was invoked.
     *
     * NOTE: this function would be sooo much simpler if we included a parameters length int in the messaging format,
     * instead of checking the remaining readable bytes for each field as we're parsing it. c'est la vie ...
     */
    @VisibleForTesting
    static boolean canReadNextParam(ByteBuf in)
    {
        in.markReaderIndex();
        // capture the readableBytes value here to avoid all the virtual function calls.
        // subtract 6 as we know we'll be reading a short and an int (for the utf and value lengths).
        final int minimumBytesRequired = 6;
        int readableBytes = in.readableBytes() - minimumBytesRequired;
        if (readableBytes < 0)
            return false;

        // this is a tad invasive, but since we know the UTF string is prefaced with a 2-byte length,
        // read that to make sure we have enough bytes to read the string itself.
        short strLen = in.readShort();
        // check if we can read that many bytes for the UTF
        if (strLen > readableBytes)
        {
            in.resetReaderIndex();
            return false;
        }
        in.skipBytes(strLen);
        readableBytes -= strLen;

        // check if we can read the value length
        if (readableBytes < PARAMETERS_VALUE_SIZE_LENGTH)
        {
            in.resetReaderIndex();
            return false;
        }
        int valueLength = in.readInt();
        // check if we read that many bytes for the value
        if (valueLength > readableBytes)
        {
            in.resetReaderIndex();
            return false;
        }

        in.resetReaderIndex();
        return true;
    }


    @Override
    MessageHeader getMessageHeader()
    {
        return messageHeader;
    }
}
