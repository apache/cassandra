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

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import com.google.common.primitives.Ints;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Parses incoming messages as per the 4.0 internode messaging protocol.
 */
public class MessageInHandler extends BaseMessageInHandler
{
    public static final Logger logger = LoggerFactory.getLogger(MessageInHandler.class);

    private MessageHeader messageHeader;

    MessageInHandler(InetAddressAndPort peer, int messagingVersion)
    {
        this (peer, messagingVersion, MESSAGING_SERVICE_CONSUMER);
    }

    public MessageInHandler(InetAddressAndPort peer, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        super(peer, messagingVersion, messageConsumer);

        assert messagingVersion >= MessagingService.VERSION_40 : String.format("wrong messaging version for this handler: got %d, but expect %d or higher",
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
                    header.from = peer;
                    messageHeader = header;
                    state = State.READ_VERB;
                    // fall-through
                case READ_VERB:
                    if (in.readableBytes() < VERB_LENGTH)
                        return;
                    messageHeader.verb = MessagingService.Verb.fromId(in.readInt());
                    state = State.READ_PARAMETERS_SIZE;
                    // fall-through
                case READ_PARAMETERS_SIZE:
                    long length = VIntCoding.readUnsignedVInt(in);
                    if (length < 0)
                        return;
                    messageHeader.parameterLength = (int) length;
                    messageHeader.parameters = messageHeader.parameterLength == 0 ? Collections.emptyMap() : new EnumMap<>(ParameterType.class);
                    state = State.READ_PARAMETERS_DATA;
                    // fall-through
                case READ_PARAMETERS_DATA:
                    if (messageHeader.parameterLength > 0)
                    {
                        if (in.readableBytes() < messageHeader.parameterLength)
                            return;
                        readParameters(in, inputPlus, messagingVersion, messageHeader.parameterLength, messageHeader.parameters);
                    }
                    state = State.READ_PAYLOAD_SIZE;
                    // fall-through
                case READ_PAYLOAD_SIZE:
                    length = VIntCoding.readUnsignedVInt(in);
                    if (length < 0)
                        return;
                    messageHeader.payloadSize = (int) length;
                    state = State.READ_PAYLOAD;
                    // fall-through
                case READ_PAYLOAD:
                    if (in.readableBytes() < messageHeader.payloadSize)
                        return;

                    // TODO consider deserializing the message not on the event loop
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

    private static void readParameters(ByteBuf buf, DataInputPlus in, int messagingVersion, int parameterLength, Map<ParameterType, Object> parameters) throws IOException
    {
        // makes the assumption we have all the bytes required to read the headers
        final int endIndex = buf.readerIndex() + parameterLength;
        while (buf.readerIndex() < endIndex)
        {
            String key = in.readUTF();
            ParameterType parameterType = ParameterType.byName.get(key);
            long valueLength =  in.readUnsignedVInt();
            byte[] value = new byte[Ints.checkedCast(valueLength)];
            in.readFully(value);
            try (DataInputBuffer buffer = new DataInputBuffer(value))
            {
                parameters.put(parameterType, parameterType.serializer.deserialize(buffer, messagingVersion));
            }
        }
    }

    private static void readParameters(BooleanSupplier isDone, DataInputPlus in, int messagingVersion, Map<ParameterType, Object> parameters) throws IOException
    {
        // makes the assumption we have all the bytes required to read the headers
        while (!isDone.getAsBoolean())
        {
            String key = in.readUTF();
            ParameterType parameterType = ParameterType.byName.get(key);
            in.readUnsignedVInt();
            parameters.put(parameterType, parameterType.serializer.deserialize(in, messagingVersion));
        }
    }

    public static MessageIn<?> deserialize(DataInputPlus in, int id, int version, InetAddressAndPort from) throws IOException
    {
        if (version >= MessagingService.VERSION_40)
            return deserialize40(in, id, version, from);
        else
            return MessageInHandlerPre40.deserializePre40(in, id, version, from);
    }

    private static MessageIn<?> deserialize40(DataInputPlus in, int id, int version, InetAddressAndPort from) throws IOException
    {
        MessagingService.Verb verb = MessagingService.Verb.fromId(in.readInt());

        Map<ParameterType, Object> parameters = Collections.emptyMap();
        int parameterLength = (int) in.readUnsignedVInt();
        if (parameterLength != 0)
        {
            parameters = new EnumMap<>(ParameterType.class);
            byte[] bytes = new byte[parameterLength];
            in.readFully(bytes);
            try (DataInputBuffer buffer = new DataInputBuffer(bytes))
            {
                readParameters(() -> buffer.available() == 0, buffer, version, parameters);
            }
        }

        Object payload = null;
        int payloadSize = (int) in.readUnsignedVInt();
        if (payloadSize > 0)
        {
            IVersionedSerializer serializer = MessagingService.getVerbSerializer(verb, id);
            if (serializer == null) in.skipBytesFully(payloadSize);
            else payload = serializer.deserialize(in, version);
        }

        return new MessageIn<>(from, payload, parameters, verb, version, System.nanoTime());
    }

    @Override
    MessageHeader getMessageHeader()
    {
        return messageHeader;
    }
}
