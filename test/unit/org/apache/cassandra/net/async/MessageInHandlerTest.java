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

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;

@RunWith(Parameterized.class)
public class MessageInHandlerTest
{
    private static final int MSG_ID = 42;
    private static InetAddressAndPort addr;

    private final int messagingVersion;

    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        addr = InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.73.101"));
    }

    public MessageInHandlerTest(int messagingVersion)
    {
        this.messagingVersion = messagingVersion;
    }

    @Parameters()
    public static Iterable<?> generateData()
    {
        return Arrays.asList(MessagingService.VERSION_30, MessagingService.VERSION_40);
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    private BaseMessageInHandler getHandler(InetAddressAndPort addr, int messagingVersion, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        return messagingVersion >= MessagingService.VERSION_40 ?
               new MessageInHandler(addr, messagingVersion, messageConsumer) :
               new MessageInHandlerPre40(addr, messagingVersion, messageConsumer);
    }

    @Test(expected = AssertionError.class)
    public void testBadVersionForHandler()
    {
        if (messagingVersion < MessagingService.VERSION_40)
           new MessageInHandler(addr, messagingVersion, null);
        else
           new MessageInHandlerPre40(addr, messagingVersion, null);
    }

    @Test
    public void decode_BadMagic()
    {
        int len = MessageInHandler.FIRST_SECTION_BYTE_COUNT;
        buf = Unpooled.buffer(len, len);
        buf.writeInt(-1);
        buf.writerIndex(len);

        BaseMessageInHandler handler = getHandler(addr, messagingVersion, null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isOpen());
        channel.writeInbound(buf);
        Assert.assertFalse(channel.isOpen());
    }

    @Test
    public void decode_HappyPath_NoParameters() throws Exception
    {
        MessageInWrapper result = decode_HappyPath(Collections.emptyMap());
        Assert.assertTrue(result.messageIn.parameters.isEmpty());
    }

    @Test
    public void decode_HappyPath_WithParameters() throws Exception
    {
        UUID uuid = UUIDGen.getTimeUUID();
        Map<ParameterType, Object> parameters = new EnumMap<>(ParameterType.class);
        parameters.put(ParameterType.FAILURE_RESPONSE, MessagingService.ONE_BYTE);
        parameters.put(ParameterType.FAILURE_REASON, Shorts.checkedCast(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
        parameters.put(ParameterType.TRACE_SESSION, uuid);
        MessageInWrapper result = decode_HappyPath(parameters);
        Assert.assertEquals(3, result.messageIn.parameters.size());
        Assert.assertTrue(result.messageIn.isFailureResponse());
        Assert.assertEquals(RequestFailureReason.READ_TOO_MANY_TOMBSTONES, result.messageIn.getFailureReason());
        Assert.assertEquals(uuid, result.messageIn.parameters.get(ParameterType.TRACE_SESSION));
    }

    private MessageInWrapper decode_HappyPath(Map<ParameterType, Object> parameters) throws Exception
    {
        MessageOut msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
        for (Map.Entry<ParameterType, Object> param : parameters.entrySet())
            msgOut = msgOut.withParameter(param.getKey(), param.getValue());
        serialize(msgOut, MSG_ID);

        MessageInWrapper wrapper = new MessageInWrapper();
        BaseMessageInHandler handler = getHandler(addr, messagingVersion, wrapper.messageConsumer);
        List<Object> out = new ArrayList<>();
        handler.decode(null, buf, out);

        Assert.assertNotNull(wrapper.messageIn);
        Assert.assertEquals(MSG_ID, wrapper.id);
        Assert.assertEquals(msgOut.from, wrapper.messageIn.from);
        Assert.assertEquals(msgOut.verb, wrapper.messageIn.verb);
        Assert.assertTrue(out.isEmpty());

        return wrapper;
    }

    private void serialize(MessageOut msgOut, int id) throws IOException
    {
        if (buf == null)
            buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        buf.writeInt(MessagingService.PROTOCOL_MAGIC);
        buf.writeInt(id); // this is the id
        buf.writeInt((int) NanoTimeToCurrentTimeMillis.convert(System.nanoTime()));

        msgOut.serialize(new ByteBufDataOutputPlus(buf), messagingVersion);
    }

    @Test
    public void decode_WithHalfReceivedParameters() throws Exception
    {
        MessageOut msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
        UUID uuid = UUIDGen.getTimeUUID();
        msgOut = msgOut.withParameter(ParameterType.TRACE_SESSION, uuid);

        serialize(msgOut, MSG_ID);

        // move the write index pointer back a few bytes to simulate like the full bytes are not present.
        // yeah, it's lame, but it tests the basics of what is happening during the deserialiization
        int originalWriterIndex = buf.writerIndex();
        buf.writerIndex(originalWriterIndex - 6);

        MessageInWrapper wrapper = new MessageInWrapper();
        BaseMessageInHandler handler = getHandler(addr, messagingVersion, wrapper.messageConsumer);
        List<Object> out = new ArrayList<>();
        handler.decode(null, buf, out);

        Assert.assertNull(wrapper.messageIn);

        BaseMessageInHandler.MessageHeader header = handler.getMessageHeader();
        Assert.assertEquals(MSG_ID, header.messageId);
        Assert.assertEquals(msgOut.verb, header.verb);
        Assert.assertEquals(msgOut.from, header.from);
        Assert.assertTrue(out.isEmpty());

        // now, set the writer index back to the original value to pretend that we actually got more bytes in
        buf.writerIndex(originalWriterIndex);
        handler.decode(null, buf, out);
        Assert.assertNotNull(wrapper.messageIn);
        Assert.assertTrue(out.isEmpty());
    }

    @Test
    public void canReadNextParam_HappyPath() throws IOException
    {
        buildParamBufPre40(13);
        Assert.assertTrue(MessageInHandlerPre40.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_OnlyFirstByte() throws IOException
    {
        buildParamBufPre40(13);
        buf.writerIndex(1);
        Assert.assertFalse(MessageInHandlerPre40.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_PartialUTF() throws IOException
    {
        buildParamBufPre40(13);
        buf.writerIndex(5);
        Assert.assertFalse(MessageInHandlerPre40.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_TruncatedValueLength() throws IOException
    {
        buildParamBufPre40(13);
        buf.writerIndex(buf.writerIndex() - 13 - 2);
        Assert.assertFalse(MessageInHandlerPre40.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_MissingLastBytes() throws IOException
    {
        buildParamBufPre40(13);
        buf.writerIndex(buf.writerIndex() - 2);
        Assert.assertFalse(MessageInHandlerPre40.canReadNextParam(buf));
    }

    private void buildParamBufPre40(int valueLength) throws IOException
    {
        buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!

        try (ByteBufDataOutputPlus output = new ByteBufDataOutputPlus(buf))
        {
            output.writeUTF("name");
            byte[] array = new byte[valueLength];
            output.writeInt(array.length);
            output.write(array);
        }
    }

    @Test
    public void exceptionHandled()
    {
        BaseMessageInHandler handler = getHandler(addr, messagingVersion, null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isOpen());
        handler.exceptionCaught(channel.pipeline().firstContext(), new EOFException());
        Assert.assertFalse(channel.isOpen());
    }

    /**
     * this is for handling the bug uncovered by CASSANDRA-14574.
     *
     * TL;DR if we run into a problem processing a message out an incoming buffer (and we close the channel, etc),
     * do not attempt to process anymore messages from the buffer (force the channel closed and
     * reject any more read attempts from the buffer).
     *
     * The idea here is to put several messages into a ByteBuf, pass that to the channel/handler, and make sure that
     * only the initial, correct messages in the buffer are processed. After one messages fails the rest of the buffer
     * should be ignored.
     */
    @Test
    public void exceptionHandled_14574() throws IOException
    {
        Map<ParameterType, Object> parameters = new EnumMap<>(ParameterType.class);
        parameters.put(ParameterType.FAILURE_RESPONSE, MessagingService.ONE_BYTE);
        parameters.put(ParameterType.FAILURE_REASON, Shorts.checkedCast(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
        MessageOut msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
        for (Map.Entry<ParameterType, Object> param : parameters.entrySet())
            msgOut = msgOut.withParameter(param.getKey(), param.getValue());

        // put one complete, correct message into the buffer
        serialize(msgOut, 1);

        // add a second message, but intentionally corrupt it by manipulating a byte in it's range
        int startPosition = buf.writerIndex();
        serialize(msgOut, 2);
        int positionToHack = startPosition + 2;
        buf.setByte(positionToHack, buf.getByte(positionToHack) - 1);

        // add one more complete, correct message into the buffer
        serialize(msgOut, 3);

        MessageIdsWrapper wrapper = new MessageIdsWrapper();
        BaseMessageInHandler handler = getHandler(addr, messagingVersion, wrapper.messageConsumer);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        Assert.assertTrue(channel.isOpen());
        channel.writeOneInbound(buf);

        Assert.assertFalse(buf.isReadable());
        Assert.assertEquals(BaseMessageInHandler.State.CLOSED, handler.getState());
        Assert.assertFalse(channel.isOpen());
        Assert.assertEquals(1, wrapper.ids.size());
        Assert.assertEquals(Integer.valueOf(1), wrapper.ids.get(0));
    }

    private static class MessageInWrapper
    {
        MessageIn messageIn;
        int id;

        final BiConsumer<MessageIn, Integer> messageConsumer = (messageIn, integer) ->
        {
            this.messageIn = messageIn;
            this.id = integer;
        };
    }

    private static class MessageIdsWrapper
    {
        private final ArrayList<Integer> ids = new ArrayList<>();

        final BiConsumer<MessageIn, Integer> messageConsumer = (messageIn, integer) -> ids.add(integer);
    }
}
