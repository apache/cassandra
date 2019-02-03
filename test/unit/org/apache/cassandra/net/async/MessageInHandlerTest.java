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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Shorts;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageIn.MessageInProcessor;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParameterType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.NanoTimeToCurrentTimeMillis;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.net.async.OutboundConnectionIdentifier.ConnectionType.SMALL_MESSAGE;

@RunWith(Parameterized.class)
public class MessageInHandlerTest
{
    static
    {
        // inject a stub Tracing impl to quiet unnessary exceptions in the logs
        System.setProperty("cassandra.custom_tracing_class", "org.apache.cassandra.net.async.MessageInHandlerTest$StubTracingImpl");
    }

    private static final int MSG_ID = 42;
    private static InetAddressAndPort addr;

    private final int messagingVersion;
    private final OutboundConnectionIdentifier connectionId;
    private final boolean handlesLargeMessages;

    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
        addr = InetAddressAndPort.getByAddress(InetAddresses.forString("127.0.73.101"));
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    public MessageInHandlerTest(int messagingVersion, boolean handlesLargeMessages)
    {
        this.messagingVersion = messagingVersion;
        this.handlesLargeMessages = handlesLargeMessages;

        connectionId = handlesLargeMessages
                       ? OutboundConnectionIdentifier.large(addr, addr)
                       : OutboundConnectionIdentifier.small(addr, addr);
    }

    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
            { MessagingService.VERSION_30, false },
            { MessagingService.VERSION_30, true },
            { MessagingService.VERSION_40, false },
            { MessagingService.VERSION_40, true },
        });
    }

    private MessageInHandler getHandler(InetAddressAndPort addr, Channel channel, BiConsumer<MessageIn, Integer> messageConsumer)
    {
        return new MessageInHandler(addr, channel, MessageIn.getProcessor(addr, messagingVersion, messageConsumer), handlesLargeMessages);
    }

    @Test
    public void channelRead_HappyPath_NoParameters() throws Exception
    {
        MessageInWrapper result = channelRead_HappyPath(Collections.emptyMap());
        Assert.assertTrue(result.messageIn.parameters.isEmpty());
    }

    @Test
    public void channelRead_HappyPath_WithParameters() throws Exception
    {
        UUID uuid = UUIDGen.getTimeUUID();
        Map<ParameterType, Object> parameters = new EnumMap<>(ParameterType.class);
        parameters.put(ParameterType.FAILURE_RESPONSE, MessagingService.ONE_BYTE);
        parameters.put(ParameterType.FAILURE_REASON, Shorts.checkedCast(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code));
        parameters.put(ParameterType.TRACE_SESSION, uuid);
        MessageInWrapper result = channelRead_HappyPath(parameters);
        Assert.assertEquals(3, result.messageIn.parameters.size());
        Assert.assertTrue(result.messageIn.isFailureResponse());
        Assert.assertEquals(RequestFailureReason.READ_TOO_MANY_TOMBSTONES, result.messageIn.getFailureReason());
        Assert.assertEquals(uuid, result.messageIn.parameters.get(ParameterType.TRACE_SESSION));
    }

    private MessageInWrapper channelRead_HappyPath(Map<ParameterType, Object> parameters) throws Exception
    {
        MessageOut msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
        for (Map.Entry<ParameterType, Object> param : parameters.entrySet())
            msgOut = msgOut.withParameter(param.getKey(), param.getValue());
        serialize(msgOut, MSG_ID);

        MessageInConsumer consumer = new MessageInConsumer(1);
        EmbeddedChannel channel = new EmbeddedChannel();
        MessageInHandler handler = getHandler(addr, channel, consumer.messageConsumer);
        channel.pipeline().addLast(handler);
        channel.writeInbound(buf);

        // need to wait until async tasks are complete, as large messages spin up a background thread
        Assert.assertTrue(consumer.latch.await(5, TimeUnit.SECONDS));

        Assert.assertEquals(1, consumer.messages.size());
        MessageInWrapper wrapper = consumer.messages.get(0);
        Assert.assertNotNull(wrapper.messageIn);
        Assert.assertEquals(MSG_ID, wrapper.id.intValue());
        Assert.assertEquals(msgOut.from, wrapper.messageIn.from);
        Assert.assertEquals(msgOut.verb, wrapper.messageIn.verb);

        return wrapper;
    }

    @Test
    public void decode_WithHalfReceivedParameters() throws Exception
    {
        // this test only makes sense for nonblocking implementations
        Assume.assumeTrue(!handlesLargeMessages);

        MessageOut msgOut = new MessageOut<>(addr, MessagingService.Verb.ECHO, null, null, ImmutableList.of(), SMALL_MESSAGE);
        UUID uuid = UUIDGen.getTimeUUID();
        msgOut = msgOut.withParameter(ParameterType.TRACE_SESSION, uuid);
        serialize(msgOut, MSG_ID);

        // move the write index pointer back a few bytes to simulate like the full bytes are not present.
        // yeah, it's lame, but it tests the basics of what is happening during the deserialiization
        int originalWriterIndex = buf.writerIndex();
        ByteBuf secondBuf = ByteBufAllocator.DEFAULT.heapBuffer(8);
        int offset = originalWriterIndex - 6;
        buf.getBytes(offset, secondBuf);
        buf.writerIndex(offset);

        MessageInConsumer consumer = new MessageInConsumer(1);
        MessageInProcessor processor = MessageIn.getProcessor(addr, messagingVersion, consumer.messageConsumer);
        EmbeddedChannel channel = new EmbeddedChannel();
        MessageInHandler handler = new MessageInHandler(addr, channel, processor, handlesLargeMessages);
        channel.pipeline().addLast(handler);
        channel.writeInbound(buf);

        Assert.assertEquals(1, consumer.latch.getCount());
        MessageInProcessor.MessageHeader header = processor.getMessageHeader();
        Assert.assertEquals(MSG_ID, header.messageId);
        Assert.assertEquals(msgOut.verb, header.verb);
        Assert.assertEquals(msgOut.from, header.from);

        // now, set the writer index back to the original value to pretend that we actually got more bytes in
        buf.writerIndex(originalWriterIndex);
        channel.writeInbound(secondBuf);
        Assert.assertEquals(1, consumer.messages.size());
    }

    private void serialize(MessageOut msgOut, int id) throws IOException
    {
        if (buf == null)
            buf = Unpooled.buffer(1024, 1024); // 1k should be enough for everybody!
        int timestamp = (int) NanoTimeToCurrentTimeMillis.convert(System.nanoTime());
        msgOut.serialize(new ByteBufDataOutputPlus(buf), messagingVersion, connectionId, id, timestamp);
    }

    @Test
    public void exceptionHandled()
    {
        EmbeddedChannel channel = new EmbeddedChannel();
        MessageInHandler handler = getHandler(addr, channel, null);
        channel.pipeline().addLast(handler);
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

        int messageCount = 3;
        MessageInConsumer consumer = new MessageInConsumer(messageCount);
        EmbeddedChannel channel = new EmbeddedChannel();
        MessageInHandler handler = getHandler(addr, channel, consumer.messageConsumer);
        channel.pipeline().addLast(handler);
        Assert.assertTrue(channel.isOpen());
        channel.writeOneInbound(buf);

        try
        {
            // need to wait until async tasks are complete, as large messages spin up a background thread
            Assert.assertFalse(consumer.latch.await(2, TimeUnit.SECONDS));
        }
        catch (InterruptedException e)
        {
            // nop - we *want* it to time out, because this is the only way to wait for the blocking implementation to
            // correctly do it's thing (in the background).
        }

        Assert.assertEquals(0, buf.refCnt());
        Assert.assertTrue(handler.isClosed());
        Assert.assertFalse(channel.isOpen());

        Assert.assertEquals(messageCount - 1, consumer.latch.getCount());
        Assert.assertEquals(1, consumer.messages.size());
        MessageInWrapper wrapper = consumer.messages.get(0);
        Assert.assertEquals(Integer.valueOf(1), wrapper.id);
    }

    private class MessageInConsumer
    {
        private CountDownLatch latch;
        private final ArrayList<MessageInWrapper> messages = new ArrayList<>();

        private MessageInConsumer(int latchCount)
        {
            latch = new CountDownLatch(latchCount);
        }

        final BiConsumer<MessageIn, Integer> messageConsumer = (messageIn, integer) ->
        {
            messages.add(new MessageInWrapper(messageIn, integer));
            this.latch.countDown();
        };
    }

    private static class MessageInWrapper
    {
        final MessageIn messageIn;
        final Integer id;

        private MessageInWrapper(MessageIn messageIn, Integer id)
        {
            this.messageIn = messageIn;
            this.id = id;
        }
    }

    public static final class StubTracingImpl extends Tracing
    {
        protected void stopSessionImpl()
        {

        }

        public TraceState begin(String request, InetAddress client, Map<String, String> parameters)
        {
            return null;
        }

        protected TraceState newTraceState(InetAddressAndPort coordinator, UUID sessionId, TraceType traceType)
        {
            return null;
        }

        public void trace(ByteBuffer sessionId, String message, int ttl)
        {
            // throw away all the things
        }
    }
}
