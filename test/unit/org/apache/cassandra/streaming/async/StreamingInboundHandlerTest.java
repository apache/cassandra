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

package org.apache.cassandra.streaming.async;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.net.async.RebufferingByteBufDataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.async.StreamingInboundHandler.SessionIdentifier;
import org.apache.cassandra.streaming.messages.CompleteMessage;
import org.apache.cassandra.streaming.messages.FileMessageHeader;
import org.apache.cassandra.streaming.messages.IncomingFileMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;

public class StreamingInboundHandlerTest
{
    private static final int VERSION = StreamMessage.CURRENT_VERSION;
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 0);

    private StreamingInboundHandler handler;
    private EmbeddedChannel channel;
    private RebufferingByteBufDataInputPlus buffers;
    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        handler = new StreamingInboundHandler(REMOTE_ADDR, VERSION, null);
        channel = new EmbeddedChannel(handler);
        buffers = new RebufferingByteBufDataInputPlus(1 << 9, 1 << 10, channel.config());
        handler.setPendingBuffers(buffers);
    }

    @After
    public void tearDown()
    {
        if (buf != null)
        {
            while (buf.refCnt() > 0)
                buf.release();
        }

        channel.close();
    }

    @Test
    public void channelRead_Normal() throws EOFException
    {
        Assert.assertEquals(0, buffers.available());
        int size = 8;
        buf = channel.alloc().buffer(size);
        buf.writerIndex(size);
        channel.writeInbound(buf);
        Assert.assertEquals(size, buffers.available());
        Assert.assertFalse(channel.releaseInbound());
    }

    @Test (expected = EOFException.class)
    public void channelRead_Closed() throws EOFException
    {
        int size = 8;
        buf = channel.alloc().buffer(size);
        Assert.assertEquals(1, buf.refCnt());
        buf.writerIndex(size);
        handler.close();
        channel.writeInbound(buf);
        Assert.assertEquals(0, buffers.available());
        Assert.assertEquals(0, buf.refCnt());
        Assert.assertFalse(channel.releaseInbound());
    }

    @Test
    public void channelRead_WrongObject() throws EOFException
    {
        channel.writeInbound("homer");
        Assert.assertEquals(0, buffers.available());
        Assert.assertFalse(channel.releaseInbound());
    }

    @Test
    public void StreamDeserializingTask_deriveSession_StreamInitMessage() throws InterruptedException, IOException
    {
        StreamInitMessage msg = new StreamInitMessage(REMOTE_ADDR.getAddress(), 0, UUID.randomUUID(), StreamOperation.REPAIR, true, UUID.randomUUID(), PreviewKind.ALL);
        StreamingInboundHandler.StreamDeserializingTask task = handler.new StreamDeserializingTask(sid -> createSession(sid), null, channel);
        StreamSession session = task.deriveSession(msg);
        Assert.assertNotNull(session);
    }

    private StreamSession createSession(SessionIdentifier sid)
    {
        return new StreamSession(sid.from, sid.from, (connectionId, protocolVersion) -> null, sid.sessionIndex, true, UUID.randomUUID(), PreviewKind.ALL);
    }

    @Test (expected = IllegalStateException.class)
    public void StreamDeserializingTask_deriveSession_NoSession() throws InterruptedException, IOException
    {
        CompleteMessage msg = new CompleteMessage();
        StreamingInboundHandler.StreamDeserializingTask task = handler.new StreamDeserializingTask(sid -> createSession(sid), null, channel);
        task.deriveSession(msg);
    }

    @Test (expected = IllegalStateException.class)
    public void StreamDeserializingTask_deriveSession_IFM_NoSession() throws InterruptedException, IOException
    {
        FileMessageHeader header = new FileMessageHeader(TableId.generate(), REMOTE_ADDR.getAddress(), UUID.randomUUID(), 0, 0,
                                                         BigFormat.latestVersion, SSTableFormat.Type.BIG, 0, new ArrayList<>(), null, 0, UUID.randomUUID(), 0 , null);
        IncomingFileMessage msg = new IncomingFileMessage(null, header);
        StreamingInboundHandler.StreamDeserializingTask task = handler.new StreamDeserializingTask(sid -> StreamManager.instance.findSession(sid.from, sid.planId, sid.sessionIndex), null, channel);
        task.deriveSession(msg);
    }

    @Test
    public void StreamDeserializingTask_deriveSession_IFM_HasSession() throws InterruptedException, IOException
    {
        UUID planId = UUID.randomUUID();
        StreamResultFuture future = StreamResultFuture.initReceivingSide(0, planId, StreamOperation.REPAIR, REMOTE_ADDR.getAddress(), channel, true, UUID.randomUUID(), PreviewKind.ALL);
        StreamManager.instance.register(future);
        FileMessageHeader header = new FileMessageHeader(TableId.generate(), REMOTE_ADDR.getAddress(), planId, 0, 0,
                                                         BigFormat.latestVersion, SSTableFormat.Type.BIG, 0, new ArrayList<>(), null, 0, UUID.randomUUID(), 0 , null);
        IncomingFileMessage msg = new IncomingFileMessage(null, header);
        StreamingInboundHandler.StreamDeserializingTask task = handler.new StreamDeserializingTask(sid -> StreamManager.instance.findSession(sid.from, sid.planId, sid.sessionIndex), null, channel);
        StreamSession session = task.deriveSession(msg);
        Assert.assertNotNull(session);
    }
}
