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

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.HandshakeProtocol.FirstHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.SecondHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.ThirdHandshakeMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class HandshakeProtocolTest
{
    private ByteBuf buf;

    @BeforeClass
    public static void before()
    {
        // Kind of stupid, but the test trigger the initialization of the MessagingService class and that require
        // DatabaseDescriptor to be configured ...
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Test
    public void firstMessageTest() throws Exception
    {
        firstMessageTest(NettyFactory.Mode.MESSAGING, false);
        firstMessageTest(NettyFactory.Mode.MESSAGING, true);
        firstMessageTest(NettyFactory.Mode.STREAMING, false);
        firstMessageTest(NettyFactory.Mode.STREAMING, true);
    }

    private void firstMessageTest(NettyFactory.Mode mode, boolean compression) throws Exception
    {
        FirstHandshakeMessage before = new FirstHandshakeMessage(MessagingService.current_version, mode, compression);
        buf = before.encode(PooledByteBufAllocator.DEFAULT);
        FirstHandshakeMessage after = FirstHandshakeMessage.maybeDecode(buf);
        assertEquals(before, after);
        assertEquals(before.hashCode(), after.hashCode());
        Assert.assertFalse(before.equals(null));
    }

    @Test
    public void secondMessageTest() throws Exception
    {
        SecondHandshakeMessage before = new SecondHandshakeMessage(MessagingService.current_version);
        buf = before.encode(PooledByteBufAllocator.DEFAULT);
        SecondHandshakeMessage after = SecondHandshakeMessage.maybeDecode(buf);
        assertEquals(before, after);
        assertEquals(before.hashCode(), after.hashCode());
        Assert.assertFalse(before.equals(null));
    }

    @Test
    public void thirdMessageTest() throws Exception
    {
        ThirdHandshakeMessage before = new ThirdHandshakeMessage(MessagingService.current_version, FBUtilities.getBroadcastAddress());
        buf = before.encode(PooledByteBufAllocator.DEFAULT);
        ThirdHandshakeMessage after = ThirdHandshakeMessage.maybeDecode(buf);
        assertEquals(before, after);
        assertEquals(before.hashCode(), after.hashCode());
        Assert.assertFalse(before.equals(null));
    }
}