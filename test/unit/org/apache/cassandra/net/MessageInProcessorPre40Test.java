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

package org.apache.cassandra.net;

import java.io.IOException;
import java.net.UnknownHostException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn.MessageInProcessorPre40;
import org.apache.cassandra.net.async.ByteBufDataOutputPlus;

public class MessageInProcessorPre40Test
{
    private static InetAddressAndPort addr;

    private MessageInProcessorPre40 processor;
    private ByteBuf buf;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        addr = InetAddressAndPort.getByName("127.0.0.1");
    }

    @Before
    public void setup()
    {
        processor = new MessageInProcessorPre40(addr, MessagingService.VERSION_30, (messageIn, integer) -> {});
    }

    @After
    public void tearDown()
    {
        if (buf != null && buf.refCnt() > 0)
            buf.release();
    }

    @Test
    public void canReadNextParam_HappyPath() throws IOException
    {
        buildParamBufPre40(13);
        Assert.assertTrue(processor.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_OnlyFirstByte() throws IOException
    {
        buildParamBufPre40(13);
        buf.writerIndex(1);
        Assert.assertFalse(processor.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_PartialUTF() throws IOException
    {
        buildParamBufPre40(13);
        buf.writerIndex(5);
        Assert.assertFalse(processor.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_TruncatedValueLength() throws IOException
    {
        buildParamBufPre40(13);
        buf.writerIndex(buf.writerIndex() - 13 - 2);
        Assert.assertFalse(processor.canReadNextParam(buf));
    }

    @Test
    public void canReadNextParam_MissingLastBytes() throws IOException
    {
        buildParamBufPre40(13);
        buf.writerIndex(buf.writerIndex() - 2);
        Assert.assertFalse(processor.canReadNextParam(buf));
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
}
