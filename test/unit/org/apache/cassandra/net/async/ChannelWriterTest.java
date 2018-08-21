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

import java.util.concurrent.TimeUnit;

import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.ChannelWriter.CoalescingChannelWriter;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

import static org.apache.cassandra.net.MessagingService.Verb.ECHO;

/**
 * with the write_Coalescing_* methods, if there's data in the channel.unsafe().outboundBuffer()
 * it means that there's something in the channel that hasn't yet been flushed to the transport (socket).
 * once a flush occurs, there will be an entry in EmbeddedChannel's outboundQueue. those two facts are leveraged in these tests.
 */
public class ChannelWriterTest
{
    private static final int COALESCE_WINDOW_MS = 10;

    private EmbeddedChannel channel;
    private ChannelWriter channelWriter;
    private NonSendingOutboundMessagingConnection omc;
    private CoalescingStrategy coalescingStrategy;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        OutboundConnectionIdentifier id = OutboundConnectionIdentifier.small(InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.1"), 0),
                                                                             InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.2"), 0));
        channel = new EmbeddedChannel();
        omc = new NonSendingOutboundMessagingConnection(id, null, null);

        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .protocolVersion(MessagingService.current_version)
                                                                  .build();
        channelWriter = ChannelWriter.create(channel, params);
        channel.pipeline().addFirst(new MessageOutHandler(id, MessagingService.current_version));
        coalescingStrategy = CoalescingStrategies.newCoalescingStrategy(CoalescingStrategies.Strategy.FIXED.name(), COALESCE_WINDOW_MS, null, "test");
    }

    @Test
    public void create_nonCoalescing()
    {
        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .protocolVersion(MessagingService.current_version)
                                                                  .build();
        Assert.assertSame(ChannelWriter.SimpleChannelWriter.class, ChannelWriter.create(channel, params).getClass());
    }

    @Test
    public void create_Coalescing()
    {
        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .protocolVersion(MessagingService.current_version)
                                                                  .coalescingStrategy(coalescingStrategy)
                                                                  .build();
        Assert.assertSame(CoalescingChannelWriter.class, ChannelWriter.create(channel, params).getClass());
    }

    @Test
    public void write_IsWritable()
    {
        Assert.assertTrue(channel.isWritable());
        channelWriter.write(new QueuedMessage(new MessageOut<>(ECHO), 42));
        Assert.assertTrue(channelWriter.flush(false));
        Assert.assertTrue(channel.isWritable());
        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void write_Coalescing_HitMinMessageCountForImmediateCoalesce()
    {
        int minMessagesForCoalesce = 3;
        channel = new EmbeddedChannel();
        coalescingStrategy = CoalescingStrategies.newCoalescingStrategy(CoalescingStrategies.Strategy.FIXED.name(), Integer.MAX_VALUE, null, "test");
        channelWriter = new CoalescingChannelWriter(channel, coalescingStrategy, minMessagesForCoalesce);
        for (int i = 0; i < minMessagesForCoalesce; i++)
        {
            channelWriter.write(new QueuedMessage(new MessageOut<>(ECHO), i));
            boolean flushed = channelWriter.flush(true);
            if (i < minMessagesForCoalesce - 1)
                Assert.assertFalse(flushed);
            else
                Assert.assertTrue(flushed);
        }

        Assert.assertTrue(channel.releaseOutbound());
    }

    @Test
    public void write_Coalescing_ScheduleFlushTask()
    {
        int minMessagesForCoalesce = 3;
        boolean[] flushed = new boolean[1];
        flushed[0] = false;
        channel = new EmbeddedChannel();
        channel.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
            public void flush(ChannelHandlerContext ctx)
            {
                flushed[0] = true;
            }
        });

        CoalescingChannelWriter channelWriter = new CoalescingChannelWriter(channel, coalescingStrategy, minMessagesForCoalesce);
        Assert.assertFalse(flushed[0]);
        Assert.assertNull(channelWriter.scheduledFlush);
        channelWriter.write(new QueuedMessage(new MessageOut<>(ECHO), 1));
        Assert.assertFalse(channelWriter.flush(true));
        Assert.assertNotNull(channelWriter.scheduledFlush);

        Uninterruptibles.sleepUninterruptibly(COALESCE_WINDOW_MS, TimeUnit.MILLISECONDS);
        Assert.assertEquals(-1, channel.runScheduledPendingTasks());
        Assert.assertTrue(flushed[0]);
        Assert.assertNull(channelWriter.scheduledFlush);
    }


    @Test
    public void close()
    {
        Assert.assertFalse(channelWriter.isClosed());
        Assert.assertTrue(channel.isOpen());
        channelWriter.close();
        Assert.assertFalse(channel.isOpen());
        Assert.assertTrue(channelWriter.isClosed());
    }

    @Test
    public void softClose()
    {
        Assert.assertFalse(channelWriter.isClosed());
        Assert.assertTrue(channel.isOpen());
        channelWriter.softClose();
        Assert.assertFalse(channel.isOpen());
        Assert.assertTrue(channelWriter.isClosed());
    }
}
