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

package org.apache.cassandra.service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.EventMessage;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

public class GracefulDisconnectTest
{
    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testGracefulDisconnectWithActiveChannels()
    {
        DatabaseDescriptor.setGracefulDisconnectMaxDrain(10000);

        ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel ch1 = new EmbeddedChannel();
        ch1.attr(StorageService.EVENT_DISPATCHER).set(msg -> {
        });
        channelGroup.add(ch1);

        AtomicBoolean wasActionRun = new AtomicBoolean(false);
        Runnable defaultAction = () -> wasActionRun.set(true);

        StorageService.instance.gracefulDisconnect(defaultAction, channelGroup);

        Assert.assertFalse("Action should wait for channels to close", wasActionRun.get());

        ch1.close();
    }

    @Test
    public void testTriggerOnLastChannelClose() throws InterruptedException
    {
        DatabaseDescriptor.setGracefulDisconnectMaxDrain(60000);

        DefaultEventLoopGroup eventLoopGroup = new DefaultEventLoopGroup(2);
        ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel ch1 = new EmbeddedChannel(DefaultChannelId.newInstance());
        EmbeddedChannel ch2 = new EmbeddedChannel(DefaultChannelId.newInstance());

        ch1.attr(StorageService.EVENT_DISPATCHER).set(msg -> {
        });
        ch2.attr(StorageService.EVENT_DISPATCHER).set(msg -> {
        });
        channelGroup.add(ch1);
        channelGroup.add(ch2);

        AtomicBoolean wasActionRun = new AtomicBoolean(false);
        StorageService.instance.gracefulDisconnect(() -> wasActionRun.set(true), channelGroup);

        ch1.close().sync();
        Assert.assertFalse("Action should not run while ch2 is still open", wasActionRun.get());

        ch2.close().sync();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(wasActionRun::get);
        Assert.assertTrue("Action should run now that all channels are closed", wasActionRun.get());

        eventLoopGroup.shutdownGracefully();
    }

    @Test
    public void testTriggerOnTimeout()
    {
        ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel ch1 = new EmbeddedChannel();
        ch1.attr(StorageService.EVENT_DISPATCHER).set(msg -> {
        });
        channelGroup.add(ch1);

        DatabaseDescriptor.setGracefulDisconnectMaxDrain(1000);
        long timeoutMs = DatabaseDescriptor.getGracefulDisconnectMaxDrain();

        AtomicBoolean wasActionRun = new AtomicBoolean(false);
        StorageService.instance.gracefulDisconnect(() -> wasActionRun.set(true), channelGroup);

        Assert.assertFalse("Should not run immediately", wasActionRun.get());

        Awaitility.await()
                  .atMost(timeoutMs + 2000, TimeUnit.MILLISECONDS)
                  .until(wasActionRun::get);

        Assert.assertTrue("Action should have run due to timeout", wasActionRun.get());
        ch1.close();
    }

    @Test
    public void testEventMessageDispatched()
    {
        ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel ch1 = new EmbeddedChannel();
        AtomicReference<EventMessage> capturedMessage = new AtomicReference<>();
        ch1.attr(StorageService.EVENT_DISPATCHER).set(capturedMessage::set);
        channelGroup.add(ch1);

        StorageService.instance.gracefulDisconnect(() -> {
        }, channelGroup);

        Assert.assertNotNull("EventMessage should have been dispatched", capturedMessage.get());
        Assert.assertTrue(capturedMessage.get().event instanceof Event.GracefulDisconnect);
    }

    @Test
    public void testResilienceToMissingDispatcherAttribute()
    {
        ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel ch1 = new EmbeddedChannel();
        channelGroup.add(ch1);

        AtomicBoolean wasActionRun = new AtomicBoolean(false);

        StorageService.instance.gracefulDisconnect(() -> wasActionRun.set(true), channelGroup);

        Assert.assertFalse("Should not have run yet, waiting for ch1 to close", wasActionRun.get());
        ch1.close();
    }
}