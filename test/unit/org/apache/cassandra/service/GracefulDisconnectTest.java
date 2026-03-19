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

import org.junit.Before;
import org.junit.Test;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.EventMessage;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;

public class GracefulDisconnectTest
{
    @Before
    public void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ClientMetrics.instance.initForTesting();
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

        assertThat(wasActionRun.get())
            .as("Action should wait for channels to close")
            .isFalse();

        ch1.close();
    }

    @Test
    public void testTriggerOnLastChannelClose() throws InterruptedException
    {
        DatabaseDescriptor.setGracefulDisconnectMaxDrain(60000);

        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(2);
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
        assertThat(wasActionRun.get())
            .as("Action should not run while ch2 is still open")
            .isFalse();

        ch2.close().sync();
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(wasActionRun::get);

        assertThat(wasActionRun.get())
            .as("Action should run now that all channels are closed")
            .isTrue();

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

        assertThat(wasActionRun.get())
            .as("Should not run immediately")
            .isFalse();

        Awaitility.await()
                  .atMost(timeoutMs + 2000, TimeUnit.MILLISECONDS)
                  .until(wasActionRun::get);

        assertThat(wasActionRun.get())
            .as("Action should have run due to timeout")
            .isTrue();

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

        assertThat(capturedMessage.get())
            .as("EventMessage should have been dispatched")
            .isNotNull();

        assertThat(capturedMessage.get().event)
            .isInstanceOf(Event.GracefulDisconnect.class);
    }

    @Test
    public void testResilienceToMissingDispatcherAttribute()
    {
        ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel ch1 = new EmbeddedChannel();
        channelGroup.add(ch1);

        AtomicBoolean wasActionRun = new AtomicBoolean(false);

        StorageService.instance.gracefulDisconnect(() -> wasActionRun.set(true), channelGroup);

        assertThat(wasActionRun.get())
            .as("Should not have run yet, waiting for ch1 to close")
            .isFalse();

        ch1.close();
    }
}