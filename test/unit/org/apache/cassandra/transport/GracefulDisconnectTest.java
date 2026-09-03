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

package org.apache.cassandra.transport;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import static org.assertj.core.api.Assertions.assertThat;

public class GracefulDisconnectLifecycleTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testCompletesImmediatelyWithNoChannels() throws Exception
    {
        DefaultChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        GracefulDisconnectLifecycle lifecycle = new GracefulDisconnectLifecycle(channelGroup, channel -> {});

        assertThat(lifecycle.run()).isEqualTo(0);
    }

    @Test
    public void testCompletesWhenChannelCloses() throws Exception
    {
        DefaultChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel channel = new EmbeddedChannel();
        channelGroup.add(channel);

        AtomicReference<Channel> closedChannel = new AtomicReference<>();

        GracefulDisconnectLifecycle lifecycle = new GracefulDisconnectLifecycle(channelGroup, closedChannel::set);

        Future<Integer> result = CompletableFuture.supplyAsync(() -> {
            try
            {
                return lifecycle.run();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });

        assertThat(result.isDone()).isFalse();

        channel.close();

        assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(0);
        assertThat(closedChannel.get()).isSameAs(channel);
    }

    @Test
    public void testForceClosesChannelAfterGracePeriod() throws Exception
    {
        DefaultChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel channel = new EmbeddedChannel();
        channelGroup.add(channel);

        GracefulDisconnectLifecycle lifecycle = new GracefulDisconnectLifecycle(channelGroup, ignored -> {}, 100, 100);

        Future<Integer> result = CompletableFuture.supplyAsync(() -> {
            try
            {
                return lifecycle.run();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });

        int forcedDisconnects = result.get(5, TimeUnit.SECONDS);

        assertThat(forcedDisconnects).isEqualTo(1);
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    public void testWaitsForAllChannelsToClose() throws Exception
    {
        DefaultChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel channel1 = new EmbeddedChannel();
        EmbeddedChannel channel2 = new EmbeddedChannel();

        channelGroup.add(channel1);
        channelGroup.add(channel2);

        GracefulDisconnectLifecycle lifecycle = new GracefulDisconnectLifecycle(channelGroup, ignored -> {}, 1000, 100);

        Future<Integer> result = CompletableFuture.supplyAsync(() -> {
            try
            {
                return lifecycle.run();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });

        channel1.close();

        assertThat(result.isDone()).isFalse();

        channel2.close();

        assertThat(result.get(5, TimeUnit.SECONDS)).isEqualTo(0);
        assertThat(channel1.isOpen()).isFalse();
        assertThat(channel2.isOpen()).isFalse();
    }

    @Test
    public void testMixedCooperativeAndUncooperativeChannels() throws Exception
    {
        DefaultChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel cooperativeChannel = new EmbeddedChannel(DefaultChannelId.newInstance());
        EmbeddedChannel uncooperativeChannel = new EmbeddedChannel(DefaultChannelId.newInstance());

        channelGroup.add(cooperativeChannel);
        channelGroup.add(uncooperativeChannel);

        GracefulDisconnectLifecycle lifecycle = new GracefulDisconnectLifecycle(channelGroup, ignored -> {}, 200, 100);

        Future<Integer> result = CompletableFuture.supplyAsync(() -> {
            try
            {
                return lifecycle.run();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });

        // Close only the cooperative channel immediately
        cooperativeChannel.close();

        // The lifecycle should wait for the grace period to expire for the uncooperative channel
        int forcedDisconnects = result.get(5, TimeUnit.SECONDS);

        assertThat(forcedDisconnects).as("Only the uncooperative channel should be force closed").isEqualTo(1);
        assertThat(cooperativeChannel.isOpen()).isFalse();
        assertThat(uncooperativeChannel.isOpen()).isFalse();
    }

    @Test
    public void testChannelAlreadyClosedBeforeLifecycleStart() throws Exception
    {
        DefaultChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel alreadyClosedChannel = new EmbeddedChannel();
        alreadyClosedChannel.close(); // Closed before lifecycle starts

        channelGroup.add(alreadyClosedChannel);

        GracefulDisconnectLifecycle lifecycle = new GracefulDisconnectLifecycle(channelGroup, ignored -> {}, 1000, 100);

        int forcedDisconnects = lifecycle.run();

        assertThat(forcedDisconnects).isEqualTo(0);
        assertThat(alreadyClosedChannel.isOpen()).isFalse();
    }

    @Test
    public void testCallbackInvokedForEachChannel() throws Exception
    {
        DefaultChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        EmbeddedChannel ch1 = new EmbeddedChannel(DefaultChannelId.newInstance());
        EmbeddedChannel ch2 = new EmbeddedChannel(DefaultChannelId.newInstance());

        channelGroup.add(ch1);
        channelGroup.add(ch2);

        AtomicInteger closedCallbackCount = new AtomicInteger(0);

        // Grace period 100ms: ch1 will close cooperatively, ch2 will be force closed
        GracefulDisconnectLifecycle lifecycle = new GracefulDisconnectLifecycle(channelGroup, ch -> closedCallbackCount.incrementAndGet(), 100, 100);

        Future<Integer> result = CompletableFuture.supplyAsync(() -> {
            try
            {
                return lifecycle.run();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        });

        ch1.close();

        result.get(5, TimeUnit.SECONDS);

        assertThat(closedCallbackCount.get())
                  .as("Callback must be invoked exactly once per channel regardless of how it closed")
                  .isEqualTo(2);
    }
}