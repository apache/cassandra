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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import static org.assertj.core.api.Assertions.assertThat;

public class GracefulDisconnectTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testEmptyChannelGroupGracefulHandling()
    {
        ChannelGroup group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        boolean completed = group.newCloseFuture().awaitUninterruptibly(100, TimeUnit.MILLISECONDS);
        assertThat(completed).isTrue();
        assertThat(group.size()).isEqualTo(0);
    }

    @Test
    public void testChannelGroupAutomaticRemovalOnClose()
    {
        ChannelGroup group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());
        group.add(channel);

        assertThat(group.size()).isEqualTo(1);
        channel.close();
        assertThat(group.size()).isEqualTo(0);
    }

    @Test
    public void testCloseFutureTriggersWhenAllChannelsTerminate() throws Exception
    {
        ChannelGroup group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel ch1 = new EmbeddedChannel(DefaultChannelId.newInstance());
        EmbeddedChannel ch2 = new EmbeddedChannel(DefaultChannelId.newInstance());
        group.add(ch1);
        group.add(ch2);

        Future<Boolean> closeFutureCompletion = CompletableFuture.supplyAsync(() -> group.newCloseFuture().awaitUninterruptibly(3000, TimeUnit.MILLISECONDS));

        assertThat(closeFutureCompletion.isDone()).isFalse();

        ch1.close();
        ch2.close();

        assertThat(closeFutureCompletion.get(2, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void testGracePeriodTimeoutTriggersBulkForceClose()
    {
        ChannelGroup group = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
        EmbeddedChannel cooperative = new EmbeddedChannel(DefaultChannelId.newInstance());
        EmbeddedChannel uncooperative = new EmbeddedChannel(DefaultChannelId.newInstance());
        group.add(cooperative);
        group.add(uncooperative);

        cooperative.close();

        boolean completedCleanly = group.newCloseFuture().awaitUninterruptibly(50, TimeUnit.MILLISECONDS);

        assertThat(completedCleanly).isFalse();

        int unclosedCount = group.size();
        assertThat(unclosedCount).isEqualTo(1);

        group.close();
        assertThat(uncooperative.isOpen()).isFalse();
    }
}
