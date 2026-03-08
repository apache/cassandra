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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.messages.EventMessage;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;

import static org.apache.cassandra.transport.Dispatcher.EVENT_DISPATCHER;

final class GracefulDisconnectLifecycle
{
    // Stable snapshot of subscribed channels.
    private final List<Channel> channels;

    // Number of channels still awaiting closure.
    private final AtomicInteger remainingChannels;

    // Signals that the graceful disconnect phase is complete.
    private final CountDownLatch completion;

    // Tracks the current lifecycle state.
    private final AtomicReference<State> state;

    private final long gracefulDisconnectGracePeriod;

    // Called when each channel is closed.
    private final Consumer<Channel> onChannelClosed;

    // Number of clients forcefully disconnected after the grace period.
    private final AtomicInteger forcedDisconnects = new AtomicInteger();

    // Best-effort upper bound on how long forced Channel#close() calls should
    // take to actually complete once triggered. Not user-configurable —
    // this is a safety margin against the drain hanging.
    private long hardDeadlineBufferMillis = 5000;

    public GracefulDisconnectLifecycle(ChannelGroup channelGroup,
                                       Consumer<Channel> onChannelClosed)
    {
        this(channelGroup,
             onChannelClosed,
             DatabaseDescriptor.getGracefulDisconnectGracePeriod(),
             5000);
    }

    GracefulDisconnectLifecycle(ChannelGroup channelGroup,
                                Consumer<Channel> onChannelClosed,
                                long gracefulDisconnectGracePeriod,
                                long hardDeadlineBufferMillis)
    {
        channels = new ArrayList<>(channelGroup);
        remainingChannels = new AtomicInteger(channels.size());
        completion = CountDownLatch.newCountDownLatch(1);
        state = new AtomicReference<>(State.WAITING_FOR_CLIENTS);
        this.gracefulDisconnectGracePeriod = gracefulDisconnectGracePeriod;
        this.onChannelClosed = onChannelClosed;
        this.hardDeadlineBufferMillis = hardDeadlineBufferMillis;
    }

    int run() throws InterruptedException, TimeoutException
    {
        startGracefulDisconnect();
        if (completion.await(gracefulDisconnectGracePeriod, TimeUnit.MILLISECONDS)) return forcedDisconnects.get();
        onGracePeriodExpired();
        if (!completion.await(hardDeadlineBufferMillis, TimeUnit.MILLISECONDS))
            throw new TimeoutException("Graceful disconnect did not complete even after forced close, " + remainingChannels.get() + " channel(s) still open");
        return forcedDisconnects.get();
    }

    private void startGracefulDisconnect()
    {
        if (remainingChannels.get() == 0)
        {
            complete();
            return;
        }

        EventMessage eventMessage = new EventMessage(new Event.GracefulDisconnect());
        channels.forEach(channel -> {
            channel.closeFuture().addListener(future -> onChannelClosed(channel));
            Consumer<EventMessage> dispatcher = channel.attr(EVENT_DISPATCHER).get();
            if (dispatcher != null)
                dispatcher.accept(eventMessage);
        });
    }

    private void complete()
    {
        State previous = state.getAndSet(State.COMPLETE);
        if (previous != State.COMPLETE)
            completion.decrement();
    }

    private void onChannelClosed(Channel channel)
    {
        onChannelClosed.accept(channel);

        if (remainingChannels.decrementAndGet() == 0)
            complete();
    }

    private void onGracePeriodExpired()
    {
        if (!state.compareAndSet(State.WAITING_FOR_CLIENTS, State.FORCE_CLOSING))
            return;

        int actuallyClosed = 0;
        for (Channel channel : channels)
        {
            if (channel.isOpen())
            {
                channel.close();
                actuallyClosed++;
            }
        }
        forcedDisconnects.set(actuallyClosed);
    }

    private enum State
    {
        WAITING_FOR_CLIENTS,
        FORCE_CLOSING,
        COMPLETE
    }
}