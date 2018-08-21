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

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * A strategy pattern for writing to and flushing a netty channel.
 *
 * <h2>Flushing</h2>
 *
 * We don't flush to the socket on every message as it's a bit of a performance drag (making the system call, copying
 * the buffer, sending out a small packet). Thus, by waiting until we have a decent chunk of data (for some definition
 * of 'decent'), we can achieve better efficiency and improved performance (yay!).
 * <p>
 * When to flush mainly depends on whether we use message coalescing or not (see {@link CoalescingStrategies}).
 * <p>
 *
 * <h3>Flushing without coalescing</h3>
 *
 * When no coalescing is in effect, we want to send new message "right away". However, as said above, flushing after
 * every message would be particularly inefficient when there is lots of message in our sending queue, and so in
 * practice we want to flush in 2 cases:
 *  1) After any message <b>if</b> there is no pending message in the send queue.
 *  2) When we've filled up or exceeded the netty outbound buffer (see {@link ChannelOutboundBuffer})
 * <p>
 *
 * <h3>Flushing with coalescing</h3>
 *
 * The goal of coalescing is to (artificially) delay the flushing of data in order to aggregate even more data before
 * sending a group of packets out. So we don't want to flush after messages even if there is no pending messages in the
 * sending queue, but we rather want to delegate the decision on when to flush to the {@link CoalescingStrategy}. In
 * pratice, when coalescing is enabled we will flush in 2 cases:
 *  1) When the coalescing strategies decides that we should.
 *  2) When we've filled up or exceeded the netty outbound buffer ({@link ChannelOutboundBuffer}), exactly like in the
 *  no coalescing case.
 *  <p>
 *  The first part is handled by {@link CoalescingChannelWriter#flush(boolean)}. Whenever a message is sent, we check
 *  if a flush has been already scheduled by the coalescing strategy. If one has, we're done, otherwise we ask the
 *  strategy when the next flush should happen and schedule one.
 *  The second part is handled exactly like in the no coalescing case, see above.
 */
abstract class ChannelWriter
{
    protected final Channel channel;
    private volatile boolean closed;

    protected ChannelWriter(Channel channel)
    {
        this.channel = channel;
    }

    /**
     * Creates a new {@link ChannelWriter} using the (assumed properly connected) provided channel, and using coalescing
     * based on the provided strategy.
     */
    static ChannelWriter create(Channel channel, OutboundConnectionParams params)
    {
        return params.coalescingStrategy != null
               ? new CoalescingChannelWriter(channel, params.coalescingStrategy)
               : new SimpleChannelWriter(channel);
    }

    /**
     * Writes a message to the {@link #channel}. If the channel is closed, the promise will be notifed as a fail
     * (due to channel closed), and let the any listeners on the promise do the reconnect magic/dance.
     *
     * @param message the message to write/send.
     */
    abstract ChannelFuture write(QueuedMessage message);

    abstract boolean flush(boolean hasRemainingMessagesInQueue);

    void close()
    {
        if (closed)
            return;

        closed = true;
        channel.close();
    }

    /**
     * Close the underlying channel but only after having make sure every pending message has been properly sent.
     */
    void softClose()
    {
        if (closed)
            return;

        closed = true;
        channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
    }

    @VisibleForTesting
    boolean isClosed()
    {
        return closed || !channel.isOpen();
    }

    /**
     * Handles the non-coalescing flush case.
     */
    @VisibleForTesting
    static class SimpleChannelWriter extends ChannelWriter
    {
        private SimpleChannelWriter(Channel channel)
        {
            super(channel);
        }

        protected ChannelFuture write(QueuedMessage message)
        {
            return channel.write(message);
        }

        protected boolean flush(boolean hasRemainingMessagesInQueue)
        {
            if (!hasRemainingMessagesInQueue)
            {
                channel.flush();
                return true;
            }

            return false;
        }
    }

    /**
     * Handles the coalescing flush case.
     */
    @VisibleForTesting
    static class CoalescingChannelWriter extends ChannelWriter
    {
        private static final int MIN_MESSAGES_FOR_COALESCE = DatabaseDescriptor.getOtcCoalescingEnoughCoalescedMessages();

        private final CoalescingStrategy strategy;
        private final int minMessagesForCoalesce;

        /**
         * Count of messages written to the channel since the last flush.
         */
        private int messagesSinceFlush;

        @VisibleForTesting
        ScheduledFuture<?> scheduledFlush;

        CoalescingChannelWriter(Channel channel, CoalescingStrategy strategy)
        {
            this (channel, strategy, MIN_MESSAGES_FOR_COALESCE);
        }

        @VisibleForTesting
        CoalescingChannelWriter(Channel channel, CoalescingStrategy strategy, int minMessagesForCoalesce)
        {
            super(channel);
            this.strategy = strategy;
            this.minMessagesForCoalesce = minMessagesForCoalesce;
        }

        protected ChannelFuture write(QueuedMessage message)
        {
            ChannelFuture future = channel.write(message);
            strategy.newArrival(message);
            messagesSinceFlush++;
            return future;
        }

        protected boolean flush(boolean hasRemainingMessagesInQueue)
        {
            long flushDelayNanos;
            // if we've hit the minimum number of messages for coalescing or we've run out of coalesce time, flush.
            if (messagesSinceFlush == minMessagesForCoalesce || (flushDelayNanos = strategy.currentCoalescingTimeNanos()) <= 0)
            {
                doFlush();
                return true;
            }
            else if (scheduledFlush == null)
            {
                scheduledFlush = channel.eventLoop().schedule(this::doFlush, flushDelayNanos, TimeUnit.NANOSECONDS);
            }

            return false;
        }

        private void doFlush()
        {
            channel.flush();
            messagesSinceFlush = 0;
            shutdownScheduledFlush();
        }

        private void shutdownScheduledFlush()
        {
            if (scheduledFlush != null)
            {
                scheduledFlush.cancel(false);
                scheduledFlush = null;
            }
        }

        void close()
        {
            super.close();
            shutdownScheduledFlush();
        }
    }
}
