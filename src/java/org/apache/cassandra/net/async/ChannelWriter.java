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

import java.io.IOException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.MessageSizeEstimator;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

/**
 * Represents a ready and post-handshake channel that can send outbound messages. This class groups a netty channel
 * with any other channel-related information we track and, most importantly, handles the details on when the channel is flushed.
 *
 * <h2>Flushing</h2>
 *
 * We don't flush to the socket on every message as it's a bit of a performance drag (making the system call, copying
 * the buffer, sending out a small packet). Thus, by waiting until we have a decent chunk of data (for some definition
 * of 'decent'), we can achieve better efficiency and improved performance (yay!).
 * <p>
 * When to flush mainly depends on whether we use message coalescing or not (see {@link CoalescingStrategies}).
 * <p>
 * Note that the callback functions are invoked on the netty event loop, which is (in almost all cases) different
 * from the thread that will be invoking {@link #write(QueuedMessage, boolean)}.
 *
 * <h3>Flushing without coalescing</h3>
 *
 * When no coalescing is in effect, we want to send new message "right away". However, as said above, flushing after
 * every message would be particularly inefficient when there is lots of message in our sending queue, and so in
 * practice we want to flush in 2 cases:
 *  1) After any message <b>if</b> there is no pending message in the send queue.
 *  2) When we've filled up or exceeded the netty outbound buffer (see {@link ChannelOutboundBuffer})
 * <p>
 * The second part is relatively simple and handled generically in {@link MessageOutHandler#write(ChannelHandlerContext, Object, ChannelPromise)} [1].
 * The first part however is made a little more complicated by how netty's event loop executes. It is woken up by
 * external callers to the channel invoking a flush, via either {@link Channel#flush} or one of the {@link Channel#writeAndFlush}
 * methods [2]. So a plain {@link Channel#write} will only queue the message in the channel, and not wake up the event loop.
 * <p>
 * This means we don't want to simply call {@link Channel#write} as we want the message processed immediately. But we
 * also don't want to flush on every message if there is more in the sending queue, so simply calling
 * {@link Channel#writeAndFlush} isn't completely appropriate either. In practice, we handle this by calling
 * {@link Channel#writeAndFlush} (so the netty event loop <b>does</b> wake up), but we override the flush behavior so
 * it actually only flushes if there are no pending messages (see how {@link MessageOutHandler#flush} delegates the flushing
 * decision back to this class through {@link #onTriggeredFlush}, and how {@link SimpleChannelWriter} makes this a no-op;
 * instead {@link SimpleChannelWriter} flushes after any message if there are no more pending ones in
 * {@link #onMessageProcessed}).
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
 *  The second part is handled exactly like in the no coalescing case, see above.
 *  The first part is handled by {@link CoalescingChannelWriter#write(QueuedMessage, boolean)}. Whenever a message is sent, we check
 *  if a flush has been already scheduled by the coalescing strategy. If one has, we're done, otherwise we ask the
 *  strategy when the next flush should happen and schedule one.
 *
 *<h2>Message timeouts and retries</h2>
 *
 * The main outward-facing method is {@link #write(QueuedMessage, boolean)}, where callers pass a
 * {@link QueuedMessage}. If a message times out, as defined in {@link QueuedMessage#isTimedOut()},
 * the message listener {@link #handleMessageFuture(Future, QueuedMessage, boolean)} is invoked
 * with the cause being a {@link ExpiredException}. The message is not retried and it is dropped on the floor.
 * <p>
 * If there is some {@link IOException} on the socket after the message has been written to the netty channel,
 * the message listener {@link #handleMessageFuture(Future, QueuedMessage, boolean)} is invoked
 * and 1) we check to see if the connection should be re-established, and 2) possibly createRetry the message.
 *
 * <h2>Failures</h2>
 *
 * <h3>Failure to make progress sending bytes</h3>
 * If we are unable to make progress sending messages, we'll receive a netty notification
 * ({@link IdleStateEvent}) at {@link MessageOutHandler#userEventTriggered(ChannelHandlerContext, Object)}.
 * We then want to close the socket/channel, and purge any messages in {@link OutboundMessagingConnection#backlog}
 * to try to free up memory as quickly as possible. Any messages in the netty pipeline will be marked as fail
 * (as we close the channel), but {@link MessageOutHandler#userEventTriggered(ChannelHandlerContext, Object)} also
 * sets a channel attribute, {@link #PURGE_MESSAGES_CHANNEL_ATTR} to true. This is essentially as special flag
 * that we can look at in the promise handler code ({@link #handleMessageFuture(Future, QueuedMessage, boolean)})
 * to indicate that any backlog should be thrown away.
 *
 * <h2>Notes</h2>
 * [1] For those desperately interested, and only after you've read the entire class-level doc: You can register a custom
 * {@link MessageSizeEstimator} with a netty channel. When a message is written to the channel, it will check the
 * message size, and if the max ({@link ChannelOutboundBuffer}) size will be exceeded, a task to signal the "channel
 * writability changed" will be executed in the channel. That task, however, will wake up the event loop.
 * Thus if coalescing is enabled, the event loop will wake up prematurely and process (and possibly flush!) the messages
 * currently in the queue, thus defeating an aspect of coalescing. Hence, we're not using that feature of netty.
 * [2]: The netty event loop is also woken up by it's internal timeout on the epoll_wait() system call.
 */
abstract class ChannelWriter
{
    /**
     * A netty channel {@link Attribute} to indicate, when a channel is closed, any backlogged messages should be purged,
     * as well. See the class-level documentation for more information.
     */
    static final AttributeKey<Boolean> PURGE_MESSAGES_CHANNEL_ATTR = AttributeKey.newInstance("purgeMessages");

    protected final Channel channel;
    private volatile boolean closed;

    /** Number of currently pending messages on this channel. */
    final AtomicLong pendingMessageCount = new AtomicLong(0);

    /**
     * A consuming function that handles the result of each message sent.
     */
    private final Consumer<MessageResult> messageResultConsumer;

    /**
     * A reusable instance to avoid creating garbage on preciessing the result of every message sent.
     * As we have the guarantee that the netty evet loop is single threaded, there should be no contention over this
     * instance, as long as it (not it's state) is shared across threads.
     */
    private final MessageResult messageResult = new MessageResult();

    protected ChannelWriter(Channel channel, Consumer<MessageResult> messageResultConsumer)
    {
        this.channel = channel;
        this.messageResultConsumer = messageResultConsumer;
        channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).set(false);
    }

    /**
     * Creates a new {@link ChannelWriter} using the (assumed properly connected) provided channel, and using coalescing
     * based on the provided strategy.
     */
    static ChannelWriter create(Channel channel, Consumer<MessageResult> messageResultConsumer, Optional<CoalescingStrategy> coalescingStrategy)
    {
        return coalescingStrategy.isPresent()
               ? new CoalescingChannelWriter(channel, messageResultConsumer, coalescingStrategy.get())
               : new SimpleChannelWriter(channel, messageResultConsumer);
    }

    /**
     * Writes a message to this {@link ChannelWriter} if the channel is writable.
     * <p>
     * We always want to write to the channel *unless* it's not writable yet still open.
     * If the channel is closed, the promise will be notifed as a fail (due to channel closed),
     * and let the handler ({@link #handleMessageFuture(Future, QueuedMessage, boolean)})
     * do the reconnect magic/dance. Thus we simplify when to reconnect by not burdening the (concurrent) callers
     * of this method, and instead keep it all in the future handler/event loop (which is single threaded).
     *
     * @param message the message to write/send.
     * @param checkWritability a flag to indicate if the status of the channel should be checked before passing
     * the message on to the {@link #channel}.
     * @return true if the message was written to the channel; else, false.
     */
    boolean write(QueuedMessage message, boolean checkWritability)
    {
        if ( (checkWritability && (channel.isWritable()) || !channel.isOpen()) || !checkWritability)
        {
            write0(message).addListener(f -> handleMessageFuture(f, message, true));
            return true;
        }
        return false;
    }

    /**
     * Handles the future of sending a particular message on this {@link ChannelWriter}.
     * <p>
     * Note: this is called from the netty event loop, so there is no race across multiple execution of this method.
     */
    @VisibleForTesting
    void handleMessageFuture(Future<? super Void> future, QueuedMessage msg, boolean allowReconnect)
    {
        messageResult.setAll(this, msg, future, allowReconnect);
        messageResultConsumer.accept(messageResult);
        messageResult.clearAll();
    }

    boolean shouldPurgeBacklog()
    {
        if (!channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).get())
            return false;

        channel.attr(PURGE_MESSAGES_CHANNEL_ATTR).set(false);
        return true;
    }

    /**
     * Writes a backlog of message to this {@link ChannelWriter}. This is mostly equivalent to calling
     * {@link #write(QueuedMessage, boolean)} for every message of the provided backlog queue, but
     * it ignores any coalescing, triggering a flush only once after all messages have been sent.
     *
     * @param backlog the backlog of message to send.
     * @return the count of items written to the channel from the queue.
     */
    int writeBacklog(Queue<QueuedMessage> backlog, boolean allowReconnect)
    {
        int count = 0;
        while (true)
        {
            if (!channel.isWritable())
                break;

            QueuedMessage msg = backlog.poll();
            if (msg == null)
                break;

            pendingMessageCount.incrementAndGet();
            ChannelFuture future = channel.write(msg);
            future.addListener(f -> handleMessageFuture(f, msg, allowReconnect));
            count++;
        }

        // as this is an infrequent operation, don't bother coordinating with the instance-level flush task
        if (count > 0)
            channel.flush();

        return count;
    }

    void close()
    {
        if (closed)
            return;

        closed = true;
        channel.close();
    }

    long pendingMessageCount()
    {
        return pendingMessageCount.get();
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
        return closed;
    }

    /**
     * Write the message to the {@link #channel}.
     * <p>
     * Note: this method, in almost all cases, is invoked from an app-level writing thread, not the netty event loop.
     */
    protected abstract ChannelFuture write0(QueuedMessage message);

    /**
     * Invoked after a message has been processed in the pipeline. Should only be used for essential bookkeeping operations.
     * <p>
     * Note: this method is invoked on the netty event loop.
     */
    abstract void onMessageProcessed(ChannelHandlerContext ctx);

    /**
     * Invoked when pipeline receives a flush request.
     * <p>
     * Note: this method is invoked on the netty event loop.
     */
    abstract void onTriggeredFlush(ChannelHandlerContext ctx);

    /**
     * Handles the non-coalescing flush case.
     */
    @VisibleForTesting
    static class SimpleChannelWriter extends ChannelWriter
    {
        private SimpleChannelWriter(Channel channel, Consumer<MessageResult> messageResultConsumer)
        {
            super(channel, messageResultConsumer);
        }

        protected ChannelFuture write0(QueuedMessage message)
        {
            pendingMessageCount.incrementAndGet();
            // We don't truly want to flush on every message but we do want to wake-up the netty event loop for the
            // channel so the message is processed right away, which is why we use writeAndFlush. This won't actually
            // flush, though, because onTriggeredFlush, which MessageOutHandler delegates to, does nothing. We will
            // flush after the message is processed though if there is no pending one due to onMessageProcessed.
            // See the class javadoc for context and much more details.
            return channel.writeAndFlush(message);
        }

        void onMessageProcessed(ChannelHandlerContext ctx)
        {
            if (pendingMessageCount.decrementAndGet() == 0)
                ctx.flush();
        }

        void onTriggeredFlush(ChannelHandlerContext ctx)
        {
            // Don't actually flush on "normal" flush calls to the channel.
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

        @VisibleForTesting
        final AtomicBoolean scheduledFlush = new AtomicBoolean(false);

        CoalescingChannelWriter(Channel channel, Consumer<MessageResult> messageResultConsumer, CoalescingStrategy strategy)
        {
            this (channel, messageResultConsumer, strategy, MIN_MESSAGES_FOR_COALESCE);
        }

        @VisibleForTesting
        CoalescingChannelWriter(Channel channel, Consumer<MessageResult> messageResultConsumer, CoalescingStrategy strategy, int minMessagesForCoalesce)
        {
            super(channel, messageResultConsumer);
            this.strategy = strategy;
            this.minMessagesForCoalesce = minMessagesForCoalesce;
        }

        protected ChannelFuture write0(QueuedMessage message)
        {
            long pendingCount = pendingMessageCount.incrementAndGet();
            ChannelFuture future = channel.write(message);
            strategy.newArrival(message);

            // if we lost the race to set the state, simply write to the channel (no flush)
            if (!scheduledFlush.compareAndSet(false, true))
                return future;

            long flushDelayNanos;
            // if we've hit the minimum number of messages for coalescing or we've run out of coalesce time, flush.
            // note: we check the exact count, instead of greater than or equal to, of message here to prevent a flush task
            // for each message (if there's messages coming in on multiple threads). There will be, of course, races
            // with the consumer decrementing the pending counter, but that's still less excessive flushes.
            if (pendingCount == minMessagesForCoalesce || (flushDelayNanos = strategy.currentCoalescingTimeNanos()) <= 0)
            {
                scheduledFlush.set(false);
                channel.flush();
            }
            else
            {
                // calling schedule() on the eventLoop will force it to wake up (if not already executing) and schedule the task
                channel.eventLoop().schedule(() -> {
                    // NOTE: this executes on the event loop
                    scheduledFlush.set(false);
                    // we execute() the flush() as an additional task rather than immediately in-line as there is a
                    // race condition when this task runs (executing on the event loop) and a thread that writes the channel (top of this method).
                    // If this task is picked up but before the scheduledFlush falg is flipped, the other thread writes
                    // and then checks the scheduledFlush (which is still true) and exits.
                    // This task changes the flag and if it calls flush() in-line, and netty flushs everything immediately (that is, what's been serialized)
                    // to the transport as we're on the event loop. The other thread's write became a task that executes *after* this task in the netty queue,
                    // and if there's not a subsequent followup flush scheduled, that write can be orphaned until another write comes in.
                    channel.eventLoop().execute(channel::flush);
                }, flushDelayNanos, TimeUnit.NANOSECONDS);
            }
            return future;
        }

        void onMessageProcessed(ChannelHandlerContext ctx)
        {
            pendingMessageCount.decrementAndGet();
        }

        void onTriggeredFlush(ChannelHandlerContext ctx)
        {
            // When coalescing, obey the flush calls normally
            ctx.flush();
        }
    }
}
