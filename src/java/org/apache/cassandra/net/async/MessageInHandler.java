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

import java.io.EOFException;
import java.io.IOError;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn.MessageInProcessor;
import org.apache.cassandra.net.async.RebufferingByteBufDataInputPlus.InputTimeoutException;

/**
 * Parses incoming messages as per the 4.0 internode messaging protocol.
 */
public class MessageInHandler extends ChannelInboundHandlerAdapter
{
    public static final Logger logger = LoggerFactory.getLogger(MessageInHandler.class);

    private final InetAddressAndPort peer;

    private final BufferHandler bufferHandler;
    private volatile boolean closed;

    public MessageInHandler(InetAddressAndPort peer, Channel channel, MessageInProcessor messageProcessor, boolean handlesLargeMessages)
    {
        this.peer = peer;

        bufferHandler = handlesLargeMessages
                        ? new BlockingBufferHandler(channel, messageProcessor)
                        : new NonblockingBufferHandler(messageProcessor);
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException
    {
        if (!closed)
        {
            bufferHandler.channelRead(ctx, (ByteBuf) msg);
        }
        else
        {
            ReferenceCountUtil.release(msg);
            ctx.close();
        }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof EOFException || (cause.getCause() != null && cause.getCause() instanceof EOFException))
            logger.trace("eof reading from socket; closing", cause);
        else if (cause instanceof UnknownTableException)
            logger.warn("Got message from unknown table while reading from socket; closing", cause);
        else if (cause instanceof IOException || cause instanceof IOError)
            logger.trace("IOException reading from socket; closing", cause);
        else
            logger.warn("Unexpected exception caught in inbound channel pipeline from " + ctx.channel().remoteAddress(), cause);

        close();
        ctx.close();
    }

    public void channelInactive(ChannelHandlerContext ctx)
    {
        logger.trace("received channel closed message for peer {} on local addr {}", ctx.channel().remoteAddress(), ctx.channel().localAddress());
        close();
        ctx.fireChannelInactive();
    }

    void close()
    {
        closed = true;
        bufferHandler.close();
    }

    boolean isClosed()
    {
        return closed;
    }

    /**
     * An abstraction around how incoming buffers are handled: either in a non-blocking manner ({@link NonblockingBufferHandler})
     * or in a blocking manner ({@link BlockingBufferHandler}).
     *
     * The methods declared here will only be invoked on the netty event loop.
     */
    interface BufferHandler
    {
        void channelRead(ChannelHandlerContext ctx, ByteBuf in) throws IOException;

        void close();
    }

    /**
     * Processes incoming buffers on the netty event loop, in a non-blocking manner. If buffers are not completely consumed,
     * it is stashed in {@link #retainedInlineBuffer}, and the next incoming buffer is combined with it.
     */
    static class NonblockingBufferHandler implements BufferHandler
    {
        private final MessageInProcessor messageProcessor;

        /**
         * If a buffer is not completely consumed, stash it here for the next invocation of
         * {@link #channelRead(ChannelHandlerContext, ByteBuf)}.
         */
        private ByteBuf retainedInlineBuffer;

        NonblockingBufferHandler(MessageInProcessor messageProcessor)
        {
            this.messageProcessor = messageProcessor;
        }

        public void channelRead(ChannelHandlerContext ctx, ByteBuf in) throws IOException
        {
            final ByteBuf toProcess;
            if (retainedInlineBuffer != null)
                toProcess = ByteToMessageDecoder.MERGE_CUMULATOR.cumulate(ctx.alloc(), retainedInlineBuffer, in);
            else
                toProcess = in;

            try
            {
                messageProcessor.process(toProcess);
            }
            finally
            {
                if (toProcess.isReadable())
                {
                    retainedInlineBuffer = toProcess;
                }
                else
                {
                    toProcess.release();
                    retainedInlineBuffer = null;
                }
            }
        }

        public void close()
        {
            if (retainedInlineBuffer != null)
            {
                retainedInlineBuffer.release();
                retainedInlineBuffer = null;
            }
        }
    }

    /**
     * Processes incoming buffers in a blocking manner; primarily useful for handling large messages to avoid allocating
     * very large buffers before the data can actually be deserialized. To perform blocking IO, we cannot do that
     * on the netty event loop itself, and thus we have a deserialization task execute on a separate background thread
     * ({@link #executorService}).
     */
    class BlockingBufferHandler implements BufferHandler
    {
        /**
         * Default time in milliseconds that {@link #queuedBuffers} should wait for new buffers to arrive.
         * See {@link RebufferingByteBufDataInputPlus} for more information.
         */
        private static final long QUEUED_BUFFERS_REBUFFER_MILLIS = 10 * 1000;

        private final MessageInProcessor messageProcessor;

        /**
         * Intended to be a single-threaded pool, and if there is no inboound activity (no incoming buffers
         * on the channel), the thread should be shut down (timed out) to conserve resources.
         */
        private final ThreadPoolExecutor executorService;

        /**
         * A queue in which to stash incoming {@link ByteBuf}s.
         */
        private final RebufferingByteBufDataInputPlus queuedBuffers;

        private final AtomicBoolean executing;

        BlockingBufferHandler(Channel channel, MessageInProcessor messageProcessor)
        {
            this.messageProcessor = messageProcessor;
            executing = new AtomicBoolean(false);
            queuedBuffers = new RebufferingByteBufDataInputPlus(channel, QUEUED_BUFFERS_REBUFFER_MILLIS);

            // bound the LBQ to a handful element so we don't inadvertently add a bunch of empty tasks to the queue,
            // but we need more than one to allow for the current executing task (so it can exit) and an entry after it
            // so we can schedule resumption tasks if channelRead
            RejectedExecutionHandler nopHandler = (r, executor) -> {};
            String threadName = "MessagingService-NettyInbound-" + peer.toString() + "-LargeMessages";
            executorService = new ThreadPoolExecutor(1, 1, 5L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(3),
                                                     new NamedThreadFactory(threadName), nopHandler);
            executorService.allowCoreThreadTimeOut(true);
            executorService.setRejectedExecutionHandler((r, executor) -> {});
        }

        /**
         * This will execute on the netty event loop
         */
        public void channelRead(ChannelHandlerContext ctx, ByteBuf in)
        {
            try
            {
                queuedBuffers.append(in);
            }
            catch (IllegalStateException ise)
            {
                // this catch block serves mainly to not make the logs a mess with multiple stack traces
                // when a race happens between inserting into the queue on the netty event loop after the background
                // task has closed the queue.
                // we would only get here if we are racing with the processInBackground() task, and it marked
                // the closed flag as false after we read it as true.
                // in any case, we are done processing, and catchException()/properly closing the channel will be soon invoked.
                ReferenceCountUtil.release(in);
                return;
            }

            // if we are racing with processInBackground(), go ahead and add another task to the queue
            // as the number of tasks in the queue is bounded (and we throw away any rejection exceptions).
            // We could get a bunch of incoming buffers and add them to the queue before the background thread has
            // a chance to start executing.
            if (executing.compareAndSet(false, true))
                executorService.submit(() -> processInBackground(ctx));
        }

        /**
         * This will execute on a thread in {@link #executorService}, not on the netty event loop.
         */
        private void processInBackground(ChannelHandlerContext ctx)
        {
            if (closed)
                return;

            try
            {
                // this will block until the either the channel is closed or there is no incoming data.
                messageProcessor.process(queuedBuffers);
            }
            catch (InputTimeoutException | EOFException e)
            {
                //nop - nothing to see here
            }
            catch (Throwable cause)
            {
                 // this is the safest way to indicate to the netty event loop that no more buffers should be processed
                closed = true;

                // close the buffers as we're on the consuming thread
                queuedBuffers.close();
                exceptionCaught(ctx, cause);
            }
            finally
            {
                executing.set(false);

                // if we are racing with channelRead(), go ahead and add another task to the queue
                // as the number of tasks in the queue is bounded (and we throw away any rejection exceptions)
                if (!closed && !queuedBuffers.isEmpty())
                    executorService.submit(() -> processInBackground(ctx));
            }
        }

        public void close()
        {
            if (queuedBuffers != null)
                queuedBuffers.markClose();
        }
    }
}
