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
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.ReferenceCountUtil;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessageIn.MessageInProcessor;

/**
 * Parses incoming messages as per the 4.0 internode messaging protocol.
 */
public class MessageInHandler extends ChannelInboundHandlerAdapter
{
    public static final Logger logger = LoggerFactory.getLogger(MessageInHandler.class);

    private final InetAddressAndPort peer;

    private final BufferHandler bufferHandler;
    private volatile boolean closed;

    public MessageInHandler(InetAddressAndPort peer, MessageInProcessor messageProcessor, boolean handlesLargeMessages)
    {
        this.peer = peer;

        bufferHandler = handlesLargeMessages
                        ? new BlockingBufferHandler(messageProcessor)
                        : new NonblockingBufferHandler(messageProcessor);
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException
    {
        if (!closed)
            bufferHandler.channelRead(ctx, (ByteBuf) msg);
        else
            ReferenceCountUtil.release(msg);
    }

    // TODO:JEB reevaluate the error handling once I switch from ByteToMessageDecoder to ChannelInboundHandlerAdapter
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        if (cause instanceof EOFException)
            logger.trace("eof reading from socket; closing", cause);
        else if (cause instanceof UnknownTableException)
            logger.warn("Got message from unknown table while reading from socket; closing", cause);
        else if (cause instanceof IOException)
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
    class NonblockingBufferHandler implements BufferHandler
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

        public void channelRead(ChannelHandlerContext ctx, ByteBuf in)
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
            catch (Throwable cause)
            {
                // TODO:JEB is this the correct error handling?
                exceptionCaught(ctx, cause);
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
         * The default low-water mark to set on {@link #queuedBuffers}.
         * See {@link RebufferingByteBufDataInputPlus} for more information.
         */
        private static final int QUEUE_LOW_WATER_MARK = 1 << 14;

        /**
         * The default high-water mark to set on {@link #queuedBuffers}.
         * See {@link RebufferingByteBufDataInputPlus} for more information.
         */
        private static final int QUEUE_HIGH_WATER_MARK = 1 << 16;

        private final MessageInProcessor messageProcessor;

        /**
         * Intended to be a single-threaded pool, and if there is no inboound activity (no incoming buffers
         * on the channel), the thread should be shut down (timed out) to conserve resources.
         */
        private final ThreadPoolExecutor executorService;

        /**
         * A queue in which to stash incoming {@link ByteBuf}s.
         */
        private RebufferingByteBufDataInputPlus queuedBuffers;

        private volatile boolean executing;

        BlockingBufferHandler(MessageInProcessor messageProcessor)
        {
            this.messageProcessor = messageProcessor;

            String threadName = "MessagingService-NettyInbound-" + peer.toString() + "-LargeMessages";


            // bound the LBQ to a handful element so we don't inadvertently add a bunch of empty tasks to the queue,
            // but we need more than one to allow for the current executing task (so it can exit) and an entry after it
            // so we can schedule resumption tasks if channelRead
            RejectedExecutionHandler nopHandler = (r, executor) -> {};
            executorService = new ThreadPoolExecutor(1, 1, 5L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(3),
                                                     new NamedThreadFactory(threadName), nopHandler);
            executorService.allowCoreThreadTimeOut(true);
        }

        public void channelRead(ChannelHandlerContext ctx, ByteBuf in)
        {
            if (queuedBuffers == null)
            {
                queuedBuffers = new RebufferingByteBufDataInputPlus(QUEUE_LOW_WATER_MARK,
                                                                    QUEUE_HIGH_WATER_MARK,
                                                                    ctx.channel().config());
            }

            queuedBuffers.append(in);

            // if we are racing with processInBackground(), go ahead and add another task to the queue
            // as the number of tasks in the queue is bounded (and we throw away any rejection exceptions).
            // We could get a bunch of incoming buffers and add them to the queue before the background thread has
            // a chance to start executing.
            if (!executing)
                executorService.submit(() -> processInBackground(ctx));
        }

        /**
         * This will execute not on the netty event loop.
         */
        private void processInBackground(ChannelHandlerContext ctx)
        {
            try
            {
                executing = true;
                // it's expected this will block until the either the channel is closed
                // or there is no incoming data.
                messageProcessor.process(queuedBuffers);
            }
            catch (EOFException eof)
            {
                // ignore this excpetion - just means we've been signalled that processing is done on this channel
            }
            catch (Throwable cause)
            {
                // TODO:JEB is this the correct error handling, especially off the event loop?
                exceptionCaught(ctx, cause);
                queuedBuffers.close();
                // TODO:JEB CHANNEL needs to die here! & queuedBuffers, cuz we're in a spot place in the stream
            }
            finally
            {
                // don't close queuesBuffers from the threadPool, only from the netty event loop
                executing = false;

                // if we are racing with channelRead(), go ahead and add another task to the queue
                // as the number of tasks in the queue is bounded (and we throw away any rejection exceptions)

                try
                {
                    if (!queuedBuffers.isEmpty())
                        executorService.submit(() -> processInBackground(ctx));
                }
                catch (EOFException e)
                {
                    // nop. If we got an EOF, we're done processing any data anyway.
                }
            }
        }

        public void close()
        {
            queuedBuffers.markClose();
        }
    }
}
