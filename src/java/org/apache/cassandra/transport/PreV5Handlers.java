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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.net.ResourceLimits;
import org.apache.cassandra.transport.messages.ErrorMessage;

/**
 *
 */
public class PreV5Handlers
{
    /**
     *
     */
    public static class LegacyDispatchHandler extends SimpleChannelInboundHandler<Message.Request>
    {
        private static final Logger logger = LoggerFactory.getLogger(LegacyDispatchHandler.class);

        private final Dispatcher dispatcher;
        private final Server.EndpointPayloadTracker endpointPayloadTracker;

        /**
         * Current count of *request* bytes that are live on the channel.
         * <p>
         * Note: should only be accessed while on the netty event loop.
         */
        private long channelPayloadBytesInFlight;
        private boolean paused;

        LegacyDispatchHandler(Dispatcher dispatcher, Server.EndpointPayloadTracker endpointPayloadTracker)
        {
            this.dispatcher = dispatcher;
            this.endpointPayloadTracker = endpointPayloadTracker;
        }

        protected void channelRead0(ChannelHandlerContext ctx, Message.Request request) throws Exception
        {
            // if we decide to handle this message, process it outside of the netty event loop
            if (shouldHandleRequest(ctx, request))
                dispatcher.dispatch(ctx.channel(), request, this::toFlushItem);
        }

        private Flusher.FlushItem.Unframed toFlushItem(Channel channel, Message.Request request, Message.Response response)
        {
            return new Flusher.FlushItem.Unframed(channel, response, request.getSourceFrame(), this::releaseItem);
        }

        private void releaseItem(Flusher.FlushItem<Message.Response> item)
        {
            long itemSize = item.sourceFrame.header.bodySizeInBytes;
            item.sourceFrame.release();

            // since the request has been processed, decrement inflight payload at channel, endpoint and global levels
            channelPayloadBytesInFlight -= itemSize;
            ResourceLimits.Outcome endpointGlobalReleaseOutcome = endpointPayloadTracker.endpointAndGlobalPayloadsInFlight.release(itemSize);

            // now check to see if we need to reenable the channel's autoRead.
            // If the current payload side is zero, we must reenable autoread as
            // 1) we allow no other thread/channel to do it, and
            // 2) there's no other events following this one (becuase we're at zero bytes in flight),
            // so no successive to trigger the other clause in this if-block
            //
            // note: this path is only relevant when part of a pre-V5 pipeline, as only in this case is
            // paused ever set to true. In pipelines configured for V5 or later, backpressure and control
            // over the inbound pipeline's autoread status are handled by the FrameDecoder/FrameProcessor.
            ChannelConfig config = item.channel.config();
            if (paused && (channelPayloadBytesInFlight == 0 || endpointGlobalReleaseOutcome == ResourceLimits.Outcome.BELOW_LIMIT))
            {
                paused = false;
                ClientMetrics.instance.unpauseConnection();
                config.setAutoRead(true);
            }
        }

        /**
         * This check for inflight payload to potentially discard the request should have been ideally in one of the
         * first handlers in the pipeline (Frame::decode()). However, incase of any exception thrown between that
         * handler (where inflight payload is incremented) and this handler (Dispatcher::channelRead0) (where inflight
         * payload in decremented), inflight payload becomes erroneous. ExceptionHandler is not sufficient for this
         * purpose since it does not have the frame associated with the exception.
         * <p>
         * Note: this method should execute on the netty event loop.
         */
        private boolean shouldHandleRequest(ChannelHandlerContext ctx, Message.Request request)
        {
            long frameSize = request.getSourceFrame().header.bodySizeInBytes;

            ResourceLimits.EndpointAndGlobal endpointAndGlobalPayloadsInFlight = endpointPayloadTracker.endpointAndGlobalPayloadsInFlight;

            // check for overloaded state by trying to allocate framesize to inflight payload trackers
            if (endpointAndGlobalPayloadsInFlight.tryAllocate(frameSize) != ResourceLimits.Outcome.SUCCESS)
            {
                if (request.connection.isThrowOnOverload())
                {
                    // discard the request and throw an exception
                    ClientMetrics.instance.markRequestDiscarded();
                    logger.trace("Discarded request of size: {}. InflightChannelRequestPayload: {}, InflightEndpointRequestPayload: {}, InflightOverallRequestPayload: {}, Request: {}",
                                 frameSize,
                                 channelPayloadBytesInFlight,
                                 endpointAndGlobalPayloadsInFlight.endpoint().using(),
                                 endpointAndGlobalPayloadsInFlight.global().using(),
                                 request);
                    throw ErrorMessage.wrap(new OverloadedException("Server is in overloaded state. Cannot accept more requests at this point"),
                                            request.getSourceFrame().header.streamId);
                }
                else
                {
                    // set backpressure on the channel, and handle the request
                    endpointAndGlobalPayloadsInFlight.allocate(frameSize);
                    ctx.channel().config().setAutoRead(false);
                    ClientMetrics.instance.pauseConnection();
                    paused = true;
                }
            }

            channelPayloadBytesInFlight += frameSize;
            return true;
        }
    }

    /**
     *
     */
    @ChannelHandler.Sharable
    public static class ProtocolDecoder extends MessageToMessageDecoder<Frame>
    {
        public static final ProtocolDecoder instance = new ProtocolDecoder();
        private ProtocolDecoder(){}

        public void decode(ChannelHandlerContext ctx, Frame frame, List results)
        {
            try
            {
                results.add(Message.Decoder.decodeMessage(ctx.channel(), frame));
            }
            catch (Throwable ex)
            {
                frame.release();
                // Remember the streamId
                throw ErrorMessage.wrap(ex, frame.header.streamId);
            }
        }
    }

    /**
     *
     */
    @ChannelHandler.Sharable
    public static class ProtocolEncoder extends MessageToMessageEncoder<Message>
    {
        public static final ProtocolEncoder instance = new ProtocolEncoder();
        private ProtocolEncoder(){}

        public void encode(ChannelHandlerContext ctx, Message message, List results)
        {
            Connection connection = ctx.channel().attr(Connection.attributeKey).get();
            // The only case the connection can be null is when we send the initial STARTUP message (client side thus)
            ProtocolVersion version = connection == null ? ProtocolVersion.CURRENT : connection.getVersion();
            results.add(message.encode(version));
        }
    }

    /**
     *
     */
    @ChannelHandler.Sharable
    public static final class ExceptionHandler extends ChannelInboundHandlerAdapter
    {
        public static final ExceptionHandler instance = new ExceptionHandler();
        private ExceptionHandler(){}

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, Throwable cause)
        {
            // Provide error message to client in case channel is still open
            ExceptionHandlers.UnexpectedChannelExceptionHandler handler = new ExceptionHandlers.UnexpectedChannelExceptionHandler(ctx.channel(), false);
            ErrorMessage errorMessage = ErrorMessage.fromException(cause, handler);
            if (ctx.channel().isOpen())
            {
                ChannelFuture future = ctx.writeAndFlush(errorMessage);
                // On protocol exception, close the channel as soon as the message have been sent
                if (cause instanceof ProtocolException)
                {
                    future.addListener(new ChannelFutureListener()
                    {
                        public void operationComplete(ChannelFuture future)
                        {
                            ctx.close();
                        }
                    });
                }
            }
        }
    }
}
