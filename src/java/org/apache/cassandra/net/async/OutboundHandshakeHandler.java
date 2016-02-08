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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.HandshakeProtocol.FirstHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.SecondHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.ThirdHandshakeMessage;

import static org.apache.cassandra.config.Config.PROPERTY_PREFIX;

/**
 * A {@link ChannelHandler} to execute the send-side of the internode communication handshake protocol.
 * As soon as the handler is added to the channel via {@link #channelActive(ChannelHandlerContext)}
 * (which is only invoked if the underlying TCP connection was properly established), the {@link FirstHandshakeMessage}
 * of the internode messaging protocol is automatically sent out. See {@link HandshakeProtocol} for full details
 * about the internode messaging hndshake protocol.
 * <p>
 * Upon completion of the handshake (on success or fail), the {@link #callback} is invoked to let the listener
 * know the result of the handshake. See {@link HandshakeResult} for details about the different result states.
 * <p>
 * This class extends {@link ByteToMessageDecoder}, which is a {@link ChannelInboundHandler}, because this handler
 * waits for the peer's handshake response (the {@link SecondHandshakeMessage} of the internode messaging handshake protocol).
 */
public class OutboundHandshakeHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundHandshakeHandler.class);

    /**
     * The number of milliseconds to wait before closing a channel if there has been no progress (when there is
     * data to be sent).See {@link IdleStateHandler} and {@link MessageOutHandler#userEventTriggered(ChannelHandlerContext, Object)}.
     */
    private static final long DEFAULT_WRITE_IDLE_MS = TimeUnit.SECONDS.toMillis(10);
    private static final String WRITE_IDLE_PROPERTY = PROPERTY_PREFIX + "outbound_write_idle_ms";
    private static final long WRITE_IDLE_MS = Long.getLong(WRITE_IDLE_PROPERTY, DEFAULT_WRITE_IDLE_MS);

    private final OutboundConnectionIdentifier connectionId;

    /**
     * The expected messaging service version to use.
     */
    private final int messagingVersion;

    /**
     * A function to invoke upon completion of the attempt, success or failure, to connect to the peer.
     */
    private final Consumer<HandshakeResult> callback;
    private final NettyFactory.Mode mode;
    private final OutboundConnectionParams params;

    OutboundHandshakeHandler(OutboundConnectionParams params)
    {
        this.params = params;
        this.connectionId = params.connectionId;
        this.messagingVersion = params.protocolVersion;
        this.callback = params.callback;
        this.mode = params.mode;
    }

    /**
     * {@inheritDoc}
     *
     * Invoked when the channel is made active, and sends out the {@link FirstHandshakeMessage}
     */
    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception
    {
        FirstHandshakeMessage msg = new FirstHandshakeMessage(messagingVersion, mode, params.compress);
        logger.trace("starting handshake with peer {}, msg = {}", connectionId.connectionAddress(), msg);
        ctx.writeAndFlush(msg.encode(ctx.alloc())).addListener(future -> firstHandshakeMessageListener(future, ctx));
        ctx.fireChannelActive();
    }

    /**
     * A simple listener to make sure we could send the {@link FirstHandshakeMessage} to the socket,
     * and fail the handshake attempt if we could not (for example, maybe we could create the TCP socket, but then
     * the connection gets closed for some reason).
     */
    void firstHandshakeMessageListener(Future<? super Void> future, ChannelHandlerContext ctx)
    {
        if (future.isSuccess())
            return;

        ChannelFuture channelFuture = (ChannelFuture)future;
        exceptionCaught(ctx, channelFuture.cause());
    }

    /**
     * {@inheritDoc}
     *
     * Invoked when we get the response back from the peer, which should contain the second message of the internode messaging handshake.
     * <p>
     * If the peer's protocol version does not equal what we were expecting, immediately close the channel (and socket);
     * do *not* send out the third message of the internode messaging handshake.
     * We will reconnect on the appropriate protocol version.
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
    {
        SecondHandshakeMessage msg = SecondHandshakeMessage.maybeDecode(in);
        if (msg == null)
            return;

        logger.trace("received second handshake message from peer {}, msg = {}", connectionId.connectionAddress(), msg);
        final int peerMessagingVersion = msg.messagingVersion;

        // we expected a higher protocol version, but it was actually lower
        if (messagingVersion > peerMessagingVersion)
        {
            logger.trace("peer's max version is {}; will reconnect with that version", peerMessagingVersion);
            try
            {
                if (DatabaseDescriptor.getSeeds().contains(connectionId.remote()))
                    logger.warn("Seed gossip version is {}; will not connect with that version", peerMessagingVersion);
            }
            catch (Throwable e)
            {
                // If invalid yaml has been added to the config since startup, getSeeds() will throw an AssertionError
                // Additionally, third party seed providers may throw exceptions if network is flakey.
                // Regardless of what's thrown, we must catch it, disconnect, and try again
                logger.warn("failed to reread yaml (on trying to connect to a seed): {}", e.getLocalizedMessage());
            }
            ctx.close();
            callback.accept(HandshakeResult.disconnect(peerMessagingVersion));
            return;
        }
        // we anticipate a version that is lower than what peer is actually running
        else if (messagingVersion < peerMessagingVersion && messagingVersion < MessagingService.current_version)
        {
            logger.trace("peer has a higher max version than expected {} (previous value {})", peerMessagingVersion, messagingVersion);
            ctx.close();
            callback.accept(HandshakeResult.disconnect(peerMessagingVersion));
            return;
        }

        try
        {
            ctx.writeAndFlush(new ThirdHandshakeMessage(MessagingService.current_version, connectionId.local()).encode(ctx.alloc()));
            ChannelWriter channelWriter = setupPipeline(ctx.channel(), peerMessagingVersion);
            callback.accept(HandshakeResult.success(channelWriter, peerMessagingVersion));
        }
        catch (Exception e)
        {
            logger.info("failed to finalize internode messaging handshake", e);
            ctx.close();
            callback.accept(HandshakeResult.failed());
        }
    }

    @VisibleForTesting
    ChannelWriter setupPipeline(Channel channel, int messagingVersion)
    {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("idleWriteHandler", new IdleStateHandler(true, 0, WRITE_IDLE_MS, 0, TimeUnit.MILLISECONDS));
        if (params.compress)
            pipeline.addLast(NettyFactory.OUTBOUND_COMPRESSOR_HANDLER_NAME, NettyFactory.createLz4Encoder(messagingVersion));

        ChannelWriter channelWriter = ChannelWriter.create(channel, params.messageResultConsumer, params.coalescingStrategy);
        pipeline.addLast("messageOutHandler", new MessageOutHandler(connectionId, messagingVersion, channelWriter, params.backlogSupplier));
        pipeline.remove(this);
        return channelWriter;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        logger.error("Failed to properly handshake with peer {}. Closing the channel.", connectionId, cause);
        ctx.close();
        callback.accept(HandshakeResult.failed());
    }

    /**
     * The result of the handshake. Handshake has 3 possible outcomes:
     *  1) it can be successful, in which case the channel and version to used is returned in this result.
     *  2) we may decide to disconnect to reconnect with another protocol version (namely, the version is passed in this result).
     *  3) we can have a negotiation failure for an unknown reason. (#sadtrombone)
     */
    public static class HandshakeResult
    {
        static final int UNKNOWN_PROTOCOL_VERSION = -1;

        /**
         * Describes the result of receiving the response back from the peer (Message 2 of the handshake)
         * and implies an action that should be taken.
         */
        enum Outcome
        {
            SUCCESS, DISCONNECT, NEGOTIATION_FAILURE
        }

        /** The channel for the connection, only set for successful handshake. */
        final ChannelWriter channelWriter;
        /** The version negotiated with the peer. Set unless this is a {@link Outcome#NEGOTIATION_FAILURE}. */
        final int negotiatedMessagingVersion;
        /** The handshake {@link Outcome}. */
        final Outcome outcome;

        private HandshakeResult(ChannelWriter channelWriter, int negotiatedMessagingVersion, Outcome outcome)
        {
            this.channelWriter = channelWriter;
            this.negotiatedMessagingVersion = negotiatedMessagingVersion;
            this.outcome = outcome;
        }

        static HandshakeResult success(ChannelWriter channel, int negotiatedMessagingVersion)
        {
            return new HandshakeResult(channel, negotiatedMessagingVersion, Outcome.SUCCESS);
        }

        static HandshakeResult disconnect(int negotiatedMessagingVersion)
        {
            return new HandshakeResult(null, negotiatedMessagingVersion, Outcome.DISCONNECT);
        }

        static HandshakeResult failed()
        {
            return new HandshakeResult(null, UNKNOWN_PROTOCOL_VERSION, Outcome.NEGOTIATION_FAILURE);
        }
    }
}
