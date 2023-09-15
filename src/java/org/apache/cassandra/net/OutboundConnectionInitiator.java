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

package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.security.cert.Certificate;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;

import io.netty.util.concurrent.Future; //checkstyle: permit this import
import io.netty.util.concurrent.Promise; //checkstyle: permit this import
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;

import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslClosedEngineException;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.MessagingSuccess;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.StreamingSuccess;
import org.apache.cassandra.security.ISslContextFactory;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.OUTBOUND;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.OUTBOUND_PRECONNECT;
import static org.apache.cassandra.net.InternodeConnectionUtils.DISCARD_HANDLER_NAME;
import static org.apache.cassandra.net.InternodeConnectionUtils.SSL_FACTORY_CONTEXT_DESCRIPTION;
import static org.apache.cassandra.net.InternodeConnectionUtils.SSL_HANDLER_NAME;
import static org.apache.cassandra.net.InternodeConnectionUtils.certificates;
import static org.apache.cassandra.net.HandshakeProtocol.*;
import static org.apache.cassandra.net.ConnectionType.STREAMING;
import static org.apache.cassandra.net.OutboundConnectionInitiator.Result.incompatible;
import static org.apache.cassandra.net.OutboundConnectionInitiator.Result.messagingSuccess;
import static org.apache.cassandra.net.OutboundConnectionInitiator.Result.streamingSuccess;
import static org.apache.cassandra.net.SocketFactory.*;

/**
 * A {@link ChannelHandler} to execute the send-side of the internode handshake protocol.
 * As soon as the handler is added to the channel via {@link ChannelInboundHandler#channelActive(ChannelHandlerContext)}
 * (which is only invoked if the underlying TCP connection was properly established), the {@link Initiate}
 * handshake is sent. See {@link HandshakeProtocol} for full details.
 * <p>
 * Upon completion of the handshake (on success or fail), the {@link #resultPromise} is completed.
 * See {@link Result} for details about the different result states.
 * <p>
 * This class extends {@link ByteToMessageDecoder}, which is a {@link ChannelInboundHandler}, because this handler
 * waits for the peer's handshake response (the {@link Accept} of the internode messaging handshake protocol).
 */
public class OutboundConnectionInitiator<SuccessType extends OutboundConnectionInitiator.Result.Success>
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundConnectionInitiator.class);

    private final ConnectionType type;
    private final SslFallbackConnectionType sslConnectionType;
    private final OutboundConnectionSettings settings;
    private final Promise<Result<SuccessType>> resultPromise;
    private boolean isClosed;

    private OutboundConnectionInitiator(ConnectionType type, SslFallbackConnectionType sslConnectionType, OutboundConnectionSettings settings,
                                        Promise<Result<SuccessType>> resultPromise)
    {
        this.type = type;
        this.sslConnectionType = sslConnectionType;

        this.settings = settings;
        this.resultPromise = resultPromise;
    }

    /**
     * Initiate a connection with the requested messaging version.
     * if the other node supports a newer version, or doesn't support this version, we will fail to connect
     * and try again with the version they reported
     *
     * The returned {@code Future} is guaranteed to be completed on the supplied eventLoop.
     */
    public static Future<Result<StreamingSuccess>> initiateStreaming(EventLoop eventLoop, OutboundConnectionSettings settings,
                                                                     SslFallbackConnectionType sslConnectionType)
    {
        return new OutboundConnectionInitiator<StreamingSuccess>(STREAMING, sslConnectionType, settings, AsyncPromise.withExecutor(eventLoop))
               .initiate(eventLoop);
    }

    /**
     * Initiate a connection with the requested messaging version.
     * if the other node supports a newer version, or doesn't support this version, we will fail to connect
     * and try again with the version they reported
     *
     * The returned {@code Future} is guaranteed to be completed on the supplied eventLoop.
     */
    static Future<Result<MessagingSuccess>> initiateMessaging(EventLoop eventLoop, ConnectionType type, SslFallbackConnectionType sslConnectionType,
                                                              OutboundConnectionSettings settings, Promise<Result<MessagingSuccess>> result)
    {
        return new OutboundConnectionInitiator<>(type, sslConnectionType, settings, result)
               .initiate(eventLoop);
    }

    private Future<Result<SuccessType>> initiate(EventLoop eventLoop)
    {
        if (logger.isTraceEnabled())
            logger.trace("creating outbound bootstrap to {}", settings);

        if (!settings.authenticator.authenticate(settings.to.getAddress(), settings.to.getPort(), null, OUTBOUND_PRECONNECT))
        {
            // interrupt other connections, so they must attempt to re-authenticate
            MessagingService.instance().interruptOutbound(settings.to);
            logger.error("Authentication failed to " + settings.connectToId());
            return ImmediateFuture.failure(new IOException("Authentication failed to " + settings.connectToId()));
        }


        // this is a bit ugly, but is the easiest way to ensure that if we timeout we can propagate a suitable error message
        // and still guarantee that, if on timing out we raced with success, the successfully created channel is handled
        AtomicBoolean timedout = new AtomicBoolean();
        io.netty.util.concurrent.Future<Void> bootstrap = createBootstrap(eventLoop)
                                 .connect()
                                 .addListener(future -> {
                                     eventLoop.execute(() -> {
                                         if (!future.isSuccess())
                                         {
                                             if (future.isCancelled() && !timedout.get())
                                                 resultPromise.cancel(true);
                                             else if (future.isCancelled())
                                                 resultPromise.tryFailure(new IOException("Timeout handshaking with " + settings.connectToId()));
                                             else
                                                 resultPromise.tryFailure(future.cause());
                                         }
                                     });
                                 });

        ScheduledFuture<?> timeout = eventLoop.schedule(() -> {
            timedout.set(true);
            bootstrap.cancel(false);
        }, TIMEOUT_MILLIS, MILLISECONDS);
        bootstrap.addListener(future -> timeout.cancel(true));

        // Note that the bootstrap future's listeners may be invoked outside of the eventLoop,
        // as Epoll failures on connection and disconnect may be run on the GlobalEventExecutor
        // Since this FutureResult's listeners are all given to our resultPromise, they are guaranteed to be invoked by the eventLoop.
        return new FutureResult<>(resultPromise, bootstrap);
    }

    /**
     * Create the {@link Bootstrap} for connecting to a remote peer. This method does <b>not</b> attempt to connect to the peer,
     * and thus does not block.
     */
    private Bootstrap createBootstrap(EventLoop eventLoop)
    {
        Bootstrap bootstrap = settings.socketFactory
                                      .newClientBootstrap(eventLoop, settings.tcpUserTimeoutInMS)
                                      .option(ChannelOption.ALLOCATOR, GlobalBufferPoolAllocator.instance)
                                      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.tcpConnectTimeoutInMS)
                                      .option(ChannelOption.SO_KEEPALIVE, true)
                                      .option(ChannelOption.SO_REUSEADDR, true)
                                      .option(ChannelOption.TCP_NODELAY, settings.tcpNoDelay)
                                      .option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, NoSizeEstimator.instance)
                                      .handler(new Initializer());

        if (settings.socketSendBufferSizeInBytes > 0)
            bootstrap.option(ChannelOption.SO_SNDBUF, settings.socketSendBufferSizeInBytes);

        InetAddressAndPort remoteAddress = settings.connectTo;
        bootstrap.remoteAddress(new InetSocketAddress(remoteAddress.getAddress(), remoteAddress.getPort()));
        return bootstrap;
    }

    public enum SslFallbackConnectionType
    {
        SERVER_CONFIG, // Original configuration of the server
        MTLS,
        SSL,
        NO_SSL
    }

    private class Initializer extends ChannelInitializer<SocketChannel>
    {
        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> server-authentication -> logger -> handshakeHandler
            if ((sslConnectionType == SslFallbackConnectionType.SERVER_CONFIG && settings.withEncryption())
                || sslConnectionType == SslFallbackConnectionType.SSL || sslConnectionType == SslFallbackConnectionType.MTLS)
            {
                SslContext sslContext = getSslContext(sslConnectionType);
                // for some reason channel.remoteAddress() will return null
                InetAddressAndPort address = settings.to;
                InetSocketAddress peer = settings.encryption.require_endpoint_verification ? new InetSocketAddress(address.getAddress(), address.getPort()) : null;
                SslHandler sslHandler = newSslHandler(channel, sslContext, peer);
                logger.trace("creating outbound netty SslContext: context={}, engine={}", sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
                pipeline.addFirst(SSL_HANDLER_NAME, sslHandler);
            }
            pipeline.addLast("server-authentication", new ServerAuthenticationHandler(settings));

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            pipeline.addLast("handshake", new Handler());
        }

        private SslContext getSslContext(SslFallbackConnectionType connectionType) throws IOException
        {
            boolean requireClientAuth = false;
            if (connectionType == SslFallbackConnectionType.MTLS || connectionType == SslFallbackConnectionType.SSL)
            {
                requireClientAuth = true;
            }
            else if (connectionType == SslFallbackConnectionType.SERVER_CONFIG)
            {
                requireClientAuth = settings.withEncryption();
            }
            return SSLFactory.getOrCreateSslContext(settings.encryption, requireClientAuth, ISslContextFactory.SocketType.CLIENT, SSL_FACTORY_CONTEXT_DESCRIPTION);
        }

    }

    /**
     * Authenticates the server before an outbound connection is established. If a connection is SSL based connection
     * Server's identity is verified during ssl handshake using root certificate in truststore. One may choose to ignore
     * outbound authentication or perform required authentication for outbound connections in the implementation
     * of IInternodeAuthenticator interface.
     */
    @VisibleForTesting
    static class ServerAuthenticationHandler extends ByteToMessageDecoder
    {
        final OutboundConnectionSettings settings;

        ServerAuthenticationHandler(OutboundConnectionSettings settings)
        {
            this.settings = settings;
        }

        @Override
        protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception
        {
            // Extract certificates from SSL handler(handler with name "ssl").
            final Certificate[] certificates = certificates(channelHandlerContext.channel());
            if (!settings.authenticator.authenticate(settings.to.getAddress(), settings.to.getPort(), certificates, OUTBOUND))
            {
                // interrupt other connections, so they must attempt to re-authenticate
                MessagingService.instance().interruptOutbound(settings.to);
                logger.error("Authentication failed to " + settings.connectToId());

                // To release all the pending buffered data, replace authentication handler with discard handler.
                // This avoids pending inbound data to be fired through the pipeline
                channelHandlerContext.pipeline().replace(this, DISCARD_HANDLER_NAME, new InternodeConnectionUtils.ByteBufDiscardHandler());
                channelHandlerContext.pipeline().close();
            }
            else
            {
                channelHandlerContext.pipeline().remove(this);
            }
        }
    }

    private class Handler extends ByteToMessageDecoder
    {
        /**
         * {@inheritDoc}
         *
         * Invoked when the channel is made active, and sends out the {@link Initiate}.
         * In the case of streaming, we do not require a full bi-directional handshake; the initial message,
         * containing the streaming protocol version, is all that is required.
         */
        @Override
        public void channelActive(final ChannelHandlerContext ctx) throws Exception
        {
            Initiate msg = new Initiate(settings.acceptVersions, type, settings.framing, settings.from);
            logger.trace("starting handshake with peer {}, msg = {}", settings.connectToId(), msg);

            AsyncChannelPromise.writeAndFlush(ctx, msg.encode(),
                      future -> { if (!future.isSuccess()) exceptionCaught(ctx, future.cause()); });

            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception
        {
            super.channelInactive(ctx);
            resultPromise.tryFailure(new ClosedChannelException());
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
        @SuppressWarnings("unchecked")
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        {
            try
            {
                Accept msg = Accept.maybeDecode(in);
                if (msg == null)
                    return;

                int useMessagingVersion = msg.useMessagingVersion;
                int peerMessagingVersion = msg.maxMessagingVersion;
                logger.trace("received second handshake message from peer {}, msg = {}", settings.connectTo, msg);

                FrameEncoder frameEncoder = null;
                Result<SuccessType> result;
                assert useMessagingVersion > 0;

                if (useMessagingVersion < settings.acceptVersions.min || useMessagingVersion > settings.acceptVersions.max)
                {
                    result = incompatible(useMessagingVersion, peerMessagingVersion);
                }
                else
                {
                    // This is a bit ugly
                    if (type.isMessaging())
                    {
                        switch (settings.framing)
                        {
                            case LZ4:
                                frameEncoder = FrameEncoderLZ4.fastInstance;
                                break;
                            case CRC:
                                frameEncoder = FrameEncoderCrc.instance;
                                break;
                            case UNPROTECTED:
                                frameEncoder = FrameEncoderUnprotected.instance;
                                break;
                        }

                        result = (Result<SuccessType>) messagingSuccess(ctx.channel(), useMessagingVersion, frameEncoder.allocator());
                    }
                    else
                    {
                        result = (Result<SuccessType>) streamingSuccess(ctx.channel(), useMessagingVersion);
                    }
                }

                ChannelPipeline pipeline = ctx.pipeline();
                if (result.isSuccess())
                {
                    BufferPools.forNetworking().setRecycleWhenFreeForCurrentThread(false);
                    if (type.isMessaging())
                    {
                        assert frameEncoder != null;
                        pipeline.addLast("frameEncoder", frameEncoder);
                    }
                    pipeline.remove(this);
                }
                else
                {
                    pipeline.close();
                }

                if (!resultPromise.trySuccess(result) && result.isSuccess())
                    result.success().channel.close();
            }
            catch (Throwable t)
            {
                exceptionCaught(ctx, t);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        {
            if (isClosed && cause instanceof SslClosedEngineException)
            {
                /*
                 * Occasionally Netty will invoke this handler to process an exception of the following kind:
                 *      io.netty.channel.unix.Errors$NativeIoException: readAddress(..) failed: Connection reset by peer
                 *
                 * When we invoke ctx.close() later in this method, the listener, set up in channelActive(), might be
                 * failed with an SslClosedEngineException("SSLEngine closed already") by Netty, and exceptionCaught() will be invoked
                 * once again, this time to handle the SSLException triggered by ctx.close().
                 *
                 * The exception at this stage is benign, and we shouldn't be double-logging the failure to connect.
                 */
                return;
            }

            try
            {
                JVMStabilityInspector.inspectThrowable(cause);
                resultPromise.tryFailure(cause);
                if (isCausedByConnectionReset(cause))
                    logger.info("Failed to connect to peer {}", settings.connectToId(), cause);
                else
                    logger.error("Failed to handshake with peer {}", settings.connectToId(), cause);
                isClosed = true;
                ctx.close();
            }
            catch (Throwable t)
            {
                logger.error("Unexpected exception in {}.exceptionCaught", this.getClass().getSimpleName(), t);
            }
        }
    }

    /**
     * The result of the handshake. Handshake has 3 possible outcomes:
     *  1) it can be successful, in which case the channel and version to used is returned in this result.
     *  2) we may decide to disconnect to reconnect with another protocol version (namely, the version is passed in this result).
     *  3) we can have a negotiation failure for an unknown reason. (#sadtrombone)
     */
    public static class Result<SuccessType extends Result.Success>
    {
        /**
         * Describes the result of receiving the response back from the peer (Message 2 of the handshake)
         * and implies an action that should be taken.
         */
        enum Outcome
        {
            SUCCESS, RETRY, INCOMPATIBLE
        }

        public static class Success<SuccessType extends Success> extends Result<SuccessType>
        {
            public final Channel channel;
            public final int messagingVersion;
            Success(Channel channel, int messagingVersion)
            {
                super(Outcome.SUCCESS);
                this.channel = channel;
                this.messagingVersion = messagingVersion;
            }
        }

        public static class StreamingSuccess extends Success<StreamingSuccess>
        {
            StreamingSuccess(Channel channel, int messagingVersion)
            {
                super(channel, messagingVersion);
            }
        }

        public static class MessagingSuccess extends Success<MessagingSuccess>
        {
            public final FrameEncoder.PayloadAllocator allocator;
            MessagingSuccess(Channel channel, int messagingVersion, FrameEncoder.PayloadAllocator allocator)
            {
                super(channel, messagingVersion);
                this.allocator = allocator;
            }
        }

        static class Retry<SuccessType extends Success> extends Result<SuccessType>
        {
            final int withMessagingVersion;
            Retry(int withMessagingVersion)
            {
                super(Outcome.RETRY);
                this.withMessagingVersion = withMessagingVersion;
            }
        }

        static class Incompatible<SuccessType extends Success> extends Result<SuccessType>
        {
            final int closestSupportedVersion;
            final int maxMessagingVersion;
            Incompatible(int closestSupportedVersion, int maxMessagingVersion)
            {
                super(Outcome.INCOMPATIBLE);
                this.closestSupportedVersion = closestSupportedVersion;
                this.maxMessagingVersion = maxMessagingVersion;
            }
        }

        final Outcome outcome;

        private Result(Outcome outcome)
        {
            this.outcome = outcome;
        }

        boolean isSuccess() { return outcome == Outcome.SUCCESS; }
        public SuccessType success() { return (SuccessType) this; }
        static MessagingSuccess messagingSuccess(Channel channel, int messagingVersion, FrameEncoder.PayloadAllocator allocator) { return new MessagingSuccess(channel, messagingVersion, allocator); }
        static StreamingSuccess streamingSuccess(Channel channel, int messagingVersion) { return new StreamingSuccess(channel, messagingVersion); }

        public Retry retry() { return (Retry) this; }
        static <SuccessType extends Success> Result<SuccessType> retry(int withMessagingVersion) { return new Retry<>(withMessagingVersion); }

        public Incompatible incompatible() { return (Incompatible) this; }
        static <SuccessType extends Success> Result<SuccessType> incompatible(int closestSupportedVersion, int maxMessagingVersion) { return new Incompatible(closestSupportedVersion, maxMessagingVersion); }
    }

}
