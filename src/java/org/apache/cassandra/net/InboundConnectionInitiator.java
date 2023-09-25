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
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.OutboundConnectionSettings.Framing;
import org.apache.cassandra.security.ISslContextFactory;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.streaming.StreamDeserializingTask;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingChannel;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.lang.Math.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.net.InternodeConnectionUtils.DISCARD_HANDLER_NAME;
import static org.apache.cassandra.net.InternodeConnectionUtils.SSL_FACTORY_CONTEXT_DESCRIPTION;
import static org.apache.cassandra.net.InternodeConnectionUtils.SSL_HANDLER_NAME;
import static org.apache.cassandra.net.InternodeConnectionUtils.certificates;
import static org.apache.cassandra.net.MessagingService.*;
import static org.apache.cassandra.net.SocketFactory.WIRETRACE;
import static org.apache.cassandra.net.SocketFactory.newSslHandler;

public class InboundConnectionInitiator
{
    private static final Logger logger = LoggerFactory.getLogger(InboundConnectionInitiator.class);

    private static class Initializer extends ChannelInitializer<SocketChannel>
    {
        private static final String PIPELINE_INTERNODE_ERROR_EXCLUSIONS = "Internode Error Exclusions";

        private final InboundConnectionSettings settings;
        private final ChannelGroup channelGroup;
        private final Consumer<ChannelPipeline> pipelineInjector;

        Initializer(InboundConnectionSettings settings, ChannelGroup channelGroup,
                    Consumer<ChannelPipeline> pipelineInjector)
        {
            this.settings = settings;
            this.channelGroup = channelGroup;
            this.pipelineInjector = pipelineInjector;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception
        {
            // if any of the handlers added fail they will send the error to the "head", so this needs to be first
            channel.pipeline().addFirst(PIPELINE_INTERNODE_ERROR_EXCLUSIONS, new InternodeErrorExclusionsHandler());

            channelGroup.add(channel);

            channel.config().setOption(ChannelOption.ALLOCATOR, GlobalBufferPoolAllocator.instance);
            channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
            channel.config().setOption(ChannelOption.SO_REUSEADDR, true);
            channel.config().setOption(ChannelOption.TCP_NODELAY, true); // we only send handshake messages; no point ever delaying

            ChannelPipeline pipeline = channel.pipeline();

            pipelineInjector.accept(pipeline);

            // order of handlers: ssl -> client-authentication -> logger -> handshakeHandler
            // For either unencrypted or transitional modes, allow Ssl optionally.
            switch(settings.encryption.tlsEncryptionPolicy())
            {
                case UNENCRYPTED:
                    // Handler checks for SSL connection attempts and cleanly rejects them if encryption is disabled
                    pipeline.addAfter(PIPELINE_INTERNODE_ERROR_EXCLUSIONS, "rejectssl", new RejectSslHandler());
                    break;
                case OPTIONAL:
                    pipeline.addAfter(PIPELINE_INTERNODE_ERROR_EXCLUSIONS, SSL_HANDLER_NAME, new OptionalSslHandler(settings.encryption));
                    break;
                case ENCRYPTED:
                    SslHandler sslHandler = getSslHandler("creating", channel, settings.encryption);
                    pipeline.addAfter(PIPELINE_INTERNODE_ERROR_EXCLUSIONS, SSL_HANDLER_NAME, sslHandler);
                    break;
            }

            // Pipeline for performing client authentication
            pipeline.addLast("client-authentication", new ClientAuthenticationHandler(settings.authenticator));

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            channel.pipeline().addLast("handshake", new Handler(settings));
        }
    }

    private static class InternodeErrorExclusionsHandler extends ChannelInboundHandlerAdapter
    {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            if (DatabaseDescriptor.getInternodeErrorReportingExclusions().contains(ctx.channel().remoteAddress()))
            {
                logger.debug("Excluding internode exception for {}; address contained in internode_error_reporting_exclusions", ctx.channel().remoteAddress(), cause);
                return;
            }
            super.exceptionCaught(ctx, cause);
        }
    }

    /**
     * Create a {@link Channel} that listens on the {@code localAddr}. This method will block while trying to bind to the address,
     * but it does not make a remote call.
     */
    private static ChannelFuture bind(Initializer initializer) throws ConfigurationException
    {
        logger.info("Listening on {}", initializer.settings);

        ServerBootstrap bootstrap = initializer.settings.socketFactory
                                    .newServerBootstrap()
                                    .option(ChannelOption.SO_BACKLOG, 1 << 9)
                                    .option(ChannelOption.ALLOCATOR, GlobalBufferPoolAllocator.instance)
                                    .option(ChannelOption.SO_REUSEADDR, true)
                                    .childHandler(initializer);

        int socketReceiveBufferSizeInBytes = initializer.settings.socketReceiveBufferSizeInBytes;
        if (socketReceiveBufferSizeInBytes > 0)
            bootstrap.childOption(ChannelOption.SO_RCVBUF, socketReceiveBufferSizeInBytes);

        InetAddressAndPort bind = initializer.settings.bindAddress;
        ChannelFuture channelFuture = bootstrap.bind(new InetSocketAddress(bind.getAddress(), bind.getPort()));

        if (!channelFuture.awaitUninterruptibly().isSuccess())
        {
            if (channelFuture.channel().isOpen())
                channelFuture.channel().close();

            Throwable failedChannelCause = channelFuture.cause();

            String causeString = "";
            if (failedChannelCause != null && failedChannelCause.getMessage() != null)
                causeString = failedChannelCause.getMessage();

            if (causeString.contains("in use"))
            {
                throw new ConfigurationException(bind + " is in use by another process.  Change listen_address:storage_port " +
                                                 "in cassandra.yaml to values that do not conflict with other services");
            }
            else if (causeString.contains("cannot assign requested address"))
            {
                throw new ConfigurationException("Unable to bind to address " + bind
                                                 + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            }
            else
            {
                throw new ConfigurationException("failed to bind to: " + bind, failedChannelCause);
            }
        }

        return channelFuture;
    }

    public static ChannelFuture bind(InboundConnectionSettings settings, ChannelGroup channelGroup,
                                     Consumer<ChannelPipeline> pipelineInjector)
    {
        return bind(new Initializer(settings, channelGroup, pipelineInjector));
    }

    /**
     * Handler to perform authentication for internode inbound connections.
     * This handler is called even before messaging handshake starts.
     */
    private static class ClientAuthenticationHandler extends ByteToMessageDecoder
    {
        private final IInternodeAuthenticator authenticator;

        public ClientAuthenticationHandler(IInternodeAuthenticator authenticator)
        {
            this.authenticator = authenticator;
        }

        @Override
        protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception
        {
            // Extract certificates from SSL handler(handler with name "ssl").
            final Certificate[] certificates = certificates(channelHandlerContext.channel());
            if (!authenticate(channelHandlerContext.channel().remoteAddress(), certificates))
            {
                logger.error("Unable to authenticate peer {} for internode authentication", channelHandlerContext.channel());

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

        private boolean authenticate(SocketAddress socketAddress, final Certificate[] certificates) throws IOException
        {
            if (socketAddress.getClass().getSimpleName().equals("EmbeddedSocketAddress"))
                return true;

            if (!(socketAddress instanceof InetSocketAddress))
                throw new IOException(String.format("Unexpected SocketAddress type: %s, %s", socketAddress.getClass(), socketAddress));

            InetSocketAddress addr = (InetSocketAddress) socketAddress;
            if (!authenticator.authenticate(addr.getAddress(), addr.getPort(), certificates, INBOUND))
            {
                // Log at info level as anything that can reach the inbound port could hit this
                // and trigger a log of noise.  Failed outbound connections to known cluster endpoints
                // still fail with an ERROR message and exception to alert operators that aren't watching logs closely.
                logger.info("Authenticate rejected inbound internode connection from {}", addr);
                return false;
            }
            return true;
        }

    }

    /**
     * 'Server-side' component that negotiates the internode handshake when establishing a new connection.
     * This handler will be the first in the netty channel for each incoming connection (secure socket (TLS) notwithstanding),
     * and once the handshake is successful, it will configure the proper handlers ({@link InboundMessageHandler}
     * or {@link StreamingInboundHandler}) and remove itself from the working pipeline.
     */
    static class Handler extends ByteToMessageDecoder
    {
        private final InboundConnectionSettings settings;

        private HandshakeProtocol.Initiate initiate;

        /**
         * A future the essentially places a timeout on how long we'll wait for the peer
         * to complete the next step of the handshake.
         */
        private Future<?> handshakeTimeout;

        Handler(InboundConnectionSettings settings)
        {
            this.settings = settings;
        }

        /**
         * On registration, immediately schedule a timeout to kill this connection if it does not handshake promptly.
         */
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception
        {
            handshakeTimeout = ctx.executor().schedule(() -> {
                logger.error("Timeout handshaking with {} (on {})", SocketFactory.addressId(initiate.from, (InetSocketAddress) ctx.channel().remoteAddress()), settings.bindAddress);
                failHandshake(ctx);
            }, HandshakeProtocol.TIMEOUT_MILLIS, MILLISECONDS);
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
        {
            if (initiate == null) initiate(ctx, in);
            else throw new IllegalStateException("Should no longer be on pipeline");
        }

        void initiate(ChannelHandlerContext ctx, ByteBuf in) throws IOException
        {
            initiate = HandshakeProtocol.Initiate.maybeDecode(in);
            if (initiate == null)
                return;

            logger.trace("Received handshake initiation message from peer {}, message = {}", ctx.channel().remoteAddress(), initiate);

            if (isEncryptionRequired(initiate.from) && !isChannelEncrypted(ctx))
            {
                logger.warn("peer {} attempted to establish an unencrypted connection (broadcast address {})",
                            ctx.channel().remoteAddress(), initiate.from);
                failHandshake(ctx);
                return;
            }

            assert initiate.acceptVersions != null;
            logger.trace("Connection version {} (min {}) from {}", initiate.acceptVersions.max, initiate.acceptVersions.min, initiate.from);

            final AcceptVersions accept;

            if (initiate.type.isStreaming())
                accept = settings.acceptStreaming;
            else
                accept = settings.acceptMessaging;

            int useMessagingVersion = max(accept.min, min(accept.max, initiate.acceptVersions.max));
            ByteBuf flush = new HandshakeProtocol.Accept(useMessagingVersion, accept.max).encode(ctx.alloc());

            AsyncChannelPromise.writeAndFlush(ctx, flush, (ChannelFutureListener) future -> {
                if (!future.isSuccess())
                    exceptionCaught(future.channel(), future.cause());
            });

            if (initiate.acceptVersions.min > accept.max)
            {
                logger.info("peer {} only supports messaging versions higher ({}) than this node supports ({})", ctx.channel().remoteAddress(), initiate.acceptVersions.min, current_version);
                failHandshake(ctx);
            }
            else if (initiate.acceptVersions.max < accept.min)
            {
                logger.info("peer {} only supports messaging versions lower ({}) than this node supports ({})", ctx.channel().remoteAddress(), initiate.acceptVersions.max, minimum_version);
                failHandshake(ctx);
            }
            else
            {
                if (initiate.type.isStreaming())
                    setupStreamingPipeline(initiate.from, ctx);
                else
                    setupMessagingPipeline(initiate.from, useMessagingVersion, initiate.acceptVersions.max, ctx.pipeline());
            }
        }

        private boolean isEncryptionRequired(InetAddressAndPort peer)
        {
            return !settings.encryption.isExplicitlyOptional() && settings.encryption.shouldEncrypt(peer);
        }

        private boolean isChannelEncrypted(ChannelHandlerContext ctx)
        {
            return ctx.pipeline().get(SslHandler.class) != null;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        {
            exceptionCaught(ctx.channel(), cause);
        }

        private void exceptionCaught(Channel channel, Throwable cause)
        {
            final SocketAddress remoteAddress = channel.remoteAddress();
            boolean reportingExclusion = DatabaseDescriptor.getInternodeErrorReportingExclusions().contains(remoteAddress);

            if (reportingExclusion)
                logger.debug("Excluding internode exception for {}; address contained in internode_error_reporting_exclusions", remoteAddress, cause);
            else
                logger.error("Failed to properly handshake with peer {}. Closing the channel.", remoteAddress, cause);

            try
            {
                failHandshake(channel);
            }
            catch (Throwable t)
            {
                if (!reportingExclusion)
                    logger.error("Unexpected exception in {}.exceptionCaught", this.getClass().getSimpleName(), t);
            }
        }

        private void failHandshake(ChannelHandlerContext ctx)
        {
            failHandshake(ctx.channel());
        }

        private void failHandshake(Channel channel)
        {
            // Cancel the handshake timeout as early as possible as it calls this method
            if (handshakeTimeout != null)
                handshakeTimeout.cancel(true);

            // prevent further decoding of buffered data by removing this handler before closing
            // otherwise the pending bytes will be decoded again on close, throwing further exceptions.
            try
            {
                channel.pipeline().remove(this);
            }
            catch (NoSuchElementException ex)
            {
                // possible race with the handshake timeout firing and removing this handler already
            }
            finally
            {
                channel.close();
            }
        }

        private void setupStreamingPipeline(InetAddressAndPort from, ChannelHandlerContext ctx)
        {
            handshakeTimeout.cancel(true);
            assert initiate.framing == Framing.UNPROTECTED;

            ChannelPipeline pipeline = ctx.pipeline();
            Channel channel = ctx.channel();

            if (from == null)
            {
                InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
                from = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
            }

            BufferPools.forNetworking().setRecycleWhenFreeForCurrentThread(false);

            // we can't infer the type of streaming connection at this point,
            // so we use CONTROL unconditionally; it's ugly but does what we want
            // (establishes an AsyncStreamingInputPlus)
            NettyStreamingChannel streamingChannel = new NettyStreamingChannel(channel, StreamingChannel.Kind.CONTROL);
            pipeline.replace(this, "streamInbound", streamingChannel);
            executorFactory().startThread(String.format("Stream-Deserializer-%s-%s", from, channel.id()),
                                          new StreamDeserializingTask(null, streamingChannel, current_version));

            logger.info("{} streaming connection established, version = {}, framing = {}, encryption = {}",
                        SocketFactory.channelId(from,
                                                (InetSocketAddress) channel.remoteAddress(),
                                                settings.bindAddress,
                                                (InetSocketAddress) channel.localAddress(),
                                                ConnectionType.STREAMING,
                                                channel.id().asShortText()),
                        current_version,
                        initiate.framing,
                        SocketFactory.encryptionConnectionSummary(pipeline.channel()));
        }

        @VisibleForTesting
        void setupMessagingPipeline(InetAddressAndPort from, int useMessagingVersion, int maxMessagingVersion, ChannelPipeline pipeline)
        {
            handshakeTimeout.cancel(true);
            // record the "true" endpoint, i.e. the one the peer is identified with, as opposed to the socket it connected over
            instance().versions.set(from, maxMessagingVersion);

            BufferPools.forNetworking().setRecycleWhenFreeForCurrentThread(false);
            BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
            if (initiate.type == ConnectionType.LARGE_MESSAGES)
            {
                // for large messages, swap the global pool allocator for a local one, to optimise utilisation of chunks
                allocator = new LocalBufferPoolAllocator(pipeline.channel().eventLoop());
                pipeline.channel().config().setAllocator(allocator);
            }

            FrameDecoder frameDecoder;
            switch (initiate.framing)
            {
                case LZ4:
                {
                    frameDecoder = FrameDecoderLZ4.fast(allocator);
                    break;
                }
                case CRC:
                {
                    frameDecoder = FrameDecoderCrc.create(allocator);
                    break;
                }
                case UNPROTECTED:
                {
                    frameDecoder = new FrameDecoderUnprotected(allocator);
                    break;
                }
                default:
                    throw new AssertionError();
            }

            frameDecoder.addLastTo(pipeline);

            InboundMessageHandler handler =
                settings.handlers.apply(from).createHandler(frameDecoder, initiate.type, pipeline.channel(), useMessagingVersion);

            logger.info("{} messaging connection established, version = {}, framing = {}, encryption = {}",
                        handler.id(true),
                        useMessagingVersion,
                        initiate.framing,
                        SocketFactory.encryptionConnectionSummary(pipeline.channel()));

            pipeline.addLast("deserialize", handler);

            try
            {
                pipeline.remove(this);
            }
            catch (NoSuchElementException ex)
            {
                // possible race with the handshake timeout firing and removing this handler already
            }
        }
    }

    private static SslHandler getSslHandler(String description, Channel channel, EncryptionOptions.ServerEncryptionOptions encryptionOptions) throws IOException
    {
        final boolean verifyPeerCertificate = true;
        SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions, verifyPeerCertificate,
                                                                 ISslContextFactory.SocketType.SERVER,
                                                                 SSL_FACTORY_CONTEXT_DESCRIPTION);
        InetSocketAddress peer = encryptionOptions.require_endpoint_verification ? (InetSocketAddress) channel.remoteAddress() : null;
        SslHandler sslHandler = newSslHandler(channel, sslContext, peer);
        logger.trace("{} inbound netty SslContext: context={}, engine={}", description, sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
        return sslHandler;
    }

    private static class OptionalSslHandler extends ByteToMessageDecoder
    {
        private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

        OptionalSslHandler(EncryptionOptions.ServerEncryptionOptions encryptionOptions)
        {
            this.encryptionOptions = encryptionOptions;
        }

        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
        {
            if (in.readableBytes() < 5)
            {
                // To detect if SSL must be used we need to have at least 5 bytes, so return here and try again
                // once more bytes a ready.
                return;
            }

            if (SslHandler.isEncrypted(in))
            {
                // Connection uses SSL/TLS, replace the detection handler with a SslHandler and so use encryption.
                SslHandler sslHandler = getSslHandler("replacing optional", ctx.channel(), encryptionOptions);
                ctx.pipeline().replace(this, SSL_HANDLER_NAME, sslHandler);
            }
            else
            {
                // Connection use no TLS/SSL encryption, just remove the detection handler and continue without
                // SslHandler in the pipeline.
                ctx.pipeline().remove(this);
            }
        }
    }

    private static class RejectSslHandler extends ByteToMessageDecoder
    {
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        {
            if (in.readableBytes() < 5)
            {
                // To detect if SSL must be used we need to have at least 5 bytes, so return here and try again
                // once more bytes a ready.
                return;
            }

            if (SslHandler.isEncrypted(in))
            {
                logger.info("Rejected incoming TLS connection before negotiating from {} to {}. TLS is explicitly disabled by configuration.",
                            ctx.channel().remoteAddress(), ctx.channel().localAddress());
                in.readBytes(in.readableBytes()); // discard the readable bytes so not called again
                ctx.close();
            }
            else
            {
                // Incoming connection did not attempt TLS/SSL encryption, just remove the detection handler and continue without
                // SslHandler in the pipeline.
                ctx.pipeline().remove(this);
            }
        }
    }
}
