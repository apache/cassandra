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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.spi.SelectorProvider;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslClosedEngineException;
import io.netty.util.concurrent.DefaultEventExecutorChooserFactory;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;

import static io.netty.channel.unix.Errors.ERRNO_ECONNRESET_NEGATIVE;
import static io.netty.channel.unix.Errors.ERROR_ECONNREFUSED_NEGATIVE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_EVENT_THREADS;
import static org.apache.cassandra.utils.Throwables.isCausedBy;

/**
 * A factory for building Netty {@link Channel}s. Channels here are setup with a pipeline to participate
 * in the internode protocol handshake, either the inbound or outbound side as per the method invoked.
 */
public final class SocketFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SocketFactory.class);

    private static final int EVENT_THREADS = INTERNODE_EVENT_THREADS.getInt(FBUtilities.getAvailableProcessors());

    /**
     * The default task queue used by {@code NioEventLoop} and {@code EpollEventLoop} is {@code MpscUnboundedArrayQueue},
     * provided by JCTools. While efficient, it has an undesirable quality for a queue backing an event loop: it is
     * not non-blocking, and can cause the event loop to busy-spin while waiting for a partially completed task
     * offer, if the producer thread has been suspended mid-offer.
     *
     * As it happens, however, we have an MPSC queue implementation that is perfectly fit for this purpose -
     * {@link ManyToOneConcurrentLinkedQueue}, that is non-blocking, and already used throughout the codebase,
     * that we can and do use here as well.
     */
    enum Provider
    {
        NIO
        {
            @Override
            NioEventLoopGroup makeEventLoopGroup(int threadCount, ThreadFactory threadFactory)
            {
                return new NioEventLoopGroup(threadCount,
                                             new ThreadPerTaskExecutor(threadFactory),
                                             DefaultEventExecutorChooserFactory.INSTANCE,
                                             SelectorProvider.provider(),
                                             DefaultSelectStrategyFactory.INSTANCE,
                                             RejectedExecutionHandlers.reject(),
                                             capacity -> new ManyToOneConcurrentLinkedQueue<>());
            }

            @Override
            ChannelFactory<NioSocketChannel> clientChannelFactory()
            {
                return NioSocketChannel::new;
            }

            @Override
            ChannelFactory<NioServerSocketChannel> serverChannelFactory()
            {
                return NioServerSocketChannel::new;
            }
        },
        EPOLL
        {
            @Override
            EpollEventLoopGroup makeEventLoopGroup(int threadCount, ThreadFactory threadFactory)
            {
                return new EpollEventLoopGroup(threadCount,
                                               new ThreadPerTaskExecutor(threadFactory),
                                               DefaultEventExecutorChooserFactory.INSTANCE,
                                               DefaultSelectStrategyFactory.INSTANCE,
                                               RejectedExecutionHandlers.reject(),
                                               capacity -> new ManyToOneConcurrentLinkedQueue<>());
            }

            @Override
            ChannelFactory<EpollSocketChannel> clientChannelFactory()
            {
                return EpollSocketChannel::new;
            }

            @Override
            ChannelFactory<EpollServerSocketChannel> serverChannelFactory()
            {
                return EpollServerSocketChannel::new;
            }
        };

        EventLoopGroup makeEventLoopGroup(int threadCount, String threadNamePrefix)
        {
            logger.debug("using netty {} event loop for pool prefix {}", name(), threadNamePrefix);
            return makeEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix, true));
        }

        abstract EventLoopGroup makeEventLoopGroup(int threadCount, ThreadFactory threadFactory);
        abstract ChannelFactory<? extends Channel> clientChannelFactory();
        abstract ChannelFactory<? extends ServerChannel> serverChannelFactory();

        static Provider optimalProvider()
        {
            return NativeTransportService.useEpoll() ? EPOLL : NIO;
        }
    }

    /** a useful addition for debugging; simply set to true to get more data in your logs */
    static final boolean WIRETRACE = false;
    static
    {
        if (WIRETRACE)
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    private final Provider provider;
    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup defaultGroup;
    // we need a separate EventLoopGroup for outbound streaming because sendFile is blocking
    private final EventLoopGroup outboundStreamingGroup;
    final ExecutorService synchronousWorkExecutor = executorFactory().pooled("Messaging-SynchronousWork", Integer.MAX_VALUE);

    SocketFactory()
    {
        this(Provider.optimalProvider());
    }

    SocketFactory(Provider provider)
    {
        this.provider = provider;
        this.acceptGroup = provider.makeEventLoopGroup(1, "Messaging-AcceptLoop");
        this.defaultGroup = provider.makeEventLoopGroup(EVENT_THREADS, NamedThreadFactory.globalPrefix() + "Messaging-EventLoop");
        this.outboundStreamingGroup = provider.makeEventLoopGroup(EVENT_THREADS, "Streaming-EventLoop");
    }

    Bootstrap newClientBootstrap(EventLoop eventLoop, int tcpUserTimeoutInMS)
    {
        if (eventLoop == null)
            throw new IllegalArgumentException("must provide eventLoop");

        Bootstrap bootstrap = new Bootstrap().group(eventLoop).channelFactory(provider.clientChannelFactory());

        if (provider == Provider.EPOLL)
            bootstrap.option(EpollChannelOption.TCP_USER_TIMEOUT, tcpUserTimeoutInMS);

        return bootstrap;
    }

    ServerBootstrap newServerBootstrap()
    {
        return new ServerBootstrap().group(acceptGroup, defaultGroup).channelFactory(provider.serverChannelFactory());
    }

    /**
     * Creates a new {@link SslHandler} from provided SslContext.
     * @param peer enables endpoint verification for remote address when not null
     */
    public static SslHandler newSslHandler(Channel channel, SslContext sslContext, @Nullable InetSocketAddress peer)
    {
        if (peer == null)
            return sslContext.newHandler(channel.alloc());

        logger.debug("Creating SSL handler for {}:{}", peer.getHostString(), peer.getPort());
        SslHandler sslHandler = sslContext.newHandler(channel.alloc(), peer.getHostString(), peer.getPort());
        SSLEngine engine = sslHandler.engine();
        SSLParameters sslParameters = engine.getSSLParameters();
        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(sslParameters);
        return sslHandler;
    }

    /**
     * Summarizes the intended encryption options, suitable for logging. Once a connection is established, use
     * {@link SocketFactory#encryptionConnectionSummary} below.
     * @param options options to summarize
     * @return description of encryption options
     */
    static String encryptionOptionsSummary(EncryptionOptions options)
    {
        if (options == null || options.tlsEncryptionPolicy() == EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED)
            return EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED.description();

        String encryptionType = SSLFactory.openSslIsAvailable() ? "openssl" : "jdk";
        return options.tlsEncryptionPolicy().description() + '(' + encryptionType + ')';
    }

    /**
     * Summarizes the encryption status of a channel, suitable for logging.
     * @return description of channel encryption
     */
    static String encryptionConnectionSummary(Channel channel)
    {
        final SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
        if (sslHandler == null)
        {
            return EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED.description();
        }
        SSLSession session = sslHandler.engine().getSession();

        return  "encrypted(factory=" +
                (SSLFactory.openSslIsAvailable() ? "openssl" : "jdk") +
                ";protocol=" +
                (session != null ? session.getProtocol() : "MISSING SESSION") +
                ";cipher=" +
                (session != null ? session.getCipherSuite() : "MISSING SESSION") +
                ')';
    }

    EventLoopGroup defaultGroup()
    {
        return defaultGroup;
    }

    public EventLoopGroup outboundStreamingGroup()
    {
        return outboundStreamingGroup;
    }

    public void shutdownNow()
    {
        acceptGroup.shutdownGracefully(0, 2, SECONDS);
        defaultGroup.shutdownGracefully(0, 2, SECONDS);
        outboundStreamingGroup.shutdownGracefully(0, 2, SECONDS);
        synchronousWorkExecutor.shutdownNow();
    }

    void awaitTerminationUntil(long deadlineNanos) throws InterruptedException, TimeoutException
    {
        List<ExecutorService> groups = ImmutableList.of(acceptGroup, defaultGroup, outboundStreamingGroup, synchronousWorkExecutor);
        ExecutorUtils.awaitTerminationUntil(deadlineNanos, groups);
    }

    static boolean isConnectionReset(Throwable t)
    {
        if (t instanceof ClosedChannelException)
            return true;
        if (t instanceof SslClosedEngineException)
            return true;
        if (t instanceof ConnectException)
            return true;
        if (t instanceof Errors.NativeIoException)
        {
            int errorCode = ((Errors.NativeIoException) t).expectedErr();
            return errorCode == ERRNO_ECONNRESET_NEGATIVE || errorCode != ERROR_ECONNREFUSED_NEGATIVE;
        }
        return IOException.class == t.getClass() && ("Broken pipe".equals(t.getMessage()) || "Connection reset by peer".equals(t.getMessage()));
    }

    static boolean isCausedByConnectionReset(Throwable t)
    {
        return isCausedBy(t, SocketFactory::isConnectionReset);
    }

    static String channelId(InetAddressAndPort from, InetSocketAddress realFrom, InetAddressAndPort to, InetSocketAddress realTo, ConnectionType type, String id)
    {
        return addressId(from, realFrom) + "->" + addressId(to, realTo) + '-' + type + '-' + id;
    }

    static String addressId(InetAddressAndPort address, InetSocketAddress realAddress)
    {
        String str = address.toString();
        if (!address.getAddress().equals(realAddress.getAddress()) || address.getPort() != realAddress.getPort())
            str += '(' + InetAddressAndPort.toString(realAddress.getAddress(), realAddress.getPort()) + ')';
        return str;
    }

    static String channelId(InetAddressAndPort from, InetAddressAndPort to, ConnectionType type, String id)
    {
        return from + "->" + to + '-' + type + '-' + id;
    }
}
