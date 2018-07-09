package org.apache.cassandra.net.async;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.zip.Checksum;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A factory for building Netty {@link Channel}s. Channels here are setup with a pipeline to participate
 * in the internode protocol handshake, either the inbound or outbound side as per the method invoked.
 */
public final class NettyFactory
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    /**
     * The block size for use with netty's lz4 code.
     */
    private static final int COMPRESSION_BLOCK_SIZE = 1 << 16;

    private static final int LZ4_HASH_SEED = 0x9747b28c;

    public enum Mode { MESSAGING, STREAMING }

    static final String SSL_CHANNEL_HANDLER_NAME = "ssl";
    private static final String OPTIONAL_SSL_CHANNEL_HANDLER_NAME = "optionalSsl";
    static final String INBOUND_COMPRESSOR_HANDLER_NAME = "inboundCompressor";
    static final String OUTBOUND_COMPRESSOR_HANDLER_NAME = "outboundCompressor";
    private static final String HANDSHAKE_HANDLER_NAME = "handshakeHandler";
    public static final String INBOUND_STREAM_HANDLER_NAME = "inboundStreamHandler";

    /** a useful addition for debugging; simply set to true to get more data in your logs */
    private static final boolean WIRETRACE = false;
    static
    {
        if (WIRETRACE)
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    private static final boolean DEFAULT_USE_EPOLL = NativeTransportService.useEpoll();

    /**
     * The size of the receive queue for the outbound channels. As outbound channels do not receive data
     * (outside of the internode messaging protocol's handshake), this value can be relatively small.
     */
    private static final int OUTBOUND_CHANNEL_RECEIVE_BUFFER_SIZE = 1 << 10;

    /**
     * The size of the send queue for the inbound channels. As inbound channels do not send data
     * (outside of the internode messaging protocol's handshake), this value can be relatively small.
     */
    private static final int INBOUND_CHANNEL_SEND_BUFFER_SIZE = 1 << 10;

    /**
     * A factory instance that all normal, runtime code should use. Separate instances should only be used for testing.
     */
    public static final NettyFactory instance = new NettyFactory(DEFAULT_USE_EPOLL);

    private final boolean useEpoll;
    private final EventLoopGroup acceptGroup;

    private final EventLoopGroup inboundGroup;
    private final EventLoopGroup outboundGroup;
    public final EventLoopGroup streamingGroup;

    /**
     * Constructor that allows modifying the {@link NettyFactory#useEpoll} for testing purposes. Otherwise, use the
     * default {@link #instance}.
     */
    @VisibleForTesting
    NettyFactory(boolean useEpoll)
    {
        this.useEpoll = useEpoll;
        acceptGroup = getEventLoopGroup(useEpoll, determineAcceptGroupSize(DatabaseDescriptor.getInternodeMessagingEncyptionOptions()),
                                        "MessagingService-NettyAcceptor-Thread", false);
        inboundGroup = getEventLoopGroup(useEpoll, FBUtilities.getAvailableProcessors(), "MessagingService-NettyInbound-Thread", false);
        outboundGroup = getEventLoopGroup(useEpoll, FBUtilities.getAvailableProcessors(), "MessagingService-NettyOutbound-Thread", true);
        streamingGroup = getEventLoopGroup(useEpoll, FBUtilities.getAvailableProcessors(), "Streaming-Netty-Thread", false);
    }

    /**
     * Determine the number of accept threads we need, which is based upon the number of listening sockets we will have.
     * The idea is one accept thread per listening socket.
     */
    public static int determineAcceptGroupSize(ServerEncryptionOptions serverEncryptionOptions)
    {
        int listenSocketCount = 1;

        boolean listenOnBroadcastAddr = MessagingService.shouldListenOnBroadcastAddress();
        if (listenOnBroadcastAddr)
            listenSocketCount++;

        if (serverEncryptionOptions.enable_legacy_ssl_storage_port)
        {
            listenSocketCount++;

            if (listenOnBroadcastAddr)
                listenSocketCount++;
        }

        return listenSocketCount;
    }

    /**
     * Create an {@link EventLoopGroup}, for epoll or nio. The {@code boostIoRatio} flag passes a hint to the netty
     * event loop threads to optimize comsuming all the tasks from the netty channel before checking for IO activity.
     * By default, netty will process some maximum number of tasks off it's queue before it will check for activity on
     * any of the open FDs, which basically amounts to checking for any incoming data. If you have a class of event loops
     * that that do almost *no* inbound activity (like cassandra's outbound channels), then it behooves us to have the
     * outbound netty channel consume as many tasks as it can before making the system calls to check up on the FDs,
     * as we're not expecting any incoming data on those sockets, anyways. Thus, we pass the magic value {@code 100}
     * to achieve the maximum consuption from the netty queue. (for implementation details, as of netty 4.1.8,
     * see {@link io.netty.channel.epoll.EpollEventLoop#run()}.
     */
    static EventLoopGroup getEventLoopGroup(boolean useEpoll, int threadCount, String threadNamePrefix, boolean boostIoRatio)
    {
        if (useEpoll)
        {
            logger.debug("using netty epoll event loop for pool prefix {}", threadNamePrefix);
            EpollEventLoopGroup eventLoopGroup = new EpollEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix, true));
            if (boostIoRatio)
                eventLoopGroup.setIoRatio(100);
            return eventLoopGroup;
        }

        logger.debug("using netty nio event loop for pool prefix {}", threadNamePrefix);
        NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix, true));
        if (boostIoRatio)
            eventLoopGroup.setIoRatio(100);
        return eventLoopGroup;
    }

    /**
     * Create a {@link Channel} that listens on the {@code localAddr}. This method will block while trying to bind to the address,
     * but it does not make a remote call.
     */
    public Channel createInboundChannel(InetAddressAndPort localAddr, InboundInitializer initializer, int receiveBufferSize) throws ConfigurationException
    {
        String nic = FBUtilities.getNetworkInterface(localAddr.address);
        logger.info("Starting Messaging Service on {} {}, encryption: {}",
                    localAddr, nic == null ? "" : String.format(" (%s)", nic), encryptionLogStatement(initializer.encryptionOptions));
        Class<? extends ServerChannel> transport = useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
        ServerBootstrap bootstrap = new ServerBootstrap().group(acceptGroup, inboundGroup)
                                                         .channel(transport)
                                                         .option(ChannelOption.SO_BACKLOG, 500)
                                                         .childOption(ChannelOption.SO_KEEPALIVE, true)
                                                         .childOption(ChannelOption.TCP_NODELAY, true)
                                                         .childOption(ChannelOption.SO_REUSEADDR, true)
                                                         .childOption(ChannelOption.SO_SNDBUF, INBOUND_CHANNEL_SEND_BUFFER_SIZE)
                                                         .childHandler(initializer);

        if (receiveBufferSize > 0)
            bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveBufferSize);

        ChannelFuture channelFuture = bootstrap.bind(new InetSocketAddress(localAddr.address, localAddr.port));

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
                throw new ConfigurationException(localAddr + " is in use by another process.  Change listen_address:storage_port " +
                                                 "in cassandra.yaml to values that do not conflict with other services");
            }
            // looking at the jdk source, solaris/windows bind failue messages both use the phrase "cannot assign requested address".
            // windows message uses "Cannot" (with a capital 'C'), and solaris (a/k/a *nux) doe not. hence we search for "annot" <sigh>
            else if (causeString.contains("annot assign requested address"))
            {
                throw new ConfigurationException("Unable to bind to address " + localAddr
                                                 + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            }
            else
            {
                throw new ConfigurationException("failed to bind to: " + localAddr, failedChannelCause);
            }
        }

        return channelFuture.channel();
    }

    /**
     * Creates a new {@link SslHandler} from provided SslContext.
     * @param peer enables endpoint verification for remote address when not null
     */
    static SslHandler newSslHandler(Channel channel, SslContext sslContext, @Nullable InetSocketAddress peer)
    {
        if (peer == null)
        {
            return sslContext.newHandler(channel.alloc());
        }
        else
        {
            logger.debug("Creating SSL handler for {}:{}", peer.getHostString(), peer.getPort());
            SslHandler sslHandler = sslContext.newHandler(channel.alloc(), peer.getHostString(), peer.getPort());
            SSLEngine engine = sslHandler.engine();
            SSLParameters sslParameters = engine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(sslParameters);
            return sslHandler;
        }
    }

    public static class InboundInitializer extends ChannelInitializer<SocketChannel>
    {
        private final IInternodeAuthenticator authenticator;
        private final ServerEncryptionOptions encryptionOptions;
        private final ChannelGroup channelGroup;

        public InboundInitializer(IInternodeAuthenticator authenticator, ServerEncryptionOptions encryptionOptions, ChannelGroup channelGroup)
        {
            this.authenticator = authenticator;
            this.encryptionOptions = encryptionOptions;
            this.channelGroup = channelGroup;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception
        {
            channelGroup.add(channel);
            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> logger -> handshakeHandler
            if (encryptionOptions.enabled)
            {
                if (encryptionOptions.optional)
                {
                    pipeline.addFirst(OPTIONAL_SSL_CHANNEL_HANDLER_NAME, new OptionalSslHandler(encryptionOptions));
                }
                else
                {
                    SslContext sslContext = SSLFactory.getSslContext(encryptionOptions, true, SSLFactory.ConnectionType.INTERNODE_MESSAGING, SSLFactory.SocketType.SERVER);
                    InetSocketAddress peer = encryptionOptions.require_endpoint_verification ? channel.remoteAddress() : null;
                    SslHandler sslHandler = newSslHandler(channel, sslContext, peer);
                    logger.trace("creating inbound netty SslContext: context={}, engine={}", sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
                    pipeline.addFirst(SSL_CHANNEL_HANDLER_NAME, sslHandler);
                }
            }

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            channel.pipeline().addLast(HANDSHAKE_HANDLER_NAME, new InboundHandshakeHandler(authenticator));
        }
    }

    private static String encryptionLogStatement(ServerEncryptionOptions options)
    {
        if (options == null)
            return "disabled";

        String encryptionType = OpenSsl.isAvailable() ? "openssl" : "jdk";
        return "enabled (" + encryptionType + ')';
    }

    /**
     * Create the {@link Bootstrap} for connecting to a remote peer. This method does <b>not</b> attempt to connect to the peer,
     * and thus does not block.
     */
    @VisibleForTesting
    public Bootstrap createOutboundBootstrap(OutboundConnectionParams params)
    {
        logger.debug("creating outbound bootstrap to peer {}, compression: {}, encryption: {}, coalesce: {}, protocolVersion: {}",
                     params.connectionId.connectionAddress(),
                     params.compress, encryptionLogStatement(params.encryptionOptions),
                     params.coalescingStrategy.isPresent() ? params.coalescingStrategy.get() : CoalescingStrategies.Strategy.DISABLED,
                     params.protocolVersion);
        Class<? extends Channel> transport = useEpoll ? EpollSocketChannel.class : NioSocketChannel.class;
        Bootstrap bootstrap = new Bootstrap().group(params.mode == Mode.MESSAGING ? outboundGroup : streamingGroup)
                              .channel(transport)
                              .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000)
                              .option(ChannelOption.SO_KEEPALIVE, true)
                              .option(ChannelOption.SO_REUSEADDR, true)
                              .option(ChannelOption.SO_SNDBUF, params.sendBufferSize)
                              .option(ChannelOption.SO_RCVBUF, OUTBOUND_CHANNEL_RECEIVE_BUFFER_SIZE)
                              .option(ChannelOption.TCP_NODELAY, params.tcpNoDelay)
                              .option(ChannelOption.WRITE_BUFFER_WATER_MARK, params.waterMark)
                              .handler(new OutboundInitializer(params));
        InetAddressAndPort remoteAddress = params.connectionId.connectionAddress();
        bootstrap.remoteAddress(new InetSocketAddress(remoteAddress.address, remoteAddress.port));
        return bootstrap;
    }

    public static class OutboundInitializer extends ChannelInitializer<SocketChannel>
    {
        private final OutboundConnectionParams params;

        OutboundInitializer(OutboundConnectionParams params)
        {
            this.params = params;
        }

        /**
         * {@inheritDoc}
         *
         * To determine if we should enable TLS, we only need to check if {@link #params#encryptionOptions} is set.
         * The logic for figuring that out is is located in {@link MessagingService#getMessagingConnection(InetAddress)};
         */
        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> logger -> handshakeHandler
            if (params.encryptionOptions != null)
            {
                SslContext sslContext = SSLFactory.getSslContext(params.encryptionOptions, true, SSLFactory.ConnectionType.INTERNODE_MESSAGING, SSLFactory.SocketType.CLIENT);
                // for some reason channel.remoteAddress() will return null
                InetAddressAndPort address = params.connectionId.remote();
                InetSocketAddress peer = params.encryptionOptions.require_endpoint_verification ? new InetSocketAddress(address.address, address.port) : null;
                SslHandler sslHandler = newSslHandler(channel, sslContext, peer);
                logger.trace("creating outbound netty SslContext: context={}, engine={}", sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
                pipeline.addFirst(SSL_CHANNEL_HANDLER_NAME, sslHandler);
            }

            if (NettyFactory.WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            pipeline.addLast(HANDSHAKE_HANDLER_NAME, new OutboundHandshakeHandler(params));
        }
    }

    public void close()
    {
        acceptGroup.shutdownGracefully();
        outboundGroup.shutdownGracefully();
        inboundGroup.shutdownGracefully();
        streamingGroup.shutdownGracefully();
    }

    static Lz4FrameEncoder createLz4Encoder(int protocolVersion)
    {
        return new Lz4FrameEncoder(LZ4Factory.fastestInstance(), false, COMPRESSION_BLOCK_SIZE, checksumForFrameEncoders(protocolVersion));
    }

    private static Checksum checksumForFrameEncoders(int protocolVersion)
    {
        if (protocolVersion >= MessagingService.current_version)
            return ChecksumType.CRC32.newInstance();
        return XXHashFactory.fastestInstance().newStreamingHash32(LZ4_HASH_SEED).asChecksum();
    }

    static Lz4FrameDecoder createLz4Decoder(int protocolVersion)
    {
        return new Lz4FrameDecoder(LZ4Factory.fastestInstance(), checksumForFrameEncoders(protocolVersion));
    }

    public static EventExecutor executorForChannelGroups()
    {
        return new DefaultEventExecutor();
    }
}
