package org.apache.cassandra.net.async;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLSession;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.HandshakeProtocol.FirstHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.SecondHandshakeMessage;
import org.apache.cassandra.net.async.HandshakeProtocol.ThirdHandshakeMessage;

/**
 * 'Server'-side component that negotiates the internode handshake when establishing a new connection.
 * This handler will be the first in the netty channel for each incoming connection (secure socket (TLS) notwithstanding),
 * and once the handshake is successful, it will configure the proper handlers (mostly {@link MessageInHandler})
 * and remove itself from the working pipeline.
 */
class InboundHandshakeHandler extends ByteToMessageDecoder
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    enum State { START, AWAITING_HANDSHAKE_BEGIN, AWAIT_STREAM_START_RESPONSE, AWAIT_MESSAGING_START_RESPONSE, MESSAGING_HANDSHAKE_COMPLETE, HANDSHAKE_FAIL }

    private State state;

    private final IInternodeAuthenticator authenticator;
    private boolean hasAuthenticated;

    /**
     * The peer's declared messaging version.
     */
    private int version;

    /**
     * Does the peer support (or want to use) compressed data?
     */
    private boolean compressed;

    /**
     * A future the essentially places a timeout on how long we'll wait for the peer
     * to complete the next step of the handshake.
     */
    private Future<?> handshakeTimeout;

    InboundHandshakeHandler(IInternodeAuthenticator authenticator)
    {
        this.authenticator = authenticator;
        state = State.START;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
    {
        try
        {
            if (!hasAuthenticated)
            {
                logSecureSocketDetails(ctx);
                if (!handleAuthenticate(ctx.channel().remoteAddress(), ctx))
                    return;
            }

            switch (state)
            {
                case START:
                    state = handleStart(ctx, in);
                    break;
                case AWAIT_MESSAGING_START_RESPONSE:
                    state = handleMessagingStartResponse(ctx, in);
                    break;
                case HANDSHAKE_FAIL:
                    throw new IllegalStateException("channel should be closed after determining the handshake failed with peer: " + ctx.channel().remoteAddress());
                default:
                    logger.error("unhandled state: " + state);
                    state = State.HANDSHAKE_FAIL;
                    ctx.close();
            }
        }
        catch (Exception e)
        {
            logger.error("unexpected error while negotiating internode messaging handshake", e);
            state = State.HANDSHAKE_FAIL;
            ctx.close();
        }
    }

    /**
     * Ensure the peer is allowed to connect to this node.
     */
    @VisibleForTesting
    boolean handleAuthenticate(SocketAddress socketAddress, ChannelHandlerContext ctx)
    {
        // the only reason addr would not be instanceof InetSocketAddress is in unit testing, when netty's EmbeddedChannel
        // uses EmbeddedSocketAddress. Normally, we'd do an instanceof for that class name, but it's marked with default visibility,
        // so we can't reference it outside of it's package (and so it doesn't compile).
        if (socketAddress instanceof InetSocketAddress)
        {
            InetSocketAddress addr = (InetSocketAddress)socketAddress;
            if (!authenticator.authenticate(addr.getAddress(), addr.getPort()))
            {
                if (logger.isTraceEnabled())
                    logger.trace("Failed to authenticate peer {}", addr);
                ctx.close();
                return false;
            }
        }
        else if (!socketAddress.getClass().getSimpleName().equals("EmbeddedSocketAddress"))
        {
            ctx.close();
            return false;
        }
        hasAuthenticated = true;
        return true;
    }

    /**
     * If the connection is using SSL/TLS, log some details about it.
     */
    private void logSecureSocketDetails(ChannelHandlerContext ctx)
    {
        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        if (sslHandler != null)
        {
            SSLSession session = sslHandler.engine().getSession();
            logger.info("connection from peer {}, protocol = {}, cipher suite = {}",
                        ctx.channel().remoteAddress(), session.getProtocol(), session.getCipherSuite());
        }
    }

    /**
     * Handles receiving the first message in the internode messaging handshake protocol. If the sender's protocol version
     * is accepted, we respond with the second message of the handshake protocol.
     */
    @VisibleForTesting
    State handleStart(ChannelHandlerContext ctx, ByteBuf in) throws IOException
    {
        FirstHandshakeMessage msg = FirstHandshakeMessage.maybeDecode(in);
        if (msg == null)
            return State.START;

        logger.trace("received first handshake message from peer {}, message = {}", ctx.channel().remoteAddress(), msg);
        version = msg.messagingVersion;

        if (msg.mode == NettyFactory.Mode.STREAMING)
        {
            // TODO fill in once streaming is moved to netty
            ctx.close();
            return State.AWAIT_STREAM_START_RESPONSE;
        }
        else
        {
            if (version < MessagingService.VERSION_30)
            {
                logger.error("Unable to read obsolete message version {} from {}; The earliest version supported is 3.0.0", version, ctx.channel().remoteAddress());
                ctx.close();
                return State.HANDSHAKE_FAIL;
            }

            logger.trace("Connection version {} from {}", version, ctx.channel().remoteAddress());
            compressed = msg.compressionEnabled;

            // if this version is < the MS version the other node is trying
            // to connect with, the other node will disconnect
            ctx.writeAndFlush(new SecondHandshakeMessage(MessagingService.current_version).encode(ctx.alloc()))
               .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);

            // outbound side will reconnect to change the version
            if (version > MessagingService.current_version)
            {
                logger.info("peer wants to use a messaging version higher ({}) than what this node supports ({})", version, MessagingService.current_version);
                ctx.close();
                return State.HANDSHAKE_FAIL;
            }

            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getRpcTimeout());
            handshakeTimeout = ctx.executor().schedule(() -> failHandshake(ctx), timeout, TimeUnit.MILLISECONDS);
            return State.AWAIT_MESSAGING_START_RESPONSE;
        }
    }

    /**
     * Handles the third (and last) message in the internode messaging handshake protocol. Grabs the protocol version and
     * IP addr the peer wants to use.
     */
    @VisibleForTesting
    State handleMessagingStartResponse(ChannelHandlerContext ctx, ByteBuf in) throws IOException
    {
        ThirdHandshakeMessage msg = ThirdHandshakeMessage.maybeDecode(in);
        if (msg == null)
            return State.AWAIT_MESSAGING_START_RESPONSE;

        logger.trace("received third handshake message from peer {}, message = {}", ctx.channel().remoteAddress(), msg);
        if (handshakeTimeout != null)
        {
            handshakeTimeout.cancel(false);
            handshakeTimeout = null;
        }

        int maxVersion = msg.messagingVersion;
        if (maxVersion > MessagingService.current_version)
        {
            logger.error("peer wants to use a messaging version higher ({}) than what this node supports ({})", maxVersion, MessagingService.current_version);
            ctx.close();
            return State.HANDSHAKE_FAIL;
        }

        // record the (true) version of the endpoint
        InetAddress from = msg.address;
        MessagingService.instance().setVersion(from, maxVersion);
        logger.trace("Set version for {} to {} (will use {})", from, maxVersion, MessagingService.instance().getVersion(from));

        setupMessagingPipeline(ctx.pipeline(), from, compressed, version);
        return State.MESSAGING_HANDSHAKE_COMPLETE;
    }

    @VisibleForTesting
    void setupMessagingPipeline(ChannelPipeline pipeline, InetAddress peer, boolean compressed, int messagingVersion)
    {
        if (compressed)
            pipeline.addLast(NettyFactory.INBOUND_COMPRESSOR_HANDLER_NAME, NettyFactory.createLz4Decoder(messagingVersion));

        pipeline.addLast("messageInHandler", new MessageInHandler(peer, messagingVersion));
        pipeline.remove(this);
    }

    @VisibleForTesting
    void failHandshake(ChannelHandlerContext ctx)
    {
        // we're not really racing on the handshakeTimeout as we're in the event loop,
        // but, hey, defensive programming is beautiful thing!
        if (state == State.MESSAGING_HANDSHAKE_COMPLETE || (handshakeTimeout != null && handshakeTimeout.isCancelled()))
            return;

        state = State.HANDSHAKE_FAIL;
        ctx.close();

        if (handshakeTimeout != null)
        {
            handshakeTimeout.cancel(false);
            handshakeTimeout = null;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        logger.trace("Failed to properly handshake with peer {}. Closing the channel.", ctx.channel().remoteAddress());
        failHandshake(ctx);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        logger.error("Failed to properly handshake with peer {}. Closing the channel.", ctx.channel().remoteAddress(), cause);
        failHandshake(ctx);
    }

    @VisibleForTesting
    public State getState()
    {
        return state;
    }

    @VisibleForTesting
    public void setState(State nextState)
    {
        state = nextState;
    }

    @VisibleForTesting
    void setHandshakeTimeout(Future<?> timeout)
    {
        handshakeTimeout = timeout;
    }
}
