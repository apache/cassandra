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

package org.apache.cassandra.distributed.test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.FutureListener;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.security.ISslContextFactory;
import org.apache.cassandra.security.SSLFactory;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.test.AbstractEncryptionOptionsImpl.ConnectResult.CONNECTING;
import static org.apache.cassandra.distributed.test.AbstractEncryptionOptionsImpl.ConnectResult.UNINITIALIZED;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

public class AbstractEncryptionOptionsImpl extends TestBaseImpl
{
    final Logger logger = LoggerFactory.getLogger(EncryptionOptions.class);
    final static String validKeyStorePath = "test/conf/cassandra_ssl_test.keystore";
    final static String validKeyStorePassword = "cassandra";
    final static String validTrustStorePath = "test/conf/cassandra_ssl_test.truststore";
    final static String validTrustStorePassword = "cassandra";

    // Base configuration map for a valid keystore that can be opened
    final static Map<String,Object> validKeystore = ImmutableMap.of("keystore", validKeyStorePath,
                                                                    "keystore_password", validKeyStorePassword,
                                                                    "truststore", validTrustStorePath,
                                                                    "truststore_password", validTrustStorePassword);

    // Configuration with a valid keystore, but an unknown protocol
    final static Map<String,Object> nonExistantProtocol = ImmutableMap.<String,Object>builder()
                                                                           .putAll(validKeystore)
                                                                           .put("accepted_protocols", Collections.singletonList("NoProtocolIKnow"))
                                                                           .build();
    // Configuration with a valid keystore, but an unknown cipher suite
    final static Map<String,Object> nonExistantCipher = ImmutableMap.<String,Object>builder()
                                                                           .putAll(validKeystore)
                                                                           .put("cipher_suites", Collections.singletonList("NoCipherIKnow"))
                                                                           .build();

    // Result of a TlsConnection.connect call.  The result is updated as the TLS connection
    // sequence takes place.  The nextOnFailure/nextOnSuccess allows the discard handler
    // to correctly update state if an unexpected exception is thrown.
    public enum ConnectResult {
        UNINITIALIZED,
        FAILED_TO_NEGOTIATE,
        NEVER_CONNECTED,
        NEGOTIATED,
        CONNECTED_AND_ABOUT_TO_NEGOTIATE(FAILED_TO_NEGOTIATE, NEGOTIATED),
        CONNECTING(NEVER_CONNECTED, CONNECTED_AND_ABOUT_TO_NEGOTIATE);

        public final ConnectResult nextOnFailure;
        public final ConnectResult nextOnSuccess;

        ConnectResult()
        {
            nextOnFailure = null;
            nextOnSuccess = null;
        }
        ConnectResult(ConnectResult nextOnFailure, ConnectResult nextOnSuccess)
        {
            this.nextOnFailure = nextOnFailure;
            this.nextOnSuccess = nextOnSuccess;
        }
    }

    public class TlsConnection
    {
        final String host;
        final int port;
        final List<String> acceptedProtocols;
        final List<String> cipherSuites;
        final EncryptionOptions encryptionOptions = new EncryptionOptions()
                                                    .withEnabled(true)
                                                    .withKeyStore(validKeyStorePath).withKeyStorePassword(validKeyStorePassword)
                                                    .withTrustStore(validTrustStorePath).withTrustStorePassword(validTrustStorePassword);
        private Throwable lastThrowable;
        private String lastProtocol;
        private String lastCipher;

        public TlsConnection(String host, int port)
        {
            this(host, port, null, null);
        }

        public TlsConnection(String host, int port, List<String> acceptedProtocols)
        {
            this(host, port, acceptedProtocols, null);
        }

        public TlsConnection(String host, int port, List<String> acceptedProtocols, List<String> cipherSuites)
        {
            this.host = host;
            this.port = port;
            this.acceptedProtocols = acceptedProtocols;
            this.cipherSuites = cipherSuites;
        }

        public synchronized Throwable lastThrowable()
        {
            return lastThrowable;
        }
        private synchronized void setLastThrowable(Throwable cause)
        {
            lastThrowable = cause;
        }

        public synchronized String lastProtocol()
        {
            return lastProtocol;
        }
        public synchronized String lastCipher()
        {
            return lastCipher;
        }
        private synchronized void setProtocolAndCipher(String protocol, String cipher)
        {
            lastProtocol = protocol;
            lastCipher = cipher;
        }

        final AtomicReference<ConnectResult> result = new AtomicReference<>(ConnectResult.UNINITIALIZED);

        void setResult(String why, ConnectResult expected, ConnectResult newResult)
        {
            if (newResult == null)
                return;
            logger.debug("Setting progress from {} to {}", expected, expected.nextOnSuccess);
            result.getAndUpdate(v -> {
                if (v == expected)
                    return newResult;
                else
                    throw new IllegalStateException(
                        String.format("CAS attempt on %s failed from %s to %s but %s did not match expected value",
                                      why, expected, newResult, v));
            });
        }
        void successProgress()
        {
            ConnectResult current = result.get();
            setResult("success", current, current.nextOnSuccess);
        }
        void failure()
        {
            ConnectResult current = result.get();
            setResult("failure", current, current.nextOnFailure);
        }

        ConnectResult connect() throws Throwable
        {
            AtomicInteger connectAttempts = new AtomicInteger(0);
            result.set(UNINITIALIZED);
            setLastThrowable(null);
            setProtocolAndCipher(null, null);

            SslContext sslContext = SSLFactory.getOrCreateSslContext(
                encryptionOptions.withAcceptedProtocols(acceptedProtocols).withCipherSuites(cipherSuites),
                true, ISslContextFactory.SocketType.CLIENT, "test");

            EventLoopGroup workerGroup = new NioEventLoopGroup();
            Bootstrap b = new Bootstrap();
            Condition attemptCompleted = newOneTimeCondition();

            // Listener on the SSL handshake makes sure that the test completes immediately as
            // the server waits to receive a message over the TLS connection, so the discardHandler.decode
            // will likely never be called. The lambda has to handle it's own exceptions as it's a listener,
            // not in the request pipeline to pass them on to discardHandler.
            FutureListener<Channel> handshakeResult = channelFuture -> {
                try
                {
                    logger.debug("handshakeFuture() listener called");
                    Channel channel = channelFuture.get();
                    SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
                    SSLSession session = sslHandler.engine().getSession();
                    setProtocolAndCipher(session.getProtocol(), session.getCipherSuite());

                    successProgress();
                }
                catch (Throwable cause)
                {
                    logger.info("handshakeFuture() threw", cause);
                    failure();
                    setLastThrowable(cause);
                }
                attemptCompleted.signalAll();
            };

            ChannelHandler connectHandler = new ByteToMessageDecoder()
            {
                @Override
                public void channelActive(ChannelHandlerContext ctx) throws Exception
                {
                    logger.debug("connectHandler.channelActive");
                    int count = connectAttempts.incrementAndGet();
                    if (count > 1)
                    {
                        logger.info("connectHandler.channelActive called more than once - {}", count);
                    }
                    successProgress();

                    // Add the handler after the connection is established to make sure the connection
                    // progress is recorded
                    final SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
                    sslHandler.handshakeFuture().addListener(handshakeResult);

                    super.channelActive(ctx);
                }

                @Override
                public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                {
                    logger.debug("connectHandler.decode - readable bytes {}", in.readableBytes());

                    ctx.pipeline().remove(this);
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                {
                    logger.debug("connectHandler.exceptionCaught", cause);
                    setLastThrowable(cause);
                    failure();
                    attemptCompleted.signalAll();
                }
            };
            ChannelHandler discardHandler = new ByteToMessageDecoder()
            {
                @Override
                public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                {
                    logger.info("discardHandler.decode - {} readable bytes made it past SSL negotiation, discarding.",
                                in.readableBytes());
                    in.readBytes(in.readableBytes());
                    attemptCompleted.signalAll();
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                {
                    logger.debug("discardHandler.exceptionCaught", cause);
                    setLastThrowable(cause);
                    failure();
                    attemptCompleted.signalAll();
                }
            };

            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.handler(new ChannelInitializer<Channel>()
            {
                @Override
                protected void initChannel(Channel channel)
                {
                    SslHandler sslHandler = sslContext.newHandler(channel.alloc());
                    channel.pipeline().addFirst(connectHandler, sslHandler, discardHandler);
                }
            });

            result.set(CONNECTING);
            ChannelFuture f = b.connect(host, port);
            try
            {
                f.sync();
                attemptCompleted.await(15, SECONDS);
            }
            finally
            {
                f.channel().close();
            }
            return result.get();
        }

        void assertCannotConnect() throws Throwable
        {
            try
            {
                connect();
            }
            catch (java.net.ConnectException ex)
            {
                // verify it was not possible to connect before starting the server
            }
        }

        void assertReceivedHandshakeException()
        {
            Assert.assertTrue("Expected a J8 handshake_failure or J11 protocol_version exception: " + lastThrowable.getMessage(),
                              lastThrowable().getMessage().contains("Received fatal alert: handshake_failure") ||
                              lastThrowable().getMessage().contains("Received fatal alert: protocol_version") ||
                              lastThrowable.getCause() instanceof  SSLHandshakeException);
        }
    }

    /* Provde the cluster cannot start with the configured options */
    void assertCannotStartDueToConfigurationException(Cluster cluster)
    {
        Throwable tr = null;
        try
        {
            cluster.startup();
        }
        catch (Throwable maybeConfigException)
        {
            tr = maybeConfigException;
        }

        if (tr == null)
        {
            Assert.fail("Expected a ConfigurationException");
        }
        else
        {
            Assert.assertEquals(ConfigurationException.class.getName(), tr.getClass().getName());
        }
    }
}
