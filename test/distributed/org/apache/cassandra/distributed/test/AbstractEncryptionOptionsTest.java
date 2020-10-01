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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public class AbstractEncryptionOptionsTest extends TestBaseImpl
{
    Logger logger = LoggerFactory.getLogger(EncryptionOptions.class);
    final static String validKeyStorePath = "test/conf/cassandra_ssl_test.keystore";
    final static String validKeyStorePassword = "cassandra";
    final static String validTrustStorePath = "test/conf/cassandra_ssl_test.truststore";
    final static String validTrustStorePassword = "cassandra";
    final static String validKeystoreYaml = "keystore: " + validKeyStorePath + '\n' +
                                            "keystore_password: " + validKeyStorePassword + '\n' +
                                            "truststore: " + validTrustStorePath + '\n' +
                                            "truststore_password: " + validTrustStorePassword + '\n';

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
        final EncryptionOptions encryptionOptions = new EncryptionOptions()
                                                    .withEnabled(true)
                                                    .withKeyStore(validKeyStorePath).withKeyStorePassword(validKeyStorePassword)
                                                    .withTrustStore(validTrustStorePath).withTrustStorePassword(validTrustStorePassword);
        private Throwable lastThrowable;

        public TlsConnection(String host, int port)
        {
            this.host = host;
            this.port = port;
        }

        public synchronized Throwable lastThrowable()
        {
            return lastThrowable;
        }
        private synchronized void setLastThrowable(Throwable cause)
        {
            lastThrowable = cause;
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
            result.set(ConnectResult.UNINITIALIZED);
            setLastThrowable(null);

            SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions, true,
                                                                     SSLFactory.SocketType.CLIENT);

            EventLoopGroup workerGroup = new NioEventLoopGroup();
            Bootstrap b = new Bootstrap();
            SimpleCondition attemptCompleted = new SimpleCondition();

            // Listener on the SSL handshake makes sure that the test completes immediately as
            // the server waits to receive a message over the TLS connection, so the discardHandler.decode
            // will likely never be called. The lambda has to handle it's own exceptions as it's a listener,
            // not in the request pipeline to pass them on to discardHandler.
            FutureListener<Channel> handshakeResult = channelFuture -> {
                try
                {
                    logger.debug("handshakeFuture() listener called");
                    channelFuture.get();
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

            result.set(ConnectResult.CONNECTING);
            ChannelFuture f = b.connect(host, port);
            try
            {
                f.sync();
                attemptCompleted.await(15, TimeUnit.SECONDS);
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
