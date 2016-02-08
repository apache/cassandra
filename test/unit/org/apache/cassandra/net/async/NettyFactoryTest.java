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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Optional;

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.cassandra.auth.AllowAllInternodeAuthenticator;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.NettyFactory.InboundInitializer;
import org.apache.cassandra.net.async.NettyFactory.OutboundInitializer;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NativeLibrary;

public class NettyFactoryTest
{
    private static final InetSocketAddress LOCAL_ADDR = new InetSocketAddress("127.0.0.1", 9876);
    private static final InetSocketAddress REMOTE_ADDR = new InetSocketAddress("127.0.0.2", 9876);
    private static final int receiveBufferSize = 1 << 16;
    private static final IInternodeAuthenticator AUTHENTICATOR = new AllowAllInternodeAuthenticator();

    private ChannelGroup channelGroup;
    private NettyFactory factory;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp()
    {
        channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    }

    @After
    public void tearDown()
    {
        if (factory != null)
            factory.close();
    }

    @Test
    public void createServerChannel_Epoll()
    {
        Channel inboundChannel = createServerChannel(true);
        if (inboundChannel == null)
            return;
        Assert.assertEquals(EpollServerSocketChannel.class, inboundChannel.getClass());
        inboundChannel.close();
    }

    private Channel createServerChannel(boolean useEpoll)
    {
        InboundInitializer inboundInitializer = new InboundInitializer(AUTHENTICATOR, null, channelGroup);
        factory = new NettyFactory(useEpoll);

        try
        {
            return factory.createInboundChannel(LOCAL_ADDR, inboundInitializer, receiveBufferSize);
        }
        catch (Exception e)
        {
            if (NativeLibrary.osType == NativeLibrary.OSType.LINUX)
                throw e;

            return null;
        }
    }

    @Test
    public void createServerChannel_Nio()
    {
        Channel inboundChannel = createServerChannel(false);
        Assert.assertNotNull("we should always be able to get a NIO channel", inboundChannel);
        Assert.assertEquals(NioServerSocketChannel.class, inboundChannel.getClass());
        inboundChannel.close();
    }

    @Test(expected = ConfigurationException.class)
    public void createServerChannel_SecondAttemptToBind()
    {
        Channel inboundChannel = null;
        try
        {
            InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 9876);
            InboundInitializer inboundInitializer = new InboundInitializer(AUTHENTICATOR, null, channelGroup);
            inboundChannel = NettyFactory.instance.createInboundChannel(LOCAL_ADDR, inboundInitializer, receiveBufferSize);
            NettyFactory.instance.createInboundChannel(LOCAL_ADDR, inboundInitializer, receiveBufferSize);
        }
        finally
        {
            if (inboundChannel != null)
                inboundChannel.close();
        }
    }

    @Test(expected = ConfigurationException.class)
    public void createServerChannel_UnbindableAddress()
    {
        InetSocketAddress addr = new InetSocketAddress("1.1.1.1", 9876);
        InboundInitializer inboundInitializer = new InboundInitializer(AUTHENTICATOR, null, channelGroup);
        NettyFactory.instance.createInboundChannel(addr, inboundInitializer, receiveBufferSize);
    }

    @Test
    public void deterineAcceptGroupSize()
    {
        Assert.assertEquals(1, NettyFactory.determineAcceptGroupSize(InternodeEncryption.none));
        Assert.assertEquals(1, NettyFactory.determineAcceptGroupSize(InternodeEncryption.all));
        Assert.assertEquals(2, NettyFactory.determineAcceptGroupSize(InternodeEncryption.rack));
        Assert.assertEquals(2, NettyFactory.determineAcceptGroupSize(InternodeEncryption.dc));

        InetAddress originalBroadcastAddr = FBUtilities.getBroadcastAddress();
        try
        {
            FBUtilities.setBroadcastInetAddress(InetAddresses.increment(FBUtilities.getLocalAddress()));
            DatabaseDescriptor.setListenOnBroadcastAddress(true);

            Assert.assertEquals(2, NettyFactory.determineAcceptGroupSize(InternodeEncryption.none));
            Assert.assertEquals(2, NettyFactory.determineAcceptGroupSize(InternodeEncryption.all));
            Assert.assertEquals(4, NettyFactory.determineAcceptGroupSize(InternodeEncryption.rack));
            Assert.assertEquals(4, NettyFactory.determineAcceptGroupSize(InternodeEncryption.dc));
        }
        finally
        {
            FBUtilities.setBroadcastInetAddress(originalBroadcastAddr);
            DatabaseDescriptor.setListenOnBroadcastAddress(false);
        }
    }

    @Test
    public void getEventLoopGroup_EpollWithIoRatioBoost()
    {
        getEventLoopGroup_Epoll(true);
    }

    private EpollEventLoopGroup getEventLoopGroup_Epoll(boolean ioBoost)
    {
        EventLoopGroup eventLoopGroup;
        try
        {
            eventLoopGroup = NettyFactory.getEventLoopGroup(true, 1, "testEventLoopGroup", ioBoost);
        }
        catch (Exception e)
        {
            if (NativeLibrary.osType == NativeLibrary.OSType.LINUX)
                throw e;

            // ignore as epoll is only available on linux platforms, so don't fail the test on other OSes
            return null;
        }

        Assert.assertTrue(eventLoopGroup instanceof EpollEventLoopGroup);
        return (EpollEventLoopGroup) eventLoopGroup;
    }

    @Test
    public void getEventLoopGroup_EpollWithoutIoRatioBoost()
    {
        getEventLoopGroup_Epoll(false);
    }

    @Test
    public void getEventLoopGroup_NioWithoutIoRatioBoost()
    {
        getEventLoopGroup_Nio(true);
    }

    private NioEventLoopGroup getEventLoopGroup_Nio(boolean ioBoost)
    {
        EventLoopGroup eventLoopGroup = NettyFactory.getEventLoopGroup(false, 1, "testEventLoopGroup", ioBoost);
        Assert.assertTrue(eventLoopGroup instanceof NioEventLoopGroup);
        return (NioEventLoopGroup) eventLoopGroup;
    }

    @Test
    public void getEventLoopGroup_NioWithIoRatioBoost()
    {
        getEventLoopGroup_Nio(true);
    }

    @Test
    public void createOutboundBootstrap_Epoll()
    {
        Bootstrap bootstrap = createOutboundBootstrap(true);
        Assert.assertEquals(EpollEventLoopGroup.class, bootstrap.config().group().getClass());
    }

    private Bootstrap createOutboundBootstrap(boolean useEpoll)
    {
        factory = new NettyFactory(useEpoll);
        OutboundConnectionIdentifier id = OutboundConnectionIdentifier.gossip(LOCAL_ADDR, REMOTE_ADDR);
        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .connectionId(id)
                                                                  .coalescingStrategy(Optional.empty())
                                                                  .protocolVersion(MessagingService.current_version)
                                                                  .build();
        return factory.createOutboundBootstrap(params);
    }

    @Test
    public void createOutboundBootstrap_Nio()
    {
        Bootstrap bootstrap = createOutboundBootstrap(false);
        Assert.assertEquals(NioEventLoopGroup.class, bootstrap.config().group().getClass());
    }

    @Test
    public void createInboundInitializer_WithoutSsl() throws Exception
    {
        InboundInitializer initializer = new InboundInitializer(AUTHENTICATOR, null, channelGroup);
        NioSocketChannel channel = new NioSocketChannel();
        initializer.initChannel(channel);
        Assert.assertNull(channel.pipeline().get(SslHandler.class));
    }

    private ServerEncryptionOptions encOptions()
    {
        ServerEncryptionOptions encryptionOptions;
        encryptionOptions = new ServerEncryptionOptions();
        encryptionOptions.keystore = "test/conf/cassandra_ssl_test.keystore";
        encryptionOptions.keystore_password = "cassandra";
        encryptionOptions.truststore = "test/conf/cassandra_ssl_test.truststore";
        encryptionOptions.truststore_password = "cassandra";
        encryptionOptions.require_client_auth = false;
        encryptionOptions.cipher_suites = new String[] {"TLS_RSA_WITH_AES_128_CBC_SHA"};
        return encryptionOptions;
    }
    @Test
    public void createInboundInitializer_WithSsl() throws Exception
    {
        ServerEncryptionOptions encryptionOptions = encOptions();
        InboundInitializer initializer = new InboundInitializer(AUTHENTICATOR, encryptionOptions, channelGroup);
        NioSocketChannel channel = new NioSocketChannel();
        Assert.assertNull(channel.pipeline().get(SslHandler.class));
        initializer.initChannel(channel);
        Assert.assertNotNull(channel.pipeline().get(SslHandler.class));
    }

    @Test
    public void createOutboundInitializer_WithSsl() throws Exception
    {
        OutboundConnectionIdentifier id = OutboundConnectionIdentifier.gossip(LOCAL_ADDR, REMOTE_ADDR);
        OutboundConnectionParams params = OutboundConnectionParams.builder()
                                                                  .connectionId(id)
                                                                  .encryptionOptions(encOptions())
                                                                  .protocolVersion(MessagingService.current_version)
                                                                  .build();
        OutboundInitializer outboundInitializer = new OutboundInitializer(params);
        NioSocketChannel channel = new NioSocketChannel();
        Assert.assertNull(channel.pipeline().get(SslHandler.class));
        outboundInitializer.initChannel(channel);
        Assert.assertNotNull(channel.pipeline().get(SslHandler.class));
    }
}
