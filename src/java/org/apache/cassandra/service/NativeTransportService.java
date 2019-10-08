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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AuthMetrics;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.ConfiguredLimit;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.Server;

/**
 * Handles native transport server lifecycle and associated resources. Lazily initialized.
 */
public class NativeTransportService
{

    private static final Logger logger = LoggerFactory.getLogger(NativeTransportService.class);

    private Collection<Server> servers = Collections.emptyList();

    private boolean initialized = false;
    private EventLoopGroup workerGroup;
    private ConfiguredLimit protocolVersionLimit;

    /**
     * Creates netty thread pools and event loops.
     */
    @VisibleForTesting
    synchronized void initialize()
    {
        if (initialized)
            return;

        if (useEpoll())
        {
            workerGroup = new EpollEventLoopGroup();
            logger.info("Netty using native Epoll event loop");
        }
        else
        {
            workerGroup = new NioEventLoopGroup();
            logger.info("Netty using Java NIO event loop");
        }

        protocolVersionLimit = ConfiguredLimit.newLimit();

        int nativePort = DatabaseDescriptor.getNativeTransportPort();
        int nativePortSSL = DatabaseDescriptor.getNativeTransportPortSSL();
        InetAddress nativeAddr = DatabaseDescriptor.getRpcAddress();

        org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                                                                .withEventLoopGroup(workerGroup)
                                                                .withProtocolVersionLimit(protocolVersionLimit)
                                                                .withHost(nativeAddr);

        if (!DatabaseDescriptor.getClientEncryptionOptions().enabled)
        {
            servers = Collections.singleton(builder.withSSL(false).withPort(nativePort).build());
        }
        else
        {
            if (nativePort != nativePortSSL)
            {
                // user asked for dedicated ssl port for supporting both non-ssl and ssl connections
                servers = Collections.unmodifiableList(
                                                      Arrays.asList(
                                                                   builder.withSSL(false).withPort(nativePort).build(),
                                                                   builder.withSSL(true).withPort(nativePortSSL).build()
                                                      )
                );
            }
            else
            {
                // ssl only mode using configured native port
                servers = Collections.singleton(builder.withSSL(true).withPort(nativePort).build());
            }
        }

        // register metrics
        ClientMetrics.instance.init(servers);

        AuthMetrics.init();

        initialized = true;
    }

    /**
     * Starts native transport servers.
     */
    public void start()
    {
        initialize();
        servers.forEach(Server::start);
    }

    /**
     * Stops currently running native transport servers.
     */
    public void stop()
    {
        servers.forEach(Server::stop);
    }

    /**
     * Ultimately stops servers and closes all resources.
     */
    public void destroy()
    {
        stop();
        servers = Collections.emptyList();

        // shutdown executors used by netty for native transport server
        workerGroup.shutdownGracefully(3, 5, TimeUnit.SECONDS).awaitUninterruptibly();

        Message.Dispatcher.shutdown();
    }

    public int getMaxProtocolVersion()
    {
        return protocolVersionLimit.getMaxVersion().asInt();
    }

    public void refreshMaxNegotiableProtocolVersion()
    {
        // lowering the max negotiable protocol version is only safe if we haven't already
        // allowed clients to connect with a higher version. This still allows the max
        // version to be raised, as that is safe.
        if (initialized)
            protocolVersionLimit.updateMaxSupportedVersion();
    }

    /**
     * @return intend to use epoll bassed event looping
     */
    public static boolean useEpoll()
    {
        final boolean enableEpoll = Boolean.parseBoolean(System.getProperty("cassandra.native.epoll.enabled", "true"));
        return enableEpoll && Epoll.isAvailable();
    }

    /**
     * @return true in case native transport server is running
     */
    public boolean isRunning()
    {
        for (Server server : servers)
            if (server.isRunning()) return true;
        return false;
    }

    @VisibleForTesting
    EventLoopGroup getWorkerGroup()
    {
        return workerGroup;
    }

    @VisibleForTesting
    Collection<Server> getServers()
    {
        return servers;
    }
}
