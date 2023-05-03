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
import io.netty.util.Version;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.NativeLibrary;

import static org.apache.cassandra.config.CassandraRelevantProperties.NATIVE_EPOLL_ENABLED;

/**
 * Handles native transport server lifecycle and associated resources. Lazily initialized.
 */
public class NativeTransportService
{

    private static final Logger logger = LoggerFactory.getLogger(NativeTransportService.class);

    private Collection<Server> servers = Collections.emptyList();

    private boolean initialized = false;
    private EventLoopGroup workerGroup;

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

        int nativePort = DatabaseDescriptor.getNativeTransportPort();
        int nativePortSSL = DatabaseDescriptor.getNativeTransportPortSSL();
        InetAddress nativeAddr = DatabaseDescriptor.getRpcAddress();

        org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                                                                .withEventLoopGroup(workerGroup)
                                                                .withHost(nativeAddr);

        EncryptionOptions.TlsEncryptionPolicy encryptionPolicy = DatabaseDescriptor.getNativeProtocolEncryptionOptions().tlsEncryptionPolicy();
        Server regularPortServer;
        Server tlsPortServer = null;

        // If an SSL port is separately supplied for the native transport, listen for unencrypted connections on the
        // regular port, and encryption / optionally encrypted connections on the ssl port.
        if (nativePort != nativePortSSL)
        {
            regularPortServer = builder.withTlsEncryptionPolicy(EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED).withPort(nativePort).build();
            switch(encryptionPolicy)
            {
                case OPTIONAL: // FALLTHRU - encryption is optional on the regular port, but encrypted on the tls port.
                case ENCRYPTED:
                    tlsPortServer = builder.withTlsEncryptionPolicy(encryptionPolicy).withPort(nativePortSSL).build();
                    break;
                case UNENCRYPTED: // Should have been caught by DatabaseDescriptor.applySimpleConfig
                    throw new IllegalStateException("Encryption must be enabled in client_encryption_options for native_transport_port_ssl");
                default:
                    throw new IllegalStateException("Unrecognized TLS encryption policy: " + encryptionPolicy);
            }
        }
        // Otherwise, if only the regular port is supplied, listen as the encryption policy specifies
        else
        {
            regularPortServer = builder.withTlsEncryptionPolicy(encryptionPolicy).withPort(nativePort).build();
        }

        if (tlsPortServer == null)
        {
            servers = Collections.singleton(regularPortServer);
        }
        else
        {
            servers = Collections.unmodifiableList(Arrays.asList(regularPortServer, tlsPortServer));
        }

        ClientMetrics.instance.init(servers);

        initialized = true;
    }

    /**
     * Starts native transport servers.
     */
    public void start()
    {
        logger.info("Using Netty Version: {}", Version.identify().entrySet());
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
        if (workerGroup != null)
            workerGroup.shutdownGracefully(3, 5, TimeUnit.SECONDS).awaitUninterruptibly();

        Dispatcher.shutdown();
    }

    /**
     * @return intend to use epoll based event looping
     */
    public static boolean useEpoll()
    {
        final boolean enableEpoll = NATIVE_EPOLL_ENABLED.getBoolean();

        if (enableEpoll && !Epoll.isAvailable() && NativeLibrary.osType == NativeLibrary.OSType.LINUX)
            logger.warn("epoll not available", Epoll.unavailabilityCause());

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

    public void clearConnectionHistory()
    {
        for (Server server : servers)
            server.clearConnectionHistory();
    }
}
