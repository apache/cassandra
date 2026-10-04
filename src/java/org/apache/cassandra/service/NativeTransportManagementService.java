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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.transport.Server;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.Version;

import static org.apache.cassandra.config.CassandraRelevantProperties.START_NATIVE_MANAGEMENT_TRANSPORT;
import static org.apache.cassandra.service.NativeTransportService.useEpoll;

/**
 * Manages the native management transport server, listening on {@code native_transport_management_port}
 * ({@code 11211} by default). This service is independent of NativeTransportService and can only be
 * enabled/disabled via configuration or system property (not at runtime).
 */
public class NativeTransportManagementService implements CassandraDaemon.Server
{
    private static final Logger logger = LoggerFactory.getLogger(NativeTransportManagementService.class);

    private Server server = null;
    private EventLoopGroup workerGroup;

    private volatile boolean initialized = false;

    @VisibleForTesting
    synchronized void initialize()
    {
        if (initialized)
            return;

        if (useEpoll())
        {
            workerGroup = new EpollEventLoopGroup();
            logger.info("Management transport Netty using native Epoll event loop");
        }
        else
        {
            workerGroup = new NioEventLoopGroup();
            logger.info("Management transport Netty using Java NIO event loop");
        }

        logger.info("Management transport Netty Version: {}", Version.identify().entrySet());

        int managementPort = DatabaseDescriptor.getNativeTransportManagementPort();
        InetAddress addr = DatabaseDescriptor.getRpcManagementAddress();

        EncryptionOptions.TlsEncryptionPolicy encryptionPolicy = DatabaseDescriptor.getNativeProtocolEncryptionOptions()
                                                                                   .tlsEncryptionPolicy();

        server = new Server.Builder()
                 .withEventLoopGroup(workerGroup)
                 .withHost(addr)
                 .withTlsEncryptionPolicy(encryptionPolicy)
                 .withPort(managementPort)
                 .withManagementConnectionFlag(true)
                 .build();

        initialized = true;
    }

    /**
     * @return whether the management transport should start: the
     * {@code cassandra.start_native_management_transport} system property wins when set,
     * otherwise the {@code start_native_transport_management} yaml setting decides.
     */
    public static boolean enabled()
    {
        if (START_NATIVE_MANAGEMENT_TRANSPORT.getString() != null)
            return START_NATIVE_MANAGEMENT_TRANSPORT.getBoolean();
        return DatabaseDescriptor.startNativeTransportManagement();
    }

    public void start()
    {
        if (!enabled())
        {
            logger.info("Management transport is disabled via configuration and will not start.");
            return;
        }

        initialize();
        server.start();
        logger.info("Management transport started on port: {}", server.socket.getPort());
    }

    public void stop()
    {
        if (server != null)
            server.stop(false);
    }

    public void destroy()
    {
        stop();
        if (server == null)
            return;

        server = null;
        if (workerGroup != null)
            workerGroup.shutdownGracefully(3, 5, TimeUnit.SECONDS).awaitUninterruptibly();

        initialized = false;
    }

    /** @return true if the management transport server is running. */
    public boolean isRunning()
    {
        return server != null && server.isRunning();
    }

    /** Clears connection history for this service's server. */
    public void clearConnectionHistory()
    {
        if (server != null)
            server.clearConnectionHistory();
    }

    /** Disconnects users matching the predicate from this service's server. */
    public void disconnect(Predicate<AuthenticatedUser> userPredicate)
    {
        if (server != null)
            server.disconnect(userPredicate);
    }

    @VisibleForTesting
    public EventLoopGroup getWorkerGroup()
    {
        return workerGroup;
    }
}
