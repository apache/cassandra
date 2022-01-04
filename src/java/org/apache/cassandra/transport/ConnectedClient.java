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
package org.apache.cassandra.transport;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.service.ClientState;

public final class ConnectedClient
{
    public static final String ADDRESS = "address";
    public static final String USER = "user";
    public static final String VERSION = "version";
    public static final String CLIENT_OPTIONS = "clientOptions";
    public static final String DRIVER_NAME = "driverName";
    public static final String DRIVER_VERSION = "driverVersion";
    public static final String REQUESTS = "requests";
    public static final String KEYSPACE = "keyspace";
    public static final String SSL = "ssl";
    public static final String CIPHER = "cipher";
    public static final String PROTOCOL = "protocol";

    private static final String UNDEFINED = "undefined";

    private final ServerConnection connection;

    ConnectedClient(ServerConnection connection)
    {
        this.connection = connection;
    }

    public ConnectionStage stage()
    {
        return connection.stage();
    }

    public InetSocketAddress remoteAddress()
    {
        return state().getRemoteAddress();
    }

    public Optional<String> username()
    {
        AuthenticatedUser user = state().getUser();

        return null != user
             ? Optional.of(user.getName())
             : Optional.empty();
    }

    public int protocolVersion()
    {
        return connection.getVersion().asInt();
    }

    public Optional<String> driverName()
    {
        return state().getDriverName();
    }

    public Optional<String> driverVersion()
    {
        return state().getDriverVersion();
    }

    public Optional<Map<String,String>> clientOptions()
    {
        return state().getClientOptions();
    }

    public long requestCount()
    {
        return connection.requests.getCount();
    }

    public Optional<String> keyspace()
    {
        return Optional.ofNullable(state().getRawKeyspace());
    }

    public boolean sslEnabled()
    {
        return null != sslHandler();
    }

    public Optional<String> sslCipherSuite()
    {
        SslHandler sslHandler = sslHandler();

        return null != sslHandler
             ? Optional.of(sslHandler.engine().getSession().getCipherSuite())
             : Optional.empty();
    }

    public Optional<String> sslProtocol()
    {
        SslHandler sslHandler = sslHandler();

        return null != sslHandler
             ? Optional.of(sslHandler.engine().getSession().getProtocol())
             : Optional.empty();
    }

    private ClientState state()
    {
        return connection.getClientState();
    }

    private SslHandler sslHandler()
    {
        return connection.channel().pipeline().get(SslHandler.class);
    }

    public Map<String, String> asMap()
    {
        return ImmutableMap.<String, String>builder()
                           .put(ADDRESS, remoteAddress().toString())
                           .put(USER, username().orElse(UNDEFINED))
                           .put(VERSION, String.valueOf(protocolVersion()))
                           .put(CLIENT_OPTIONS, Joiner.on(", ")
                                                      .withKeyValueSeparator("=")
                                                      .join(clientOptions().orElse(Collections.emptyMap())))
                           .put(DRIVER_NAME, driverName().orElse(UNDEFINED))
                           .put(DRIVER_VERSION, driverVersion().orElse(UNDEFINED))
                           .put(REQUESTS, String.valueOf(requestCount()))
                           .put(KEYSPACE, keyspace().orElse(""))
                           .put(SSL, Boolean.toString(sslEnabled()))
                           .put(CIPHER, sslCipherSuite().orElse(UNDEFINED))
                           .put(PROTOCOL, sslProtocol().orElse(UNDEFINED))
                           .build();
    }
}
