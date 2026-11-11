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
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.service.ClientState;

import io.netty.handler.ssl.SslHandler;

import static org.apache.cassandra.auth.IAuthenticator.AuthenticationMode.UNAUTHENTICATED;

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
    public static final String AUTHENTICATION_MODE = "authenticationMode";
    public static final String AUTHENTICATION_METADATA = "authenticationMetadata";

    private static final String[] SEARCH  = { "\\",   ",",   "="   };
    private static final String[] REPLACE = { "\\\\", "\\,", "\\=" };

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
        return connection.requestCount();
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

    public Map<String, Object> authenticationMetadata()
    {
        AuthenticatedUser user = state().getUser();

        return null != user
                ? user.getMetadata()
                : Collections.emptyMap();
    }

    public IAuthenticator.AuthenticationMode authenticationMode()
    {
        AuthenticatedUser user = state().getUser();

        return null != user
                ? user.getAuthenticationMode()
                : UNAUTHENTICATED;
    }

    private ClientState state()
    {
        return connection.getClientState();
    }

    private SslHandler sslHandler()
    {
        return connection.channel().pipeline().get(SslHandler.class);
    }

    /**
     * @return true once the client has authenticated (has a user).
     * Unauthenticated clients are still visible in operator-facing views, but the metadata they supplied in STARTUP
     * (the options and the driver name and version taken from them) is withheld until authentication.
     */
    public boolean loggedIn()
    {
        return state() != null && state().getUser() != null;
    }

    public Map<String, String> asMap()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                                                                   .put(ADDRESS, remoteAddress().toString())
                                                                   .put(USER, username().orElse(UNDEFINED))
                                                                   .put(VERSION, String.valueOf(protocolVersion()))
                                                                   .put(REQUESTS, String.valueOf(requestCount()))
                                                                   .put(KEYSPACE, keyspace().orElse(""))
                                                                   .put(SSL, Boolean.toString(sslEnabled()))
                                                                   .put(CIPHER, sslCipherSuite().orElse(UNDEFINED))
                                                                   .put(PROTOCOL, sslProtocol().orElse(UNDEFINED))
                                                                   .put(AUTHENTICATION_MODE, authenticationMode().toString())
                                                                   .put(AUTHENTICATION_METADATA, joinEscaped(authenticationMetadata()));
        // Only the STARTUP options and the driver name and version taken from them are supplied by the client before
        // it authenticates; every entry above is derived by the server.
        if (loggedIn())
        {
            builder.put(CLIENT_OPTIONS, joinEscaped(clientOptions().orElse(Collections.emptyMap())))
                   .put(DRIVER_NAME, driverName().orElse(UNDEFINED))
                   .put(DRIVER_VERSION, driverVersion().orElse(UNDEFINED));
        }
        return builder.build();
    }

    /**
     * Renders a map as {@code key=value, key=value} with every key and value escaped by
     * {@link #escapeOptionString(String)}, so that a key or value containing a delimiter cannot forge extra entries
     * in operator-facing output. Used for the client options and the authentication metadata alike: both reach the
     * same consumers and neither is under the operator's control, since an authenticator takes the metadata from
     * what the client presents.
     */
    static String joinEscaped(Map<String, ?> map)
    {
        return map.entrySet()
                  .stream()
                  .map(e -> escapeOptionString(e.getKey()) + '=' + escapeOptionString(String.valueOf(e.getValue())))
                  .collect(Collectors.joining(", "));
    }

    /**
     * Escapes backslashes first, then special delimiter characters (commas and equals signs)
     * in keys and values to prevent format injection in operator-facing maps.
     * <p>
     * Example: {@code foo,bar=baz} becomes {@code foo\\,bar\\=baz}
     *
     * @param str the raw option string
     * @return the safely escaped string
     */
    static String escapeOptionString(String str)
    {
        if (str == null)
            return "";
        return StringUtils.replaceEach(str, SEARCH, REPLACE);
    }
}
