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
package org.apache.cassandra.transport.messages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Compressor;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.ServerConnection;
import org.apache.cassandra.utils.CassandraVersion;

import io.netty.buffer.ByteBuf;

import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;
import static org.apache.cassandra.utils.LocalizeString.toUpperCaseLocalized;

/**
 * The initial message of the protocol.
 * Sets up a number of connection options.
 */
public class StartupMessage extends Message.Request
{
    public static final String CQL_VERSION = "CQL_VERSION";
    public static final String COMPRESSION = "COMPRESSION";
    public static final String PROTOCOL_VERSIONS = "PROTOCOL_VERSIONS";
    public static final String DRIVER_NAME = "DRIVER_NAME";
    public static final String DRIVER_VERSION = "DRIVER_VERSION";
    public static final String THROW_ON_OVERLOAD = "THROW_ON_OVERLOAD";
    public static final int MAX_OPTION_KEY_LENGTH = 256;
    public static final int MAX_OPTION_VALUE_LENGTH = 1024;
    public static final int MAX_OPTIONS_ENTRIES = 64;

    public static final Message.Codec<StartupMessage> codec = new Message.Codec<StartupMessage>()
    {
        public StartupMessage decode(ByteBuf body, ProtocolVersion version)
        {
            return new StartupMessage(upperCaseKeys(CBUtil.readStringMap(body)));
        }

        public void encode(StartupMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeStringMap(msg.options, dest);
        }

        public int encodedSize(StartupMessage msg, ProtocolVersion version)
        {
            return CBUtil.sizeOfStringMap(msg.options);
        }
    };

    private static final byte[] EMPTY_CLIENT_RESPONSE = new byte[0];

    public final Map<String, String> options;

    public StartupMessage(Map<String, String> options)
    {
        super(Message.Type.STARTUP);
        this.options = validateAndSanitizeOptions(options);
    }

    @Override
    protected Message.Response execute(QueryState state, Dispatcher.RequestTime requestTime, boolean traceRequest)
    {
        String cqlVersion = options.get(CQL_VERSION);
        if (cqlVersion == null)
            throw new ProtocolException("Missing value CQL_VERSION in STARTUP message");

        try
        {
            if (new CassandraVersion(cqlVersion).compareTo(new CassandraVersion("2.99.0")) < 0)
                throw new ProtocolException(String.format("CQL version %s is not supported by the binary protocol (supported version are >= 3.0.0)", cqlVersion));
        }
        catch (IllegalArgumentException e)
        {
            throw new ProtocolException(e.getMessage());
        }

        if (options.containsKey(COMPRESSION))
        {
            String compression = toLowerCaseLocalized(options.get(COMPRESSION));
            if (compression.equals("snappy"))
            {
                if (Compressor.SnappyCompressor.instance == null)
                    throw new ProtocolException("This instance does not support Snappy compression");

                if (getSource().header.version.isGreaterOrEqualTo(ProtocolVersion.V5))
                    throw new ProtocolException("Snappy compression is not supported in protocol V5");

                connection.setCompressor(Compressor.SnappyCompressor.instance);
            }
            else if (compression.equals("lz4"))
            {
                connection.setCompressor(Compressor.LZ4Compressor.instance);
            }
            else
            {
                throw new ProtocolException(String.format("Unknown compression algorithm: %s", compression));
            }
        }

        connection.setThrowOnOverload("1".equals(options.get(THROW_ON_OVERLOAD)));

        ClientState clientState = state.getClientState();
        clientState.setClientOptions(options);
        String driverName = options.get(DRIVER_NAME);
        String driverVersion = options.get(DRIVER_VERSION);
        if (null != driverName)
        {
            clientState.setDriverName(driverName);
            clientState.setDriverVersion(driverVersion);
        }

        Guardrails.minimumClientDriverVersion.guard(driverName, driverVersion, clientState);

        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        if (authenticator.requireAuthentication())
        {
            // If the authenticator supports early authentication, attempt to authenticate.
            if (authenticator.supportsEarlyAuthentication())
            {
                IAuthenticator.SaslNegotiator negotiator = ((ServerConnection) connection).getSaslNegotiator(state);
                // If the negotiator determines that sending an authenticate message is not necessary, attempt to authenticate here,
                // otherwise, send an Authenticate message to begin the traditional authentication flow.
                if (!negotiator.shouldSendAuthenticateMessage())
                {
                    // Attempt to authenticate the user.
                    return AuthUtil.handleLogin(connection, state, EMPTY_CLIENT_RESPONSE, (negotiationComplete, challenge) ->
                    {
                        if (negotiationComplete)
                        {
                            // Authentication was successful, proceed.
                            return new ReadyMessage();
                        } else
                        {
                            // It's expected that any negotiator that requires a challenge will likely not support early
                            // authentication, in this case we can just go through the traditional auth flow.
                            return authenticator.getAuthenticateMessage(clientState);
                        }
                    });
                }
            }
            return authenticator.getAuthenticateMessage(clientState);
        }
        else
            return new ReadyMessage();
    }

    private static Map<String, String> upperCaseKeys(Map<String, String> options)
    {
        Map<String, String> newMap = new HashMap<String, String>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet())
            newMap.put(toUpperCaseLocalized(entry.getKey()), entry.getValue());
        return newMap;
    }

    /**
     * Validates and sanitizes the startup options.
     * @param options The startup options to validate and sanitize.
     * @return The validated and sanitized startup options.
     * @throws ProtocolException If the startup options are invalid. Validation conditions to be found in
     * ticket <a href="https://issues.apache.org/jira/browse/CASSANDRA-21755">CASSANDRA-21755</a>.
     */
    private static Map<String, String> validateAndSanitizeOptions(Map<String, String> options)
    {
        if (options == null || options.isEmpty())
            return Collections.emptyMap();
        if (options.size() > MAX_OPTIONS_ENTRIES)
            throw new ProtocolException(String.format("Too many startup options provided: %d (maximum allowed is %d)", options.size(), MAX_OPTIONS_ENTRIES));
        Map<String, String> sanitized = new HashMap<>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet())
        {
            String key = entry.getKey();
            String value = entry.getValue();

            if (key == null)
                throw new ProtocolException("Startup option keys cannot be null");
            if (key.length() > MAX_OPTION_KEY_LENGTH)
                throw new ProtocolException(String.format("Startup option key exceeds maximum length: %d (maximum allowed is %d)", key.length(), MAX_OPTION_KEY_LENGTH));
            if (value != null)
            {
                if (value.length() > MAX_OPTION_VALUE_LENGTH)
                    throw new ProtocolException(String.format("Startup option value exceeds maximum length: %d (maximum allowed is %d)", value.length(), MAX_OPTION_VALUE_LENGTH));

                for (int i = 0; i < value.length(); i++)
                {
                    char c = value.charAt(i);
                    if (Character.isISOControl(c) && c != '\t' && c != '\n' && c != '\r')
                        throw new ProtocolException("Startup option values cannot contain control characters");
                }
            }
            sanitized.put(key, value);
        }
        return sanitized;
    }

    @Override
    public String toString()
    {
        return "STARTUP " + options;
    }
}
