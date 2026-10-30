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

import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.CassandraRelevantProperties;
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
    public static final String PAGE_UNIT = "PAGE_UNIT";

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
        this.options = options;
    }

    @Override
    protected Message.Response execute(QueryState state, Dispatcher.RequestTime requestTime, boolean traceRequest)
    {
        validateOptions(options);
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
     * Validates and sanitizes a startup option key or value string.
     * Enforces byte-length bounds, blocks ISO control characters, newlines,
     * and ensures keys are non-null.
     *
     * @param str the string to validate
     * @param isKey true if validating a key, false if validating a value
     * @throws ProtocolException if the string violates validation constraints
     */
    private static void validateString(String str, boolean isKey, int maxLength)
    {
        if (str == null)
        {
            if (isKey)
                throw new ProtocolException("Startup option keys cannot be null");
            return;
        }

        int utf8Length = 0;
        for (int i = 0; i < str.length(); )
        {
            int cp = str.codePointAt(i);

            if (cp == '\n' || cp == '\r')
                throw new ProtocolException(String.format("Startup option %s contains a newline character at index %d", isKey ? "key" : "value", i));

            int type = Character.getType(cp);
            if ((Character.isISOControl(cp) && cp != '\t') ||
                type == Character.SURROGATE ||
                type == Character.FORMAT ||
                type == Character.LINE_SEPARATOR ||
                type == Character.PARAGRAPH_SEPARATOR)
            {
                throw new ProtocolException(String.format("Startup option %s contains a forbidden control or formatting character at index %d", isKey ? "key" : "value", i));
            }

            // accumulate the UTF-8 byte length here to avoid a second pass and a throwaway byte[] (lone
            // surrogates are rejected above, so cp is always a valid scalar value by this point)
            utf8Length += utf8ByteCount(cp);
            i += Character.charCount(cp);
        }

        // Byte-length limit is opt-in; a negative limit means unlimited.
        if (maxLength < 0 || utf8Length <= maxLength)
            return;

        String target = isKey
                        ? String.format("key '%s'", StringEscapeUtils.escapeJava(StringUtils.abbreviate(str, 23)))
                        : "value";

        throw new ProtocolException(String.format("Startup option %s exceeds maximum byte length: %d (maximum allowed is %d)",
                                                  target, utf8Length, maxLength));
    }

    private static int utf8ByteCount(int codePoint)
    {
        if (codePoint < 0x80)
            return 1;
        if (codePoint < 0x800)
            return 2;
        if (codePoint < 0x10000)
            return 3;
        return 4;
    }

    @VisibleForTesting
    static void validateOptions(Map<String, String> options)
    {
        if (options == null || options.isEmpty())
            return;

        int maxCount = CassandraRelevantProperties.STARTUP_CLIENT_OPTION_MAX_COUNT.getInt();
        int maxKeyLength = CassandraRelevantProperties.STARTUP_CLIENT_OPTION_KEY_MAX_LENGTH.getInt();
        int maxValueLength = CassandraRelevantProperties.STARTUP_CLIENT_OPTION_VALUE_MAX_LENGTH.getInt();

        if (maxCount >= 0 && options.size() > maxCount)
            throw new ProtocolException(String.format("Too many startup options provided: %d (maximum allowed is %d)", options.size(), maxCount));

        for (Map.Entry<String, String> entry : options.entrySet())
        {
            validateString(entry.getKey(), true, maxKeyLength);
            validateString(entry.getValue(), false, maxValueLength);
        }
    }

    @Override
    public String toString()
    {
        return "STARTUP " + options;
    }
}
