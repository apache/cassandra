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

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class CredentialsMessage extends Message.Request
{
    public static final Message.Codec<CredentialsMessage> codec = new Message.Codec<CredentialsMessage>()
    {
        public CredentialsMessage decode(ByteBuf body, int version)
        {
            if (version > 1)
                throw new ProtocolException("Legacy credentials authentication is not supported in " +
                        "protocol versions > 1. Please use SASL authentication via a SaslResponse message");

            Map<String, String> credentials = CBUtil.readStringMap(body);
            return new CredentialsMessage(credentials);
        }

        public void encode(CredentialsMessage msg, ByteBuf dest, int version)
        {
            CBUtil.writeStringMap(msg.credentials, dest);
        }

        public int encodedSize(CredentialsMessage msg, int version)
        {
            return CBUtil.sizeOfStringMap(msg.credentials);
        }
    };

    public final Map<String, String> credentials;

    public CredentialsMessage()
    {
        this(new HashMap<String, String>());
    }

    private CredentialsMessage(Map<String, String> credentials)
    {
        super(Message.Type.CREDENTIALS);
        this.credentials = credentials;
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().legacyAuthenticate(credentials);
            state.getClientState().login(user);
        }
        catch (AuthenticationException e)
        {
            return ErrorMessage.fromException(e);
        }

        return new ReadyMessage();
    }

    @Override
    public String toString()
    {
        return "CREDENTIALS " + credentials;
    }
}
