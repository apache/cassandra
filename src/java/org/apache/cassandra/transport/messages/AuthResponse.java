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

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.ServerConnection;

/**
 * A SASL token message sent from client to server. Some SASL
 * mechanisms and clients may send an initial token before
 * receiving a challenge from the server.
 */
public class AuthResponse extends Message.Request
{
    public static final Message.Codec<AuthResponse> codec = new Message.Codec<AuthResponse>()
    {
        public AuthResponse decode(ByteBuf body, ProtocolVersion version)
        {
            if (version == ProtocolVersion.V1)
                throw new ProtocolException("SASL Authentication is not supported in version 1 of the protocol");

            ByteBuffer b = CBUtil.readValue(body);
            byte[] token = new byte[b.remaining()];
            b.get(token);
            return new AuthResponse(token);
        }

        public void encode(AuthResponse response, ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeValue(response.token, dest);
        }

        public int encodedSize(AuthResponse response, ProtocolVersion version)
        {
            return CBUtil.sizeOfValue(response.token);
        }
    };

    private final byte[] token;

    public AuthResponse(byte[] token)
    {
        super(Message.Type.AUTH_RESPONSE);
        assert token != null;
        this.token = token;
    }

    @Override
    protected Response execute(QueryState queryState, boolean traceRequest)
    {
        AuditLogManager auditLogManager = AuditLogManager.getInstance();

        try
        {
            IAuthenticator.SaslNegotiator negotiator = ((ServerConnection) connection).getSaslNegotiator(queryState);
            byte[] challenge = negotiator.evaluateResponse(token);
            if (negotiator.isComplete())
            {
                AuthenticatedUser user = negotiator.getAuthenticatedUser();
                queryState.getClientState().login(user);
                ClientMetrics.instance.markAuthSuccess();
                if (auditLogManager.isAuditingEnabled())
                    logSuccess(queryState);
                // authentication is complete, send a ready message to the client
                return new AuthSuccess(challenge);
            }
            else
            {
                return new AuthChallenge(challenge);
            }
        }
        catch (AuthenticationException e)
        {
            ClientMetrics.instance.markAuthFailure();
            if (auditLogManager.isAuditingEnabled())
                logException(queryState, e);
            return ErrorMessage.fromException(e);
        }
    }

    private void logSuccess(QueryState state)
    {
        AuditLogEntry entry =
            new AuditLogEntry.Builder(state)
                             .setOperation("LOGIN SUCCESSFUL")
                             .setType(AuditLogEntryType.LOGIN_SUCCESS)
                             .build();
        AuditLogManager.getInstance().log(entry);
    }

    private void logException(QueryState state, AuthenticationException e)
    {
        AuditLogEntry entry =
            new AuditLogEntry.Builder(state)
                             .setOperation("LOGIN FAILURE")
                             .setType(AuditLogEntryType.LOGIN_ERROR)
                             .build();
        AuditLogManager.getInstance().log(entry, e);
    }
}
