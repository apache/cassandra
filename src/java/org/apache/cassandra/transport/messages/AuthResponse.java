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
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.metrics.AuthMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.*;

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
    public Response execute(QueryState queryState, long queryStartNanoTime)
    {
        try
        {
            IAuthenticator.SaslNegotiator negotiator = ((ServerConnection) connection).getSaslNegotiator(queryState);
            byte[] challenge = negotiator.evaluateResponse(token);
            if (negotiator.isComplete())
            {
                AuthenticatedUser user = negotiator.getAuthenticatedUser();
                queryState.getClientState().login(user);
                AuthMetrics.instance.markSuccess();
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
            AuthMetrics.instance.markFailure();
            return ErrorMessage.fromException(e);
        }
    }
}
