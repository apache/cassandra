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

import java.util.function.BiFunction;

import org.apache.cassandra.auth.AuthEvents;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Message.Response;
import org.apache.cassandra.transport.ServerConnection;

public final class AuthUtil
{

    /**
     * Attempts to authenticate a user on the current connection by negotiating with the underlying
     * {@link org.apache.cassandra.auth.IAuthenticator.SaslNegotiator} using the given state of the connection
     * (e.g. client certificates), the provided query state and the provided token.
     * <p>
     * If unsuccessful an {@link ErrorMessage} is returned and the authentication failure is recorded.
     * <p>
     * If negotiation is complete, a successful login is recorded and the given function is called with
     * <code>true</code> and the challenge returned from
     * {@link org.apache.cassandra.auth.IAuthenticator.SaslNegotiator#evaluateResponse(byte[])}.
     * <p>
     * If negotiation is incomplete, <code>false</code> and the challenge is passed to the given function.
     *
     * @param connection                      The connection to authenticate
     * @param queryState                      The current query state
     * @param token                           The token provided in an {@link AuthResponse} from the client (or empty
     *                                        if not handling an AuthResponse).
     * @param messageToSendBasedOnNegotiation Determines what response to return on based on whether sasl negotiation
     *                                        is complete (1st parameter) and the challenege token returned from the
     *                                        negotiator (2nd parameter).
     * @return the response to send back to the client.
     */
    static Response handleLogin(Connection connection, QueryState queryState, byte[] token,
                                BiFunction<Boolean, byte[], Response> messageToSendBasedOnNegotiation)
    {
        IAuthenticator.SaslNegotiator negotiator = ((ServerConnection) connection).getSaslNegotiator(queryState);
        try
        {
            // client-side timeout can disconnect while sitting in auth executor queue so (client default 12s)
            // discard if connection closed anyway
            if (!connection.channel().isActive())
            {
                throw new AuthenticationException("Auth check after connection closed");
            }
            byte[] challenge = negotiator.evaluateResponse(token);
            if (negotiator.isComplete())
            {
                AuthenticatedUser user = negotiator.getAuthenticatedUser();
                queryState.getClientState().login(user);
                ClientMetrics.instance.markAuthSuccess(user.getAuthenticationMode());
                AuthEvents.instance.notifyAuthSuccess(queryState);
                // authentication is complete, complete the authentication flow.
                return messageToSendBasedOnNegotiation.apply(true, challenge);
            }
            else
            {
                // authentication is incomplete, continue the authentication flow.
                return messageToSendBasedOnNegotiation.apply(false, challenge);
            }
        }
        catch (AuthenticationException e)
        {
            ClientMetrics.instance.markAuthFailure(negotiator.getAuthenticationMode());
            AuthEvents.instance.notifyAuthFailure(queryState, e);
            return ErrorMessage.fromException(e);
        }
    }

    /**
     * The class must not be instantiated.
     */
    private AuthUtil()
    {
    }
}
