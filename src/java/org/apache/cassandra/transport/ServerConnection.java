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

import java.security.cert.Certificate;
import javax.net.ssl.SSLPeerUnverifiedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import com.codahale.metrics.Counter;
import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

public class ServerConnection extends Connection
{
    private static final Logger logger = LoggerFactory.getLogger(ServerConnection.class);

    private volatile IAuthenticator.SaslNegotiator saslNegotiator;
    private final ClientState clientState;
    private volatile ConnectionStage stage;
    public final Counter requests = new Counter();

    ServerConnection(Channel channel, ProtocolVersion version, Connection.Tracker tracker)
    {
        super(channel, version, tracker);

        clientState = ClientState.forExternalCalls(channel.remoteAddress());
        stage = ConnectionStage.ESTABLISHED;
    }

    public ClientState getClientState()
    {
        return clientState;
    }

    ConnectionStage stage()
    {
        return stage;
    }

    QueryState validateNewMessage(Message.Type type, ProtocolVersion version)
    {
        switch (stage)
        {
            case ESTABLISHED:
                if (type != Message.Type.STARTUP && type != Message.Type.OPTIONS)
                    throw new ProtocolException(String.format("Unexpected message %s, expecting STARTUP or OPTIONS", type));
                break;
            case AUTHENTICATING:
                // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
                if (type != Message.Type.AUTH_RESPONSE && type != Message.Type.CREDENTIALS)
                    throw new ProtocolException(String.format("Unexpected message %s, expecting %s", type, version == ProtocolVersion.V1 ? "CREDENTIALS" : "SASL_RESPONSE"));
                break;
            case READY:
                if (type == Message.Type.STARTUP)
                    throw new ProtocolException("Unexpected message STARTUP, the connection is already initialized");
                break;
            default:
                throw new AssertionError();
        }

        return new QueryState(clientState);
    }

    void applyStateTransition(Message.Type requestType, Message.Type responseType)
    {
        switch (stage)
        {
            case ESTABLISHED:
                if (requestType == Message.Type.STARTUP)
                {
                    if (responseType == Message.Type.AUTHENTICATE)
                        stage = ConnectionStage.AUTHENTICATING;
                    else if (responseType == Message.Type.READY)
                        stage = ConnectionStage.READY;
                }
                break;
            case AUTHENTICATING:
                // Support both SASL auth from protocol v2 and the older style Credentials auth from v1
                assert requestType == Message.Type.AUTH_RESPONSE || requestType == Message.Type.CREDENTIALS;

                if (responseType == Message.Type.READY || responseType == Message.Type.AUTH_SUCCESS)
                {
                    stage = ConnectionStage.READY;
                    // we won't use the authenticator again, null it so that it can be GC'd
                    saslNegotiator = null;
                }
                break;
            case READY:
                break;
            default:
                throw new AssertionError();
        }
    }

    public IAuthenticator.SaslNegotiator getSaslNegotiator(QueryState queryState)
    {
        if (saslNegotiator == null)
            saslNegotiator = DatabaseDescriptor.getAuthenticator()
                                               .newSaslNegotiator(queryState.getClientAddress(), certificates());
        return saslNegotiator;
    }

    private Certificate[] certificates()
    {
        SslHandler sslHandler = (SslHandler) channel().pipeline()
                                                      .get("ssl");
        Certificate[] certificates = null;

        if (sslHandler != null)
        {
            try
            {
                certificates = sslHandler.engine()
                                         .getSession()
                                         .getPeerCertificates();
            }
            catch (SSLPeerUnverifiedException e)
            {
                logger.debug("Failed to get peer certificates for peer {}", channel().remoteAddress(), e);
            }
        }
        return certificates;
    }
}
