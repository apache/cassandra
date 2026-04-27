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
package org.apache.cassandra.auth;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Implements the business logic for selecting an authenticator for a given session, when both the node and the
 * client support authenticator negotiation.
 */
public class AuthenticatorNegotiator
{
    private static final Logger logger = LoggerFactory.getLogger(AuthenticatorNegotiator.class);

    /**
     * Selects an {@link IAuthenticator authenticator} based on client-provided authentication modes and this
     * node's own prioritized list of supported authenticators.
     *
     * @param clientAuthenticators Comma-separated list of {@link IAuthenticator.AuthenticationMode} names supported
     *                             by the client. May be null or empty if the client doesn't support or is not
     *                             configured for negotiation
     * @return The node's most preferred authenticator that supports at least one of the client's offered authentication
     *         modes, or its default authenticator if no modes are provided or none match.
     */
    public static IAuthenticator negotiateAuthenticator(@Nonnull Set<String> clientAuthenticators)
    {
        Set<IAuthenticator.AuthenticationMode> clientAuthenticationModes =
            clientAuthenticators.stream()
                                .map(name -> new IAuthenticator.AuthenticationMode(name) {})
                                .collect(Collectors.toSet());

        for (IAuthenticator authenticator : DatabaseDescriptor.getNegotiableAuthenticators())
        {
            if (!Collections.disjoint(clientAuthenticationModes, authenticator.getSupportedAuthenticationModes()))
            {
                logger.info("Negotiated authenticator with client with options {}: selected {}",
                            clientAuthenticationModes, authenticator.getClass().getName());
                return authenticator;
            }
        }

        logger.info("Auth negotiation failed for client options {}: continuing with default authenticator", clientAuthenticators);

        return DatabaseDescriptor.getDefaultAuthenticator();
    }
}
