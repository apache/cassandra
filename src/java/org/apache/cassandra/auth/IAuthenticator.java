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

import java.util.Map;
import java.util.Set;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;

public interface IAuthenticator
{
    /**
     * Whether or not the authenticator requires explicit login.
     * If false will instantiate user with AuthenticatedUser.ANONYMOUS_USER.
     */
    boolean requireAuthentication();

     /**
     * Set of resources that should be made inaccessible to users and only accessible internally.
     *
     * @return Keyspaces, column families that will be unmodifiable by users; other resources.
     */
    Set<? extends IResource> protectedResources();

    /**
     * Validates configuration of IAuthenticator implementation (if configurable).
     *
     * @throws ConfigurationException when there is a configuration error.
     */
    void validateConfiguration() throws ConfigurationException;

    /**
     * Setup is called once upon system startup to initialize the IAuthenticator.
     *
     * For example, use this method to create any required keyspaces/column families.
     */
    void setup();

    /**
     * Provide a SASL handler to perform authentication for an single connection. SASL
     * is a stateful protocol, so a new instance must be used for each authentication
     * attempt.
     * @return org.apache.cassandra.auth.IAuthenticator.SaslNegotiator implementation
     * (see {@link org.apache.cassandra.auth.PasswordAuthenticator.PlainTextSaslAuthenticator})
     */
    SaslNegotiator newSaslNegotiator();

    /**
     * For implementations which support the Thrift login method that accepts arbitrary
     * key/value pairs containing credentials data.
     * Also used by CQL native protocol v1, in which username and password are sent from
     * client to server in a {@link org.apache.cassandra.transport.messages.CredentialsMessage}
     * Implementations where support for Thrift and CQL protocol v1 is not required should make
     * this an unsupported operation.
     *
     * Should never return null - always throw AuthenticationException instead.
     * Returning AuthenticatedUser.ANONYMOUS_USER is an option as well if authentication is not required.
     *
     * @param credentials implementation specific key/value pairs
     * @return non-null representation of the authenticated subject
     * @throws AuthenticationException
     */
    AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException;

    /**
     * Performs the actual SASL negotiation for a single authentication attempt.
     * SASL is stateful, so a new instance should be used for each attempt.
     * Non-trivial implementations may delegate to an instance of {@link javax.security.sasl.SaslServer}
     */
    public interface SaslNegotiator
    {
        /**
         * Evaluates the client response data and generates a byte[] reply which may be a further challenge or purely
         * informational in the case that the negotiation is completed on this round.
         *
         * This method is called each time a {@link org.apache.cassandra.transport.messages.AuthResponse} is received
         * from a client. After it is called, {@link isComplete()} is checked to determine whether the negotiation has
         * finished. If so, an AuthenticatedUser is obtained by calling {@link getAuthenticatedUser()} and that user
         * associated with the active connection and the byte[] sent back to the client via an
         * {@link org.apache.cassandra.transport.messages.AuthSuccess} message. If the negotiation is not yet complete,
         * the byte[] is returned to the client as a further challenge in an
         * {@link org.apache.cassandra.transport.messages.AuthChallenge} message. This continues until the negotiation
         * does complete or an error is encountered.
         *
         * @param clientResponse The non-null (but possibly empty) response sent by the client
         * @return The possibly null response to send to the client.
         * @throws AuthenticationException
         * see {@link javax.security.sasl.SaslServer#evaluateResponse(byte[])}
         */
        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException;

        /**
         * Called after each invocation of {@link evaluateResponse(byte[])} to determine whether the  authentication has
         * completed successfully or should be continued.
         *
         * @return true if the authentication exchange has completed; false otherwise.
         * see {@link javax.security.sasl.SaslServer#isComplete()}
         */
        public boolean isComplete();

        /**
         * Following a sucessful negotiation, get the AuthenticatedUser representing the logged in subject.
         * This method should only be called if {@link isComplete()} returns true.
         * Should never return null - always throw AuthenticationException instead.
         * Returning AuthenticatedUser.ANONYMOUS_USER is an option if authentication is not required.
         *
         * @return non-null representation of the authenticated subject
         * @throws AuthenticationException
         */
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException;
    }
}
