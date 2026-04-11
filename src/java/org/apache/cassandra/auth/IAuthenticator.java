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

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.AuthenticateMessage;

public interface IAuthenticator
{
    /**
     * Whether the authenticator requires explicit login.
     * If false will instantiate user with AuthenticatedUser.ANONYMOUS_USER.
     */
    boolean requireAuthentication();

    /**
     * Whether the authenticator supports 'early' authentication, meaning that it can be authenticated without
     * the need to send an AUTHENTICATE request to the client after receiveing a STARTUP message from the client.
     * <p>
     * An example use case of this would be if the client could be authenticated using certificates present
     * on a TLS-encrypted connection, as is done in {@link MutualTlsAuthenticator}.  In this case, an AUTHENTICATE
     * message is not needed because the client can be identified by information present in its provided certificate.
     * <p>
     * If an authenticator supports requires early authentication, this should be <code>true</code>;
     * otherwise if a client cannot authenticate using its connection details (e.g. a certificate), an AUTHENTICATE
     * will be sent to the client. This may confuse a driver implementation into thinking an authenticator is needed,
     * when instead the real issue is that something is missing on the connection, such as a certificate.
     * <p>
     * If <code>false</code> (default behavior), an AUTHENTICATE request will be sent to the client.
     */
    default boolean supportsEarlyAuthentication()
    {
        return false;
    }

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
     * <p>
     * For example, use this method to create any required keyspaces/column families.
     */
    void setup();

    /**
     * Allows custom authenticators to return their own {@link AuthenticateMessage} based on
     * {@link ClientState} information. For example, this allows returning the FQCN of a driver's
     * known authenticator (e.g. "com.datastax.bdp.cassandra.auth.DseAuthenticator") to enable
     * SASL scheme negotiation.
     */
    default AuthenticateMessage getAuthenticateMessage(ClientState clientState)
    {
        return new AuthenticateMessage(getClass().getName());
    }

    /**
     * Provide a SASL handler to perform authentication for an single connection. SASL
     * is a stateful protocol, so a new instance must be used for each authentication
     * attempt.
     *
     * @param clientAddress the IP address of the client whom we wish to authenticate, or null
     *                      if an internal client (one not connected over the remote transport).
     * @return org.apache.cassandra.auth.IAuthenticator.SaslNegotiator implementation
     * (see {@link org.apache.cassandra.auth.PasswordAuthenticator.PlainTextSaslAuthenticator})
     */
    SaslNegotiator newSaslNegotiator(InetAddress clientAddress);

    /**
     * Provide a SASL handler to perform authentication for an single connection. SASL
     * is a stateful protocol, so a new instance must be used for each authentication
     * attempt. This method accepts certificates as well. Authentication strategies can
     * override this method to gain access to client's certificate chain, if present.
     *
     * @param clientAddress the IP address of the client whom we wish to authenticate, or null
     *                      if an internal client (one not connected over the remote transport).
     * @param certificates  the peer's Certificate chain, if present.
     *                      It is expected that these will all be instances of {@link java.security.cert.X509Certificate},
     *                      but we pass them as the base {@link Certificate} in case future implementations leverage
     *                      other certificate types.
     * @return org.apache.cassandra.auth.IAuthenticator.SaslNegotiator implementation
     * (see {@link org.apache.cassandra.auth.PasswordAuthenticator.PlainTextSaslAuthenticator})
     */
    default SaslNegotiator newSaslNegotiator(InetAddress clientAddress, Certificate[] certificates)
    {
        return newSaslNegotiator(clientAddress);
    }

    /**
     * @return The supported authentication 'modes' of this authenticator scheme.
     * <p>
     * This is currently only used for registering metrics tied to authentication by mode.
     */
    default Set<AuthenticationMode> getSupportedAuthenticationModes()
    {
        return Collections.emptySet();
    }

    /**
     * A legacy method that is still used by JMX authentication.
     * <p>
     * You should implement this for having JMX authentication through your
     * authenticator.
     * <p>
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
         * Evaluates the client response data and generates a byte[] response which may be a further challenge or purely
         * informational in the case that the negotiation is completed on this round.
         * <p>
         * This method is called each time a {@link org.apache.cassandra.transport.messages.AuthResponse} is received
         * from a client. After it is called, {@link #isComplete()} is checked to determine whether the negotiation has
         * finished. If so, an AuthenticatedUser is obtained by calling {@link #getAuthenticatedUser()} and that user
         * associated with the active connection and the byte[] sent back to the client via an
         * {@link org.apache.cassandra.transport.messages.AuthSuccess} message. If the negotiation is not yet complete,
         * the byte[] is returned to the client as a further challenge in an
         * {@link org.apache.cassandra.transport.messages.AuthChallenge} message. This continues until the negotiation
         * does complete or an error is encountered.
         *
         * @param clientResponse The non-null (but possibly empty) response sent by the client
         * @return The possibly null response to send to the client.
         * @throws AuthenticationException see {@link javax.security.sasl.SaslServer#evaluateResponse(byte[])}
         */
        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException;

        /**
         * Called after each invocation of {@link #evaluateResponse(byte[])} to determine whether the  authentication has
         * completed successfully or should be continued.
         *
         * @return true if the authentication exchange has completed; false otherwise.
         * see {@link javax.security.sasl.SaslServer#isComplete()}
         */
        public boolean isComplete();

        /**
         * Following a sucessful negotiation, get the AuthenticatedUser representing the logged in subject.
         * This method should only be called if {@link #isComplete()} returns true.
         * Should never return null - always throw AuthenticationException instead.
         * Returning AuthenticatedUser.ANONYMOUS_USER is an option if authentication is not required.
         *
         * @return non-null representation of the authenticated subject
         * @throws AuthenticationException
         */
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException;

        /**
         * Whether it is determined that an AUTHENTICATE message should be sent to properly negotiate.
         * This is used in conjunction with {@link #supportsEarlyAuthentication()} in determining if authentication
         * can be done early, for example if the client can be authenticated using client certificates when handling
         * a STARTUP message.
         * <p>
         * If <code>true</code> (default behavior), an AUTHENTICATE message will be sent in response to a STARTUP,
         * otherwise {@link #evaluateResponse(byte[])} will be called with an empty byte array when handling STARTUP.
         */
        default boolean shouldSendAuthenticateMessage()
        {
            return true;
        }

        /**
         * @return The assumed mode of authentication attempted using this negotiator, this will usually be some value
         * of {@link AuthenticationMode#toString()}} unless an implementor provides their own custom authentication
         * scheme.
         */
        default AuthenticationMode getAuthenticationMode()
        {
            return AuthenticationMode.UNAUTHENTICATED;
        }
    }

    /**
     * Known modes of authentication supported by Cassandra's provided {@link IAuthenticator} implementations.
     */
    abstract class AuthenticationMode
    {
        private final String displayName;

        /**
         * @param displayName How this mode should be displayed in tooling and JMX beans.  Note that it is desirable
         *                    for this name to not have spaces as it may not work well with tooling around JMX.
         */
        public AuthenticationMode(@Nonnull String displayName)
        {
            this.displayName = displayName;
        }

        /**
         * User was not authenticated in any particular way.
         */
        public static final AuthenticationMode UNAUTHENTICATED = new AuthenticationMode("Unauthenticated") {};

        /**
         * User authenticated using a password.
         */
        public static final AuthenticationMode PASSWORD = new AuthenticationMode("Password") {};

        /**
         * User authenticated using a trusted identity in their client certificate.
         */
        public static final AuthenticationMode MTLS = new AuthenticationMode("MutualTls") {};

        @Override
        public String toString()
        {
            return displayName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AuthenticationMode that = (AuthenticationMode) o;
            return displayName.equals(that.displayName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(displayName);
        }
    }
}
