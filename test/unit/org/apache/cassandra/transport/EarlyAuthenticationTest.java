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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.MutualTlsAuthenticator;
import org.apache.cassandra.auth.SpiffeCertificateValidator;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ReadyMessage;
import org.apache.cassandra.transport.messages.StartupMessage;

import static org.apache.cassandra.auth.AuthTestUtils.addIdentityToRole;
import static org.apache.cassandra.auth.AuthTestUtils.truncateIdentityRolesTable;
import static org.apache.cassandra.transport.messages.StartupMessage.CQL_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.psjava.util.AssertStatus.assertTrue;

/**
 * Verifies the behavior of Cassandra given an authenticator ({@link MutualTlsAuthenticator}) returning
 * {@link IAuthenticator#supportsEarlyAuthentication()} as <code>true</code>.
 */
public class EarlyAuthenticationTest extends CQLTester
{

    // configures server with client encryption enabled.
    static final Consumer<Server.Builder> serverConfigurator = server -> server.withTlsEncryptionPolicy(EncryptionOptions.TlsEncryptionPolicy.ENCRYPTED);

    static final Map<String, String> authenticatorParams = ImmutableMap.of("validator_class_name", SpiffeCertificateValidator.class.getSimpleName());

    @BeforeClass
    public static void setup()
    {
        setupWithAuthenticator(new MutualTlsAuthenticator(authenticatorParams));
    }

    static void setupWithAuthenticator(IAuthenticator authenticator)
    {
        requireNativeProtocolClientEncryption();
        requireNetwork(serverConfigurator, cluster -> {
        });
        requireAuthentication(authenticator);
    }

    @Before
    public void initNetwork() throws IOException, TimeoutException
    {
        truncateIdentityRolesTable();
        AuthCacheService.instance.invalidateCaches();
        AuthCacheService.instance.warmCaches();
        reinitializeNetwork(serverConfigurator, cluster -> {
        });
    }

    private EncryptionOptions clientEncryptionOptions(boolean presentClientCertificate)
    {
        EncryptionOptions encryptionOptions = new EncryptionOptions()
                                              .withEnabled(true)
                                              .withRequireClientAuth(EncryptionOptions.ClientAuth.OPTIONAL)
                                              .withTrustStore(TlsTestUtils.CLIENT_TRUSTSTORE_PATH)
                                              .withTrustStorePassword(TlsTestUtils.CLIENT_TRUSTSTORE_PASSWORD)
                                              .withSslContextFactory(new ParameterizedClass(SimpleClientSslContextFactory.class.getName()));

        if (presentClientCertificate)
        {
            encryptionOptions = encryptionOptions.withKeyStore(TlsTestUtils.CLIENT_SPIFFE_KEYSTORE_PATH)
                             .withStoreType("JKS")
                             .withKeyStorePassword(TlsTestUtils.CLIENT_SPIFFE_KEYSTORE_PASSWORD);
        }

        return new EncryptionOptions(encryptionOptions);
    }

    @Test
    public void testEarlyAuthSuccess()
    {
        // given server is configured with a Mutual TLS Authenticator and the identity in the client's keystore is bound to cassandra.
        addIdentityToRole(TlsTestUtils.CLIENT_SPIFFE_IDENTITY, "cassandra");

        // when connecting, we expect to get a 'READY' message after sending a 'STARTUP' as MutualTlsAuthenticator
        // supports early authentication and the client presented a cert with an identity that was bound to a role.
        testStartupResponse(true, startupResponse -> {
            if (!(startupResponse instanceof ReadyMessage))
            {
                fail("Expected an READY in response to a STARTUP, got: " + startupResponse);
            }
        });
    }

    @Test
    public void testEarlyAuthFailureLoginDisallowed()
    {
        // given server is configured with a Mutual TLS Authenticator and the identity in the client's keystore is bound
        // to a role that is not permitted to log in.
        // when connecting, we expect an 'ERROR' message.
        addIdentityToRole(TlsTestUtils.CLIENT_SPIFFE_IDENTITY, "readonly_user");
        testStartupResponse(true, expectAuthenticationError("readonly_user is not permitted to log in"));
    }

    @Test
    public void testEarlyAuthFailureMissingIdentity()
    {
        // given server is configured with a Mutual TLS Authenticator, but no identities are bound to roles.
        // when connecting, we expect an 'ERROR' message.
        testStartupResponse(true, expectAuthenticationError(String.format("Certificate identity '%s' not authorized", TlsTestUtils.CLIENT_SPIFFE_IDENTITY)));
    }

    @Test
    public void testNoClientCertificatePresented()
    {
        // given server is configured with a Mutual TLS Authenticator, but no client certificate is presented.
        // when connecting, we expect an 'ERROR' message.
        testStartupResponse(false, expectAuthenticationError("No certificate present on connection"));
    }

    public void testStartupResponse(boolean presentClientCertificate, Consumer<Message.Response> testFn)
    {
        SimpleClient.Builder builder = SimpleClient.builder(nativeAddr.getHostAddress(), nativePort)
                                                   .encryption(clientEncryptionOptions(presentClientCertificate));
        try (SimpleClient client = builder.build())
        {
            client.establishConnection();

            // Send a StartupMessage and pass the response to the handling function
            StartupMessage startup = new StartupMessage(ImmutableMap.of(CQL_VERSION, QueryProcessor.CQL_VERSION.toString()));
            Message.Response startupResponse = client.execute(startup, false);
            testFn.accept(startupResponse);
        }
        catch (IOException e)
        {
            fail("Error establishing connection");
        }
    }

    public Consumer<Message.Response> expectAuthenticationError(final String expectedMessage)
    {
        return startupResponse -> {
            if (startupResponse instanceof ErrorMessage)
            {
                ErrorMessage errorMessage = (ErrorMessage) startupResponse;
                assertTrue(errorMessage.error instanceof AuthenticationException, "Expected an AuthenticationException, got: " + errorMessage.error);
                AuthenticationException authException = (AuthenticationException) errorMessage.error;
                assertEquals(expectedMessage, authException.getMessage());
            }
            else
            {
                fail("Expected an ErrorMessage but got: " + startupResponse);
            }
        };
    }

}

