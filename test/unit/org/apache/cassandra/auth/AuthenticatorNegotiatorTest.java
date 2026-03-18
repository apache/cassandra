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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.auth.AuthCache.MBEAN_NAME_BASE;
import static org.apache.cassandra.config.YamlConfigurationLoaderTest.load;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests the authenticator negotiation logic in {@link AuthenticatorNegotiator}.
 */
public class AuthenticatorNegotiatorTest
{
    private static final String IDENTITIES_CACHE_MBEAN = MBEAN_NAME_BASE + MutualTlsAuthenticator.CACHE_NAME;
    private static final String CREDENTIALS_CACHE_MBEAN = MBEAN_NAME_BASE + PasswordAuthenticator.CredentialsCacheMBean.CACHE_NAME;

    @Before
    public void setup()
    {
        AuthConfig.reset();
    }

    @After
    public void teardown()
    {
        unregisterCaches();
    }

    private void unregisterCaches()
    {
        try
        {
            org.apache.cassandra.utils.MBeanWrapper.instance.unregisterMBean(IDENTITIES_CACHE_MBEAN);
        }
        catch (Exception ignored) {}

        try
        {
            org.apache.cassandra.utils.MBeanWrapper.instance.unregisterMBean(CREDENTIALS_CACHE_MBEAN);
        }
        catch (Exception ignored) {}
    }

    // Server will use the default authenticator if the client doesn't provide any options.
    @Test
    public void testEmptyClientAuthenticators()
    {
        Config config = load("cassandra-auth-negotiation.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(() -> config);

        IAuthenticator result = AuthenticatorNegotiator.negotiateAuthenticator(Collections.emptySet());

        assertSame(DatabaseDescriptor.getDefaultAuthenticator(), result);
    }

    // Server supports MTLS, Password and AllowAll. Client supports MTLS and Password. Server should pick MTLS
    // as the most preferred shared preference.
    @Test
    public void testMatchesServersPreferredAuthenticator()
    {
        Config config = load("cassandra-auth-negotiation.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(() -> config);

        Set<String> clientModes = Set.of("MutualTls", "Password");
        IAuthenticator result = AuthenticatorNegotiator.negotiateAuthenticator(clientModes);

        assertTrue(result instanceof MutualTlsAuthenticator);
    }

    // Server supports MTLS, Password and AllowAll. Client supports Password or no-auth. Server should choose Password
    // auth as it's the most preferred option of the ones the client can support (even though the server would prefer
    // MTLS).
    @Test
    public void testMatchesServersAcceptedAuthenticator()
    {
        Config config = load("cassandra-auth-negotiation.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(() -> config);

        Set<String> clientModes = Set.of("Password", "Unauthenticated");
        IAuthenticator result = AuthenticatorNegotiator.negotiateAuthenticator(clientModes);

        // Should return Password authenticator
        assertTrue(result instanceof PasswordAuthenticator);
    }

    // Server supports MTLS, Password and AllowAll. Client supports Kerberos, JWT and OAuth. Since the server and
    // client don't appear able to support any common authentication scheme, the server will offer its default
    // authenticator and hope the client can work with it.
    @Test
    public void testNoMatchingAuthenticatorUsesDefaultAuthenticator()
    {
        Config config = load("cassandra-auth-negotiation.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(() -> config);

        Set<String> clientModes = Set.of("Kerberos", "JWT", "OAuth");
        IAuthenticator result = AuthenticatorNegotiator.negotiateAuthenticator(clientModes);

        assertSame(DatabaseDescriptor.getDefaultAuthenticator(), result);
    }

    // Server supports MTLS, Password and AllowAll. Client supports Password and MTLS but doesn't agree on
    // case (for whatever reason). Server should correctly settle on MTLS regardless.
    @Test
    public void testCaseInsensitiveMatching()
    {
        Config config = load("cassandra-auth-negotiation.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(() -> config);

        Set<String> clientModes = Set.of("password", "MUTUALTLS");
        IAuthenticator result = AuthenticatorNegotiator.negotiateAuthenticator(clientModes);

        assertTrue(result instanceof MutualTlsAuthenticator);
    }

    // Server is not configured to support negotiation. Client attempts to offer authentication options anyway.
    // Server should simplu respond with its default authenticator (password auth).
    @Test
    public void testNegotiationDisabled()
    {
        Config config = load("cassandra-passwordauth.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(() -> config);

        Set<String> clientModes = Set.of("MutualTls");
        IAuthenticator result = AuthenticatorNegotiator.negotiateAuthenticator(clientModes);

        assertSame(DatabaseDescriptor.getDefaultAuthenticator(), result);
    }

    // Server supports MTLS, Password and AllowAll. Client supports no-auth, Password and MTLS. Server should
    // select its most preferred option (MTLS) even though it's not the first option offered by the client.
    @Test
    public void testPriorityOrderWithMultipleMatches()
    {
        Config config = load("cassandra-auth-negotiation.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(() -> config);

        Set<String> clientModes = Set.of("Unauthenticated", "Password", "MutualTls");
        IAuthenticator result = AuthenticatorNegotiator.negotiateAuthenticator(clientModes);

        assertTrue(result instanceof MutualTlsAuthenticator);
    }

    // Client sends duplicate authentication modes including case variations. Server should handle this gracefully
    // and select based on its priority order, not be confused by the duplicates.
    @Test
    public void testDuplicateClientAuthenticators()
    {
        Config config = load("cassandra-auth-negotiation.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(() -> config);

        Set<String> clientModes = Set.of("Password", "MutualTls", "password");
        IAuthenticator result = AuthenticatorNegotiator.negotiateAuthenticator(clientModes);

        assertTrue(result instanceof MutualTlsAuthenticator);
    }
}
