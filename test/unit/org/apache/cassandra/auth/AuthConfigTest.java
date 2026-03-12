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

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.auth.AuthCache.MBEAN_NAME_BASE;
import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.apache.cassandra.config.YamlConfigurationLoaderTest.load;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests instantiation of various authenticators through AuthConfig, and the accessibility of the configured
 * authenticators and role manager options through DatabaseDescriptor.
 */
public class AuthConfigTest
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

    @Test
    public void testNewInstanceForMutualTlsInternodeAuthenticator() throws IOException, CertificateException
    {
        Config config = load("cassandra-mtls.yaml");
        config.internode_authenticator.class_name = "org.apache.cassandra.auth.MutualTlsInternodeAuthenticator";
        config.internode_authenticator.parameters = Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator");
        DatabaseDescriptor.unsafeDaemonInitialization(()->config);

        MutualTlsInternodeAuthenticator authenticator = ParameterizedClass.newInstance(config.internode_authenticator,
                                                                                       Arrays.asList("", "org.apache.cassandra.auth."));
        assertNotNull(authenticator);

        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");

        Certificate[] authorizedCertificates = loadCertificateChain("auth/SampleMtlsClientCertificate.pem");
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), authorizedCertificates, INBOUND));

        Certificate[] unauthorizedCertificates = loadCertificateChain("auth/SampleUnauthorizedMtlsClientCertificate.pem");
        assertFalse(authenticator.authenticate(address.getAddress(), address.getPort(), unauthorizedCertificates, INBOUND));
        unregisterCaches();
    }

    @Test
    public void testNewInstanceForPasswordAuthenticator()
    {
        Config config = load("cassandra-passwordauth.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(()->config);

        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        assertNotNull(authenticator);

        assertThat(DatabaseDescriptor.getAuthenticator(PasswordAuthenticator.class))
            .isPresent()
            .get()
            .isSameAs(authenticator);
        assertThat(DatabaseDescriptor.getAuthenticator(MutualTlsAuthenticator.class)).isEmpty();
        assertThat(DatabaseDescriptor.getAuthenticator(MutualTlsWithPasswordFallbackAuthenticator.class)).isEmpty();
        assertTrue(DatabaseDescriptor.getRoleManager().supportedOptions().containsAll(CassandraRoleManager.DEFAULT_SUPPORTED_ROLE_OPTIONS));
        assertTrue(DatabaseDescriptor.getRoleManager().supportedOptions().containsAll(PasswordAuthenticator.SUPPORTED_ROLE_OPTIONS));
        assertTrue(DatabaseDescriptor.getRoleManager().alterableOptions().containsAll(CassandraRoleManager.DEFAULT_ALTERABLE_ROLE_OPTIONS));
        assertTrue(DatabaseDescriptor.getRoleManager().alterableOptions().containsAll(PasswordAuthenticator.ALTERABLE_ROLE_OPTIONS));
    }

    @Test
    public void testNewInstanceForMutualTlsWithPasswordFallbackAuthenticator()
    {
        Config config = load("cassandra-mtls.yaml");
        config.authenticator.class_name = "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator";
        config.authenticator.parameters = Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator");
        DatabaseDescriptor.unsafeDaemonInitialization(()->config);

        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        assertNotNull(authenticator);

        // MutualTlsWithPasswordFallbackAuthenticator is-a PasswordAuthenticator, so we expect getAuthenticator to
        // return it when asked to return a PasswordAuthenticator.
        assertThat(DatabaseDescriptor.getAuthenticator(PasswordAuthenticator.class))
            .isPresent()
            .get()
            .isSameAs(authenticator);
        assertThat(DatabaseDescriptor.getAuthenticator(MutualTlsAuthenticator.class)).isEmpty();
        assertThat(DatabaseDescriptor.getAuthenticator(MutualTlsWithPasswordFallbackAuthenticator.class))
            .isPresent()
            .get()
            .isSameAs(authenticator);

        // Similarly, we expect the same role options as for PasswordAuthenticator.
        assertTrue(DatabaseDescriptor.getRoleManager().supportedOptions().containsAll(CassandraRoleManager.DEFAULT_SUPPORTED_ROLE_OPTIONS));
        assertTrue(DatabaseDescriptor.getRoleManager().supportedOptions().containsAll(PasswordAuthenticator.SUPPORTED_ROLE_OPTIONS));
        assertTrue(DatabaseDescriptor.getRoleManager().alterableOptions().containsAll(CassandraRoleManager.DEFAULT_ALTERABLE_ROLE_OPTIONS));
        assertTrue(DatabaseDescriptor.getRoleManager().alterableOptions().containsAll(PasswordAuthenticator.ALTERABLE_ROLE_OPTIONS));
    }

    @Test
    public void testNewInstanceForMutualTlsAuthenticator()
    {
        Config config = load("cassandra-mtls.yaml");
        DatabaseDescriptor.unsafeDaemonInitialization(()->config);

        IAuthenticator authenticator = DatabaseDescriptor.getAuthenticator();
        assertNotNull(authenticator);

        assertThat(DatabaseDescriptor.getAuthenticator(PasswordAuthenticator.class)).isEmpty();
        assertThat(DatabaseDescriptor.getAuthenticator(MutualTlsAuthenticator.class))
            .isPresent()
            .get()
            .isSameAs(authenticator);
        assertThat(DatabaseDescriptor.getAuthenticator(MutualTlsWithPasswordFallbackAuthenticator.class)).isEmpty();

        assertTrue(DatabaseDescriptor.getRoleManager().supportedOptions().containsAll(CassandraRoleManager.DEFAULT_SUPPORTED_ROLE_OPTIONS));
        assertTrue(DatabaseDescriptor.getRoleManager().supportedOptions().containsAll(authenticator.getSupportedRoleOptions()));
        assertTrue(DatabaseDescriptor.getRoleManager().alterableOptions().containsAll(CassandraRoleManager.DEFAULT_ALTERABLE_ROLE_OPTIONS));
        assertTrue(DatabaseDescriptor.getRoleManager().alterableOptions().containsAll(authenticator.getAlterableRoleOptions()));
    }

    private void unregisterCaches()
    {
        safeUnregisterMbean(IDENTITIES_CACHE_MBEAN);
        safeUnregisterMbean(CREDENTIALS_CACHE_MBEAN);
    }

    private void safeUnregisterMbean(String mbeanName)
    {
        if (MBeanWrapper.instance.isRegistered(mbeanName))
            MBeanWrapper.instance.unregisterMBean(mbeanName);
    }
}
