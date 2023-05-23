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
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.OUTBOUND;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MutualTlsInternodeAuthenticatorTest
{
    private static final String VALIDATOR_CLASS_NAME = "validator_class_name";
    private static final String TRUSTED_PEER_IDENTITIES = "trusted_peer_identities";
    private static final String NODE_IDENTITY = "node_identity";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Parameterized.Parameter(0)
    public String certificatePath;
    @Parameterized.Parameter(1)
    public String identity;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return Collections.singletonList(new Object[]{ "auth/SampleMtlsClientCertificate.pem", "spiffe://testdomain.com/testIdentifier/testValue" });
    }

    @BeforeClass
    public static void initialize()
    {
        CASSANDRA_CONFIG.setString("cassandra-mtls.yaml");
        SchemaLoader.loadSchema();
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.initServer(0);
    }

    @Before
    public void before()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        config.server_encryption_options = config.server_encryption_options.withOutboundKeystore("test/conf/cassandra_ssl_test_outbound.keystore")
                                                                           .withOutboundKeystorePassword("cassandra");
    }

    String getValidatorClass()
    {
        return "org.apache.cassandra.auth.SpiffeCertificateValidator";
    }

    @Test
    public void testAuthenticateWithoutCertificatesShouldThrowUnsupportedOperation() throws UnknownHostException
    {
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(getParams());
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("mTLS Authenticator only supports certificate based authenticate method");
        authenticator.authenticate(address.getAddress(), address.getPort());
    }

    @Test
    public void testAuthenticationOfOutboundConnectionsShouldBeSuccess() throws UnknownHostException
    {
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(getParams());
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), new Certificate[0], OUTBOUND));
    }

    @Test
    public void testAuthorizedUsersTrustedPeersConfigured() throws UnknownHostException, CertificateException
    {
        Map<String, String> params = new HashMap<>(getParams());
        params.put(TRUSTED_PEER_IDENTITIES, "spiffe://testdomain.com/testIdentifier/testValue");
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(params);
        Certificate[] clientCertificates = loadCertificateChain(certificatePath);
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), clientCertificates, INBOUND));
    }

    @Test
    public void testAuthorizedUsersTrustedPeersNotConfigured() throws IOException, CertificateException
    {
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(getParams());
        Certificate[] clientCertificates = loadCertificateChain(certificatePath);
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), clientCertificates, INBOUND));
    }

    @Test
    public void testNodeIdentityMismatch()
    {
        Map<String, String> params = new HashMap<>(getParams());
        params.put(NODE_IDENTITY, "id1");
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("Configured node identity is not matching identity extracted" +
                                        "from the keystore");
        new MutualTlsInternodeAuthenticator(params);
    }

    @Test
    public void testUnauthorizedUser() throws IOException, CertificateException, TimeoutException
    {
        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        Map<String, String> parameters = getParams();
        IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(parameters);
        Certificate[] clientCertificates = loadCertificateChain("auth/SampleUnauthorizedMtlsClientCertificate.pem");
        assertFalse(authenticator.authenticate(address.getAddress(), address.getPort(), clientCertificates, INBOUND));
    }

    @Test
    public void testNoValidatorClassNameInConfig()
    {
        Map<String, String> parameters = new HashMap<>(getParams());
        parameters.put(VALIDATOR_CLASS_NAME, null);
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("internode_authenticator.parameters.validator_class_name is not set");
        new MutualTlsInternodeAuthenticator(parameters);
    }


    @Test
    public void testNoIdentitiesInKeystore()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        config.server_encryption_options = config.server_encryption_options.withOutboundKeystore("test/conf/cassandra_ssl_test.keystore")
                                                                           .withOutboundKeystorePassword("cassandra");
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("No identity was extracted from the outbound keystore 'test/conf/cassandra_ssl_test.keystore'");
        new MutualTlsInternodeAuthenticator(getParams());
    }

    @Test
    public void testGetIdentitiesFromKeystore()
    {
        List<String> identities = new MutualTlsInternodeAuthenticator(getParams()).getIdentitiesFromKeyStore("test/conf/cassandra_ssl_test_outbound.keystore", "cassandra", "JKS");
        assertFalse(identities.isEmpty());
        assertTrue(identities.contains(identity));
    }

    Map<String, String> getParams()
    {
        return Collections.singletonMap(VALIDATOR_CLASS_NAME, getValidatorClass());
    }
}
