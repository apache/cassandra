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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.apache.cassandra.config.YamlConfigurationLoaderTest.load;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AuthConfigTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testNewInstanceForMutualTlsInternodeAuthenticator() throws IOException, CertificateException
    {
        Config config = load("cassandra-mtls.yaml");
        config.internode_authenticator.class_name = "org.apache.cassandra.auth.MutualTlsInternodeAuthenticator";
        config.internode_authenticator.parameters = Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator");
        config.server_encryption_options = config.server_encryption_options.withOutboundKeystore("test/conf/cassandra_ssl_test_outbound.keystore")
                                                                           .withOutboundKeystorePassword("cassandra");
        DatabaseDescriptor.setConfig(config);
        MutualTlsInternodeAuthenticator authenticator = ParameterizedClass.newInstance(config.internode_authenticator,
                                                                                       Arrays.asList("", "org.apache.cassandra.auth."));

        InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");

        Certificate[] authorizedCertificates = loadCertificateChain("auth/SampleMtlsClientCertificate.pem");
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), authorizedCertificates, INBOUND));

        Certificate[] unauthorizedCertificates = loadCertificateChain("auth/SampleUnauthorizedMtlsClientCertificate.pem");
        assertFalse(authenticator.authenticate(address.getAddress(), address.getPort(), unauthorizedCertificates, INBOUND));
    }

    @Test
    public void testNewInstanceForMutualTlsWithPasswordFallbackAuthenticator()
    {
        Config config = load("cassandra-mtls.yaml");
        config.client_encryption_options.applyConfig();
        config.authenticator.class_name = "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator";
        config.authenticator.parameters = Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator");
        DatabaseDescriptor.setConfig(config);
        MutualTlsWithPasswordFallbackAuthenticator authenticator = ParameterizedClass.newInstance(config.authenticator,
                                                                                                  Arrays.asList("", "org.apache.cassandra.auth."));
        assertNotNull(authenticator);
        unregisterIdentitesCache();
    }

    @Test
    public void testNewInstanceForMutualTlsAuthenticator() throws IOException, CertificateException
    {
        Config config = load("cassandra-mtls.yaml");
        config.client_encryption_options.applyConfig();
        DatabaseDescriptor.setConfig(config);
        MutualTlsAuthenticator authenticator = ParameterizedClass.newInstance(config.authenticator,
                                                                              Arrays.asList("", "org.apache.cassandra.auth."));
        assertNotNull(authenticator);
        unregisterIdentitesCache();
    }

    private void unregisterIdentitesCache()
    {
        MBeanWrapper.instance.unregisterMBean("org.apache.cassandra.auth:type=IdentitiesCache");
    }
}
