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

import java.io.InputStream;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collections;
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.auth.AuthTestUtils.getMockInetAddress;
import static org.junit.Assert.assertNotNull;
import static org.psjava.util.AssertStatus.assertTrue;

public class MutualTlsWithPasswordFallbackAuthenticatorTest
{
    private static MutualTlsWithPasswordFallbackAuthenticator fallbackAuthenticator;
    private Certificate[] clientCertificatesCorp;

    @BeforeClass
    public static void initialize()
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.initServer(0);
        Config config = DatabaseDescriptor.getRawConfig();
        config.client_encryption_options = config.client_encryption_options.withEnabled(true)
                                                                           .withRequireClientAuth(true);
        Map<String, String> parameters = Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator");
        fallbackAuthenticator = new MutualTlsWithPasswordFallbackAuthenticator(parameters);
        fallbackAuthenticator.setup();
    }

    @Before
    public void setup() throws CertificateException
    {
        InputStream inputStreamCorp = getClass().getClassLoader().getResourceAsStream("auth/SampleMtlsClientCertificate.pem");
        assertNotNull(inputStreamCorp);
        Certificate corpCertificate = CertificateFactory.getInstance("X.509").generateCertificate(inputStreamCorp);
        clientCertificatesCorp = new Certificate[]{ corpCertificate };
    }

    @Test
    public void testFallbackToPasswordAuthentication() throws UnknownHostException
    {
        // If client certificate chain is not present fallback to password authentication
        IAuthenticator.SaslNegotiator passwordNegotiator = fallbackAuthenticator.newSaslNegotiator(getMockInetAddress());
        assertTrue(passwordNegotiator instanceof PasswordAuthenticator.PlainTextSaslAuthenticator);

        // If client certificate chain is null fallback to password authentication
        IAuthenticator.SaslNegotiator passwordNegotiator1 = fallbackAuthenticator.newSaslNegotiator(getMockInetAddress(), null);
        assertTrue(passwordNegotiator1 instanceof PasswordAuthenticator.PlainTextSaslAuthenticator);

        // If client certificate chain length is zero fallback to password authentication
        IAuthenticator.SaslNegotiator passwordNegotiator2 = fallbackAuthenticator.newSaslNegotiator(getMockInetAddress(), new Certificate[0]);
        assertTrue(passwordNegotiator2 instanceof PasswordAuthenticator.PlainTextSaslAuthenticator);
    }

    @Test
    public void testUsesMtlsAuthenticationWhenCertificatesPresent() throws UnknownHostException
    {
        // If client certificate chain present and valid use mTLS authentication
        IAuthenticator.SaslNegotiator mutualtlsAuthenticator = fallbackAuthenticator.newSaslNegotiator(getMockInetAddress(), clientCertificatesCorp);
        assertTrue(mutualtlsAuthenticator instanceof MutualTlsAuthenticator.CertificateNegotiator);
    }
}
