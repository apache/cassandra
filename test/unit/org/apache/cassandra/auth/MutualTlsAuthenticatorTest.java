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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.auth.AuthTestUtils.getMockInetAddress;
import static org.apache.cassandra.auth.AuthTestUtils.initializeIdentityRolesTable;
import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.apache.cassandra.config.EncryptionOptions.ClientAuth.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


@RunWith(Parameterized.class)
public class MutualTlsAuthenticatorTest
{
    @Parameterized.Parameter(0)
    public String certificatePath;
    @Parameterized.Parameter(1)
    public String identity;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return Collections.singletonList(new Object[]{ "auth/SampleMtlsClientCertificate.pem", "spiffe://testdomain.com/testIdentifier/testValue" });
    }

    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
        MessagingService.instance().waitUntilListeningUnchecked();
        StorageService.instance.initServer();
        ((CassandraRoleManager)DatabaseDescriptor.getRoleManager()).loadIdentityStatement();
        final Config config = DatabaseDescriptor.getRawConfig();
        config.client_encryption_options = config.client_encryption_options.withEnabled(true)
                                                                           .withRequireClientAuth(REQUIRED);
    }

    @After
    public void after() throws IOException, TimeoutException
    {
        MBeanWrapper.instance.unregisterMBean("org.apache.cassandra.auth:type=IdentitiesCache", MBeanWrapper.OnException.IGNORE);
        StorageService.instance.truncate(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.IDENTITY_TO_ROLES);
    }

    String getValidatorClass()
    {
        return "org.apache.cassandra.auth.SpiffeCertificateValidator";
    }

    @Test
    public void testAuthorizedUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeIdentityRolesTable(identity);
        Certificate[] chain = loadCertificateChain(certificatePath);

        // Verify authenticated user is as expected
        IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        AuthenticatedUser authenticatedUser = saslNegotiator.getAuthenticatedUser();
        assertNotNull(authenticatedUser);
        assertEquals("readonly_user", authenticatedUser.getName());
    }

    @Test
    public void testUnauthorizedUsers() throws CertificateException, IOException, TimeoutException
    {
        // As identity of certificate is not added to identity_role_table, connection should fail
        Certificate[] chain = loadCertificateChain(certificatePath);
        IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        expectedException.expect(AuthenticationException.class);
        expectedException.expectMessage(String.format("Certificate identity '%s' not authorized", identity));
        saslNegotiator.getAuthenticatedUser();
    }

    @Test
    public void testInvalidUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeIdentityRolesTable(identity);
        Certificate[] clientCertificates = loadCertificateChain("auth/SampleInvalidCertificate.pem");
        IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), clientCertificates);
        expectedException.expect(AuthenticationException.class);
        expectedException.expectMessage("Unable to extract Spiffe from the certificate");
        saslNegotiator.getAuthenticatedUser();
    }

    @Test
    public void testChangeInValidUrns() throws CertificateException, IOException, TimeoutException
    {
        DatabaseDescriptor.setCredentialsValidity(10);
        initializeIdentityRolesTable(identity);
        Certificate[] chain = loadCertificateChain(certificatePath);
        IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        assertEquals("readonly_user", saslNegotiator.getAuthenticatedUser().getName());
        // following call truncates identity table. After removing the identity of certificate, we should get
        // authentication exception
        initializeIdentityRolesTable("another_id");
        expectedException.expect(AuthenticationException.class);
        expectedException.expectMessage(String.format("Certificate identity '%s' not authorized", identity));
        saslNegotiator.getAuthenticatedUser();
    }

    @Test
    public void testValidatorClassNameIsNotSet()
    {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("authenticator.parameters.validator_class_name is not set");
        new MutualTlsAuthenticator(Collections.emptyMap());
    }

    @Test
    public void testInvalidMaxCertificateAge()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid duration: not-a-valid-input Accepted units:[MINUTES, HOURS, DAYS] where case matters and only non-negative values.");

        Map<String, String> parameters = new HashMap<>();
        parameters.put("validator_class_name", getValidatorClass());
        parameters.put("max_certificate_age", "not-a-valid-input");
        new MutualTlsAuthenticator(parameters);
    }

    @Test
    public void testNegativeMaxCertificateAge()
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Invalid duration: -2d Accepted units:[MINUTES, HOURS, DAYS] where case matters and only non-negative values.");

        Map<String, String> parameters = new HashMap<>();
        parameters.put("validator_class_name", getValidatorClass());
        parameters.put("max_certificate_age", "-2d");
        new MutualTlsAuthenticator(parameters);
    }

    @Test
    public void testValidMaxCertificateAgeConfiguration()
    {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("validator_class_name", getValidatorClass());

        List<Pair<String, Integer>> testCases = List.of(Pair.create("2d", (int) TimeUnit.DAYS.toMinutes(2)),
                                                        Pair.create("1m", 1),
                                                        Pair.create("365d", (int) TimeUnit.DAYS.toMinutes(365)));

        for (Pair<String, Integer> testCase : testCases)
        {
            parameters.put("max_certificate_age", testCase.left);
            MutualTlsAuthenticator authenticator = new MutualTlsAuthenticator(parameters);
            assertEquals(String.format("max_certificate_age=%s is expected to parse %d minutes", testCase.left, testCase.right),
                         testCase.right.intValue(), authenticator.maxCertificateAgeMinutes);
            MBeanWrapper.instance.unregisterMBean("org.apache.cassandra.auth:type=IdentitiesCache");
        }
    }

    @Test
    public void testAddingAndRemovingIdentitiesToTableReflectsInCache() throws IOException, TimeoutException
    {
        DatabaseDescriptor.setCredentialsValidity(10);
        String identity1 = "id1";
        String identity2 = "id2";

        initializeIdentityRolesTable(identity1);
        MutualTlsAuthenticator.IdentityCache urnCache = new MutualTlsAuthenticator.IdentityCache();
        assertEquals("readonly_user", urnCache.get(identity1));

        initializeIdentityRolesTable(identity2);
        assertNull(urnCache.get(identity1));
        assertEquals("readonly_user", urnCache.get(identity2));
    }

    @Test
    public void testValidateConfiguration()
    {
        // In strict mTLS mode mtls authenticator should check for require_client_auth to be true
        final Config config = DatabaseDescriptor.getRawConfig();
        String msg = "MutualTlsAuthenticator requires client_encryption_options.enabled to be true" +
                     " & client_encryption_options.require_client_auth to be true";
        MutualTlsAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();

        config.client_encryption_options = config.client_encryption_options.withEnabled(true)
                                                                           .withRequireClientAuth(EncryptionOptions.ClientAuth.NOT_REQUIRED);
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage(msg);
        mutualTlsAuthenticator.validateConfiguration();
    }

    MutualTlsAuthenticator createAndInitializeMtlsAuthenticator()
    {
        Map<String, String> parameters = Collections.singletonMap("validator_class_name", getValidatorClass());
        MutualTlsAuthenticator mutualTlsAuthenticator = new MutualTlsAuthenticator(parameters);
        mutualTlsAuthenticator.setup();
        return mutualTlsAuthenticator;
    }
}
