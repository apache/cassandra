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

package org.apache.cassandra.distributed.test.auth;

import java.net.InetAddress;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.audit.InMemoryAuditLogger;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.utils.tls.CertificateBuilder;
import org.apache.cassandra.utils.tls.CertificateBundle;

import static org.apache.cassandra.audit.AuditLogEntryType.LOGIN_ERROR;
import static org.apache.cassandra.audit.AuditLogEntryType.LOGIN_SUCCESS;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_NAME;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_PASSWORD;
import static org.apache.cassandra.transport.TlsTestUtils.SERVER_KEYSTORE_PASSWORD;
import static org.apache.cassandra.transport.TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD;
import static org.apache.cassandra.transport.TlsTestUtils.configureIdentity;
import static org.apache.cassandra.transport.TlsTestUtils.generateClientCertificate;
import static org.apache.cassandra.transport.TlsTestUtils.generateSelfSignedCertificate;
import static org.apache.cassandra.transport.TlsTestUtils.getSSLOptions;
import static org.apache.cassandra.transport.TlsTestUtils.withAuthenticatedSession;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests authentication audit logging events
 */
public class AuthAuditLoggingTest extends TestBaseImpl
{
    public static final String NON_SPIFFE_IDENTITY = "nonspiffe://test.cassandra.apache.org/dTest/mtls";
    public static final String NON_MAPPED_IDENTITY = "spiffe://test.cassandra.apache.org/dTest/notMapped";
    private static ICluster<IInvokableInstance> CLUSTER;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    static CertificateBundle CA;
    static Path truststorePath;
    static SSLOptions sslOptions;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        Cluster.Builder builder = Cluster.build(1).withDynamicPortAllocation(true);

        CA = new CertificateBuilder().subject("CN=Apache Cassandra Root CA, OU=Certification Authority, O=Unknown, C=Unknown")
                                     .alias("fakerootca")
                                     .isCertificateAuthority(true)
                                     .buildSelfSigned();

        truststorePath = CA.toTempKeyStorePath(tempFolder.getRoot().toPath(),
                                               SERVER_TRUSTSTORE_PASSWORD.toCharArray(),
                                               SERVER_TRUSTSTORE_PASSWORD.toCharArray());


        CertificateBundle keystore = new CertificateBuilder().subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                             .addSanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
                                                             .addSanDnsName(InetAddress.getLocalHost().getHostName())
                                                             .buildIssuedBy(CA);

        Path serverKeystorePath = keystore.toTempKeyStorePath(tempFolder.getRoot().toPath(),
                                                              SERVER_KEYSTORE_PASSWORD.toCharArray(),
                                                              SERVER_KEYSTORE_PASSWORD.toCharArray());

        builder.withConfig(c -> c.set("authenticator.class_name", "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator")
                                 .set("authenticator.parameters", Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator"))
                                 .set("role_manager", "CassandraRoleManager")
                                 .set("authorizer", "CassandraAuthorizer")
                                 .set("client_encryption_options.enabled", "true")
                                 .set("client_encryption_options.require_client_auth", "optional")
                                 .set("client_encryption_options.keystore", serverKeystorePath.toString())
                                 .set("client_encryption_options.keystore_password", SERVER_KEYSTORE_PASSWORD)
                                 .set("client_encryption_options.truststore", truststorePath.toString())
                                 .set("client_encryption_options.truststore_password", SERVER_TRUSTSTORE_PASSWORD)
                                 .set("client_encryption_options.require_endpoint_verification", "false")
                                 .set("audit_logging_options.enabled", "true")
                                 .set("audit_logging_options.logger.class_name", "InMemoryAuditLogger")
                                 .set("audit_logging_options.included_categories", "AUTH")
                                 .with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP, Feature.NETWORK));
        CLUSTER = builder.start();

        sslOptions = getSSLOptions(null, truststorePath);
        configureIdentity(CLUSTER, sslOptions);
    }

    @AfterClass
    public static void teardown() throws Exception
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Before
    public void beforeEach()
    {
        // drain the audit log entries, so we can start fresh for each test
        CLUSTER.get(1).runOnInstance(() -> ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).internalQueue().clear());
        maybeRestoreMutualTlsWithPasswordFallbackAuthenticator();
    }

    @Test
    public void testPasswordAuthenticationSuccessfulAuth()
    {
        CharSequence expectedLogStringRegex = "^user:cassandra\\|host:.*/127.0.0.1:\\d+\\|source:/127.0.0.1" +
                                              "\\|port:\\d+\\|timestamp:\\d+\\|type:LOGIN_SUCCESS\\|category:AUTH" +
                                              "\\|operation:LOGIN SUCCESSFUL$";

        withAuthenticatedSession(CLUSTER.get(1), DEFAULT_SUPERUSER_NAME, DEFAULT_SUPERUSER_PASSWORD, session -> {
            session.execute("DESCRIBE KEYSPACES");

            CLUSTER.get(1).runOnInstance(() -> {
                // We should have events recorded for the control connection and the session connection
                AuditLogEntry entry1 = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).internalQueue().poll();
                assertThat(entry1).isNotNull();
                assertThat(entry1.getHost().toString(false)).matches(".*/127.0.0.1");
                assertThat(entry1.getSource().toString(false)).isEqualTo("/127.0.0.1");
                assertThat(entry1.getUser()).isEqualTo("cassandra");
                assertThat(entry1.getType()).isEqualTo(LOGIN_SUCCESS);
                assertThat(entry1.getLogString()).matches(expectedLogStringRegex);
                AuditLogEntry entry2 = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).internalQueue().poll();
                assertThat(entry2).isNotNull();
                assertThat(entry2.getHost().toString(false)).matches(".*/127.0.0.1");
                assertThat(entry2.getSource().toString(false)).isEqualTo("/127.0.0.1");
                assertThat(entry2.getUser()).isEqualTo("cassandra");
                assertThat(entry2.getType()).isEqualTo(LOGIN_SUCCESS);
                assertThat(entry2.getLogString()).matches(expectedLogStringRegex);
            });
        }, sslOptions);
    }

    @Test
    public void testPasswordAuthenticationFailedAuth()
    {
        CharSequence expectedLogStringRegex = "^user:null\\|host:/127.0.0.1:\\d+\\|source:/127.0.0.1" +
                                              "\\|port:\\d+\\|timestamp:\\d+\\|type:LOGIN_ERROR\\|category:AUTH" +
                                              "\\|operation:LOGIN FAILURE; Provided username cassandra and/or .*$";
        try
        {
            withAuthenticatedSession(CLUSTER.get(1), DEFAULT_SUPERUSER_NAME, "bad password", session -> {
            }, sslOptions);
            fail("Authentication should fail with a bad password");
        }
        catch (com.datastax.driver.core.exceptions.AuthenticationException authenticationException)
        {
            CLUSTER.get(1).runOnInstance(() -> {
                Queue<AuditLogEntry> auditLogEntries = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).internalQueue();
                AuditLogEntry entry = auditLogEntries.poll();
                assertThat(entry).isNotNull();
                assertThat(entry.getHost().toString(false)).isEqualTo("/127.0.0.1");
                assertThat(entry.getSource().toString(false)).isEqualTo("/127.0.0.1");
                assertThat(entry.getUser()).isNull();
                assertThat(entry.getType()).isEqualTo(LOGIN_ERROR);
                assertThat(entry.getLogString()).matches(expectedLogStringRegex);
            });
        }
    }

    @Test
    public void testMutualTlsAuthenticationSuccessfulAuth() throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(null, tempFolder.getRoot(), CA);
        CharSequence expectedLogStringRegex = "^user:cassandra_ssl_test\\|host:.*/127.0.0.1:\\d+\\|source:/127.0.0.1" +
                                              "\\|port:\\d+\\|timestamp:\\d+\\|type:LOGIN_SUCCESS\\|category:AUTH" +
                                              "\\|operation:LOGIN SUCCESSFUL\\|identity:spiffe://test.cassandra.apache.org/unitTest/mtls$";

        try (com.datastax.driver.core.Cluster c = JavaDriverUtils.create(CLUSTER, null, b -> b.withSSL(getSSLOptions(clientKeystorePath, truststorePath)));
             Session session = c.connect())
        {
            session.execute("DESCRIBE KEYSPACES");

            CLUSTER.get(1).runOnInstance(() -> {
                // We should have events recorded for the control connection and the session connection
                AuditLogEntry entry1 = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).internalQueue().poll();
                assertThat(entry1).isNotNull();
                assertThat(entry1.getHost().toString(false)).matches(".*/127.0.0.1");
                assertThat(entry1.getSource().toString(false)).isEqualTo("/127.0.0.1");
                assertThat(entry1.getUser()).isEqualTo("cassandra_ssl_test");
                assertThat(entry1.getType()).isEqualTo(LOGIN_SUCCESS);
                assertThat(entry1.getLogString()).matches(expectedLogStringRegex);
                AuditLogEntry entry2 = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).internalQueue().poll();
                assertThat(entry2).isNotNull();
                assertThat(entry2.getHost().toString(false)).matches(".*/127.0.0.1");
                assertThat(entry2.getSource().toString(false)).isEqualTo("/127.0.0.1");
                assertThat(entry2.getUser()).isEqualTo("cassandra_ssl_test");
                assertThat(entry2.getType()).isEqualTo(LOGIN_SUCCESS);
                assertThat(entry2.getLogString()).matches(expectedLogStringRegex);
            });
        }
    }

    @Test
    public void testMutualTlsAuthenticationFailedWithUntrustedCertificate() throws Exception
    {
        configureMutualTlsAuthenticator();
        // optionally match source/port because in MacOS source/port are null
        CharSequence expectedLogStringRegex = "^user:null\\|host:.*/127.0.0.1:\\d+(\\|source:/127.0.0.1\\|port:\\d+)?" +
                                              "\\|timestamp:\\d+\\|type:LOGIN_ERROR\\|category:AUTH" +
                                              "\\|operation:LOGIN FAILURE; Empty client certificate chain.*$";
        Path untrustedCertPath = generateSelfSignedCertificate(null, tempFolder.getRoot());

        testMtlsAuthenticationFailure(untrustedCertPath, "Authentication should fail with a self-signed certificate", expectedLogStringRegex);
    }

    @Test
    public void testMutualTlsAuthenticationFailedWithExpiredCertificate() throws Exception
    {
        // optionally match source/port because in MacOS source/port are null
        CharSequence expectedLogStringRegex = "^user:null\\|host:.*/127.0.0.1:\\d+(\\|source:/127.0.0.1\\|port:\\d+)?" +
                                              "\\|timestamp:\\d+\\|type:LOGIN_ERROR\\|category:AUTH" +
                                              "\\|operation:LOGIN FAILURE; PKIX path validation failed.*$";

        Path expiredCertPath = generateClientCertificate(b -> b.notBefore(Instant.now().minus(30, ChronoUnit.DAYS))
                                                               .notAfter(Instant.now().minus(10, ChronoUnit.DAYS)), tempFolder.getRoot(), CA);

        testMtlsAuthenticationFailure(expiredCertPath, "Authentication should fail with an expired certificate", expectedLogStringRegex);
    }

    @Test
    public void testMutualTlsAuthenticationFailedWithInvalidSpiffeCertificate() throws Exception
    {
        CharSequence expectedLogStringRegex = "^user:null\\|host:.*/127.0.0.1:\\d+\\|source:/127.0.0.1" +
                                              "\\|port:\\d+\\|timestamp:\\d+\\|type:LOGIN_ERROR\\|category:AUTH" +
                                              "\\|operation:LOGIN FAILURE; Unable to extract Spiffe from the certificate.*$";

        Path invalidSpiffeCertPath = generateClientCertificate(b -> b.clearSubjectAlternativeNames()
                                                                     .addSanUriName(NON_SPIFFE_IDENTITY), tempFolder.getRoot(), CA);

        testMtlsAuthenticationFailure(invalidSpiffeCertPath, "Authentication should fail with an invalid spiffe certificate", expectedLogStringRegex);
    }

    @Test
    public void testMutualTlsAuthenticationFailedWithIdentityThatDoesNotMapToARole() throws Exception
    {
        CharSequence expectedLogStringRegex = "^user:null\\|host:.*/127.0.0.1:\\d+\\|source:/127.0.0.1" +
                                              "\\|port:\\d+\\|timestamp:\\d+\\|type:LOGIN_ERROR\\|category:AUTH" +
                                              "\\|operation:LOGIN FAILURE; Certificate identity 'spiffe://test.cassandra.apache.org/dTest/notMapped' not authorized.*$";

        Path unmappedIdentityCertPath = generateClientCertificate(b -> b.clearSubjectAlternativeNames()
                                                                        .addSanUriName(NON_MAPPED_IDENTITY), tempFolder.getRoot(), CA);

        testMtlsAuthenticationFailure(unmappedIdentityCertPath, "Authentication should fail with a certificate that doesn't map to a role", expectedLogStringRegex);
    }

    static void testMtlsAuthenticationFailure(Path clientKeystorePath, String failureMessage, CharSequence expectedLogStringRegex)
    {
        try (com.datastax.driver.core.Cluster c = JavaDriverUtils.create(CLUSTER, null, b -> b.withSSL(getSSLOptions(clientKeystorePath, truststorePath)));
             Session ignored = c.connect())
        {
            fail(failureMessage);
        }
        catch (com.datastax.driver.core.exceptions.NoHostAvailableException exception)
        {
            CLUSTER.get(1).runOnInstance(() -> {
                // We should have events recorded for the control connection and the session connection
                Queue<AuditLogEntry> auditLogEntries = ((InMemoryAuditLogger) AuditLogManager.instance.getLogger()).internalQueue();
                AuditLogEntry entry = maybeGetAuditLogEntry(auditLogEntries);
                assertThat(entry).isNotNull();
                assertThat(entry.getHost().toString(false)).matches(".*/127.0.0.1");
                assertThat(entry.getUser()).isNull();
                assertThat(entry.getType()).isEqualTo(LOGIN_ERROR);
                assertThat(entry.getLogString()).matches(expectedLogStringRegex);
            });
        }
    }

    static void configureMutualTlsAuthenticator()
    {
        IInvokableInstance instance = CLUSTER.get(1);
        ClusterUtils.stopUnchecked(instance);
        instance.config().set("authenticator.class_name", "org.apache.cassandra.auth.MutualTlsAuthenticator");
        instance.config().set("client_encryption_options.require_client_auth", "required");
        instance.startup();
    }

    static void maybeRestoreMutualTlsWithPasswordFallbackAuthenticator()
    {
        IInvokableInstance instance = CLUSTER.get(1);

        if ("org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator".equals(instance.config().getString("authenticator.class_name")))
        {
            return;
        }

        ClusterUtils.stopUnchecked(instance);
        instance.config().set("authenticator.class_name", "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator");
        instance.config().set("client_encryption_options.require_client_auth", "optional");
        instance.startup();
    }

    static AuditLogEntry maybeGetAuditLogEntry(Queue<AuditLogEntry> auditLogEntries)
    {
        int attempts = 0;
        AuditLogEntry entry = auditLogEntries.poll();

        while (entry == null && attempts++ < 10)
        {
            // wait until the entry is propagated
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            entry = auditLogEntries.poll();
        }
        return entry;
    }
}
