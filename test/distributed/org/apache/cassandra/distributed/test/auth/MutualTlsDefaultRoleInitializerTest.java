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
import java.util.Collections;
import java.util.Map;

import com.datastax.driver.core.Session;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.tls.CertificateBuilder;
import org.apache.cassandra.utils.tls.CertificateBundle;

import static org.apache.cassandra.transport.TlsTestUtils.CLIENT_SPIFFE_IDENTITY;
import static org.apache.cassandra.transport.TlsTestUtils.SERVER_KEYSTORE_PASSWORD;
import static org.apache.cassandra.transport.TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD;
import static org.apache.cassandra.transport.TlsTestUtils.generateClientCertificate;
import static org.apache.cassandra.transport.TlsTestUtils.getSSLOptions;
import static org.apache.cassandra.transport.TlsTestUtils.withAuthenticatedSession;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@code default_role_initializer: MutualTlsDefaultRoleInitializer} bootstraps a passwordless
 * superuser role and its identity mapping without any post-startup CQL step, that a client presenting a
 * matching certificate can authenticate as that role with superuser access, and that password authentication
 * against the same role fails cleanly since it has no {@code salted_hash}.
 */
public class MutualTlsDefaultRoleInitializerTest extends TestBaseImpl
{
    private static final String TEST_ROLE = "cassandra_mtls_bootstrap_test_role";

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    private static ICluster<IInvokableInstance> CLUSTER;
    private static CertificateBundle CA;
    private static Path truststorePath;
    private static Path clientKeystorePath;

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

        CertificateBundle serverKeystore = new CertificateBuilder().subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                                   .addSanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
                                                                   .addSanDnsName(InetAddress.getLocalHost().getHostName())
                                                                   .buildIssuedBy(CA);

        Path serverKeystorePath = serverKeystore.toTempKeyStorePath(tempFolder.getRoot().toPath(),
                                                                     SERVER_KEYSTORE_PASSWORD.toCharArray(),
                                                                     SERVER_KEYSTORE_PASSWORD.toCharArray());

        builder.withConfig(c -> c.set("authenticator.class_name", "org.apache.cassandra.auth.MutualTlsWithPasswordFallbackAuthenticator")
                                 .set("authenticator.parameters", Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator"))
                                 .set("role_manager", "CassandraRoleManager")
                                 .set("authorizer", "CassandraAuthorizer")
                                 .set("default_role_initializer.class_name", "org.apache.cassandra.auth.MutualTlsDefaultRoleInitializer")
                                 .set("default_role_initializer.parameters", Map.of("role", TEST_ROLE, "identity", CLIENT_SPIFFE_IDENTITY))
                                 .set("client_encryption_options.enabled", "true")
                                 .set("client_encryption_options.require_client_auth", "optional")
                                 .set("client_encryption_options.keystore", serverKeystorePath.toString())
                                 .set("client_encryption_options.keystore_password", SERVER_KEYSTORE_PASSWORD)
                                 .set("client_encryption_options.truststore", truststorePath.toString())
                                 .set("client_encryption_options.truststore_password", SERVER_TRUSTSTORE_PASSWORD)
                                 .set("client_encryption_options.require_endpoint_verification", "false")
                                 .with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP, Feature.NETWORK));
        CLUSTER = builder.start();

        clientKeystorePath = generateClientCertificate(null, tempFolder.getRoot(), CA);
    }

    @AfterClass
    public static void teardown() throws Exception
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void testDefaultRoleAndIdentityCreatedAtBootstrap()
    {
        String identity = CLIENT_SPIFFE_IDENTITY;

        Object[] roleRow = CLUSTER.get(1).callOnInstance((SerializableCallable<Object[]>) () -> {
            UntypedResultSet result = QueryProcessor.executeInternal(
                String.format("SELECT is_superuser, can_login, salted_hash FROM %s.%s WHERE role = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, TEST_ROLE));
            Assert.assertNotNull(result);
            if (result.isEmpty())
                return null;
            UntypedResultSet.Row row = result.one();
            return new Object[]{ row.getBoolean("is_superuser"), row.getBoolean("can_login"), row.has("salted_hash") };
        });

        assertThat(roleRow).isNotNull();
        assertThat((Boolean) roleRow[0]).as("is_superuser").isTrue();
        assertThat((Boolean) roleRow[1]).as("can_login").isTrue();
        assertThat((Boolean) roleRow[2]).as("has salted_hash").isFalse();

        String mappedRole = CLUSTER.get(1).callOnInstance((SerializableCallable<String>) () -> {
            UntypedResultSet result = QueryProcessor.executeInternal(
                String.format("SELECT role FROM %s.%s WHERE identity = '%s'",
                              SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.IDENTITY_TO_ROLES, identity));
            Assert.assertNotNull(result);
            return result.isEmpty() ? null : result.one().getString("role");
        });

        assertThat(mappedRole).isEqualTo(TEST_ROLE);
    }

    @Test
    public void testCertificateAuthenticationGrantsSuperuser() throws Exception
    {
        try (com.datastax.driver.core.Cluster c = JavaDriverUtils.create(CLUSTER, null, b -> b.withSSL(getSSLOptions(clientKeystorePath, truststorePath)));
             Session session = c.connect())
        {
            // system_auth.roles is a protected resource (CassandraRoleManager#protectedResources); reading it
            // without ever being granted SELECT proves this connection authenticated with superuser status.
            com.datastax.driver.core.ResultSet rows = session.execute("SELECT role FROM system_auth.roles WHERE role = ?", TEST_ROLE);
            assertThat(rows.one().getString("role")).isEqualTo(TEST_ROLE);
        }
    }

    @Test
    public void testPasswordAuthenticationFailsCleanly() throws Exception
    {
        assertThatThrownBy(() -> withAuthenticatedSession(CLUSTER.get(1), TEST_ROLE, "irrelevant-password", session -> {
        }, getSSLOptions(null, truststorePath)))
            .isInstanceOf(com.datastax.driver.core.exceptions.AuthenticationException.class);
    }
}
