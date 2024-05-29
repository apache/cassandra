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
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.Histogram;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.JavaDriverUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.ClearableHistogram;
import org.apache.cassandra.metrics.MutualTlsMetrics;
import org.apache.cassandra.utils.tls.CertificateBuilder;
import org.apache.cassandra.utils.tls.CertificateBundle;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;

import static org.apache.cassandra.transport.TlsTestUtils.CLIENT_SPIFFE_IDENTITY;
import static org.apache.cassandra.transport.TlsTestUtils.SERVER_KEYSTORE_PASSWORD;
import static org.apache.cassandra.transport.TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD;
import static org.apache.cassandra.transport.TlsTestUtils.configureIdentity;
import static org.apache.cassandra.transport.TlsTestUtils.generateClientCertificate;
import static org.apache.cassandra.transport.TlsTestUtils.getSSLOptions;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests mTLS certificate validity period functionality
 */
public class MutualTlsCertificateValidityPeriodTest extends TestBaseImpl
{
    private static ICluster<IInvokableInstance> CLUSTER;

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    static CertificateBundle CA;
    static Path truststorePath;

    @BeforeClass
    public static void setupClass() throws Exception
    {
        Cluster.Builder builder = Cluster.build(1);

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
                                 .set("client_encryption_options.max_certificate_validity_period", "30d")
                                 .set("client_encryption_options.certificate_validity_warn_threshold", "5d")
                                 .with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP));
        CLUSTER = builder.start();

        configureIdentity(CLUSTER, getSSLOptions(null, truststorePath));
    }

    @AfterClass
    public static void teardown() throws Exception
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @After
    public void afterEach()
    {
        // reset metrics
        CLUSTER.get(1).runOnInstance(() -> {
            Histogram client = MutualTlsMetrics.instance.clientCertificateExpirationDays;
            Histogram internode = MutualTlsMetrics.instance.internodeCertificateExpirationDays;

            if (client instanceof ClearableHistogram)
            {
                ((ClearableHistogram) client).clear();
            }

            if (internode instanceof ClearableHistogram)
            {
                ((ClearableHistogram) internode).clear();
            }
        });
    }

    @Test
    public void testExpiringCertificate() throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(null, tempFolder.getRoot(), CA);

        com.datastax.driver.core.Cluster driver = JavaDriverUtils.create(CLUSTER, null, b -> b.withSSL(getSSLOptions(clientKeystorePath, truststorePath)));

        testWithDriver(driver, (Session session) -> {
            ResultSet clientView = session.execute(new SimpleStatement("SELECT * FROM system_views.clients"));
            Assertions.assertThat(clientView).isNotNull().isNotEmpty();

            Optional<Row> thisClient = StreamSupport.stream(clientView.spliterator(), false)
                                                    .filter(row -> "cassandra_ssl_test".equals(row.getString("username")))
                                                    .findFirst();

            Assertions.assertThat(thisClient).isPresent();
            Row row = thisClient.get();
            Map<String, String> authenticationMetadata = row.getMap("authentication_metadata", String.class, String.class);

            Assertions.assertThat(authenticationMetadata).isNotNull().hasSize(1)
                      .containsKey("identity")
                      .extractingByKey("identity", as(InstanceOfAssertFactories.STRING)).isEqualTo(CLIENT_SPIFFE_IDENTITY);
            Assertions.assertThat(row.getString("authentication_mode")).isEqualTo("MutualTls");
            Assertions.assertThat(CLUSTER.get(1).logs().grep("Certificate with identity '" + CLIENT_SPIFFE_IDENTITY + "' will expire").getResult())
                      .isNotEmpty();
            CLUSTER.get(1).runOnInstance(() -> Assertions.assertThat(MutualTlsMetrics.instance.clientCertificateExpirationDays.getCount()).isEqualTo(2));
        });
    }

    @Test
    public void testCertificateReachingMaxValidityPeriod() throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(b -> b.notBefore(Instant.now().minus(26, ChronoUnit.DAYS))
                                                                  .notAfter(Instant.now().plus(4, ChronoUnit.DAYS).minus(1, ChronoUnit.MINUTES)), tempFolder.getRoot(), CA);

        com.datastax.driver.core.Cluster driver = JavaDriverUtils.create(CLUSTER, null, b -> b.withSSL(getSSLOptions(clientKeystorePath, truststorePath)));

        testWithDriver(driver, (Session session) -> {
            ResultSet clientView = session.execute(new SimpleStatement("SELECT * FROM system_views.clients"));
            Assertions.assertThat(clientView).isNotNull().isNotEmpty();

            Optional<Row> thisClient = StreamSupport.stream(clientView.spliterator(), false)
                                                    .filter(row -> "cassandra_ssl_test".equals(row.getString("username")))
                                                    .findFirst();

            Assertions.assertThat(thisClient).isPresent();
            Row row = thisClient.get();
            Map<String, String> authenticationMetadata = row.getMap("authentication_metadata", String.class, String.class);

            Assertions.assertThat(authenticationMetadata).isNotNull().hasSize(1)
                      .containsKey("identity")
                      .extractingByKey("identity", as(InstanceOfAssertFactories.STRING)).isEqualTo(CLIENT_SPIFFE_IDENTITY);
            Assertions.assertThat(row.getString("authentication_mode")).isEqualTo("MutualTls");
            Assertions.assertThat(CLUSTER.get(1).logs().grep("Certificate with identity '" + CLIENT_SPIFFE_IDENTITY + "' will expire").getResult())
                      .isNotEmpty();
            CLUSTER.get(1).runOnInstance(() -> Assertions.assertThat(MutualTlsMetrics.instance.clientCertificateExpirationDays.getCount()).isGreaterThanOrEqualTo(2));
        });
    }

    @Test
    public void testFailsWhenCertificateExceedsMaxAllowedValidityPeriod() throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(b -> b.notAfter(Instant.now().plus(365, ChronoUnit.DAYS)), tempFolder.getRoot(), CA);

        com.datastax.driver.core.Cluster driver = JavaDriverUtils.create(CLUSTER, null, b -> b.withSSL(getSSLOptions(clientKeystorePath, truststorePath)));

        try
        {
            testWithDriver(driver, null);
            fail("Should not be able to connect when the certificate exceeds the maximum allowed validity period");
        }
        catch (com.datastax.driver.core.exceptions.NoHostAvailableException exception)
        {
            Assertions.assertThat(exception)
                      .hasMessageContaining("The validity period of the provided certificate (366 days) exceeds the maximum allowed validity period of 30 days");
        }
    }

    @Test
    public void testFailsWhenCertificateIsExpired() throws Exception
    {
        Path clientKeystorePath = generateClientCertificate(b -> b.notBefore(Instant.now().minus(30, ChronoUnit.DAYS))
                                                                  .notAfter(Instant.now().minus(10, ChronoUnit.DAYS)), tempFolder.getRoot(), CA);

        com.datastax.driver.core.Cluster driver = JavaDriverUtils.create(CLUSTER, null, b -> b.withSSL(getSSLOptions(clientKeystorePath, truststorePath)));

        try
        {
            testWithDriver(driver,
                           session -> CLUSTER.get(1).runOnInstance(() -> Assertions.assertThat(MutualTlsMetrics.instance.clientCertificateExpirationDays.getCount()).isZero()));
            fail("Should not be able to connect when the certificate is expired");
        }
        catch (com.datastax.driver.core.exceptions.NoHostAvailableException exception)
        {
            Assertions.assertThat(exception).hasMessageContaining("Channel has been closed");
        }
    }

    private void testWithDriver(com.datastax.driver.core.Cluster providedDriver, Consumer<Session> consumer)
    {
        try (com.datastax.driver.core.Cluster driver = providedDriver;
             Session session = driver.connect())
        {
            if (consumer != null)
            {
                consumer.accept(session);
            }
        }
    }
}
