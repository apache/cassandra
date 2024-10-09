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

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.net.ssl.SSLException;

import com.google.common.collect.ImmutableMap;

import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.util.Auth;
import org.apache.cassandra.distributed.util.SingleHostLoadBalancingPolicy;
import org.apache.cassandra.security.ISslContextFactory;
import org.apache.cassandra.utils.tls.CertificateBuilder;
import org.apache.cassandra.utils.tls.CertificateBundle;

import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_NAME;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_PASSWORD;

public class TlsTestUtils
{
    public static String SERVER_KEYSTORE_PATH = "test/conf/cassandra_ssl_test.keystore";
    public static String SERVER_KEYSTORE_PATH_PEM = "test/conf/cassandra_ssl_test.keystore.pem";
    public static String SERVER_KEYSTORE_PATH_UNENCRYPTED_PEM = "test/conf/cassandra_ssl_test.unencrypted_keystore.pem";
    public static String SERVER_KEYSTORE_PASSWORD = "cassandra";

    public static String SERVER_KEYSTORE_ENDPOINT_VERIFY_PATH = "test/conf/cassandra_ssl_test_endpoint_verify.keystore";
    public static String SERVER_KEYSTORE_ENDPOINT_VERIFY_PASSWORD = "cassandra";

    public static String SERVER_OUTBOUND_KEYSTORE_PATH = "test/conf/cassandra_ssl_test_outbound.keystore";
    public static String SERVER_OUTBOUND_KEYSTORE_PASSWORD = "cassandra";

    public static String SERVER_TRUSTSTORE_PATH = "test/conf/cassandra_ssl_test.truststore";
    public static String SERVER_TRUSTSTORE_PEM_PATH = "test/conf/cassandra_ssl_test.truststore.pem";
    public static String SERVER_TRUSTSTORE_PASSWORD = "cassandra";

    // To regenerate:
    // 1. generate keystore
    //    keytool -genkeypair -keystore test/conf/cassandra_ssl_test_spiffe.keystore -validity 100000 -keyalg RSA -dname "CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown" -keypass cassandra -storepass cassandra -alias spiffecert -ext SAN=URI:spiffe://test.cassandra.apache.org/unitTest/mtls -storetype jks
    // 2. export cert
    //    keytool -export -alias spiffecert -file spiffecert.cer -keystore test/conf/cassandra_ssl_test_spiffe.keystore
    // 3. import cert into truststore
    //    keytool -import -v -trustcacerts -alias spiffecert -file spiffecert.cer -keystore test/conf/cassandra_ssl_test.truststore
    public static String CLIENT_SPIFFE_KEYSTORE_PATH = "test/conf/cassandra_ssl_test_spiffe.keystore";
    public static String CLIENT_SPIFFE_KEYSTORE_PASSWORD = "cassandra";
    public static String CLIENT_SPIFFE_IDENTITY = "spiffe://test.cassandra.apache.org/unitTest/mtls";

    public static String CLIENT_TRUSTSTORE_PATH = "test/conf/cassandra_ssl_test.truststore";
    public static String CLIENT_TRUSTSTORE_PASSWORD = "cassandra";

    public static EncryptionOptions getClientEncryptionOptions()
    {
        return new EncryptionOptions(new EncryptionOptions()
                              .withEnabled(true)
                              .withRequireClientAuth(EncryptionOptions.ClientAuth.OPTIONAL)
                              .withOptional(true)
                              .withKeyStore(SERVER_KEYSTORE_PATH)
                              .withKeyStorePassword(SERVER_KEYSTORE_PASSWORD)
                              .withTrustStore(SERVER_TRUSTSTORE_PATH)
                              .withTrustStorePassword(SERVER_TRUSTSTORE_PASSWORD)
                              .withRequireEndpointVerification(false));
    }

    public static void configureWithMutualTlsWithPasswordFallbackAuthenticator(Config config)
    {
        // Configure an authenticator that supports multiple authentication mechanisms.
        Map<String, String> parameters = Collections.singletonMap("validator_class_name", "org.apache.cassandra.auth.SpiffeCertificateValidator");
        config.authenticator = new ParameterizedClass("MutualTlsWithPasswordFallbackAuthenticator", parameters);
        // Configure client encryption such that we can optionally connect with SSL.
        config.client_encryption_options = TlsTestUtils.getClientEncryptionOptions();
        config.role_manager = new ParameterizedClass("CassandraRoleManager");
        config.authorizer = new ParameterizedClass("CassandraAuthorizer");
    }

    public static ISslContextFactory getClientSslContextFactory(boolean provideClientCert)
    {
        ImmutableMap.Builder<String, Object> params = ImmutableMap.<String, Object>builder()
                                                                  .put("truststore", CLIENT_TRUSTSTORE_PATH)
                                                                  .put("truststore_password", CLIENT_TRUSTSTORE_PASSWORD);

        if (provideClientCert)
        {
            params.put("keystore", CLIENT_SPIFFE_KEYSTORE_PATH)
                  .put("keystore_password", CLIENT_SPIFFE_KEYSTORE_PASSWORD);
        }

        return new SimpleClientSslContextFactory(params.build());
    }

    public static SSLOptions getSSLOptions(boolean provideClientCert) throws SSLException
    {
        return RemoteEndpointAwareJdkSSLOptions.builder()
                                               .withSSLContext(getClientSslContextFactory(provideClientCert)
                                                               .createJSSESslContext(EncryptionOptions.ClientAuth.OPTIONAL))
                                               .build();
    }

    public static SSLOptions getSSLOptions(Path keystorePath, Path truststorePath) throws RuntimeException
    {
        try
        {
            return RemoteEndpointAwareJdkSSLOptions.builder()
                                                   .withSSLContext(getClientSslContextFactory(keystorePath, truststorePath)
                                                                   .createJSSESslContext(EncryptionOptions.ClientAuth.OPTIONAL))
                                                   .build();
        }
        catch (SSLException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static ISslContextFactory getClientSslContextFactory(Path keystorePath, Path truststorePath)
    {
        ImmutableMap.Builder<String, Object> params = ImmutableMap.<String, Object>builder()
                                                                  .put("truststore", truststorePath.toString())
                                                                  .put("truststore_password", CLIENT_TRUSTSTORE_PASSWORD);

        if (keystorePath != null)
        {
            params.put("keystore", keystorePath.toString())
                  .put("keystore_password", "cassandra");
        }

        return new SimpleClientSslContextFactory(params.build());
    }

    public static void configureIdentity(ICluster<IInvokableInstance> cluster, SSLOptions sslOptions)
    {
        withAuthenticatedSession(cluster.get(1), DEFAULT_SUPERUSER_NAME, DEFAULT_SUPERUSER_PASSWORD, session -> {
            session.execute("CREATE ROLE cassandra_ssl_test WITH LOGIN = true");
            session.execute(String.format("ADD IDENTITY '%s' TO ROLE 'cassandra_ssl_test'", CLIENT_SPIFFE_IDENTITY));
            // GRANT select to cassandra_ssl_test to be able to query the system_views.clients virtual table
            session.execute("GRANT SELECT ON ALL KEYSPACES to cassandra_ssl_test");
        }, sslOptions);
    }

    public static Path generateSelfSignedCertificate(Function<CertificateBuilder, CertificateBuilder> customizeCertificate, File targetDirectory) throws Exception
    {
        return generateClientCertificate(customizeCertificate, targetDirectory, null);
    }

    public static Path generateClientCertificate(Function<CertificateBuilder, CertificateBuilder> customizeCertificate, File targetDirectory, CertificateBundle ca) throws Exception
    {
        CertificateBuilder builder = new CertificateBuilder().subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                                                             .notBefore(Instant.now().minus(1, ChronoUnit.DAYS))
                                                             .notAfter(Instant.now().plus(1, ChronoUnit.DAYS))
                                                             .alias("spiffecert")
                                                             .addSanUriName(CLIENT_SPIFFE_IDENTITY)
                                                             .rsa2048Algorithm();
        if (customizeCertificate != null)
        {
            builder = customizeCertificate.apply(builder);
        }
        CertificateBundle ssc = ca != null
                                ? builder.buildIssuedBy(ca)
                                : builder.buildSelfSigned();
        return ssc.toTempKeyStorePath(targetDirectory.toPath(), SERVER_KEYSTORE_PASSWORD.toCharArray(), SERVER_KEYSTORE_PASSWORD.toCharArray());
    }

    public static void withAuthenticatedSession(IInvokableInstance instance,
                                         String username,
                                         String password,
                                         Consumer<Session> consumer,
                                         SSLOptions sslOptions)
    {
        // wait for existing roles
        Auth.waitForExistingRoles(instance);

        InetSocketAddress nativeInetSocketAddress = ClusterUtils.getNativeInetSocketAddress(instance);
        InetAddress address = nativeInetSocketAddress.getAddress();
        LoadBalancingPolicy lbc = new SingleHostLoadBalancingPolicy(address);

        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder()
                                                                                           .withLoadBalancingPolicy(lbc)
                                                                                           .withSSL(sslOptions)
                                                                                           .withAuthProvider(new PlainTextAuthProvider(username, password))
                                                                                           .addContactPoint(address.getHostAddress())
                                                                                           .withPort(nativeInetSocketAddress.getPort());

        try (com.datastax.driver.core.Cluster c = builder.build(); Session session = c.connect())
        {
            consumer.accept(session);
        }
    }

}
