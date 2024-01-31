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

import java.util.Collections;
import java.util.Map;
import javax.net.ssl.SSLException;

import com.google.common.collect.ImmutableMap;

import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.security.ISslContextFactory;

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
        config.role_manager = "CassandraRoleManager";
        config.authorizer = "CassandraAuthorizer";
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

}
