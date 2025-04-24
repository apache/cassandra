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

package org.apache.cassandra.security;

import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.transport.TlsTestUtils;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;

import static org.apache.cassandra.config.EncryptionOptions.ClientEncryptionOptions.ClientAuth.NOT_REQUIRED;

public class FileBasedSslContextFactoryTest
{
    private EncryptionOptions.ServerEncryptionOptions encryptionOptions;
    private EncryptionOptions.ServerEncryptionOptions.Builder encryptionOptionsBuilder;

    static WithProperties properties;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        CASSANDRA_CONFIG.reset();
        properties = new WithProperties();
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor()
    {
        properties.close();
    }

    @Before
    public void setup()
    {
        encryptionOptionsBuilder = new EncryptionOptions.ServerEncryptionOptions.Builder();
        encryptionOptions = encryptionOptionsBuilder
                            .withOutboundKeystore(TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PATH)
                            .withOutboundKeystorePassword(TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PASSWORD)
                            .withSslContextFactory(new ParameterizedClass(TestFileBasedSSLContextFactory.class.getName(),
                                                                          new HashMap<>()))
                            .withTrustStore(TlsTestUtils.SERVER_TRUSTSTORE_PATH)
                            .withTrustStorePassword(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD)
                            .withRequireClientAuth(NOT_REQUIRED)
                            .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA")
                            .withKeyStore(TlsTestUtils.SERVER_KEYSTORE_PATH)
                            .withKeyStorePassword(TlsTestUtils.SERVER_KEYSTORE_PASSWORD)
                            .build();
    }

    @Test
    public void testHappyPath() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptions;

        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertNotNull("keystore_password must not be null", localEncryptionOptions.keystore_password);
        Assert.assertNotNull("outbound_keystore_password must not be null", localEncryptionOptions.outbound_keystore_password);

        TestFileBasedSSLContextFactory sslContextFactory =
        (TestFileBasedSSLContextFactory) localEncryptionOptions.sslContextFactoryInstance;
        sslContextFactory.buildKeyManagerFactory();
        sslContextFactory.buildTrustManagerFactory();
    }

    /**
     * Tests that empty {@code keystore_password} and {@code outbound_keystore_password} are allowed.
     */
    @Test
    public void testEmptyKeystorePasswords() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptionsBuilder
                                                                           .withOutboundKeystorePassword("")
                                                                           .withOutboundKeystore("test/conf/cassandra_ssl_test_nopassword.keystore")
                                                                           .withKeyStorePassword("")
                                                                           .withKeyStore("test/conf/cassandra_ssl_test_nopassword.keystore")
                                                                           .build();

        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertEquals("keystore_password must be empty", "", localEncryptionOptions.keystore_password);
        Assert.assertEquals("outbound_keystore_password must be empty", "", localEncryptionOptions.outbound_keystore_password);

        TestFileBasedSSLContextFactory sslContextFactory =
        (TestFileBasedSSLContextFactory) localEncryptionOptions.sslContextFactoryInstance;

        sslContextFactory.buildKeyManagerFactory();
        sslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void testKeystorePasswordFile() throws SSLException
    {
        // Here we only override password configuration and specify password_file configuration since keystore paths
        // are already loaded in the `encryptionOptions`
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptionsBuilder
                                                                           .withOutboundKeystorePassword(null)
                                                                           .withOutboundKeystorePasswordFile(TlsTestUtils.SERVER_OUTBOUND_KEYSTORE_PASSWORD_FILE)
                                                                           .withKeyStorePassword(null)
                                                                           .withKeyStorePasswordFile(TlsTestUtils.SERVER_KEYSTORE_PASSWORD_FILE)
                                                                           .withTrustStorePassword(null)
                                                                           .withTrustStorePasswordFile(TlsTestUtils.SERVER_TRUSTSTORE_PASSWORD_FILE)
                                                                           .build();

        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        TestFileBasedSSLContextFactory sslContextFactory =
        (TestFileBasedSSLContextFactory) localEncryptionOptions.sslContextFactoryInstance;

        sslContextFactory.buildKeyManagerFactory();
        sslContextFactory.buildTrustManagerFactory();
    }

    /**
     * Tests for missing password configuration and non-existance file specified in the password_file configuration.
     * @throws SSLException
     */
    @Test(expected =  ConfigurationException.class)
    public void testBadKeystorePasswordFile() throws SSLException
    {
        // Here we only override password configuration and specify password_file configuration since keystore paths
        // are already loaded in the `encryptionOptions`
        encryptionOptionsBuilder
        .withOutboundKeystorePassword(null)
        .withOutboundKeystorePasswordFile("/path/to/non-existance-password-file")
        .withKeyStorePassword(null)
        .withKeyStorePasswordFile("/path/to/non-existance-password-file")
        .withTrustStorePassword(null)
        .withTrustStorePasswordFile("/path/to/non-existance-password-file")
        .build();
    }

    /**
     * Tests that an absent keystore_password for the {@code keystore} is disallowed.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullKeystorePasswordDisallowed() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptionsBuilder
                                                                           .withKeyStorePassword(null)
                                                                           .build();

        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertNull("keystore_password must be null", localEncryptionOptions.keystore_password);
        Assert.assertNotNull("outbound_keystore_password must not be null", localEncryptionOptions.outbound_keystore_password);

        TestFileBasedSSLContextFactory sslContextFactory =
        (TestFileBasedSSLContextFactory) localEncryptionOptions.sslContextFactoryInstance;
        try
        {
            sslContextFactory.buildKeyManagerFactory();
            sslContextFactory.buildTrustManagerFactory();
        }
        catch (Exception e)
        {
            Assert.assertEquals("'keystore_password' must be specified", e.getMessage());
            throw e;
        }
    }

    /**
     * Tests for the empty password for the {@code outbound_keystore}. Since the {@code outbound_keystore_password} defaults
     * to the {@code keystore_password}, this test should pass without exceptions.
     */
    @Test
    public void testOnlyEmptyOutboundKeystorePassword() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptionsBuilder
                                                                           .withOutboundKeystorePassword(null)
                                                                           .build();

        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertNotNull("keystore_password must not be null", localEncryptionOptions.keystore_password);
        Assert.assertNull("outbound_keystore_password must be null", localEncryptionOptions.outbound_keystore_password);

        TestFileBasedSSLContextFactory sslContextFactory =
        (TestFileBasedSSLContextFactory) localEncryptionOptions.sslContextFactoryInstance;
        sslContextFactory.buildKeyManagerFactory();
        sslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void testEmptyTruststorePassword() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptionsBuilder
                                                                           .withTrustStorePassword(null)
                                                                           .build();
        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertNotNull("keystore_password must not be null", localEncryptionOptions.keystore_password);
        Assert.assertNotNull("outbound_keystore_password must not be null", localEncryptionOptions.outbound_keystore_password);
        Assert.assertNull("truststore_password must be null", localEncryptionOptions.truststore_password);

        TestFileBasedSSLContextFactory sslContextFactory =
        (TestFileBasedSSLContextFactory) localEncryptionOptions.sslContextFactoryInstance;
        sslContextFactory.buildTrustManagerFactory();
    }

    public static class TestFileBasedSSLContextFactory extends FileBasedSslContextFactory
    {
        public TestFileBasedSSLContextFactory(Map<String, Object> parameters)
        {
            super(parameters);
        }
    }
}
