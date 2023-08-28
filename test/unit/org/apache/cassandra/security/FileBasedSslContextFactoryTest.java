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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;

public class FileBasedSslContextFactoryTest
{
    private static final Logger logger = LoggerFactory.getLogger(FileBasedSslContextFactoryTest.class);

    private EncryptionOptions.ServerEncryptionOptions encryptionOptions;

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
        encryptionOptions = new EncryptionOptions.ServerEncryptionOptions()
                            .withSslContextFactory(new ParameterizedClass(TestFileBasedSSLContextFactory.class.getName(),
                                                                          new HashMap<>()))
                            .withTrustStore("test/conf/cassandra_ssl_test.truststore")
                            .withTrustStorePassword("cassandra")
                            .withRequireClientAuth(false)
                            .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA")
                            .withKeyStore("test/conf/cassandra_ssl_test.keystore")
                            .withKeyStorePassword("cassandra")
                            .withOutboundKeystore("test/conf/cassandra_ssl_test_outbound.keystore")
                            .withOutboundKeystorePassword("cassandra");
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
     * Tests that empty {@code keystore_password} and {@code outbound_keystore_password} is allowed.
     */
    @Test
    public void testEmptyKeystorePasswords() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptions
                                                                           .withKeyStorePassword("")
                                                                           .withKeyStore("test/conf/cassandra_ssl_test_nopassword.keystore")
                                                                           .withOutboundKeystorePassword("")
                                                                           .withOutboundKeystore("test/conf/cassandra_ssl_test_nopassword.keystore");

        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertEquals("keystore_password must be empty", "", localEncryptionOptions.keystore_password);
        Assert.assertEquals("outbound_keystore_password must empty", "", localEncryptionOptions.outbound_keystore_password);

        TestFileBasedSSLContextFactory sslContextFactory =
        (TestFileBasedSSLContextFactory) localEncryptionOptions.sslContextFactoryInstance;

        sslContextFactory.buildKeyManagerFactory();
        sslContextFactory.buildTrustManagerFactory();
    }

    /**
     * Tests that an absent keystore_password for the {@code keystore} is disallowed.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testNullKeystorePasswordDisallowed() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptions.withKeyStorePassword(null);

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
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptions.withOutboundKeystorePassword(null);

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
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptions.withTrustStorePassword(null);
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
