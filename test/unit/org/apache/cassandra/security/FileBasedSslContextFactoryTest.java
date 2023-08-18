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

public class FileBasedSslContextFactoryTest
{
    private static final Logger logger = LoggerFactory.getLogger(FileBasedSslContextFactoryTest.class);

    private EncryptionOptions.ServerEncryptionOptions encryptionOptions;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        System.setProperty("cassandra.config", "cassandra.yaml");
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor()
    {
        System.clearProperty("cassandra.config");
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
                            .withKeyStorePassword("cassandra");
    }

    @Test
    public void testHappyPath() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptions;

        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertNotNull("keystore_password must not be null", localEncryptionOptions.keystore_password);

        TestFileBasedSSLContextFactory sslContextFactory =
        (TestFileBasedSSLContextFactory) localEncryptionOptions.sslContextFactoryInstance;
        sslContextFactory.buildKeyManagerFactory();
        sslContextFactory.buildTrustManagerFactory();
    }

    /**
     * Tests that empty {@code keystore_password} is allowed.
     */
    @Test
    public void testEmptyKeystorePasswords() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptions
                                                                           .withKeyStorePassword("")
                                                                           .withKeyStore("test/conf/cassandra_ssl_test_nopassword.keystore");

        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertEquals("keystore_password must be empty", "", localEncryptionOptions.keystore_password);

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

    @Test
    public void testEmptyTruststorePassword() throws SSLException
    {
        EncryptionOptions.ServerEncryptionOptions localEncryptionOptions = encryptionOptions.withTrustStorePassword(null);
        Assert.assertEquals("org.apache.cassandra.security.FileBasedSslContextFactoryTest$TestFileBasedSSLContextFactory",
                            localEncryptionOptions.ssl_context_factory.class_name);
        Assert.assertNotNull("keystore_password must not be null", localEncryptionOptions.keystore_password);
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
