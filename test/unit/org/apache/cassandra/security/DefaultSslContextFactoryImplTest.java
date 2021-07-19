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

import java.io.IOException;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.EncryptionOptions;

public class DefaultSslContextFactoryImplTest
{

    private DefaultSslContextFactoryImpl defaultSslContextFactoryImpl = new DefaultSslContextFactoryImpl();

    private EncryptionOptions.ServerEncryptionOptions encryptionOptions;

    @Before
    public void setup()
    {
        encryptionOptions = new EncryptionOptions.ServerEncryptionOptions()
                            .withTrustStore("test/conf/cassandra_ssl_test.truststore")
                            .withTrustStorePassword("cassandra")
                            .withRequireClientAuth(false)
                            .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA");

        defaultSslContextFactoryImpl.checkedExpiry = false;
    }

    private EncryptionOptions.ServerEncryptionOptions addKeystoreOptions(EncryptionOptions.ServerEncryptionOptions options)
    {
        return options.withKeyStore("test/conf/cassandra_ssl_test.keystore")
                      .withKeyStorePassword("cassandra");
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactory_NoFile() throws IOException
    {
        defaultSslContextFactoryImpl.buildTrustManagerFactory(encryptionOptions.withTrustStore("/this/is/probably/not/a/file/on/your/test/machine"));
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactory_BadPassword() throws IOException
    {
        defaultSslContextFactoryImpl.buildTrustManagerFactory(encryptionOptions.withTrustStorePassword("HomeOfBadPasswords"));
    }

    @Test
    public void buildTrustManagerFactory_HappyPath() throws IOException
    {
        TrustManagerFactory trustManagerFactory = defaultSslContextFactoryImpl.buildTrustManagerFactory(encryptionOptions);
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactory_NoFile() throws IOException
    {
        EncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                    .withKeyStore("/this/is/probably/not/a/file/on/your/test/machine");
        defaultSslContextFactoryImpl.buildKeyManagerFactory(options);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactory_BadPassword() throws IOException
    {
        EncryptionOptions options = addKeystoreOptions(encryptionOptions)
                                    .withKeyStorePassword("HomeOfBadPasswords");
        defaultSslContextFactoryImpl.buildKeyManagerFactory(options);
    }

    @Test
    public void buildKeyManagerFactory_HappyPath() throws IOException
    {
        Assert.assertFalse(defaultSslContextFactoryImpl.checkedExpiry);
        EncryptionOptions options = addKeystoreOptions(encryptionOptions);
        defaultSslContextFactoryImpl.buildKeyManagerFactory(options);
        Assert.assertTrue(defaultSslContextFactoryImpl.checkedExpiry);
    }
}
