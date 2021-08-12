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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.TrustManagerFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DefaultSslContextFactoryTest
{
    private Map<String,Object> commonConfig = new HashMap<>();

    @Before
    public void setup()
    {
        commonConfig.put("truststore", "test/conf/cassandra_ssl_test.truststore");
        commonConfig.put("truststore_password", "cassandra");
        commonConfig.put("require_client_auth", Boolean.FALSE);
        commonConfig.put("cipher_suites", Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA"));
    }

    private void addKeystoreOptions(Map<String,Object> config)
    {
        config.put("keystore", "test/conf/cassandra_ssl_test.keystore");
        config.put("keystore_password", "cassandra");
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactoryWithInvalidTruststoreFile() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("truststore", "/this/is/probably/not/a/file/on/your/test/machine");

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        defaultSslContextFactoryImpl.buildTrustManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactoryWithBadPassword() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("truststore_password", "HomeOfBadPasswords");

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        defaultSslContextFactoryImpl.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = defaultSslContextFactoryImpl.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithInvalidKeystoreFile() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("keystore", "/this/is/probably/not/a/file/on/your/test/machine");

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        defaultSslContextFactoryImpl.buildKeyManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithBadPassword() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeystoreOptions(config);
        config.put("keystore_password", "HomeOfBadPasswords");

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        defaultSslContextFactoryImpl.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryHappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        DefaultSslContextFactory defaultSslContextFactoryImpl = new DefaultSslContextFactory(config);
        // Make sure the exiry check didn't happen so far for the private key
        Assert.assertFalse(defaultSslContextFactoryImpl.checkedExpiry);

        addKeystoreOptions(config);
        DefaultSslContextFactory defaultSslContextFactoryImpl2 = new DefaultSslContextFactory(config);
        // Trigger the private key loading. That will also check for expired private key
        defaultSslContextFactoryImpl2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(defaultSslContextFactoryImpl2.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check
        DefaultSslContextFactory defaultSslContextFactoryImpl3 = new DefaultSslContextFactory(config);
        Assert.assertFalse(defaultSslContextFactoryImpl3.checkedExpiry);
        defaultSslContextFactoryImpl3.buildKeyManagerFactory();
        Assert.assertTrue(defaultSslContextFactoryImpl3.checkedExpiry);
    }
}
