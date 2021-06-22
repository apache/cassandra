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

public class DefaultSslContextFactoryImplTest
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
    public void buildTrustManagerFactory_NoFile() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("truststore", "/this/is/probably/not/a/file/on/your/test/machine");

        DefaultSslContextFactoryImpl defaultSslContextFactoryImpl = new DefaultSslContextFactoryImpl(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        defaultSslContextFactoryImpl.buildTrustManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactory_BadPassword() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("truststore_password", "HomeOfBadPasswords");

        DefaultSslContextFactoryImpl defaultSslContextFactoryImpl = new DefaultSslContextFactoryImpl(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        defaultSslContextFactoryImpl.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactory_HappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        DefaultSslContextFactoryImpl defaultSslContextFactoryImpl = new DefaultSslContextFactoryImpl(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = defaultSslContextFactoryImpl.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactory_NoFile() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put("keystore", "/this/is/probably/not/a/file/on/your/test/machine");

        DefaultSslContextFactoryImpl defaultSslContextFactoryImpl = new DefaultSslContextFactoryImpl(config);
        defaultSslContextFactoryImpl.checkedExpiry = false;
        defaultSslContextFactoryImpl.buildKeyManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactory_BadPassword() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeystoreOptions(config);
        config.put("keystore_password", "HomeOfBadPasswords");

        DefaultSslContextFactoryImpl defaultSslContextFactoryImpl = new DefaultSslContextFactoryImpl(config);
        defaultSslContextFactoryImpl.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactory_HappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        DefaultSslContextFactoryImpl defaultSslContextFactoryImpl = new DefaultSslContextFactoryImpl(config);
        Assert.assertFalse(defaultSslContextFactoryImpl.checkedExpiry);

        addKeystoreOptions(config);
        DefaultSslContextFactoryImpl defaultSslContextFactoryImpl2 = new DefaultSslContextFactoryImpl(config);
        defaultSslContextFactoryImpl2.buildKeyManagerFactory();
        Assert.assertTrue(defaultSslContextFactoryImpl2.checkedExpiry);
    }
}
