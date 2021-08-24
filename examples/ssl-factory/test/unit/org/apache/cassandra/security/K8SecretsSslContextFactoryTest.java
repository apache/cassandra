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
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.security.K8SecretsSslContextFactory.ConfigKeys.KEYSTORE_PASSWORD_ENV_VAR;
import static org.apache.cassandra.security.K8SecretsSslContextFactory.ConfigKeys.KEYSTORE_PATH;
import static org.apache.cassandra.security.K8SecretsSslContextFactory.ConfigKeys.TRUSTSTORE_PASSWORD_ENV_VAR;
import static org.apache.cassandra.security.K8SecretsSslContextFactory.ConfigKeys.TRUSTSTORE_PATH;

public class K8SecretsSslContextFactoryTest
{
    private static final Logger logger = LoggerFactory.getLogger(K8SecretsSslContextFactoryTest.class);

    private Map<String,Object> commonConfig = new HashMap<>();

    private static class K8SecretsSslContextFactoryForTestOnly extends K8SecretsSslContextFactory {

        public K8SecretsSslContextFactoryForTestOnly() {
        }

        public K8SecretsSslContextFactoryForTestOnly(Map<String,Object> config) {
            super(config);
        }

        /*
         * This is overriden to first give priority to the input map configuration since we should not be setting env
         * variables from the unit tests. However, if the input map configuration doesn't have the value for the
         * given key then fallback to loading from the real environment variables.
         */
        @Override
        String getValueFromEnv(String envVarName, String defaultValue) {
            String envVarValue = parameters.get(envVarName) != null ? parameters.get(envVarName).toString() : null;
            if (StringUtils.isEmpty(envVarValue)) {
                logger.info("Configuration doesn't have env variable {}. Will use parent's implementation", envVarName);
                return super.getValueFromEnv(envVarName, defaultValue);
            } else {
                logger.info("Configuration has env variable {} with value {}. Will use that.",
                            envVarName, envVarValue);
                return envVarValue;
            }
        }
    }

    @Before
    public void setup()
    {
        commonConfig.put(TRUSTSTORE_PATH, "test/conf/cassandra_ssl_test.truststore");
        commonConfig.put(TRUSTSTORE_PASSWORD_ENV_VAR, "MY_TRUSTSTORE_PASSWORD");
        /*
         * In order to test with real 'env' variables comment out this line and set appropriate env variable. This is
         *  done to avoid having a dependency on env in the unit test.
         */
        commonConfig.put("MY_TRUSTSTORE_PASSWORD", "cassandra");
        commonConfig.put("require_client_auth", Boolean.FALSE);
        commonConfig.put("cipher_suites", Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA"));
    }

    private void addKeystoreOptions(Map<String,Object> config)
    {
        config.put(KEYSTORE_PATH, "test/conf/cassandra_ssl_test" +
                                  ".keystore");
        config.put(KEYSTORE_PASSWORD_ENV_VAR, "MY_KEYSTORE_PASSWORD");
        /*
         * In order to test with real 'env' variables comment out this line and set appropriate env variable. This is
         *  done to avoid having a dependency on env in the unit test.
         */
        config.put("MY_KEYSTORE_PASSWORD", "cassandra");
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactoryWithInvalidTruststoreFile() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(TRUSTSTORE_PATH, "/this/is/probably/not/a/file/on/your/test/machine");

        K8SecretsSslContextFactory k8SecretsSslContextFactory = new K8SecretsSslContextFactoryForTestOnly(config);
        k8SecretsSslContextFactory.checkedExpiry = false;
        k8SecretsSslContextFactory.buildTrustManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactoryWithBadPassword() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.remove(TRUSTSTORE_PASSWORD_ENV_VAR);
        config.put(K8SecretsSslContextFactory.DEFAULT_TRUSTSTORE_PASSWORD_ENV_VAR_NAME, "HomeOfBadPasswords");

        K8SecretsSslContextFactory k8SecretsSslContextFactory = new K8SecretsSslContextFactoryForTestOnly(config);
        k8SecretsSslContextFactory.checkedExpiry = false;
        k8SecretsSslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        K8SecretsSslContextFactory k8SecretsSslContextFactory = new K8SecretsSslContextFactoryForTestOnly(config);
        k8SecretsSslContextFactory.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = k8SecretsSslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithInvalidKeystoreFile() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(KEYSTORE_PATH, "/this/is/probably/not/a/file/on/your/test/machine");

        K8SecretsSslContextFactory k8SecretsSslContextFactory = new K8SecretsSslContextFactoryForTestOnly(config);
        k8SecretsSslContextFactory.checkedExpiry = false;
        k8SecretsSslContextFactory.buildKeyManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithBadPassword() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(KEYSTORE_PATH, "test/conf/cassandra_ssl_test.keystore");
        config.put(K8SecretsSslContextFactory.DEFAULT_KEYSTORE_PASSWORD_ENV_VAR_NAME, "HomeOfBadPasswords");

        K8SecretsSslContextFactory k8SecretsSslContextFactory = new K8SecretsSslContextFactoryForTestOnly(config);
        k8SecretsSslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryHappyPath() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        K8SecretsSslContextFactory k8SecretsSslContextFactory1 = new K8SecretsSslContextFactoryForTestOnly(config);
        // Make sure the exiry check didn't happen so far for the private key
        Assert.assertFalse(k8SecretsSslContextFactory1.checkedExpiry);

        addKeystoreOptions(config);
        K8SecretsSslContextFactory k8SecretsSslContextFactory2 = new K8SecretsSslContextFactoryForTestOnly(config);
        // Trigger the private key loading. That will also check for expired private key
        k8SecretsSslContextFactory2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(k8SecretsSslContextFactory2.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check
        K8SecretsSslContextFactory k8SecretsSslContextFactory3 = new K8SecretsSslContextFactoryForTestOnly(config);
        Assert.assertFalse(k8SecretsSslContextFactory3.checkedExpiry);
        k8SecretsSslContextFactory3.buildKeyManagerFactory();
        Assert.assertTrue(k8SecretsSslContextFactory3.checkedExpiry);
    }

    @Test
    public void checkTruststoreUpdateReloading() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);

        K8SecretsSslContextFactory k8SecretsSslContextFactory = new K8SecretsSslContextFactoryForTestOnly(config);
        k8SecretsSslContextFactory.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = k8SecretsSslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
        Assert.assertFalse(k8SecretsSslContextFactory.shouldReload());

        config.put(K8SecretsSslContextFactory.DEFAULT_TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR,
                   String.valueOf(System.nanoTime()));
        Assert.assertTrue(k8SecretsSslContextFactory.shouldReload());

        config.remove(K8SecretsSslContextFactory.DEFAULT_TRUSTSTORE_UPDATED_TIMESTAMP_ENV_VAR);
        Assert.assertFalse(k8SecretsSslContextFactory.shouldReload());
    }

    @Test
    public void checkKeystoreUpdateReloading() throws IOException
    {
        Map<String,Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeystoreOptions(config);

        K8SecretsSslContextFactory k8SecretsSslContextFactory = new K8SecretsSslContextFactoryForTestOnly(config);
        k8SecretsSslContextFactory.checkedExpiry = false;
        KeyManagerFactory keyManagerFactory = k8SecretsSslContextFactory.buildKeyManagerFactory();
        Assert.assertNotNull(keyManagerFactory);
        Assert.assertFalse(k8SecretsSslContextFactory.shouldReload());

        config.put(K8SecretsSslContextFactory.DEFAULT_KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR,
                   String.valueOf(System.nanoTime()));
        Assert.assertTrue(k8SecretsSslContextFactory.shouldReload());

        config.remove(K8SecretsSslContextFactory.DEFAULT_KEYSTORE_UPDATED_TIMESTAMP_ENV_VAR);
        Assert.assertFalse(k8SecretsSslContextFactory.shouldReload());
    }
}
