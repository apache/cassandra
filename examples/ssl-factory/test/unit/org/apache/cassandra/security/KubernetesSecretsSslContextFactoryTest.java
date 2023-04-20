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
import java.io.OutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.EncryptionOptions;

import static org.apache.cassandra.security.KubernetesSecretsSslContextFactory.ConfigKeys.KEYSTORE_PASSWORD_ENV_VAR;
import static org.apache.cassandra.security.KubernetesSecretsSslContextFactory.ConfigKeys.KEYSTORE_UPDATED_TIMESTAMP_PATH;
import static org.apache.cassandra.security.KubernetesSecretsSslContextFactory.ConfigKeys.TRUSTSTORE_PASSWORD_ENV_VAR;
import static org.apache.cassandra.security.KubernetesSecretsSslContextFactory.ConfigKeys.TRUSTSTORE_UPDATED_TIMESTAMP_PATH;

public class KubernetesSecretsSslContextFactoryTest
{
    private static final Logger logger = LoggerFactory.getLogger(KubernetesSecretsSslContextFactoryTest.class);
    private static final String TRUSTSTORE_PATH = EncryptionOptions.ConfigKey.TRUSTSTORE.toString();
    private static final String KEYSTORE_PATH = EncryptionOptions.ConfigKey.KEYSTORE.toString();
    private final static String truststoreUpdatedTimestampFilepath = "build/test/conf/cassandra_truststore_last_updatedtime";
    private final static String keystoreUpdatedTimestampFilepath = "build/test/conf/cassandra_keystore_last_updatedtime";
    private final Map<String, Object> commonConfig = new HashMap<>();

    @BeforeClass
    public static void prepare()
    {
        deleteFileIfExists(truststoreUpdatedTimestampFilepath);
        deleteFileIfExists(keystoreUpdatedTimestampFilepath);
    }

    private static void deleteFileIfExists(String file)
    {
        boolean deleted = new File(file).delete();
        if (!deleted)
        {
            logger.warn("File {} could not be deleted.", file);
        }
    }

    @Before
    public void setup()
    {
        commonConfig.put(TRUSTSTORE_PATH, "build/test/conf/cassandra_ssl_test.truststore");
        commonConfig.put(TRUSTSTORE_PASSWORD_ENV_VAR, "MY_TRUSTSTORE_PASSWORD");
        commonConfig.put(TRUSTSTORE_UPDATED_TIMESTAMP_PATH, truststoreUpdatedTimestampFilepath);
        /*
         * In order to test with real 'env' variables comment out this line and set appropriate env variable. This is
         * done to avoid having a dependency on env in the unit test.
         */
        commonConfig.put("MY_TRUSTSTORE_PASSWORD", "cassandra");
        commonConfig.put("require_client_auth", Boolean.FALSE);
        commonConfig.put("cipher_suites", Arrays.asList("TLS_RSA_WITH_AES_128_CBC_SHA"));
    }

    private void addKeystoreOptions(Map<String, Object> config)
    {
        config.put(KEYSTORE_PATH, "build/test/conf/cassandra_ssl_test.keystore");
        config.put(KEYSTORE_PASSWORD_ENV_VAR, "MY_KEYSTORE_PASSWORD");
        config.put(KEYSTORE_UPDATED_TIMESTAMP_PATH, keystoreUpdatedTimestampFilepath);
        /*
         * In order to test with real 'env' variables comment out this line and set appropriate env variable. This is
         *  done to avoid having a dependency on env in the unit test.
         */
        config.put("MY_KEYSTORE_PASSWORD", "cassandra");
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactoryWithInvalidTruststoreFile() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(TRUSTSTORE_PATH, "/this/is/probably/not/a/file/on/your/test/machine");

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.trustStoreContext.checkedExpiry = false;
        kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildTrustManagerFactoryWithBadPassword() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.remove(TRUSTSTORE_PASSWORD_ENV_VAR);
        config.put(KubernetesSecretsSslContextFactory.DEFAULT_TRUSTSTORE_PASSWORD_ENV_VAR_NAME, "HomeOfBadPasswords");

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.trustStoreContext.checkedExpiry = false;
        kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryWithEmptyPassword() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(TRUSTSTORE_PATH, "build/test/conf/cassandra_ssl_test.truststore-without-password");
        config.remove(TRUSTSTORE_PASSWORD_ENV_VAR);
        config.put(KubernetesSecretsSslContextFactory.DEFAULT_TRUSTSTORE_PASSWORD_ENV_VAR_NAME, "");

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.trustStoreContext.checkedExpiry = false;
        kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void buildTrustManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.trustStoreContext.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithInvalidKeystoreFile() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(KEYSTORE_PATH, "/this/is/probably/not/a/file/on/your/test/machine");
        config.put(KEYSTORE_PASSWORD_ENV_VAR, "MY_KEYSTORE_PASSWORD");
        config.put("MY_KEYSTORE_PASSWORD","ThisWontMatter");

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.keystoreContext.checkedExpiry = false;
        kubernetesSecretsSslContextFactory.buildKeyManagerFactory();
    }

    @Test(expected = IOException.class)
    public void buildKeyManagerFactoryWithBadPassword() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        config.put(KEYSTORE_PATH, "build/test/conf/cassandra_ssl_test.keystore");
        config.put(KubernetesSecretsSslContextFactory.DEFAULT_KEYSTORE_PASSWORD_ENV_VAR_NAME, "HomeOfBadPasswords");

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.buildKeyManagerFactory();
    }

    @Test
    public void buildKeyManagerFactoryHappyPath() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory1 = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        // Make sure the exiry check didn't happen so far for the private key
        Assert.assertFalse(kubernetesSecretsSslContextFactory1.keystoreContext.checkedExpiry);

        addKeystoreOptions(config);
        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory2 = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        // Trigger the private key loading. That will also check for expired private key
        kubernetesSecretsSslContextFactory2.buildKeyManagerFactory();
        // Now we should have checked the private key's expiry
        Assert.assertTrue(kubernetesSecretsSslContextFactory2.keystoreContext.checkedExpiry);

        // Make sure that new factory object preforms the fresh private key expiry check
        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory3 = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        Assert.assertFalse(kubernetesSecretsSslContextFactory3.keystoreContext.checkedExpiry);
        kubernetesSecretsSslContextFactory3.buildKeyManagerFactory();
        Assert.assertTrue(kubernetesSecretsSslContextFactory3.keystoreContext.checkedExpiry);
    }

    @Test
    public void checkTruststoreUpdateReloading() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeystoreOptions(config);

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.trustStoreContext.checkedExpiry = false;
        TrustManagerFactory trustManagerFactory = kubernetesSecretsSslContextFactory.buildTrustManagerFactory();
        Assert.assertNotNull(trustManagerFactory);
        Assert.assertFalse(kubernetesSecretsSslContextFactory.shouldReload());

        updateTimestampFile(config, TRUSTSTORE_UPDATED_TIMESTAMP_PATH);
        Assert.assertTrue(kubernetesSecretsSslContextFactory.shouldReload());

        config.remove(TRUSTSTORE_UPDATED_TIMESTAMP_PATH);
        Assert.assertFalse(kubernetesSecretsSslContextFactory.shouldReload());
    }

    @Test
    public void checkKeystoreUpdateReloading() throws IOException
    {
        Map<String, Object> config = new HashMap<>();
        config.putAll(commonConfig);
        addKeystoreOptions(config);

        KubernetesSecretsSslContextFactory kubernetesSecretsSslContextFactory = new KubernetesSecretsSslContextFactoryForTestOnly(config);
        kubernetesSecretsSslContextFactory.keystoreContext.checkedExpiry = false;
        KeyManagerFactory keyManagerFactory = kubernetesSecretsSslContextFactory.buildKeyManagerFactory();
        Assert.assertNotNull(keyManagerFactory);
        Assert.assertFalse(kubernetesSecretsSslContextFactory.shouldReload());

        updateTimestampFile(config, KEYSTORE_UPDATED_TIMESTAMP_PATH);
        Assert.assertTrue(kubernetesSecretsSslContextFactory.shouldReload());

        config.remove(KEYSTORE_UPDATED_TIMESTAMP_PATH);
        Assert.assertFalse(kubernetesSecretsSslContextFactory.shouldReload());
    }

    private void updateTimestampFile(Map<String, Object> config, String filePathKey)
    {
        String filePath = config.containsKey(filePathKey) ? config.get(filePathKey).toString() : null;
        try (OutputStream os = Files.newOutputStream(Paths.get(filePath)))
        {
            String timestamp = String.valueOf(System.nanoTime());
            os.write(timestamp.getBytes());
            logger.info("Successfully wrote to file {}", filePath);
        }
        catch (IOException e)
        {
            logger.warn("Failed to write to filePath {} from the mounted volume", filePath, e);
        }
    }

    private static class KubernetesSecretsSslContextFactoryForTestOnly extends KubernetesSecretsSslContextFactory
    {

        public KubernetesSecretsSslContextFactoryForTestOnly()
        {
        }

        public KubernetesSecretsSslContextFactoryForTestOnly(Map<String, Object> config)
        {
            super(config);
        }

        /*
         * This is overriden to first give priority to the input map configuration since we should not be setting env
         * variables from the unit tests. However, if the input map configuration doesn't have the value for the
         * given key then fallback to loading from the real environment variables.
         */
        @Override
        String getValueFromEnv(String envVarName, String defaultValue)
        {
            String envVarValue = parameters.get(envVarName) != null ? parameters.get(envVarName).toString() : null;
            if (StringUtils.isEmpty(envVarValue))
            {
                logger.info("Configuration doesn't have env variable {}. Will use parent's implementation", envVarName);
                return super.getValueFromEnv(envVarName, defaultValue);
            }
            else
            {
                logger.info("Configuration has env variable {} with value {}. Will use that.",
                            envVarName, envVarValue);
                return envVarValue;
            }
        }
    }
}
